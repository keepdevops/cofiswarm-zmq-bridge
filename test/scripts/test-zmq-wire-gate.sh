#!/usr/bin/env bash
# End-to-end ZMQ wire gate: proves the real ZMQ carrier works, not just the HTTP control
# plane. Starts the bridge binary with COFISWARM_BUS=zmq (ingress SUB + egress PUB on real
# TCP sockets), then verifies a native ZMQ SUB on the egress wire receives a re-emitted
# frame — once via the HTTP /v1/publish injection path, once via a pure native PUB on the
# ingress wire (the production component -> bridge -> observer path, no HTTP involved).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${ROOT}/bin/cofiswarm-zmq-bridge"
PROBE="${ROOT}/bin/egress-probe"
TOPICS="${ROOT}/spec/topics.yaml"

# High ports to avoid colliding with a running stack (5555/5556/5557).
CTRL=15555
INGRESS=15556
EGRESS=15557

echo "== building bridge + egress-probe =="
go build -o "$BIN" "${ROOT}/cmd/cofiswarm-zmq-bridge"
go build -o "$PROBE" "${ROOT}/test/cmd/egress-probe"

echo "== starting bridge (COFISWARM_BUS=zmq) =="
COFISWARM_BUS=zmq \
COFISWARM_ZMQ_ADDR="tcp://*:${INGRESS}" \
COFISWARM_ZMQ_EGRESS_ADDR="tcp://*:${EGRESS}" \
COFISWARM_ZMQ_REQ_ADDR=off \
"$BIN" -listen ":${CTRL}" -topics "$TOPICS" &
BPID=$!
trap 'kill $BPID 2>/dev/null || true' EXIT
sleep 1.5

kill -0 "$BPID" 2>/dev/null || { echo "fail: bridge exited at startup" >&2; exit 1; }
curl -sf --max-time 5 "http://127.0.0.1:${CTRL}/v1/topics" | grep -q swarm.slot.erase \
  || { echo "fail: control plane /v1/topics missing swarm.slot.erase" >&2; exit 1; }
echo "ok: bridge up, control plane reachable"

# --- Case 1: HTTP /v1/publish -> egress wire ---------------------------------------------
echo "== case 1: /v1/publish -> egress SUB =="
"$PROBE" "tcp://127.0.0.1:${EGRESS}" 8 >/tmp/zmq-wire-gate.c1 2>&1 &
SUB=$!
sleep 0.8
# Resend: PUB->SUB is a slow joiner; keep injecting until the probe exits or we give up.
for _ in $(seq 1 20); do
  kill -0 "$SUB" 2>/dev/null || break
  curl -sf --max-time 5 -X POST "http://127.0.0.1:${CTRL}/v1/publish" \
    -H 'Content-Type: application/json' \
    -d '{"topic":"swarm.slot.erase","payload":{"gate":"http"}}' >/dev/null || true
  sleep 0.3
done
wait "$SUB" || { echo "fail: no egress frame via /v1/publish" >&2; cat /tmp/zmq-wire-gate.c1 >&2; exit 1; }
grep -q '^GOT swarm.slot.erase' /tmp/zmq-wire-gate.c1 \
  || { echo "fail: unexpected probe output" >&2; cat /tmp/zmq-wire-gate.c1 >&2; exit 1; }
echo "ok: $(cat /tmp/zmq-wire-gate.c1)"

# --- Case 2: native PUB ingress -> bridge -> egress wire (pure-wire production path) ------
echo "== case 2: native PUB -> ingress -> egress SUB =="
"$PROBE" "tcp://127.0.0.1:${EGRESS}" 8 >/tmp/zmq-wire-gate.c2 2>&1 &
SUB=$!
sleep 0.8
# Drive ingress with a short-lived native PUB built from the helper's zmq4 dep via go run.
# Disown so killing it doesn't print job-control "Terminated" noise to the gate output.
go run "${ROOT}/test/cmd/ingress-pub" "tcp://127.0.0.1:${INGRESS}" >/dev/null 2>&1 &
PUB=$!
disown "$PUB" 2>/dev/null || true
wait "$SUB" || { echo "fail: no egress frame via native ingress PUB" >&2; cat /tmp/zmq-wire-gate.c2 >&2; kill $PUB 2>/dev/null || true; exit 1; }
kill "$PUB" 2>/dev/null || true
grep -q '^GOT swarm.slot.erase' /tmp/zmq-wire-gate.c2 \
  || { echo "fail: unexpected probe output" >&2; cat /tmp/zmq-wire-gate.c2 >&2; exit 1; }
echo "ok: $(cat /tmp/zmq-wire-gate.c2)"

echo "ok: zmq end-to-end wire — HTTP publish + native ingress both reach egress SUB"
