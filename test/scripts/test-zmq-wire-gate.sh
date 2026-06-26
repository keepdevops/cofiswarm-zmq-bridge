#!/usr/bin/env bash
# End-to-end ZMQ wire gate: proves the real ZMQ carrier works, not just the HTTP control
# plane. Starts the bridge binary with COFISWARM_BUS=zmq (ingress SUB + egress PUB + ROUTER
# request/reply on real TCP sockets), then verifies three wire paths:
#   1. HTTP /v1/publish -> egress SUB receives the re-emitted frame
#   2. native PUB -> ingress -> bridge -> egress SUB (component->observer path, no HTTP)
#   3. /v1/request -> ROUTER -> responder -> reply (unary request/reply round-trip)
#   4. /v1/request/stream -> ROUTER -> responder -> CHUNK... END (streaming request/reply)
# Helpers are built to bin/ and run as binaries (not `go run`, which orphans a child the
# kill can't reach); every child PID is tracked and torn down on EXIT.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${ROOT}/bin/cofiswarm-zmq-bridge"
PROBE="${ROOT}/bin/egress-probe"
PUBBIN="${ROOT}/bin/ingress-pub"
RESPBIN="${ROOT}/bin/req-responder"
TOPICS="${ROOT}/spec/topics.yaml"

# High ports to avoid colliding with a running stack (5555/5556/5557/5558).
CTRL=15555
INGRESS=15556
EGRESS=15557
REQ=15558
BACKEND=15560
SUBJECT=swarm.test.echo
STREAM_SUBJECT=swarm.test.stream

# Track every spawned child so the EXIT trap reaps them even on early failure.
CHILDREN=()
cleanup() { for p in "${CHILDREN[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

echo "== building bridge + helpers =="
go build -o "$BIN" "${ROOT}/cmd/cofiswarm-zmq-bridge"
go build -o "$PROBE" "${ROOT}/test/cmd/egress-probe"
go build -o "$PUBBIN" "${ROOT}/test/cmd/ingress-pub"
go build -o "$RESPBIN" "${ROOT}/test/cmd/req-responder"

echo "== starting bridge (COFISWARM_BUS=zmq, req leg on) =="
COFISWARM_BUS=zmq \
COFISWARM_ZMQ_ADDR="tcp://*:${INGRESS}" \
COFISWARM_ZMQ_EGRESS_ADDR="tcp://*:${EGRESS}" \
COFISWARM_ZMQ_REQ_ADDR="tcp://*:${REQ}" \
"$BIN" -listen ":${CTRL}" -topics "$TOPICS" &
BPID=$!
CHILDREN+=("$BPID")
sleep 1.5

kill -0 "$BPID" 2>/dev/null || { echo "fail: bridge exited at startup" >&2; exit 1; }
curl -sf --max-time 5 "http://127.0.0.1:${CTRL}/v1/topics" | grep -q swarm.slot.erase \
  || { echo "fail: control plane /v1/topics missing swarm.slot.erase" >&2; exit 1; }
echo "ok: bridge up, control plane reachable"

# --- Case 1: HTTP /v1/publish -> egress wire ---------------------------------------------
echo "== case 1: /v1/publish -> egress SUB =="
"$PROBE" "tcp://127.0.0.1:${EGRESS}" 8 >/tmp/zmq-wire-gate.c1 2>&1 &
SUB=$!; CHILDREN+=("$SUB")
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
SUB=$!; CHILDREN+=("$SUB")
sleep 0.8
"$PUBBIN" "tcp://127.0.0.1:${INGRESS}" >/dev/null 2>&1 &
PUB=$!; CHILDREN+=("$PUB")
disown "$PUB" 2>/dev/null || true  # suppress job-control "Terminated" noise; reaped by PID in trap
wait "$SUB" || { echo "fail: no egress frame via native ingress PUB" >&2; cat /tmp/zmq-wire-gate.c2 >&2; exit 1; }
kill "$PUB" 2>/dev/null || true
grep -q '^GOT swarm.slot.erase' /tmp/zmq-wire-gate.c2 \
  || { echo "fail: unexpected probe output" >&2; cat /tmp/zmq-wire-gate.c2 >&2; exit 1; }
echo "ok: $(cat /tmp/zmq-wire-gate.c2)"

# --- Case 3: request/reply over the ROUTER leg -------------------------------------------
# A responder (DEALER) registers SUBJECT on the broker and relays to an in-process HTTP echo
# backend. /v1/request routes a payload to it over the wire and returns the echoed reply.
echo "== case 3: /v1/request -> ROUTER -> responder -> reply =="
"$RESPBIN" "tcp://127.0.0.1:${REQ}" "$SUBJECT" "127.0.0.1:${BACKEND}" "$STREAM_SUBJECT" >/tmp/zmq-wire-gate.c3 2>&1 &
RESP=$!; CHILDREN+=("$RESP")
disown "$RESP" 2>/dev/null || true  # suppress job-control "Terminated" noise; reaped by PID in trap
# Poll: wait for the responder to dial + READY-register, then for a reply to come back.
reply=""
for _ in $(seq 1 30); do
  kill -0 "$RESP" 2>/dev/null || { echo "fail: req-responder exited early" >&2; cat /tmp/zmq-wire-gate.c3 >&2; exit 1; }
  reply="$(curl -sf --max-time 5 -X POST "http://127.0.0.1:${CTRL}/v1/request" \
    -H 'Content-Type: application/json' \
    -d '{"subject":"'"$SUBJECT"'","payload":{"q":42},"timeout_ms":3000}' 2>/dev/null || true)"
  echo "$reply" | grep -q '"pong":true' && break
  sleep 0.3
done
# Leave the responder running — case 4 reuses it; the EXIT trap reaps it.
echo "$reply" | grep -q '"pong":true' \
  || { echo "fail: no request/reply over ROUTER leg (last reply: ${reply:-<none>})" >&2; cat /tmp/zmq-wire-gate.c3 >&2; exit 1; }
echo "$reply" | grep -q '"echo"' \
  || { echo "fail: reply missing echoed payload: $reply" >&2; exit 1; }
echo "ok: unary request/reply round-trip — $reply"

# --- Case 4: streaming request/reply over the ROUTER leg ---------------------------------
# Same responder serves STREAM_SUBJECT against an SSE backend. /v1/request/stream relays the
# backend's multi-frame stream through the ROUTER as CHUNK...END; assert every frame arrives
# in order. The route is already live from case 3 (responder started + heartbeating).
echo "== case 4: /v1/request/stream -> ROUTER -> responder -> CHUNK...END =="
stream=""
for _ in $(seq 1 20); do
  stream="$(curl -sf --max-time 8 -N -X POST "http://127.0.0.1:${CTRL}/v1/request/stream" \
    -H 'Content-Type: application/json' \
    -d '{"subject":"'"$STREAM_SUBJECT"'","payload":{"stream":true},"timeout_ms":5000}' 2>/dev/null || true)"
  echo "$stream" | grep -q '\[DONE\]' && break
  sleep 0.3
done
kill "$RESP" 2>/dev/null || true
for marker in '"i":0' '"i":1' '"i":2' '[DONE]'; do
  echo "$stream" | grep -qF "$marker" \
    || { echo "fail: streamed body missing $marker" >&2; echo "got: $stream" >&2; exit 1; }
done
# Order check: the [DONE] terminator must come after the last data frame.
last_i="$(printf '%s\n' "$stream" | grep -n '"i":2' | head -1 | cut -d: -f1)"
done_ln="$(printf '%s\n' "$stream" | grep -n '\[DONE\]' | head -1 | cut -d: -f1)"
[ -n "$last_i" ] && [ -n "$done_ln" ] && [ "$done_ln" -gt "$last_i" ] \
  || { echo "fail: stream frames out of order (i:2@${last_i:-?}, DONE@${done_ln:-?})" >&2; echo "got: $stream" >&2; exit 1; }
echo "ok: streaming request/reply — $(printf '%s' "$stream" | grep -c '^data:') data frames, ordered, terminated by [DONE]"

echo "ok: zmq end-to-end wire — publish (HTTP + native) reach egress SUB; unary + streaming request/reply over ROUTER"
