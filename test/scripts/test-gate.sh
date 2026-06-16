#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${ROOT}/bin/cofiswarm-zmq-bridge"
PORT=15555
"$BIN" -listen ":$PORT" -topics "${ROOT}/spec/topics.yaml" &
PID=$!
trap 'kill $PID 2>/dev/null' EXIT
sleep 1
curl -s "http://127.0.0.1:$PORT/v1/topics" | grep -q swarm.kvpool.evict
curl -s "http://127.0.0.1:$PORT/v1/topics" | grep -q swarm.dispatch.session
echo "ok: zmq topics from common contract"
