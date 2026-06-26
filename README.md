# cofiswarm-zmq-bridge

In-process bus + HTTP control plane for `cofiswarm-common/zmq/topics.yaml`.

Control: `:5555` · ZMQ ingress wire: `:5556` · ZMQ egress wire: `:5557` · ZMQ request/reply: `:5558`

Backends (`COFISWARM_BUS`): `mem` (default), `nats`, `zmq`. The `zmq` backend is a
forwarder:

```
components (PUB) ─▶ ingress SUB :5556 ─▶ [ring + /v1/stream SSE] ─▶ egress PUB :5557 ─▶ observer (SUB)
```

The ingress SUB binds `COFISWARM_ZMQ_ADDR` (default `tcp://*:5556`) and collects every
`swarm.*` frame published by component PUB sockets. Each frame feeds the recent-events
ring and the `/v1/subscribe` and `/v1/stream` SSE endpoints, and is re-emitted on the
egress PUB bound to `COFISWARM_ZMQ_EGRESS_ADDR` (default `tcp://*:5557`; set to `off` for
ingress-only) so the observer dashboard receives it over a real ZMQ socket.

## Request/reply over ZMQ

Telemetry is one-way (PUB/SUB above); bidirectional calls flow over a separate ROUTER leg
bound to `COFISWARM_ZMQ_REQ_ADDR` (default `tcp://*:5558`; set to `off` to disable). This
makes the `zmq` backend a `Requester`, so the `POST /v1/request` gateway works on ZMQ as it
does on `nats` (mem still returns 501).

```
client ─▶ POST /v1/request {subject,payload} ─▶ ROUTER :5558 ─▶ responder (DEALER) ─▶ backend HTTP
                                                       ◀── reply ──────────────────────────┘
```

Responders connect with a DEALER, register the subject they serve (`READY`), heartbeat
(`PING`), and relay each request to a backend URL. Because ROUTER/DEALER gives no
disconnect signal, liveness is heartbeat-based: a crashed responder falls out of the route
table within `responderTTL` (15s) and callers get `503 no responders` (a slow one gets
`504`) — matching NATS semantics. Run one with `cofiswarm-bus-responder`:

```
COFISWARM_BUS=zmq COFISWARM_ZMQ_REQ_ADDR=tcp://127.0.0.1:5558 \
  cofiswarm-bus-responder -name flat \
  -subject swarm.observer.mode.flat -backend http://127.0.0.1:8081/v1/execute
```

Client side: `cofiswarm-dispatch` routes both mode execution and agent inference through
`/v1/request` (subjects `swarm.observer.mode.<mode>` and `swarm.observer.model.<port>`) when
`COFISWARM_ROUTE_BUS` and `COFISWARM_BRIDGE_URL` are set.

### Streaming (`/v1/request/stream`)

`POST /v1/request/stream {subject,payload,timeout_ms}` opens a streamed reply: the responder
relays the backend's response **stream** (set `"stream":true` in the payload for an SSE backend)
as `CHUNK` frames, which the gateway writes through verbatim — the response body is a transparent
byte passthrough of the upstream stream, ending when the body closes. Requires the zmq backend
(`StreamRequester`); 501 otherwise, 503 for no responders. Dispatch's `Stream` uses this path
under the same `COFISWARM_ROUTE_BUS` knob.

Resilience: the responder's DEALER auto-reconnects and re-registers if the bridge restarts
(capped backoff), so the request/reply plane self-heals.
