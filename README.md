# cofiswarm-zmq-bridge

In-process bus + HTTP control plane for `cofiswarm-common/zmq/topics.yaml`.

Control: `:5555` · ZMQ ingress wire: `:5556` · ZMQ egress wire: `:5557`

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
