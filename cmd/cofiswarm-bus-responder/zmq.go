package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/responder"
)

// serveZmq dials the bridge ROUTER and serves the backend over the ZMQ request/reply plane,
// returning a stop func. Presence is announced to the bridge control plane (/v1/publish) so
// the responder appears in the observer roster; liveness for routing is the responder's own
// heartbeat, so an ungraceful exit drops the route within the broker TTL.
func serveZmq(name, subject, backend string) func() {
	broker := os.Getenv("COFISWARM_ZMQ_REQ_ADDR")
	if broker == "" {
		broker = "tcp://127.0.0.1:5558"
	}
	zmqPresence(name, "online")
	stop, err := responder.ServeZmq(broker, subject, backend, requestTimeout)
	if err != nil {
		log.Fatalf("serve zmq %s on %s: %v", subject, broker, err)
	}
	log.Printf("bus-responder %q serving %s -> %s (zmq via %s)", name, subject, backend, broker)
	return func() {
		zmqPresence(name, "offline")
		stop()
	}
}

// zmqPresence publishes a presence frame to the bridge control plane. COFISWARM_BRIDGE_URL
// overrides the default control listen address.
func zmqPresence(name, status string) {
	base := os.Getenv("COFISWARM_BRIDGE_URL")
	if base == "" {
		base = "http://127.0.0.1:5555"
	}
	body, _ := json.Marshal(map[string]any{
		"topic": "swarm.observer.presence",
		"payload": map[string]any{
			"component_id": "responder-" + name, "status": status,
			"info": map[string]any{"name": name},
		},
	})
	resp, err := http.Post(strings.TrimRight(base, "/")+"/v1/publish", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("zmq presence %s (%s): %v", name, status, err)
		return
	}
	_ = resp.Body.Close()
}
