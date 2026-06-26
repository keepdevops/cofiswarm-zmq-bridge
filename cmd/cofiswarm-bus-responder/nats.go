package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/nats-io/nats.go"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/responder"
)

// serveNats subscribes the backend to a NATS request subject and returns a stop func.
// Presence is announced online now and offline on stop; an ungraceful exit yields
// NATS no-responders.
func serveNats(name, subject, backend string) func() {
	url := os.Getenv("COFISWARM_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	nc, err := nats.Connect(url, nats.Name("responder-"+name), nats.MaxReconnects(-1))
	if err != nil {
		log.Fatalf("connect %s: %v", url, err)
	}
	natsPresence(nc, name, "online")
	sub, err := responder.Serve(nc, subject, backend, requestTimeout)
	if err != nil {
		log.Fatalf("subscribe %s: %v", subject, err)
	}
	log.Printf("bus-responder %q serving %s -> %s (nats)", name, subject, backend)
	return func() {
		natsPresence(nc, name, "offline") // graceful; ungraceful exit -> no-responders
		_ = sub.Unsubscribe()
		_ = nc.Drain()
	}
}

func natsPresence(nc *nats.Conn, name, status string) {
	// The presence subject carries the payload object directly (bridge records it,
	// observer reads component_id/status/info from it).
	data, _ := json.Marshal(map[string]any{
		"component_id": "responder-" + name, "status": status,
		"info": map[string]any{"name": name},
	})
	_ = nc.Publish("swarm.observer.presence", data)
}
