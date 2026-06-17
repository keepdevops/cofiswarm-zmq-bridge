// cofiswarm-bus-responder: a reference responder that makes an HTTP inference backend
// answerable over the bus. Subscribes to a request subject and relays to a backend URL;
// announces presence online on start and offline on shutdown. If this process dies, a
// requester sees NATS no-responders — the diagram's crash detection, no heartbeat.
//
//	cofiswarm-bus-responder -name architect \
//	  -subject swarm.observer.model.architect -backend http://127.0.0.1:8086/v1/chat/completions
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/responder"
)

func main() {
	name := flag.String("name", "", "component name (required)")
	subject := flag.String("subject", "", "NATS request subject to serve (required)")
	backend := flag.String("backend", "", "HTTP backend URL to relay requests to (required)")
	flag.Parse()
	if *name == "" || *subject == "" || *backend == "" {
		log.Fatal("-name, -subject and -backend are required")
	}
	url := os.Getenv("COFISWARM_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}

	nc, err := nats.Connect(url, nats.Name("responder-"+*name), nats.MaxReconnects(-1))
	if err != nil {
		log.Fatalf("connect %s: %v", url, err)
	}
	defer nc.Drain()

	presence(nc, *name, "online")
	sub, err := responder.Serve(nc, *subject, *backend, 600*time.Second)
	if err != nil {
		log.Fatalf("subscribe %s: %v", *subject, err)
	}
	defer sub.Unsubscribe()
	log.Printf("bus-responder %q serving %s -> %s", *name, *subject, *backend)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	presence(nc, *name, "offline") // graceful; ungraceful exit -> no-responders
	log.Printf("bus-responder %q stopping", *name)
}

func presence(nc *nats.Conn, name, status string) {
	// The presence subject carries the payload object directly (bridge records it,
	// observer reads component_id/status/info from it).
	data, _ := json.Marshal(map[string]any{
		"component_id": "responder-" + name, "status": status,
		"info": map[string]any{"name": name},
	})
	_ = nc.Publish("swarm.observer.presence", data)
}
