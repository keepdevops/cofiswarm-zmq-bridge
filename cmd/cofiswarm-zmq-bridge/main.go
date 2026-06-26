package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/bus"
	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/httpapi"
	"github.com/keepdevops/cofiswarm-observer-sdk/pkg/buspresence"
)

func main() {
	addr := flag.String("listen", ":5555", "control listen address")
	topicsFile := flag.String("topics", "", "topics yaml")
	flag.Parse()
	if *topicsFile == "" {
		*topicsFile = filepath.Join("spec", "topics.yaml")
		if v := os.Getenv("COFISWARM_ZMQ_TOPICS"); v != "" {
			*topicsFile = v
		}
	}
	cfg, err := bus.LoadConfig(*topicsFile)
	if err != nil {
		log.Fatal(err)
	}
	wildcard := streamWildcard(cfg)
	b, kind, err := newBackend(cfg, wildcard)
	if err != nil {
		log.Fatalf("bus backend: %v", err)
	}
	defer b.Close()

	go func() {
		log.Printf("zmq-bridge listening on %s (backend=%s, %d topics)", *addr, kind, len(cfg.Topics))
		log.Fatal(http.ListenAndServe(*addr, httpapi.New(b, wildcard).Handler()))
	}()

	// Self-presence: the bridge is the carrier, but announce it to its own /v1/publish so it
	// also appears in the observer roster (re-announces on hello). COFISWARM_BRIDGE_SELF_URL
	// overrides; default is the local listen address.
	selfURL := os.Getenv("COFISWARM_BRIDGE_SELF_URL")
	if selfURL == "" {
		selfURL = "http://127.0.0.1" + *addr
	}
	_ = buspresence.StartPresence(selfURL, "zmq-bridge", map[string]any{"name": "zmq-bridge"})

	select {} // serve forever (HTTP runs in the goroutine above)
}

// streamWildcard is the subject /v1/stream and the NATS recent-events tail subscribe to.
// COFISWARM_BUS_WILDCARD overrides; otherwise "<prefix>.>" (or ">" if no prefix).
func streamWildcard(cfg bus.Config) string {
	if w := os.Getenv("COFISWARM_BUS_WILDCARD"); w != "" {
		return w
	}
	if cfg.Prefix != "" {
		return cfg.Prefix + ".>"
	}
	return ">"
}

// newBackend selects the bus implementation. Default "mem" preserves prior behavior.
// COFISWARM_BUS=nats switches to a real NATS broker (the diagram's middle man), with
// COFISWARM_NATS_URL (default nats://127.0.0.1:4222). COFISWARM_BUS=zmq switches to the
// real ZMQ carrier: an ingress SUB bound to COFISWARM_ZMQ_ADDR (default tcp://*:5556) that
// collects every swarm.* frame from component PUBs, plus an egress PUB bound to
// COFISWARM_ZMQ_EGRESS_ADDR (default tcp://*:5557, "off" to disable) that re-emits each
// frame for the observer. wildcard is the recent-events tail.
func newBackend(cfg bus.Config, wildcard string) (bus.Backend, string, error) {
	switch os.Getenv("COFISWARM_BUS") {
	case "nats":
		url := os.Getenv("COFISWARM_NATS_URL")
		if url == "" {
			url = "nats://127.0.0.1:4222"
		}
		nb, err := bus.NewNats(url, cfg.Topics, wildcard)
		return nb, "nats", err
	case "zmq":
		addr := os.Getenv("COFISWARM_ZMQ_ADDR")
		if addr == "" {
			addr = "tcp://*:5556"
		}
		egress := os.Getenv("COFISWARM_ZMQ_EGRESS_ADDR")
		if egress == "" {
			egress = "tcp://*:5557"
		}
		if egress == "off" {
			egress = ""
		}
		// Request/reply leg (ROUTER): "off" disables it (telemetry-only carrier).
		reqAddr := os.Getenv("COFISWARM_ZMQ_REQ_ADDR")
		if reqAddr == "" {
			reqAddr = "tcp://*:5558"
		}
		if reqAddr == "off" {
			reqAddr = ""
		}
		filter := ""
		if cfg.Prefix != "" {
			filter = cfg.Prefix + "."
		}
		zb, err := bus.NewZmq(addr, egress, reqAddr, filter, cfg.Topics)
		return zb, "zmq", err
	default:
		return bus.NewMem(cfg.Topics), "mem", nil
	}
}
