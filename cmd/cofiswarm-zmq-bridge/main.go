package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/bus"
	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/httpapi"
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

	log.Printf("zmq-bridge listening on %s (backend=%s, %d topics)", *addr, kind, len(cfg.Topics))
	log.Fatal(http.ListenAndServe(*addr, httpapi.New(b, wildcard).Handler()))
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

// newBackend selects the bus implementation. Default "mem" preserves prior behavior;
// COFISWARM_BUS=nats switches to a real NATS broker (the diagram's middle man), with
// COFISWARM_NATS_URL (default nats://127.0.0.1:4222). wildcard is the recent-events tail.
func newBackend(cfg bus.Config, wildcard string) (bus.Backend, string, error) {
	if os.Getenv("COFISWARM_BUS") != "nats" {
		return bus.NewMem(cfg.Topics), "mem", nil
	}
	url := os.Getenv("COFISWARM_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	nb, err := bus.NewNats(url, cfg.Topics, wildcard)
	return nb, "nats", err
}
