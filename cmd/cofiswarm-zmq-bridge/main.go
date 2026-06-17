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
	b, kind, err := newBackend(cfg)
	if err != nil {
		log.Fatalf("bus backend: %v", err)
	}
	defer b.Close()

	log.Printf("zmq-bridge listening on %s (backend=%s, %d topics)", *addr, kind, len(cfg.Topics))
	log.Fatal(http.ListenAndServe(*addr, httpapi.New(b).Handler()))
}

// newBackend selects the bus implementation. Default "mem" preserves prior behavior;
// COFISWARM_BUS=nats switches to a real NATS broker (the diagram's middle man), with
// COFISWARM_NATS_URL (default nats://127.0.0.1:4222) and COFISWARM_BUS_WILDCARD
// (default "<prefix>.>") controlling the connection and the recent-events tail.
func newBackend(cfg bus.Config) (bus.Backend, string, error) {
	if os.Getenv("COFISWARM_BUS") != "nats" {
		return bus.NewMem(cfg.Topics), "mem", nil
	}
	url := os.Getenv("COFISWARM_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	wildcard := os.Getenv("COFISWARM_BUS_WILDCARD")
	if wildcard == "" {
		if cfg.Prefix != "" {
			wildcard = cfg.Prefix + ".>"
		} else {
			wildcard = ">"
		}
	}
	nb, err := bus.NewNats(url, cfg.Topics, wildcard)
	return nb, "nats", err
}
