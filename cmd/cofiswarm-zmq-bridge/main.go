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
	topics, err := bus.LoadTopics(*topicsFile)
	if err != nil {
		log.Fatal(err)
	}
	b := bus.New(topics)
	log.Printf("zmq-bridge listening on %s (%d topics)", *addr, len(topics))
	log.Fatal(http.ListenAndServe(*addr, httpapi.New(b).Handler()))
}
