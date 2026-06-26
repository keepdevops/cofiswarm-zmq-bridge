// cofiswarm-bus-responder: a reference responder that makes an HTTP inference backend
// answerable over the bus. Subscribes to a request subject and relays to a backend URL;
// announces presence online on start and offline on shutdown. If this process dies, a
// requester sees no-responders — the diagram's crash detection, no heartbeat needed on NATS;
// over ZMQ the responder heartbeats and the broker drops the route on miss (same effect).
//
//	cofiswarm-bus-responder -name architect \
//	  -subject swarm.observer.model.architect -backend http://127.0.0.1:8086/v1/chat/completions
//
// Transport follows COFISWARM_BUS: "zmq" dials the bridge ROUTER (COFISWARM_ZMQ_REQ_ADDR,
// default tcp://127.0.0.1:5558); anything else uses NATS (COFISWARM_NATS_URL).
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	name := flag.String("name", "", "component name (required)")
	subject := flag.String("subject", "", "request subject to serve (required)")
	backend := flag.String("backend", "", "HTTP backend URL to relay requests to (required)")
	flag.Parse()
	if *name == "" || *subject == "" || *backend == "" {
		log.Fatal("-name, -subject and -backend are required")
	}

	var stop func()
	if os.Getenv("COFISWARM_BUS") == "zmq" {
		stop = serveZmq(*name, *subject, *backend)
	} else {
		stop = serveNats(*name, *subject, *backend)
	}
	defer stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("bus-responder %q stopping", *name)
}

// requestTimeout bounds how long the responder waits on the backend before giving up.
const requestTimeout = 600 * time.Second
