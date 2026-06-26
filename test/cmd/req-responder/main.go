// req-responder is a test helper for the end-to-end zmq request/reply gate. It runs a tiny
// in-process HTTP echo backend and registers a ZMQ responder (DEALER -> bridge ROUTER) that
// relays requests for a subject to that backend. The echo backend wraps the request payload
// as {"ok":true,"pong":true,"echo":<payload>} so the gate can assert a real round-trip over
// the request/reply wire. Runs until SIGINT/SIGTERM, then tears the responder down so the
// broker drops the route.
//
// Usage: req-responder <broker-addr> <subject> <http-listen-addr>
//
//	req-responder tcp://127.0.0.1:15558 swarm.test.echo 127.0.0.1:15560
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/responder"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatal("usage: req-responder <broker-addr> <subject> <http-listen-addr>")
	}
	broker, subject, listenAddr := os.Args[1], os.Args[2], os.Args[3]

	// In-process echo backend: echoes the request payload back inside a JSON object so the
	// reply is identifiable end-to-end. A bad body still yields a JSON object reply.
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload any
		if err := json.Unmarshal(body, &payload); err != nil {
			payload = string(body)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "pong": true, "echo": payload})
	})
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}
	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("echo backend: %v", err)
		}
	}()

	stop, err := responder.ServeZmq(broker, subject, "http://"+listenAddr+"/echo", 10*time.Second)
	if err != nil {
		log.Fatalf("serve zmq %s on %s: %v", subject, broker, err)
	}
	log.Printf("req-responder serving %s -> http://%s/echo (broker %s)", subject, listenAddr, broker)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
