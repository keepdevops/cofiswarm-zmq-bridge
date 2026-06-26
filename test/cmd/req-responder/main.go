// req-responder is a test helper for the end-to-end zmq request/reply gates. It runs a tiny
// in-process HTTP backend and registers ZMQ responders (DEALER -> bridge ROUTER) that relay
// to it:
//   - /echo  : unary — wraps the request payload as {"ok":true,"pong":true,"echo":<payload>}.
//   - /stream: streaming — emits an SSE sequence (data: {"i":N} ... data: [DONE]) so the
//     streaming gate can assert multiple CHUNK frames arrive in order before END.
//
// The unary subject is always served. If a stream subject is given, it is also served against
// /stream so one process backs both gate cases. Runs until SIGINT/SIGTERM, then tears the
// responders down so the broker drops their routes.
//
// Usage: req-responder <broker-addr> <unary-subject> <http-listen-addr> [stream-subject]
//
//	req-responder tcp://127.0.0.1:15558 swarm.test.echo 127.0.0.1:15560 swarm.test.stream
package main

import (
	"context"
	"encoding/json"
	"fmt"
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

// streamEvents is how many data frames /stream emits before [DONE].
const streamEvents = 3

func main() {
	if len(os.Args) < 4 {
		log.Fatal("usage: req-responder <broker-addr> <unary-subject> <http-listen-addr> [stream-subject]")
	}
	broker, subject, listenAddr := os.Args[1], os.Args[2], os.Args[3]
	streamSubject := ""
	if len(os.Args) > 4 {
		streamSubject = os.Args[4]
	}

	srv := startBackend(listenAddr)

	stops := []func(){serve(broker, subject, "http://"+listenAddr+"/echo")}
	if streamSubject != "" {
		stops = append(stops, serve(broker, streamSubject, "http://"+listenAddr+"/stream"))
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	for _, stop := range stops {
		stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// startBackend listens on addr and serves the /echo (unary) and /stream (SSE) endpoints.
func startBackend(addr string) *http.Server {
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
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < streamEvents; i++ {
			fmt.Fprintf(w, "data: {\"i\":%d}\n\n", i)
			flusher.Flush()
			time.Sleep(50 * time.Millisecond) // distinct chunks so the test sees real streaming
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("backend: %v", err)
		}
	}()
	return srv
}

// serve registers one ZMQ responder for subject relaying to backendURL, returning its stop func.
func serve(broker, subject, backendURL string) func() {
	stop, err := responder.ServeZmq(broker, subject, backendURL, 10*time.Second)
	if err != nil {
		log.Fatalf("serve zmq %s on %s: %v", subject, broker, err)
	}
	log.Printf("req-responder serving %s -> %s (broker %s)", subject, backendURL, broker)
	return stop
}
