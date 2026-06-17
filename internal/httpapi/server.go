package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/bus"
)

type Server struct {
	bus    bus.Backend
	stream string // subject for /v1/stream ("all topics"), e.g. "swarm.>"
}

// New builds the control plane. streamSubject is what /v1/stream subscribes to (usually
// the bus wildcard); pass "" to disable /v1/stream.
func New(b bus.Backend, streamSubject string) *Server {
	return &Server{bus: b, stream: streamSubject}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/v1/topics", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"topics": s.bus.Topics()})
	})
	mux.HandleFunc("/v1/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Topic   string         `json:"topic"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Topic == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := s.bus.Publish(body.Topic, body.Payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/v1/events", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"events": s.bus.Recent()})
	})
	// SSE: stream a single subject (NATS wildcards allowed) to non-NATS clients.
	mux.HandleFunc("/v1/subscribe", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "topic query param required", http.StatusBadRequest)
			return
		}
		s.sse(w, r, topic)
	})
	// SSE: stream all topics (the configured wildcard) — for the observer dashboard.
	mux.HandleFunc("/v1/stream", func(w http.ResponseWriter, r *http.Request) {
		if s.stream == "" {
			http.Error(w, "stream subject not configured", http.StatusNotImplemented)
			return
		}
		s.sse(w, r, s.stream)
	})
	return mux
}

// sse subscribes to topic and relays each message to the client as Server-Sent Events.
func (s *Server) sse(w http.ResponseWriter, r *http.Request, topic string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events := make(chan []byte, 64)
	cancel, err := s.bus.Subscribe(topic, func(subject string, payload map[string]any) {
		data, mErr := json.Marshal(map[string]any{"topic": subject, "payload": payload})
		if mErr != nil {
			return
		}
		select {
		case events <- data: // deliver
		default: // drop if the client is too slow rather than block the bus
		}
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer cancel()

	fmt.Fprintf(w, ": subscribed to %s\n\n", topic)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-events:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}
