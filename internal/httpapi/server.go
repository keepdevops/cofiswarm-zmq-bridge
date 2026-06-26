package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

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
	// HTTP -> bus request/reply gateway: lets non-bus clients route over the carrier.
	mux.HandleFunc("/v1/request", s.handleRequest)
	// HTTP -> bus streaming request/reply gateway: SSE of raw response chunks.
	mux.HandleFunc("/v1/request/stream", s.handleRequestStream)
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

// handleRequest is the HTTP -> NATS request/reply gateway. POST {subject, payload, timeout_ms}.
// 503 = no responders (dead/absent), 504 = timeout (slow), 501 = backend can't do request/reply.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	requester, ok := s.bus.(bus.Requester)
	if !ok {
		http.Error(w, "request/reply requires the nats backend", http.StatusNotImplemented)
		return
	}
	var body struct {
		Subject   string         `json:"subject"`
		Payload   map[string]any `json:"payload"`
		TimeoutMs int            `json:"timeout_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Subject == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	timeout := 30 * time.Second
	if body.TimeoutMs > 0 {
		timeout = time.Duration(body.TimeoutMs) * time.Millisecond
	}
	reply, err := requester.Request(body.Subject, body.Payload, timeout)
	switch {
	case errors.Is(err, bus.ErrNoResponders):
		http.Error(w, "no responders for subject", http.StatusServiceUnavailable)
	case errors.Is(err, bus.ErrRequestTimeout):
		http.Error(w, "request timed out", http.StatusGatewayTimeout)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		_ = json.NewEncoder(w).Encode(reply)
	}
}

// handleRequestStream is the HTTP -> bus streaming request/reply gateway. POST
// {subject, payload, timeout_ms}; the response is a transparent byte passthrough of the
// backend's response stream — CHUNK frames are written verbatim, in order, so a consumer
// reconstructs the exact upstream stream (e.g. the backend's own SSE). End of stream is the
// response body close. 501 = backend can't stream; 503 = no responders.
func (s *Server) handleRequestStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	streamer, ok := s.bus.(bus.StreamRequester)
	if !ok {
		http.Error(w, "streaming request/reply requires the zmq backend", http.StatusNotImplemented)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	var body struct {
		Subject   string         `json:"subject"`
		Payload   map[string]any `json:"payload"`
		TimeoutMs int            `json:"timeout_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Subject == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	timeout := 120 * time.Second
	if body.TimeoutMs > 0 {
		timeout = time.Duration(body.TimeoutMs) * time.Millisecond
	}
	ch, err := streamer.RequestStream(body.Subject, body.Payload, timeout)
	if err != nil {
		switch {
		case errors.Is(err, bus.ErrNoResponders):
			http.Error(w, "no responders for subject", http.StatusServiceUnavailable)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	// Mirror the upstream content type (our use is SSE backends); raw bytes pass through so the
	// framing is whatever the backend produced.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, open := <-ch:
			if !open {
				return // stream ended (END/ERR/timeout) -> close the body
			}
			_, _ = w.Write(chunk)
			flusher.Flush()
		}
	}
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
