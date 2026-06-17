// Package responder makes any HTTP inference backend answerable over the bus.
//
// It subscribes to a NATS request subject (e.g. swarm.observer.model.<name>) and relays
// each request to a backend HTTP endpoint, replying with the backend's response. This is
// the piece that enables the diagram's "offline on crash" detection: if the responder
// process is gone, a requester gets NATS no-responders — no heartbeat needed.
package responder

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
)

// Relay POSTs the request bytes to the backend and returns the response body.
func Relay(client *http.Client, backend string, data []byte) ([]byte, error) {
	resp, err := client.Post(backend, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// Serve subscribes to subject and relays each request to backend, replying with the result.
// Returns the subscription so the caller can drain/unsubscribe.
func Serve(nc *nats.Conn, subject, backend string, timeout time.Duration) (*nats.Subscription, error) {
	client := &http.Client{Timeout: timeout}
	return nc.Subscribe(subject, func(m *nats.Msg) {
		body, err := Relay(client, backend, m.Data)
		if err != nil {
			_ = m.Respond([]byte(`{"ok":false,"error":"backend unreachable"}`))
			return
		}
		_ = m.Respond(body)
	})
}
