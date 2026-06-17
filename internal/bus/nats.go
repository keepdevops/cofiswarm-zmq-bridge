package bus

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Request failure sentinels, surfaced by the HTTP layer as 503 / 504.
var (
	ErrNoResponders   = errors.New("no responders")
	ErrRequestTimeout = errors.New("request timed out")
)

// NatsBackend backs the bus with a real NATS broker — the diagram's always-on "middle man".
// Publish fans out through the broker; Subscribe delivers to live subscribers; a wildcard
// subscription feeds the recent-events ring so /v1/events keeps working unchanged.
type NatsBackend struct {
	nc     *nats.Conn
	topics []string
	mu     sync.RWMutex
	events []map[string]any
}

// NewNats connects to the broker and (if wildcard != "") tails it into the recent ring.
func NewNats(url string, topics []string, wildcard string) (*NatsBackend, error) {
	nc, err := nats.Connect(url, nats.Name("cofiswarm-zmq-bridge"), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	b := &NatsBackend{nc: nc, topics: append([]string(nil), topics...), events: []map[string]any{}}
	if wildcard != "" {
		if _, err := nc.Subscribe(wildcard, func(m *nats.Msg) { b.record(m.Subject, m.Data) }); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return b, nil
}

func (b *NatsBackend) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, len(b.topics))
	copy(out, b.topics)
	return out
}

func (b *NatsBackend) Publish(topic string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return b.nc.Publish(topic, data)
}

func (b *NatsBackend) Subscribe(topic string, fn Handler) (func(), error) {
	sub, err := b.nc.Subscribe(topic, func(m *nats.Msg) {
		var payload map[string]any
		_ = json.Unmarshal(m.Data, &payload)
		fn(m.Subject, payload)
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

func (b *NatsBackend) Recent() []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]map[string]any, len(b.events))
	copy(out, b.events)
	return out
}

// Request does a NATS request/reply: publish to subject and wait for one reply.
// A missing responder returns ErrNoResponders; a slow one returns ErrRequestTimeout.
func (b *NatsBackend) Request(subject string, payload map[string]any, timeout time.Duration) (map[string]any, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	msg, err := b.nc.Request(subject, data, timeout)
	if err != nil {
		switch {
		case errors.Is(err, nats.ErrNoResponders):
			return nil, ErrNoResponders
		case errors.Is(err, nats.ErrTimeout):
			return nil, ErrRequestTimeout
		}
		return nil, err
	}
	var reply map[string]any
	if json.Unmarshal(msg.Data, &reply) != nil {
		return map[string]any{"raw": string(msg.Data)}, nil // reply wasn't a JSON object
	}
	return reply, nil
}

func (b *NatsBackend) Close() error { return b.nc.Drain() }

func (b *NatsBackend) record(subject string, data []byte) {
	var payload map[string]any
	_ = json.Unmarshal(data, &payload)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, map[string]any{"topic": subject, "payload": payload})
	if len(b.events) > 256 {
		b.events = b.events[len(b.events)-256:]
	}
}
