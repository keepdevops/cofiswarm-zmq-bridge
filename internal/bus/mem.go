package bus

import "sync"

// MemBackend is an in-process bus: publishes fan out to in-process subscribers and into a
// bounded recent-events ring. It is the default backend and is used in tests/offline.
type MemBackend struct {
	mu     sync.RWMutex
	topics []string
	subs   map[string][]*subscription
	events []map[string]any
}

type subscription struct {
	fn Handler
}

// NewMem builds an in-process backend over the declared topics.
func NewMem(topics []string) *MemBackend {
	return &MemBackend{
		topics: append([]string(nil), topics...),
		subs:   map[string][]*subscription{},
		events: []map[string]any{},
	}
}

func (b *MemBackend) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, len(b.topics))
	copy(out, b.topics)
	return out
}

func (b *MemBackend) Publish(topic string, payload map[string]any) error {
	b.mu.Lock()
	b.events = append(b.events, map[string]any{"topic": topic, "payload": payload})
	if len(b.events) > 256 {
		b.events = b.events[len(b.events)-256:]
	}
	subs := append([]*subscription(nil), b.subs[topic]...)
	b.mu.Unlock()
	for _, s := range subs {
		s.fn(topic, payload)
	}
	return nil
}

func (b *MemBackend) Subscribe(topic string, fn Handler) (func(), error) {
	sub := &subscription{fn: fn}
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], sub)
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		list := b.subs[topic]
		for i, s := range list {
			if s == sub {
				b.subs[topic] = append(list[:i], list[i+1:]...)
				return
			}
		}
	}
	return cancel, nil
}

func (b *MemBackend) Recent() []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]map[string]any, len(b.events))
	copy(out, b.events)
	return out
}

func (b *MemBackend) Close() error { return nil }
