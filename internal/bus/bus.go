package bus

import (
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Prefix string   `yaml:"prefix"`
	Topics []string `yaml:"topics"`
}

type Bus struct {
	mu     sync.RWMutex
	topics []string
	events []map[string]any
}

func LoadTopics(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return cfg.Topics, nil
}

func New(topics []string) *Bus {
	return &Bus{topics: append([]string(nil), topics...), events: []map[string]any{}}
}

func (b *Bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, len(b.topics))
	copy(out, b.topics)
	return out
}

func (b *Bus) Publish(topic string, payload map[string]any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, map[string]any{"topic": topic, "payload": payload})
	if len(b.events) > 256 {
		b.events = b.events[len(b.events)-256:]
	}
}

func (b *Bus) Recent() []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]map[string]any, len(b.events))
	copy(out, b.events)
	return out
}
