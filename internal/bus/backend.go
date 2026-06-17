package bus

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Handler receives a delivered message: the subject and its decoded JSON payload.
type Handler func(topic string, payload map[string]any)

// Backend is the bus transport. MemBackend (in-process) and NatsBackend (real broker)
// both implement it, so the HTTP control plane is agnostic to which one is wired in.
type Backend interface {
	Topics() []string
	Publish(topic string, payload map[string]any) error
	Subscribe(topic string, fn Handler) (cancel func(), err error)
	Recent() []map[string]any
	Close() error
}

// Config is the topics.yaml shape (see cofiswarm-common/zmq/topics.yaml).
type Config struct {
	Prefix string   `yaml:"prefix"`
	Topics []string `yaml:"topics"`
}

// LoadConfig reads the topics yaml (prefix + declared topics).
func LoadConfig(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// LoadTopics is retained for backward compatibility with earlier callers.
func LoadTopics(path string) ([]string, error) {
	cfg, err := LoadConfig(path)
	return cfg.Topics, err
}
