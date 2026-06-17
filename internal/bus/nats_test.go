package bus

import (
	"testing"
	"time"
)

// Round-trips a message through a real broker. Skips if no nats-server is reachable.
func TestNatsRoundTrip(t *testing.T) {
	b, err := NewNats("nats://127.0.0.1:4222", []string{"swarm.test"}, "")
	if err != nil {
		t.Skipf("no nats-server reachable: %v", err)
	}
	defer b.Close()

	got := make(chan map[string]any, 1)
	cancel, err := b.Subscribe("swarm.test.x", func(_ string, p map[string]any) { got <- p })
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	if err := b.Publish("swarm.test.x", map[string]any{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if p["k"] != "v" {
			t.Fatalf("payload = %v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for NATS delivery")
	}
}
