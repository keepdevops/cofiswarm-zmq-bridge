package bus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
)

func TestMatchSubject(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"swarm.slot.erase", "swarm.slot.erase", true},
		{"swarm.>", "swarm.slot.erase", true},
		{"swarm.>", "swarm", false},            // ">" needs at least one trailing token
		{"swarm.*", "swarm.slot", true},        // "*" matches one token
		{"swarm.*", "swarm.slot.erase", false}, // "*" is exactly one token
		{"swarm.*.erase", "swarm.slot.erase", true},
		{">", "swarm.slot", true},
		{"swarm.slot", "swarm.slot.erase", false},
	}
	for _, c := range cases {
		if got := matchSubject(c.pattern, c.subject); got != c.want {
			t.Errorf("matchSubject(%q,%q)=%v want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

// In-process Publish fans out to matching wildcard subscribers and lands in the ring,
// without touching the wire.
func TestZmqPublishInProcess(t *testing.T) {
	b := newZmqForTest(t, "tcp://127.0.0.1:55621")
	defer b.Close()

	got := make(chan map[string]any, 1)
	cancel, err := b.Subscribe("swarm.>", func(_ string, p map[string]any) { got <- p })
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	if err := b.Publish("swarm.slot.erase", map[string]any{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if p["k"] != "v" {
			t.Fatalf("payload = %v", p)
		}
	case <-time.After(time.Second):
		t.Fatal("wildcard subscriber not delivered")
	}
	if len(b.Recent()) != 1 {
		t.Fatalf("recent = %d, want 1", len(b.Recent()))
	}
}

// Round-trips a frame over a real ZMQ socket: a component PUB connects to the bound
// carrier and the wildcard subscriber receives it.
func TestZmqWireRoundTrip(t *testing.T) {
	const addr = "tcp://127.0.0.1:55622"
	b := newZmqForTest(t, addr)
	defer b.Close()

	got := make(chan string, 1)
	cancel, err := b.Subscribe("swarm.>", func(topic string, _ map[string]any) { got <- topic })
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	pub := zmq4.NewPub(ctx)
	if err := pub.Dial(addr); err != nil {
		t.Skipf("cannot dial carrier: %v", err)
	}
	defer pub.Close()

	payload, _ := json.Marshal(map[string]any{"k": "v"})
	frame := zmq4.NewMsgFrom([]byte("swarm.slot.erase"), payload)
	// PUB->SUB is a slow joiner: resend until the subscription propagates or we time out.
	deadline := time.After(3 * time.Second)
	for {
		if err := pub.Send(frame); err != nil {
			t.Fatalf("send: %v", err)
		}
		select {
		case topic := <-got:
			if topic != "swarm.slot.erase" {
				t.Fatalf("topic = %q", topic)
			}
			return
		case <-deadline:
			t.Fatal("timeout waiting for wire delivery")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// The carrier forwards ingress frames out the egress PUB: a component PUB connects to the
// ingress wire and an observer SUB on the egress wire receives the re-emitted frame.
func TestZmqEgressForward(t *testing.T) {
	const ingressAddr = "tcp://127.0.0.1:55624"
	const egressAddr = "tcp://127.0.0.1:55625"
	b, err := NewZmq(ingressAddr, egressAddr, "swarm.", []string{"swarm.slot.erase"})
	if err != nil {
		t.Skipf("cannot bind ZMQ carrier: %v", err)
	}
	defer b.Close()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	observer := zmq4.NewSub(ctx)
	if err := observer.Dial(egressAddr); err != nil {
		t.Skipf("cannot dial egress: %v", err)
	}
	defer observer.Close()
	if err := observer.SetOption(zmq4.OptionSubscribe, "swarm."); err != nil {
		t.Fatal(err)
	}

	got := make(chan string, 1)
	go func() {
		msg, err := observer.Recv()
		if err != nil || len(msg.Frames) < 1 {
			return
		}
		got <- string(msg.Frames[0])
	}()

	// Inject via the bridge (Publish takes the same deliver->forward path as wire ingress).
	// Resend until the egress subscription propagates (PUB->SUB slow joiner).
	deadline := time.After(3 * time.Second)
	for {
		if err := b.Publish("swarm.slot.erase", map[string]any{"k": "v"}); err != nil {
			t.Fatal(err)
		}
		select {
		case topic := <-got:
			if topic != "swarm.slot.erase" {
				t.Fatalf("egress topic = %q", topic)
			}
			return
		case <-deadline:
			t.Fatal("timeout waiting for egress delivery")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func newZmqForTest(t *testing.T, addr string) *ZmqBackend {
	t.Helper()
	b, err := NewZmq(addr, "", "swarm.", []string{"swarm.slot.erase"})
	if err != nil {
		t.Skipf("cannot bind ZMQ carrier on %s: %v", addr, err)
	}
	return b
}
