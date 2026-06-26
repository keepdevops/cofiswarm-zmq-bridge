package bus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
)

// A DEALER that registers (READY) and echoes one request round-trips through the ROUTER leg:
// Request routes to it and returns its reply.
func TestZmqRequestReply(t *testing.T) {
	const reqAddr = "tcp://127.0.0.1:55631"
	b, err := NewZmq("tcp://127.0.0.1:55630", "", reqAddr, "swarm.", []string{"swarm.observer.mode.flat"})
	if err != nil {
		t.Skipf("cannot bind ZMQ carrier: %v", err)
	}
	defer b.Close()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	dealer := zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("responder-test")))
	if err := dealer.Dial(reqAddr); err != nil {
		t.Skipf("cannot dial broker: %v", err)
	}
	defer dealer.Close()

	// Responder: register, then relay each REQUEST back as a REPLY echoing the payload.
	go func() {
		_ = dealer.Send(zmq4.NewMsgFrom([]byte(""), []byte("READY"), []byte("swarm.observer.mode.flat")))
		for {
			msg, err := dealer.Recv()
			if err != nil {
				return
			}
			if len(msg.Frames) < 5 || string(msg.Frames[1]) != "REQUEST" {
				continue
			}
			reqid := msg.Frames[2]
			reply, _ := json.Marshal(map[string]any{"ok": true, "echo": string(msg.Frames[4])})
			_ = dealer.Send(zmq4.NewMsgFrom([]byte(""), []byte("REPLY"), reqid, reply))
		}
	}()

	// READY registration races the first Request; retry until the route is live (or give up).
	deadline := time.After(3 * time.Second)
	for {
		reply, err := b.Request("swarm.observer.mode.flat", map[string]any{"prompt": "hi"}, 500*time.Millisecond)
		if err == nil {
			if reply["ok"] != true {
				t.Fatalf("reply = %v", reply)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("request never succeeded: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// With no responder registered, Request fails fast as ErrNoResponders (surfaced as 503).
func TestZmqRequestNoResponders(t *testing.T) {
	b, err := NewZmq("tcp://127.0.0.1:55633", "", "tcp://127.0.0.1:55634", "swarm.", nil)
	if err != nil {
		t.Skipf("cannot bind ZMQ carrier: %v", err)
	}
	defer b.Close()
	if _, err := b.Request("swarm.observer.mode.flat", map[string]any{}, 200*time.Millisecond); !errors.Is(err, ErrNoResponders) {
		t.Fatalf("err = %v, want ErrNoResponders", err)
	}
}

// When the request/reply leg is disabled (no reqAddr), Request reports ErrNoResponders rather
// than panicking on a nil router.
func TestZmqRequestDisabled(t *testing.T) {
	b := newZmqForTest(t, "tcp://127.0.0.1:55635")
	defer b.Close()
	if _, err := b.Request("swarm.observer.mode.flat", nil, 100*time.Millisecond); !errors.Is(err, ErrNoResponders) {
		t.Fatalf("err = %v, want ErrNoResponders", err)
	}
}
