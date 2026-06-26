package bus

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
)

// ZmqBackend is the real ZMQ carrier from the architecture diagram, wired as a forwarder:
//
//	components (PUB) ──▶ ingress SUB :5556 ──▶ [ring + in-process subs] ──▶ egress PUB :5557 ──▶ observer (SUB)
//
// The ingress SUB binds the :5556 wire and collects every swarm.* frame published by
// components. Each frame feeds the recent-events ring and any in-process subscribers
// (which power the /v1/subscribe and /v1/stream SSE endpoints), and is re-emitted on the
// egress PUB so external subscribers — the observer :8016 dashboard — receive it over a
// real ZMQ socket. Bridge-originated Publish (the /v1/publish control-plane injection)
// takes the same path, so it reaches the egress wire too. The egress leg is optional:
// when egressAddr is "", ZmqBackend behaves as ingress-only.
type ZmqBackend struct {
	sub    zmq4.Socket
	pub    zmq4.Socket // egress; nil when no egress leg is wired
	req    *zmqRouter  // request/reply broker leg; nil when no req leg is wired
	topics []string

	ctx    context.Context
	cancel context.CancelFunc

	pubMu  sync.Mutex // serializes egress Send (zmq4 sockets are not concurrency-safe)
	mu     sync.RWMutex
	subs   []*zmqSub
	events []map[string]any
}

type zmqSub struct {
	pattern string
	fn      Handler
}

// NewZmq binds an ingress SUB socket on addr (e.g. tcp://*:5556), filtered to the topic
// prefix filter ("" subscribes to everything), and starts the receive loop. If egressAddr
// is non-empty it also binds an egress PUB socket there (e.g. tcp://*:5557) onto which
// every delivered frame is re-emitted for external subscribers. If reqAddr is non-empty it
// also binds a ROUTER socket there (e.g. tcp://*:5558) for request/reply, making the backend
// a bus.Requester. topics is the declared set reported by /v1/topics.
func NewZmq(addr, egressAddr, reqAddr, filter string, topics []string) (*ZmqBackend, error) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := zmq4.NewSub(ctx)
	if err := sub.Listen(addr); err != nil {
		cancel()
		return nil, err
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, filter); err != nil {
		cancel()
		_ = sub.Close()
		return nil, err
	}
	var pub zmq4.Socket
	if egressAddr != "" {
		pub = zmq4.NewPub(ctx)
		if err := pub.Listen(egressAddr); err != nil {
			cancel()
			_ = sub.Close()
			_ = pub.Close()
			return nil, err
		}
	}
	var req *zmqRouter
	if reqAddr != "" {
		r, err := newZmqRouter(reqAddr)
		if err != nil {
			cancel()
			_ = sub.Close()
			if pub != nil {
				_ = pub.Close()
			}
			return nil, err
		}
		req = r
	}
	b := &ZmqBackend{
		sub:    sub,
		pub:    pub,
		req:    req,
		topics: append([]string(nil), topics...),
		ctx:    ctx,
		cancel: cancel,
		subs:   []*zmqSub{},
		events: []map[string]any{},
	}
	go b.recvLoop()
	return b, nil
}

// recvLoop reads multipart [topic, json-payload] frames off the wire until Close.
func (b *ZmqBackend) recvLoop() {
	for {
		msg, err := b.sub.Recv()
		if err != nil {
			// Recv errors once the socket is closed on shutdown; log anything else.
			if b.ctx.Err() == nil {
				log.Printf("zmq recv: %v", err)
			}
			return
		}
		if len(msg.Frames) < 2 {
			log.Printf("zmq recv: dropping malformed message (%d frames)", len(msg.Frames))
			continue
		}
		topic := string(msg.Frames[0])
		var payload map[string]any
		if err := json.Unmarshal(msg.Frames[1], &payload); err != nil {
			log.Printf("zmq recv %s: bad json payload: %v", topic, err)
			continue
		}
		b.deliver(topic, payload)
	}
}

// deliver records the message to the ring, fans out to matching in-process subscribers,
// and re-emits it on the egress wire for external subscribers.
func (b *ZmqBackend) deliver(topic string, payload map[string]any) {
	b.mu.Lock()
	b.events = append(b.events, map[string]any{"topic": topic, "payload": payload})
	if len(b.events) > 256 {
		b.events = b.events[len(b.events)-256:]
	}
	subs := append([]*zmqSub(nil), b.subs...)
	b.mu.Unlock()
	for _, s := range subs {
		if matchSubject(s.pattern, topic) {
			s.fn(topic, payload)
		}
	}
	b.forward(topic, payload)
}

// forward re-emits a message on the egress PUB socket as multipart [topic, json-payload].
// No-op when the egress leg is not wired.
func (b *ZmqBackend) forward(topic string, payload map[string]any) {
	if b.pub == nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("zmq egress %s: marshal payload: %v", topic, err)
		return
	}
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	if err := b.pub.Send(zmq4.NewMsgFrom([]byte(topic), data)); err != nil {
		if b.ctx.Err() == nil {
			log.Printf("zmq egress %s: send: %v", topic, err)
		}
	}
}

func (b *ZmqBackend) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, len(b.topics))
	copy(out, b.topics)
	return out
}

// Publish injects a bridge-originated message: it fans out to in-process subscribers and
// the recent ring, and is re-emitted on the egress wire so external subscribers see it too.
func (b *ZmqBackend) Publish(topic string, payload map[string]any) error {
	b.deliver(topic, payload)
	return nil
}

func (b *ZmqBackend) Subscribe(topic string, fn Handler) (func(), error) {
	sub := &zmqSub{pattern: topic, fn: fn}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, s := range b.subs {
			if s == sub {
				b.subs = append(b.subs[:i], b.subs[i+1:]...)
				return
			}
		}
	}
	return cancel, nil
}

// Request routes payload to a live responder for subject over the ROUTER leg and blocks for
// its reply, satisfying bus.Requester. Returns ErrNoResponders when the req leg is disabled
// (no reqAddr) or no live responder serves the subject; ErrRequestTimeout when one is slow.
func (b *ZmqBackend) Request(subject string, payload map[string]any, timeout time.Duration) (map[string]any, error) {
	if b.req == nil {
		return nil, ErrNoResponders
	}
	return b.req.Request(subject, payload, timeout)
}

// RequestStream routes a streaming request over the ROUTER leg, satisfying bus.StreamRequester.
// Returns ErrNoResponders when the req leg is disabled or no live responder serves the subject.
func (b *ZmqBackend) RequestStream(subject string, payload map[string]any, timeout time.Duration) (<-chan []byte, error) {
	if b.req == nil {
		return nil, ErrNoResponders
	}
	return b.req.RequestStream(subject, payload, timeout)
}

func (b *ZmqBackend) Recent() []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]map[string]any, len(b.events))
	copy(out, b.events)
	return out
}

func (b *ZmqBackend) Close() error {
	b.cancel()
	err := b.sub.Close()
	if b.pub != nil {
		if perr := b.pub.Close(); err == nil {
			err = perr
		}
	}
	if b.req != nil {
		if perr := b.req.Close(); err == nil {
			err = perr
		}
	}
	return err
}
