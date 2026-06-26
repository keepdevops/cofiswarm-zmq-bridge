package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
)

// ROUTER/DEALER gives no peer-disconnect signal, so request/reply liveness is heartbeat
// based: a responder PINGs well inside responderTTL, and a crashed one falls out of the
// route table after it — surfacing to callers as ErrNoResponders, the same crash signal
// NATS gives for free. The reaper sweeps stale registrations on reaperEvery.
const (
	responderTTL = 15 * time.Second
	reaperEvery  = 5 * time.Second
)

// zmqRouter is the request/reply broker leg of the ZMQ carrier (separate wire from the
// PUB/SUB telemetry legs). It binds a ROUTER socket; responders connect with DEALER sockets,
// register the subject they serve (READY frame) and keepalive (PING). Request() routes a
// payload to a live responder for the subject and blocks for its reply, mirroring NATS
// request/reply semantics. The wire framing follows go-zeromq's ROUTER/DEALER convention:
// the DEALER sends [empty-delimiter, type, args...] and the ROUTER recv prepends the peer
// identity → [identity, "", type, args...]; replies route by that identity.
type zmqRouter struct {
	sock   zmq4.Socket
	sendMu sync.Mutex // serializes Send (zmq4 sockets are not concurrency-safe)

	mu      sync.RWMutex
	routes  map[string]string              // subject -> responder identity
	seen    map[string]time.Time           // identity -> last heartbeat
	pending map[string]chan map[string]any // reqid -> unary reply sink
	streams map[string]*streamSink         // reqid -> streaming reply sink

	seq    atomic.Uint64
	ctx    context.Context
	cancel context.CancelFunc
}

// newZmqRouter binds a ROUTER socket on addr (e.g. tcp://*:5558) and starts the receive and
// reaper loops.
func newZmqRouter(addr string) (*zmqRouter, error) {
	ctx, cancel := context.WithCancel(context.Background())
	sock := zmq4.NewRouter(ctx, zmq4.WithID(zmq4.SocketIdentity("zmq-bridge")))
	if err := sock.Listen(addr); err != nil {
		cancel()
		return nil, err
	}
	r := &zmqRouter{
		sock:    sock,
		routes:  map[string]string{},
		seen:    map[string]time.Time{},
		pending: map[string]chan map[string]any{},
		streams: map[string]*streamSink{},
		ctx:     ctx,
		cancel:  cancel,
	}
	go r.recvLoop()
	go r.reapLoop()
	return r, nil
}

// recvLoop reads [identity, "", type, args...] frames from responders until Close.
func (r *zmqRouter) recvLoop() {
	for {
		msg, err := r.sock.Recv()
		if err != nil {
			if r.ctx.Err() == nil {
				log.Printf("zmq req recv: %v", err)
			}
			return
		}
		if len(msg.Frames) < 3 {
			log.Printf("zmq req recv: dropping malformed message (%d frames)", len(msg.Frames))
			continue
		}
		id := string(msg.Frames[0])
		switch string(msg.Frames[2]) {
		case "READY":
			if len(msg.Frames) >= 4 {
				r.register(string(msg.Frames[3]), id)
			}
		case "PING":
			r.touch(id)
		case "REPLY":
			if len(msg.Frames) >= 5 {
				r.deliverReply(string(msg.Frames[3]), msg.Frames[4])
			}
		case "CHUNK":
			if len(msg.Frames) >= 5 {
				r.deliverChunk(string(msg.Frames[3]), msg.Frames[4])
			}
		case "END":
			if len(msg.Frames) >= 4 {
				r.closeStream(string(msg.Frames[3]), nil)
			}
		case "ERR":
			if len(msg.Frames) >= 5 {
				r.closeStream(string(msg.Frames[3]), msg.Frames[4])
			}
		default:
			log.Printf("zmq req recv: unknown frame type %q", msg.Frames[2])
		}
	}
}

// register records that id serves subject and marks it freshly seen.
func (r *zmqRouter) register(subject, id string) {
	r.mu.Lock()
	r.routes[subject] = id
	r.seen[id] = time.Now()
	r.mu.Unlock()
}

// touch refreshes a known responder's heartbeat. PINGs from an unregistered id are ignored
// (it must READY first), so a responder that missed its READY can't appear half-alive.
func (r *zmqRouter) touch(id string) {
	r.mu.Lock()
	if _, ok := r.seen[id]; ok {
		r.seen[id] = time.Now()
	}
	r.mu.Unlock()
}

// deliverReply hands a reply payload to the waiting Request, if it hasn't already timed out.
func (r *zmqRouter) deliverReply(reqid string, data []byte) {
	r.mu.Lock()
	ch, ok := r.pending[reqid]
	delete(r.pending, reqid)
	r.mu.Unlock()
	if !ok {
		return // late reply: caller already gave up (timeout) and unregistered
	}
	var reply map[string]any
	if json.Unmarshal(data, &reply) != nil {
		reply = map[string]any{"raw": string(data)} // reply wasn't a JSON object
	}
	ch <- reply
}

// Request routes payload to a live responder for subject and blocks for its reply.
// No live responder returns ErrNoResponders; a slow one returns ErrRequestTimeout.
func (r *zmqRouter) Request(subject string, payload map[string]any, timeout time.Duration) (map[string]any, error) {
	id, err := r.liveResponder(subject)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	reqid := fmt.Sprintf("%s-%d", id, r.seq.Add(1))
	ch := make(chan map[string]any, 1)
	r.mu.Lock()
	r.pending[reqid] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.pending, reqid)
		r.mu.Unlock()
	}()
	if err := r.send(id, "REQUEST", reqid, subject, string(data), "unary"); err != nil {
		return nil, err
	}
	select {
	case reply := <-ch:
		return reply, nil
	case <-time.After(timeout):
		return nil, ErrRequestTimeout
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

// liveResponder returns the identity serving subject if one is registered and freshly seen,
// else ErrNoResponders. Shared by unary Request and streaming RequestStream.
func (r *zmqRouter) liveResponder(subject string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.routes[subject]
	if !ok || time.Since(r.seen[id]) >= responderTTL {
		return "", ErrNoResponders
	}
	return id, nil
}

// send delivers a multipart [identity, "", parts...] message to a responder.
func (r *zmqRouter) send(id string, parts ...string) error {
	frames := make([][]byte, 0, len(parts)+2)
	frames = append(frames, []byte(id), []byte("")) // routing identity + empty delimiter
	for _, p := range parts {
		frames = append(frames, []byte(p))
	}
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	return r.sock.Send(zmq4.NewMsgFrom(frames...))
}

func (r *zmqRouter) reapLoop() {
	ticker := time.NewTicker(reaperEvery)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.reap()
		}
	}
}

// reap drops responders that haven't heartbeat within responderTTL and any routes they held.
func (r *zmqRouter) reap() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, t := range r.seen {
		if now.Sub(t) < responderTTL {
			continue
		}
		delete(r.seen, id)
		for subj, sid := range r.routes {
			if sid == id {
				delete(r.routes, subj)
			}
		}
	}
}

func (r *zmqRouter) Close() error {
	r.cancel()
	return r.sock.Close()
}
