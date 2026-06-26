package bus

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// streamBuffer bounds the per-stream chunk queue. A fast SSE consumer keeps it near-empty; a
// slow one back-pressures the router recv loop (chunks block until read or the stream closes),
// so keep it large enough to absorb bursts without unbounded memory.
const streamBuffer = 256

// streamSink is the router-side endpoint of one streaming request: chunks flow on ch until the
// responder sends END/ERR or the deadline fires, at which point ch and done are closed once.
type streamSink struct {
	ch   chan []byte
	done chan struct{}
	once sync.Once
}

// RequestStream routes payload to a live responder for subject in streaming mode and returns a
// channel of raw response chunks (the responder forwards the backend's body verbatim, e.g. an
// SSE token stream). The channel closes on END, ERR, the timeout, or Close. ErrNoResponders if
// no live responder serves the subject. The payload should request a streaming backend response
// (e.g. {"stream":true}) — the responder just relays whatever the backend streams.
func (r *zmqRouter) RequestStream(subject string, payload map[string]any, timeout time.Duration) (<-chan []byte, error) {
	id, err := r.liveResponder(subject)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	reqid := fmt.Sprintf("%s-s%d", id, r.seq.Add(1))
	sink := &streamSink{ch: make(chan []byte, streamBuffer), done: make(chan struct{})}
	r.mu.Lock()
	r.streams[reqid] = sink
	r.mu.Unlock()
	if err := r.send(id, "REQUEST", reqid, subject, string(data), "stream"); err != nil {
		r.closeStream(reqid, nil)
		return nil, err
	}
	// Overall deadline + shutdown guard: closeStream is idempotent (sync.Once), so whichever
	// fires first wins and the rest are no-ops.
	go func() {
		select {
		case <-sink.done:
		case <-time.After(timeout):
			r.closeStream(reqid, []byte("stream timeout"))
		case <-r.ctx.Done():
			r.closeStream(reqid, nil)
		}
	}()
	return sink.ch, nil
}

// deliverChunk forwards one response chunk to the waiting stream. zmq4 reuses frame buffers
// across Recv calls, so the chunk is copied before it leaves the recv loop. A slow consumer
// back-pressures here (bounded by streamBuffer) rather than dropping tokens.
func (r *zmqRouter) deliverChunk(reqid string, data []byte) {
	r.mu.RLock()
	sink, ok := r.streams[reqid]
	r.mu.RUnlock()
	if !ok {
		return // unknown/closed stream: late chunk after END or timeout
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case sink.ch <- cp:
	case <-sink.done:
	case <-r.ctx.Done():
	}
}

// closeStream terminates a stream once: unregisters it and closes its channel so the consumer
// unblocks. A non-nil reason (ERR/timeout) is logged; a clean END passes nil.
func (r *zmqRouter) closeStream(reqid string, reason []byte) {
	r.mu.Lock()
	sink, ok := r.streams[reqid]
	delete(r.streams, reqid)
	r.mu.Unlock()
	if !ok {
		return
	}
	if reason != nil {
		log.Printf("zmq req stream %s closed: %s", reqid, reason)
	}
	sink.once.Do(func() {
		close(sink.done)
		close(sink.ch)
	})
}
