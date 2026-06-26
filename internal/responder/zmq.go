package responder

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
)

// zmqHeartbeat is how often the responder PINGs the broker. It must be comfortably below the
// broker's responderTTL so a healthy responder never falls out of the route table.
const zmqHeartbeat = 5 * time.Second

// reconnect backoff bounds: go-zeromq's DEALER does not transparently re-establish a dropped
// peer, so the supervisor re-dials on any socket error (e.g. the bridge restarting) and
// re-registers, growing the wait up to reconnectMax.
const (
	reconnectMin = 500 * time.Millisecond
	reconnectMax = 10 * time.Second
)

// zmqResponder serves one request subject over the ZMQ request/reply plane: a DEALER socket
// dialed to the bridge ROUTER. It registers the subject (READY), keepalives (PING), and relays
// each REQUEST to an HTTP backend — unary (one REPLY) or streamed (CHUNK… then END/ERR). The
// supervisor re-dials and re-registers if the socket errors, so a bridge restart self-heals
// without bouncing the process. DEALER wire framing: [empty-delimiter, type, args...].
type zmqResponder struct {
	subject string
	backend string
	broker  string
	client  *http.Client

	mu     sync.Mutex // guards sock + serializes Send (zmq4 sockets are not concurrency-safe)
	sock   zmq4.Socket
	ctx    context.Context
	cancel context.CancelFunc
}

// ServeZmq makes an HTTP backend answerable over the ZMQ request/reply plane. It dials the
// bridge ROUTER at brokerAddr (e.g. tcp://127.0.0.1:5558) with a DEALER socket, registers the
// subject it serves, heartbeats so the broker keeps the route live, and relays each request to
// backend with the given per-request timeout. Returns a stop func that tears the supervisor
// down. An ungraceful exit stops the heartbeats, so the broker drops the route within its TTL
// and callers see no-responders — the same crash signal NATS gives for free.
//
// One responder per subject: the DEALER identity is derived from the subject, so a second
// responder for the same subject would collide. Run distinct subjects per process.
func ServeZmq(brokerAddr, subject, backend string, timeout time.Duration) (func(), error) {
	ctx, cancel := context.WithCancel(context.Background())
	z := &zmqResponder{
		subject: subject,
		backend: backend,
		broker:  brokerAddr,
		client:  &http.Client{Timeout: timeout},
		ctx:     ctx,
		cancel:  cancel,
	}
	// First dial is synchronous so an immediate misconfig (bad address) surfaces to the caller;
	// thereafter the supervisor owns the socket lifecycle.
	if err := z.dial(); err != nil {
		cancel()
		return nil, err
	}
	go z.supervise()
	return func() { cancel(); z.closeSock() }, nil
}

// dial opens a fresh DEALER, connects to the broker, and sends READY. On success the new socket
// becomes the live one.
func (z *zmqResponder) dial() error {
	sock := zmq4.NewDealer(z.ctx, zmq4.WithID(zmq4.SocketIdentity("responder-"+z.subject)))
	if err := sock.Dial(z.broker); err != nil {
		_ = sock.Close()
		return err
	}
	z.mu.Lock()
	z.sock = sock
	z.mu.Unlock()
	return z.send("READY", z.subject)
}

// supervise runs one recv session per connection, re-dialing with backoff on any error until
// the responder is stopped. Heartbeats run for the lifetime (send no-ops between sessions).
func (z *zmqResponder) supervise() {
	go z.heartbeatLoop()
	backoff := reconnectMin
	for z.ctx.Err() == nil {
		z.recvSession() // blocks until the socket errors or we're stopped
		if z.ctx.Err() != nil {
			return
		}
		log.Printf("zmq responder %s: connection lost, re-dialing in %v", z.subject, backoff)
		select {
		case <-z.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err := z.dial(); err != nil {
			log.Printf("zmq responder %s: re-dial failed: %v", z.subject, err)
			if backoff < reconnectMax {
				backoff *= 2
			}
			continue
		}
		backoff = reconnectMin
	}
}

// recvSession reads requests off the current socket until it errors (then returns so the
// supervisor can re-dial).
func (z *zmqResponder) recvSession() {
	z.mu.Lock()
	sock := z.sock
	z.mu.Unlock()
	if sock == nil {
		return
	}
	for {
		msg, err := sock.Recv()
		if err != nil {
			if z.ctx.Err() == nil {
				log.Printf("zmq responder %s: recv: %v", z.subject, err)
			}
			return
		}
		// ["", "REQUEST", reqid, subject, payload, mode]
		if len(msg.Frames) < 5 || string(msg.Frames[1]) != "REQUEST" {
			log.Printf("zmq responder %s: dropping malformed request (%d frames)", z.subject, len(msg.Frames))
			continue
		}
		reqid := string(msg.Frames[2])
		payload := msg.Frames[4]
		mode := "unary"
		if len(msg.Frames) >= 6 {
			mode = string(msg.Frames[5])
		}
		if mode == "stream" {
			go z.handleStream(reqid, payload)
		} else {
			go z.handle(reqid, payload)
		}
	}
}

// send delivers a multipart [empty-delimiter, parts...] message on the current socket.
func (z *zmqResponder) send(parts ...string) error {
	frames := make([][]byte, 0, len(parts)+1)
	frames = append(frames, []byte("")) // empty delimiter (DEALER convention)
	for _, p := range parts {
		frames = append(frames, []byte(p))
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.sock == nil {
		return errNoSocket
	}
	return z.sock.Send(zmq4.NewMsgFrom(frames...))
}

func (z *zmqResponder) closeSock() {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.sock != nil {
		_ = z.sock.Close()
		z.sock = nil
	}
}

func (z *zmqResponder) heartbeatLoop() {
	ticker := time.NewTicker(zmqHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-z.ctx.Done():
			return
		case <-ticker.C:
			if err := z.send("PING"); err != nil && z.ctx.Err() == nil {
				log.Printf("zmq responder %s: heartbeat: %v", z.subject, err)
			}
		}
	}
}

// handle relays one unary request to the backend and sends the reply. A backend failure or a
// non-JSON body still produces a JSON object reply so the broker always decodes a map.
func (z *zmqResponder) handle(reqid string, payload []byte) {
	body, err := Relay(z.client, z.backend, payload)
	if err != nil {
		log.Printf("zmq responder %s: backend unreachable: %v", z.subject, err)
		body = []byte(`{"ok":false,"error":"backend unreachable"}`)
	} else if !json.Valid(body) {
		body, _ = json.Marshal(map[string]any{"raw": string(body)})
	}
	if err := z.send("REPLY", reqid, string(body)); err != nil && z.ctx.Err() == nil {
		log.Printf("zmq responder %s: reply: %v", z.subject, err)
	}
}
