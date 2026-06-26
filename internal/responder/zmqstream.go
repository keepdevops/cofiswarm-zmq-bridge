package responder

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
)

// errNoSocket is returned by send when the DEALER is mid-reconnect (no live socket). The caller
// logs it; the supervisor re-dials and the next request re-registers.
var errNoSocket = errors.New("zmq responder: no live socket")

// streamReadChunk bounds one wire CHUNK frame. The backend's SSE stream is forwarded as opaque
// byte chunks (the consumer re-parses SSE), so the size only trades frame count vs latency.
const streamReadChunk = 4096

// handleStream relays a streaming request: it POSTs the payload to the backend (which must
// itself stream, e.g. an OpenAI SSE response when the payload sets "stream":true) and forwards
// the response body as CHUNK frames, terminating with END on clean EOF or ERR on failure. The
// broker fans these to the waiting RequestStream caller keyed by reqid.
func (z *zmqResponder) handleStream(reqid string, payload []byte) {
	req, err := http.NewRequestWithContext(z.ctx, http.MethodPost, z.backend, bytes.NewReader(payload))
	if err != nil {
		z.streamErr(reqid, "build request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := z.client.Do(req)
	if err != nil {
		log.Printf("zmq responder %s: stream backend unreachable: %v", z.subject, err)
		z.streamErr(reqid, "backend unreachable")
		return
	}
	defer resp.Body.Close()

	buf := make([]byte, streamReadChunk)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if serr := z.send("CHUNK", reqid, string(buf[:n])); serr != nil {
				if z.ctx.Err() == nil {
					log.Printf("zmq responder %s: stream chunk: %v", z.subject, serr)
				}
				return // socket gone; supervisor will re-dial. Don't send END on a dead socket.
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				if err := z.send("END", reqid); err != nil && z.ctx.Err() == nil {
					log.Printf("zmq responder %s: stream end: %v", z.subject, err)
				}
			} else if z.ctx.Err() == nil {
				log.Printf("zmq responder %s: stream read: %v", z.subject, rerr)
				z.streamErr(reqid, "stream read error")
			}
			return
		}
	}
}

// streamErr sends a terminal ERR frame so the caller's stream closes with a signal rather than
// hanging until the request deadline.
func (z *zmqResponder) streamErr(reqid, msg string) {
	if err := z.send("ERR", reqid, msg); err != nil && z.ctx.Err() == nil {
		log.Printf("zmq responder %s: stream err frame: %v", z.subject, err)
	}
}
