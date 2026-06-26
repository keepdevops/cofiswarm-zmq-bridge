package httpapi_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/bus"
	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/httpapi"
	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/responder"
)

// End-to-end through the real chain:
//   HTTP POST /v1/request -> ZmqBackend.Request -> ROUTER :reqAddr -> DEALER responder.ServeZmq
//   -> backend HTTP -> REPLY -> JSON to client.
func TestE2EUnaryRequestOverZmq(t *testing.T) {
	gw, reqAddr, stop := e2eBridge(t, "tcp://127.0.0.1:55650", "tcp://127.0.0.1:55651")
	defer stop()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "echo": in["prompt"]})
	}))
	defer backend.Close()

	stopR, err := responder.ServeZmq(reqAddr, "swarm.observer.mode.flat", backend.URL, 5*time.Second)
	if err != nil {
		t.Skipf("cannot start responder: %v", err)
	}
	defer stopR()

	reply := e2ePostUntilReady(t, gw+"/v1/request", `{"subject":"swarm.observer.mode.flat","payload":{"prompt":"hi"},"timeout_ms":1000}`)
	var out map[string]any
	if err := json.Unmarshal(reply, &out); err != nil {
		t.Fatalf("reply not json: %v (%s)", err, reply)
	}
	if out["ok"] != true || out["echo"] != "hi" {
		t.Fatalf("reply = %v", out)
	}
}

// End-to-end streaming: the backend emits SSE frames; the bridge relays them as `data:` events
// over /v1/request/stream and closes with `: end`.
func TestE2EStreamRequestOverZmq(t *testing.T) {
	gw, reqAddr, stop := e2eBridge(t, "tcp://127.0.0.1:55652", "tcp://127.0.0.1:55653")
	defer stop()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fl, _ := w.(http.Flusher)
		for _, tok := range []string{"Hel", "lo", " world"} {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", tok)
			if fl != nil {
				fl.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer backend.Close()

	stopR, err := responder.ServeZmq(reqAddr, "swarm.observer.model.8086", backend.URL, 5*time.Second)
	if err != nil {
		t.Skipf("cannot start responder: %v", err)
	}
	defer stopR()

	// Wait for registration via the unary gateway (503 until READY lands), then open the stream.
	e2ePostUntilReady(t, gw+"/v1/request", `{"subject":"swarm.observer.model.8086","payload":{},"timeout_ms":1000}`)

	resp, err := http.Post(gw+"/v1/request/stream", "application/json",
		strings.NewReader(`{"subject":"swarm.observer.model.8086","payload":{"stream":true},"timeout_ms":3000}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("stream status %d: %s", resp.StatusCode, b)
	}

	// Raw passthrough reconstructs the backend's own SSE stream verbatim, ending with [DONE].
	var content strings.Builder
	sawDone := false
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(line[len("data:"):])
		if data == "[DONE]" {
			sawDone = true
			continue
		}
		var f struct {
			Choices []struct {
				Delta struct{ Content string } `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(data), &f) == nil && len(f.Choices) > 0 {
			content.WriteString(f.Choices[0].Delta.Content)
		}
	}
	if !sawDone {
		t.Fatalf("stream did not reach [DONE]; got %q", content.String())
	}
	if content.String() != "Hello world" {
		t.Fatalf("stream content = %q, want %q", content.String(), "Hello world")
	}
}

// e2eBridge builds a real ZMQ backend (ingress + req leg) fronted by the HTTP gateway, returning
// the gateway base URL, the request-leg dial address, and a teardown.
func e2eBridge(t *testing.T, ingress, reqAddr string) (gateway, dialAddr string, stop func()) {
	t.Helper()
	b, err := bus.NewZmq(ingress, "", reqAddr, "swarm.", []string{"swarm.observer.mode.flat"})
	if err != nil {
		t.Skipf("cannot bind ZMQ carrier: %v", err)
	}
	srv := httptest.NewServer(httpapi.New(b, "").Handler())
	return srv.URL, reqAddr, func() { srv.Close(); b.Close() }
}

// e2ePostUntilReady POSTs body until the gateway stops returning 503 (responder READY race),
// then returns the success body. Fails the test if it never becomes ready.
func e2ePostUntilReady(t *testing.T, url, body string) []byte {
	t.Helper()
	deadline := time.After(4 * time.Second)
	for {
		resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return raw
			}
		}
		select {
		case <-deadline:
			t.Fatalf("gateway never became ready (last err=%v)", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
