package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/bus"
)

// fakeReq is a MemBackend that also satisfies bus.Requester with canned results.
type fakeReq struct {
	*bus.MemBackend
	reply map[string]any
	err   error
}

func (f *fakeReq) Request(string, map[string]any, time.Duration) (map[string]any, error) {
	return f.reply, f.err
}

func postRequest(t *testing.T, h http.Handler) *http.Response {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	resp, err := http.Post(ts.URL+"/v1/request", "application/json",
		strings.NewReader(`{"subject":"swarm.observer.model.x","payload":{"prompt":"hi"}}`))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestRequestReturnsReply(t *testing.T) {
	be := &fakeReq{MemBackend: bus.NewMem(nil), reply: map[string]any{"reply": "pong"}}
	resp := postRequest(t, New(be, "").Handler())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out["reply"] != "pong" {
		t.Errorf("reply = %v", out)
	}
}

func TestRequestNoResponders503(t *testing.T) {
	be := &fakeReq{MemBackend: bus.NewMem(nil), err: bus.ErrNoResponders}
	if resp := postRequest(t, New(be, "").Handler()); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestRequestUnsupportedBackend501(t *testing.T) {
	// MemBackend is not a Requester -> 501.
	if resp := postRequest(t, New(bus.NewMem(nil), "").Handler()); resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}
