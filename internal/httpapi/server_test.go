package httpapi

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/keepdevops/cofiswarm-zmq-bridge/internal/bus"
)

func TestSubscribeSSEDeliversPublished(t *testing.T) {
	b := bus.NewMem([]string{"t"})
	ts := httptest.NewServer(New(b, "t").Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/subscribe?topic=t", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// Subscription is registered by the time headers arrive; publish a beat later.
	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = b.Publish("t", map[string]any{"k": "v"})
	}()

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data:") {
			if !strings.Contains(line, `"k":"v"`) {
				t.Fatalf("unexpected SSE data: %s", line)
			}
			return // delivered
		}
	}
	t.Fatal("no SSE data line received")
}

func TestSubscribeRequiresTopic(t *testing.T) {
	ts := httptest.NewServer(New(bus.NewMem(nil), "").Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/subscribe")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestStreamDisabledWhenUnconfigured(t *testing.T) {
	ts := httptest.NewServer(New(bus.NewMem(nil), "").Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}
