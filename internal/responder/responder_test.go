package responder

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRelayForwardsAndReturnsBody(t *testing.T) {
	var got []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"reply":"pong"}`))
	}))
	defer ts.Close()

	out, err := Relay(&http.Client{Timeout: 2 * time.Second}, ts.URL, []byte(`{"prompt":"ping"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"prompt":"ping"}` {
		t.Errorf("backend received %s", got)
	}
	if string(out) != `{"reply":"pong"}` {
		t.Errorf("relay returned %s", out)
	}
}

func TestRelayErrorsWhenBackendUnreachable(t *testing.T) {
	_, err := Relay(&http.Client{Timeout: time.Second}, "http://127.0.0.1:0/x", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for unreachable backend")
	}
}
