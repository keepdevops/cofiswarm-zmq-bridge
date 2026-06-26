// ingress-pub is a test helper for the end-to-end zmq wire gate: it dials the bridge
// ingress SUB wire as a native ZMQ PUB and repeatedly publishes swarm.slot.erase frames
// (absorbing the PUB->SUB slow-joiner by resending) until killed or its deadline. This is
// the production component -> bridge path used by buspresence/zmqpub.go, exercised here
// over a real socket with no HTTP involved.
//
// Usage: ingress-pub <ingress-addr> [seconds]
//
//	ingress-pub tcp://127.0.0.1:15556 6
package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/go-zeromq/zmq4"
)

func main() {
	if len(os.Args) < 2 {
		panic("usage: ingress-pub <ingress-addr> [seconds]")
	}
	addr := os.Args[1]
	dur := 6 * time.Second
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil && n > 0 {
			dur = time.Duration(n) * time.Second
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub := zmq4.NewPub(ctx)
	if err := pub.Dial(addr); err != nil {
		panic(err)
	}
	defer pub.Close()

	payload, _ := json.Marshal(map[string]any{"gate": "native", "src": "ingress-pub"})
	frame := zmq4.NewMsgFrom([]byte("swarm.slot.erase"), payload)
	deadline := time.After(dur)
	for {
		select {
		case <-deadline:
			return
		default:
			_ = pub.Send(frame)
			time.Sleep(200 * time.Millisecond)
		}
	}
}
