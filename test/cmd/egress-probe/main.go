// egress-probe is a test helper for the end-to-end zmq wire gate: it dials the bridge
// egress PUB wire as a native ZMQ SUB, waits for one swarm.* frame, prints
// "GOT <topic> <payload>" and exits 0. It exits 2 on timeout and 3 on a socket error,
// so the gate script can assert real wire delivery (not just the HTTP control plane).
//
// Usage: egress-probe <egress-addr> [timeout-seconds]
//
//	egress-probe tcp://127.0.0.1:15557 8
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-zeromq/zmq4"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: egress-probe <egress-addr> [timeout-seconds]")
		os.Exit(3)
	}
	addr := os.Args[1]
	timeout := 8 * time.Second
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := zmq4.NewSub(ctx)
	if err := sub.Dial(addr); err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		os.Exit(3)
	}
	defer sub.Close()
	if err := sub.SetOption(zmq4.OptionSubscribe, "swarm."); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(3)
	}

	done := make(chan zmq4.Msg, 1)
	go func() {
		if msg, err := sub.Recv(); err == nil {
			done <- msg
		}
	}()

	select {
	case msg := <-done:
		if len(msg.Frames) >= 2 {
			fmt.Printf("GOT %s %s\n", msg.Frames[0], msg.Frames[1])
		} else {
			fmt.Printf("GOT %s\n", msg.Frames[0])
		}
		os.Exit(0)
	case <-time.After(timeout):
		fmt.Fprintln(os.Stderr, "TIMEOUT: no egress frame received")
		os.Exit(2)
	}
}
