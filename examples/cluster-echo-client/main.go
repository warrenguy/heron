//go:build example

// cluster-echo-client sends one ingress message to a running Java Aeron cluster
// and waits for EchoService to return the same bytes on egress.
//
// Prerequisites: Java cluster with embedded media driver (see README). Go client
// shares AERON_DIR with that driver; do not start a separate aeronmd for IPC.
//
// Flow: connect transport, open cluster session on IPC channels, offer a ping
// on ingress, poll the session until egress delivers the echo. Run:
// go run -tags=example ./examples/cluster-echo-client/
package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
)

const (
	defaultAeronDir = "/dev/shm/aeron"
	echoTimeout     = 30 * time.Second
	ipcChannel      = "aeron:ipc?term-length=64k"
)

var pingPayload = []byte("heron-cluster-echo-ping")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Same absolute path as the Java cluster container (default from README).
	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), defaultAeronDir)

	// Connect to the cluster's embedded media driver.
	//
	// ErrorHandler logs asynchronous driver faults not returned by the calls below.
	transport, err := aeron.Connect(context.Background(), aeron.Config{
		AeronDir:            aeronDir,
		InterServiceTimeout: 10 * time.Second,
		ErrorHandler: func(err error) {
			log.Printf("cluster-echo-client: aeron error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("cluster-echo-client: connect: %v", err)
	}
	defer closeWithLog(transport.Close)

	var echoed []byte

	// Open a cluster client session on IPC ingress and egress. EgressHandler
	// captures the first message the cluster returns on the egress stream.
	session, err := client.Connect(ctx, client.Config{
		Aeron:          transport,
		AeronDir:       aeronDir,
		IngressChannel: ipcChannel,
		EgressChannel:  ipcChannel,
		EgressHandler: &client.EgressHandlers{
			MessageBytesFunc: func(_, _ int64, payload []byte, _ *aeron.Header) {
				if len(echoed) == 0 {
					echoed = payload
				}
			},
		},
		ClientName:     "cluster-echo-client",
		MessageTimeout: 60 * time.Second,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatal(err)
	}
	defer closeWithLog(session.Close)

	// Send the ping on cluster ingress, retrying while the stream is momentarily full or busy.
	pos, err := session.IngressOfferUntilPositive(ctx, client.IngressOfferConfig{}, func() int64 {
		return session.Offer(pingPayload)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatalf("cluster-echo-client: ingress offer failed: %v", err)
	}
	log.Printf("cluster-echo-client: offered ping at position %d", pos)

	echoCtx, cancel := context.WithTimeout(ctx, echoTimeout)
	defer cancel()

	// Poll the session until the message we sent on ingress arrives on egress.
	for len(echoed) == 0 {
		if err := echoCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Fatalf(
				"cluster-echo-client: echo timeout after %s (sessionState=%v)",
				echoTimeout,
				session.SessionState(),
			)
		}
		session.SpinOnce()
	}

	if !bytes.Equal(echoed, pingPayload) {
		log.Fatalf("cluster-echo-client: echo mismatch: got %q want %q", echoed, pingPayload)
	}
	fmt.Printf("cluster-echo-client: echo ok (%q)\n", echoed)
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
