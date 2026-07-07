//go:build example

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
	"sync/atomic"
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

	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), defaultAeronDir)

	var hadTransportError atomic.Bool

	transport, err := aeron.Connect(aeron.Context{
		AeronDir:            aeronDir,
		InterServiceTimeout: 10 * time.Second,
		ErrorHandler: func(err error) {
			hadTransportError.Store(true)
			log.Printf("cluster-echo-client: aeron error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("cluster-echo-client: connect: %v", err)
	}
	defer closeWithLog(transport.Close)

	var echoed []byte
	session, err := client.Connect(ctx, client.ClusterContext{
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

	pos, err := session.IngressOfferUntilPositive(ctx, client.IngressOfferConfig{}, func() int64 {
		fatalOnTransportError(&hadTransportError, "ingress offer")
		return session.Offer(pingPayload)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatalf(
			"cluster-echo-client: ingress offer failed: %v transportError=%v",
			err,
			hadTransportError.Load(),
		)
	}
	log.Printf("cluster-echo-client: offered ping at position %d", pos)

	echoCtx, cancel := context.WithTimeout(ctx, echoTimeout)
	defer cancel()

	for len(echoed) == 0 {
		fatalOnTransportError(&hadTransportError, "echo poll")
		if err := echoCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Fatalf(
				"cluster-echo-client: echo timeout after %s (sessionState=%v transportError=%v)",
				echoTimeout, session.SessionState(), hadTransportError.Load(),
			)
		}
		session.SpinOnce()
	}

	if !bytes.Equal(echoed, pingPayload) {
		log.Fatalf("cluster-echo-client: echo mismatch: got %q want %q", echoed, pingPayload)
	}
	fmt.Printf("cluster-echo-client: echo ok (%q)\n", echoed)
}

func fatalOnTransportError(had *atomic.Bool, phase string) {
	if had.Load() {
		log.Fatalf("cluster-echo-client: transport error during %s", phase)
	}
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
