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
	"syscall"

	"github.com/warrenguy/heron/aeron"
)

const (
	streamID = 1001
	channel  = "aeron:ipc"
)

var echoPayload = []byte("hello")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), "/tmp/aeron")

	client, err := aeron.Connect(aeron.Context{
		AeronDir: aeronDir,
		ErrorHandler: func(err error) {
			log.Printf("ipc-echo: aeron error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("ipc-echo: connect: %v", err)
	}
	defer closeWithLog(client.Close)

	pub, err := client.AddPublication(streamID, channel)
	if err != nil {
		log.Fatalf("ipc-echo: add publication: %v", err)
	}
	defer closeWithLog(pub.Close)

	sub, err := client.AddSubscription(streamID, channel)
	if err != nil {
		log.Fatalf("ipc-echo: add subscription: %v", err)
	}
	defer closeWithLog(sub.Close)

	if err := aeron.PollUntilConnected(ctx, client, pub); err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: poll until connected: %v", err)
	}
	if err := aeron.PollUntilImageCount(ctx, client, sub, 1); err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: poll until image count: %v", err)
	}

	pos, err := aeron.OfferWithRetry(ctx, client, pub, echoPayload)
	if err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: offer: %v", err)
	}
	log.Printf("ipc-echo: offered at position %d", pos)

	received, err := aeron.PollOneMessage(ctx, client, sub)
	if err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: receive: %v", err)
	}
	if !bytes.Equal(received, echoPayload) {
		log.Fatalf("ipc-echo: echo mismatch: got %q want %q", received, echoPayload)
	}
	log.Printf("ipc-echo: echo ok (%q)", received)
	fmt.Println(string(received))
}

func exitOnCancel(err error) bool {
	return errors.Is(err, context.Canceled)
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
