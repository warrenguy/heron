//go:build example

// ipc-echo is a minimal in-process Aeron pub/sub sample.
//
// Prerequisites: run aeronmd with AERON_DIR set to the same directory this
// process uses (see README). No cluster framing; only the public aeron
// Connect, Add*, and (*Client) poll/offer helpers.
//
// Flow: register a publication and subscription on one IPC stream, poll until
// the link is live, offer one payload, receive it on the subscription, verify
// bytes. Run: go run -tags=example ./examples/ipc-echo/
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

	// Must match the media driver directory; default suits local aeronmd from README.
	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), "/tmp/aeron")

	// Connect to aeronmd over shared memory.
	//
	// ErrorHandler logs asynchronous driver faults not returned by the calls below.
	client, err := aeron.Connect(context.Background(), aeron.Config{
		AeronDir: aeronDir,
		ErrorHandler: func(err error) {
			log.Printf("ipc-echo: aeron error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("ipc-echo: connect: %v", err)
	}
	defer closeWithLog(client.Close)

	// aeron:ipc shares memory inside one driver; same stream ID pairs pub with sub.
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

	// Wait until the publication sees the subscription before sending data.
	if err := pub.WaitUntilConnected(ctx); err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: poll until connected: %v", err)
	}

	// Wait until the subscription is linked to the publication before receiving.
	if err := sub.WaitUntilImageCount(ctx, 1); err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: poll until image count: %v", err)
	}

	// Send the payload, retrying while the stream is momentarily full or busy.
	pos, err := pub.OfferWithRetry(ctx, echoPayload)
	if err != nil {
		if exitOnCancel(err) {
			return
		}
		log.Fatalf("ipc-echo: offer: %v", err)
	}
	log.Printf("ipc-echo: offered at position %d", pos)

	// Poll the subscription until the message we sent on the publication arrives.
	received, err := sub.PollOneMessage(ctx)
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
