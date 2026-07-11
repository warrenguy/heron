//go:build example

// cluster-auction-client sends timed auction bids to a running Java cluster
// and logs egress bid results from BasicAuctionClusteredService.
//
// Prerequisites: upstream Java UDP cluster and a separate aeronmd for this Go
// client (dual-driver topology; see README).
//
// Flow: parse CLI, connect transport, open cluster session, run the bid and
// keepalive duty cycle until num-bids are sent. Run:
// go run -tags=example ./examples/cluster-auction-client/
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// CLI fills ingress endpoints, channels, and bid timing (see README flags).
	config, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	// Connect to this process's own aeronmd, not the Java cluster drivers.
	//
	// ErrorHandler logs asynchronous driver faults not returned by the calls below.
	transport, err := aeron.Connect(aeron.Context{
		AeronDir: config.AeronDir,
		ErrorHandler: func(err error) {
			log.Printf("cluster-auction-client: aeron error: %v", err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer closeWithLog(transport.Close)

	// Bid state is shared between egress handlers and the duty-cycle loop.
	clientState := newClientState()

	// UDP ingress endpoints target the running cluster members.
	clusterCtx := config.clusterContext(transport, newEgressHandlers(clientState))

	// Open a cluster client session against the configured ingress and egress.
	session, err := client.Connect(ctx, clusterCtx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatal(err)
	}
	defer closeWithLog(session.Close)

	// Timed bids and keepalives until num-bids are exhausted.
	if err := runDutyCycle(ctx, session, config, clientState); err != nil {
		log.Fatal(err)
	}
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
