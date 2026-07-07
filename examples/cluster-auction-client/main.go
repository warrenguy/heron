//go:build example

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

	config, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

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

	clientState := newClientState()
	clusterCtx := config.clusterContext(transport, newEgressHandlers(clientState))

	session, err := client.Connect(ctx, clusterCtx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatal(err)
	}
	defer closeWithLog(session.Close)

	if err := runDutyCycle(
		ctx,
		session,
		config,
		clientState,
		session.Context().IdleStrategy,
	); err != nil {
		log.Fatal(err)
	}
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
