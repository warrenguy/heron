//go:build example

package main

import (
	"context"
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
	normalizedCtx := clusterCtx.Normalize()

	asyncConnect, err := client.NewAsyncConnect(clusterCtx)
	if err != nil {
		log.Fatal(err)
	}
	defer closeWithLog(asyncConnect.Close)

	idleStrategy := normalizedCtx.IdleStrategy
	var result *client.ConnectResult
	for result == nil {
		if err := ctx.Err(); err != nil {
			return
		}
		transport.DoWork()
		result, err = asyncConnect.Poll()
		if err != nil {
			log.Fatal(err)
		}
		if result == nil {
			idleStrategy.Idle(0)
		}
	}

	session, err := client.NewAeronCluster(result)
	if err != nil {
		log.Fatal(err)
	}
	defer closeWithLog(session.Close)

	if err := runDutyCycle(
		ctx,
		transport,
		session,
		config,
		clientState,
		normalizedCtx.IdleStrategy,
	); err != nil {
		log.Fatal(err)
	}
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
