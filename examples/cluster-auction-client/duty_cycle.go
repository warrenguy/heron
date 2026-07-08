//go:build example

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/examples/cluster-auction-client/auctionwire"
	"github.com/warrenguy/heron/idlestrategy"
)

var auctionLog = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)

func newEgressHandlers(clientState *ClientState) *client.EgressHandlers {
	return &client.EgressHandlers{
		MessageFunc: func(
			clusterSessionID int64,
			_ int64,
			buf atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			_ *aeron.Header,
		) {
			bid, ok := auctionwire.DecodeEgress(buf, offset, length)
			if !ok {
				log.Printf(
					"cluster-auction-client: egress decode failed (length %d, need %d)",
					length,
					auctionwire.EgressMessageLength,
				)
				return
			}
			clientState.recordEgress(bid)
			auctionLog.Printf(
				"SessionMessage(%d, %d,%d, %d, %t)",
				clusterSessionID,
				bid.CorrelationID,
				bid.CustomerID,
				bid.Price,
				bid.BidSucceeded,
			)
		},
		SessionEventFunc: func(
			correlationID int64,
			_ int64,
			leadershipTermID int64,
			leaderMemberID int32,
			code client.EventCode,
			detail []byte,
		) {
			auctionLog.Printf(
				"SessionEvent(%d, %d, %d, %s, %s)",
				correlationID,
				leadershipTermID,
				leaderMemberID,
				code,
				detail,
			)
		},
		NewLeaderFunc: func(
			clusterSessionID int64,
			leadershipTermID int64,
			leaderMemberID int32,
			_ []byte,
		) {
			auctionLog.Printf(
				"NewLeader(%d, %d, %d)",
				clusterSessionID,
				leadershipTermID,
				leaderMemberID,
			)
		},
	}
}

func runDutyCycle(
	ctx context.Context,
	session *client.AeronCluster,
	config Config,
	clientState *ClientState,
	idleStrategy idlestrategy.IdleStrategy,
) error {
	offerConfig := client.IngressOfferConfig{IdleStrategy: idleStrategy}
	clientState.nextBidDeadline = time.Now().Add(randomDelayMs(config.BidIntervalMs))
	clientState.bidsLeftToSend = config.NumBids

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		now := time.Now()

		if clientState.bidDue(now) && clientState.bidsLeftToSend > 0 {
			price := clientState.lastBidSeen + int64(rand.IntN(10))
			correlationID := clientState.nextCorrelation()
			err := sendBid(ctx, session, config, correlationID, price, offerConfig)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("cluster-auction-client: bid offer: %w", err)
			}
			completed := time.Now()
			clientState.nextBidDeadline = completed.Add(randomDelayMs(config.BidIntervalMs))
			clientState.keepAliveDeadline = completed.Add(time.Second)
			clientState.bidsLeftToSend--
			auctionLog.Printf(
				"Sent(%d, %d, %d) bidsRemaining=%d",
				correlationID,
				config.CustomerID,
				price,
				clientState.bidsLeftToSend,
			)
		} else if clientState.keepAliveDue(now) {
			if clientState.bidsLeftToSend <= 0 {
				break
			}
			_, err := session.SendKeepAliveUntilPositive(ctx, offerConfig)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("cluster-auction-client: keepalive: %w", err)
			}
			clientState.keepAliveDeadline = time.Now().Add(time.Second)
		}

		session.Aeron().DoWork()
		idleStrategy.Idle(session.PollEgress())
	}

	return nil
}

func sendBid(
	ctx context.Context,
	session *client.AeronCluster,
	config Config,
	correlationID int64,
	price int64,
	offerConfig client.IngressOfferConfig,
) error {
	var buf [auctionwire.BidMessageLength]byte
	auctionwire.EncodeBid(buf[:], correlationID, config.CustomerID, price)
	_, err := session.IngressOfferUntilPositive(
		ctx,
		offerConfig,
		func() int64 {
			return session.Offer(buf[:])
		},
	)
	return err
}

func randomDelayMs(maxMs int) time.Duration {
	return time.Duration(rand.IntN(maxMs)) * time.Millisecond
}
