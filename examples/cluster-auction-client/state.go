//go:build example

package main

import (
	"time"

	"github.com/warrenguy/heron/examples/cluster-auction-client/auctionwire"
)

// ClientState holds mutable auction client duty-cycle fields.
type ClientState struct {
	nextCorrelationID int64
	lastBidSeen       int64
	bidsLeftToSend    int

	nextBidDeadline   time.Time
	keepAliveDeadline time.Time
}

func newClientState() *ClientState {
	return &ClientState{
		lastBidSeen: 100,
	}
}

func (clientState *ClientState) nextCorrelation() int64 {
	correlationID := clientState.nextCorrelationID
	clientState.nextCorrelationID++
	return correlationID
}

func (clientState *ClientState) recordEgress(bid auctionwire.EgressBid) {
	clientState.lastBidSeen = bid.Price
}

func (clientState *ClientState) bidDue(now time.Time) bool {
	return !now.Before(clientState.nextBidDeadline)
}

func (clientState *ClientState) keepAliveDue(now time.Time) bool {
	return !now.Before(clientState.keepAliveDeadline)
}
