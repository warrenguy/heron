// Package auctionwire encodes and decodes fixed-width auction cluster application
// payloads and derives the tutorial cluster's client-facing ingress endpoints.
//
// Bid and egress layout matches upstream Aeron BasicAuctionClusterClient.
//
// Decode helpers validate the Aeron payload length but not the backing slice; callers
// must ensure len(buf.Bytes()) is at least offset+messageLength.
package auctionwire

import (
	agronaatomic "github.com/warrenguy/heron/agrona/atomic"
)

const (
	CorrelationIDOffset = 0
	CustomerIDOffset    = 8
	PriceOffset         = 16
	BidMessageLength    = 24
	BidSucceededOffset  = BidMessageLength
	EgressMessageLength = BidMessageLength + 1
)

// EncodeBid writes a 24-byte ingress bid into dst.
//
// dst must be at least BidMessageLength bytes.
func EncodeBid(dst []byte, correlationID, customerID, price int64) {
	EncodeBidBuffer(agronaatomic.Wrap(dst), 0, correlationID, customerID, price)
}

// EncodeBidBuffer writes a 24-byte ingress bid at offset in buf.
//
// len(buf.Bytes()) must be at least offset+BidMessageLength.
func EncodeBidBuffer(
	buf agronaatomic.AtomicBuffer,
	offset int,
	correlationID, customerID, price int64,
) {
	buf.PutInt64(offset+CorrelationIDOffset, correlationID)
	buf.PutInt64(offset+CustomerIDOffset, customerID)
	buf.PutInt64(offset+PriceOffset, price)
}

// EgressBid is the decoded 25-byte cluster egress auction payload.
type EgressBid struct {
	CorrelationID int64
	CustomerID    int64 // winning customer after the bid attempt
	Price         int64
	BidSucceeded  bool
}

// DecodeEgress reads an egress auction payload from buf at offset.
//
// ok is false when length is less than EgressMessageLength.
func DecodeEgress(buf agronaatomic.AtomicBuffer, offset, length int) (EgressBid, bool) {
	if length < EgressMessageLength {
		return EgressBid{}, false
	}
	return EgressBid{
		CorrelationID: buf.GetInt64(offset + CorrelationIDOffset),
		CustomerID:    buf.GetInt64(offset + CustomerIDOffset),
		Price:         buf.GetInt64(offset + PriceOffset),
		// AtomicBuffer has no GetByte; upstream Java uses getByte for bidSucceeded.
		BidSucceeded: buf.Bytes()[offset+BidSucceededOffset] != 0,
	}, true
}
