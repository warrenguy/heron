package auctionwire_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/examples/cluster-auction-client/auctionwire"
)

func wrapSizedBuffer(offset, length int) (atomicbuffer.AtomicBuffer, []byte) {
	payload := make([]byte, offset+length)
	return atomicbuffer.Wrap(payload), payload
}

func TestEncodeBid_lengthAndLayout(t *testing.T) {
	const (
		correlationID int64 = 100
		customerID    int64 = 200
		price         int64 = 300
	)
	want := []byte{
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x2c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	dst := make([]byte, auctionwire.BidMessageLength)
	auctionwire.EncodeBid(dst, correlationID, customerID, price)
	require.Equal(t, want, dst)

	buf := atomicbuffer.Wrap(make([]byte, auctionwire.BidMessageLength+4))
	auctionwire.EncodeBidBuffer(buf, 4, correlationID, customerID, price)
	require.Equal(t, want, buf.Bytes()[4:4+auctionwire.BidMessageLength])
}

func TestDecodeEgress(t *testing.T) {
	const (
		correlationID int64 = 11
		customerID    int64 = 22
		price         int64 = 33
	)
	wantBid := auctionwire.EgressBid{
		CorrelationID: correlationID,
		CustomerID:    customerID,
		Price:         price,
		BidSucceeded:  true,
	}
	cases := []struct {
		name         string
		offset       int
		length       int
		bidSucceeded byte
		want         auctionwire.EgressBid
		wantOK       bool
	}{
		{
			name: "valid_at_offset_zero", offset: 0,
			length: auctionwire.EgressMessageLength, bidSucceeded: 1,
			want: wantBid, wantOK: true,
		},
		{
			name: "valid_at_non_zero_offset", offset: 4,
			length: auctionwire.EgressMessageLength, bidSucceeded: 1,
			want: wantBid, wantOK: true,
		},
		{name: "short", offset: 0, length: auctionwire.EgressMessageLength - 1, wantOK: false},
		{
			name: "bid_succeeded_zero", offset: 0,
			length: auctionwire.EgressMessageLength, bidSucceeded: 0,
			want: auctionwire.EgressBid{
				CorrelationID: correlationID,
				CustomerID:    customerID,
				Price:         price,
			},
			wantOK: true,
		},
		{
			name: "bid_succeeded_non_zero", offset: 0,
			length: auctionwire.EgressMessageLength, bidSucceeded: 0xff,
			want: wantBid, wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, payload := wrapSizedBuffer(tc.offset, tc.length)
			if tc.wantOK {
				buf.PutInt64(tc.offset+auctionwire.CorrelationIDOffset, correlationID)
				buf.PutInt64(tc.offset+auctionwire.CustomerIDOffset, customerID)
				buf.PutInt64(tc.offset+auctionwire.PriceOffset, price)
				payload[tc.offset+auctionwire.BidSucceededOffset] = tc.bidSucceeded
			}

			got, ok := auctionwire.DecodeEgress(buf, tc.offset, tc.length)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.want, got)
			}
		})
	}
}
