package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	testOfferPayload = [64]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	pubSink          int64
)

func setupZeroAllocPublication() *aeron.Publication {
	pub := aeron.NewTestPublicationWithoutTBForRapid(
		aeron.DefaultTestPublicationLogConfig(),
		int64(1)<<62,
	)
	pubSink = pub.Offer(testOfferPayload[:])
	return pub
}

func TestPublication_Offer_ZeroAllocs(t *testing.T) {
	cases := []struct {
		name  string
		setup func() *aeron.Publication
		fn    func(pub *aeron.Publication)
	}{
		{
			name:  "Offer_SmallFixedPayload",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				pubSink = aeron.OfferRetryForTest(pub, testOfferPayload[:])
			},
		},
		{
			name:  "OfferWithOffset",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				pubSink = aeron.OfferWithOffsetRetryForTest(pub, testOfferPayload[:], 0, 64)
			},
		},
		{
			name:  "TryClaim_Commit",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(pub, 64)
				if err != nil || position <= 0 {
					panic("tryClaim failed")
				}
				termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
				termBuf.PutBytes(frameOffset+logbuffer.DataHeaderLength, testOfferPayload[:])
				if err := pub.Commit(termIndex, frameOffset); err != nil {
					panic(err)
				}
			},
		},
		{
			name:  "TryClaim_Abort",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(pub, 64)
				if err != nil || position <= 0 {
					panic("tryClaim failed")
				}
				if err := pub.Abort(termIndex, frameOffset); err != nil {
					panic(err)
				}
			},
		},
		{
			name:  "Position",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				pubSink = pub.Position()
			},
		},
		{
			name:  "IsConnected",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				if pub.IsConnected() {
					pubSink = 1
				}
			},
		},
		{
			name:  "PositionLimit",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				pubSink = pub.PositionLimit()
			},
		},
		{
			name:  "AvailableWindow",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				pubSink = pub.AvailableWindow()
			},
		},
		{
			name:  "ChannelStatus",
			setup: setupZeroAllocPublication,
			fn: func(pub *aeron.Publication) {
				status, err := pub.ChannelStatus()
				if err != nil {
					panic(err)
				}
				pubSink = status
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := tc.setup()
			testutil.AssertZeroAllocs(t, tc.name, 1000, func() {
				tc.fn(pub)
			})
		})
	}
}
