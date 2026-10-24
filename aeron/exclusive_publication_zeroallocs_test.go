package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

var exclusivePubSink int64

func setupZeroAllocExclusivePublication() *aeron.ExclusivePublication {
	ep := aeron.NewTestExclusivePublicationWithoutTBForRapid(
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<62,
	)
	exclusivePubSink = ep.Offer(testOfferPayload[:])
	return ep
}

func TestExclusivePublication_Offer_ZeroAllocs(t *testing.T) {
	cases := []struct {
		name string
		fn   func(ep *aeron.ExclusivePublication)
	}{
		{
			name: "Offer_SmallFixedPayload",
			fn: func(ep *aeron.ExclusivePublication) {
				exclusivePubSink = aeron.OfferRetryForTest(ep, testOfferPayload[:])
			},
		},
		{
			name: "OfferWithOffset",
			fn: func(ep *aeron.ExclusivePublication) {
				exclusivePubSink = aeron.OfferWithOffsetRetryForTest(ep, testOfferPayload[:], 0, 64)
			},
		},
		{
			name: "OfferWithReservedValue",
			fn: func(ep *aeron.ExclusivePublication) {
				exclusivePubSink = aeron.RetryOnAdminActionForTest(func() int64 {
					return ep.OfferWithReservedValue(
						testOfferPayload[:],
						0,
						len(testOfferPayload),
						testReservedValueSupplier,
					)
				})
			},
		},
		{
			name: "OfferTwoParts",
			fn: func(ep *aeron.ExclusivePublication) {
				// payload is a stack copy. The row fails if a part escapes.
				payload := testOfferPayload
				exclusivePubSink = aeron.RetryOnAdminActionForTest(func() int64 {
					return ep.OfferTwoParts(payload[:8], payload[8:], nil)
				})
			},
		},
		{
			name: "OfferVector_fragmented",
			fn: func(ep *aeron.ExclusivePublication) {
				parts := [3][]byte{
					testFragmentedOfferPayload[:8],
					testFragmentedOfferPayload[8:1400],
					testFragmentedOfferPayload[1400:],
				}
				exclusivePubSink = aeron.RetryOnAdminActionForTest(func() int64 {
					return ep.OfferVector(parts[:], testReservedValueSupplier)
				})
			},
		},
		{
			name: "TryClaim_Commit",
			fn: func(ep *aeron.ExclusivePublication) {
				termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(ep, 64)
				if err != nil || position <= 0 {
					panic("tryClaim failed")
				}
				termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
				termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, testOfferPayload[:])
				if err := ep.Commit(termIndex, frameOffset); err != nil {
					panic(err)
				}
			},
		},
		{
			name: "TryClaim_Abort",
			fn: func(ep *aeron.ExclusivePublication) {
				termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(ep, 64)
				if err != nil || position <= 0 {
					panic("tryClaim failed")
				}
				if err := ep.Abort(termIndex, frameOffset); err != nil {
					panic(err)
				}
			},
		},
		{
			name: "Position",
			fn: func(ep *aeron.ExclusivePublication) {
				exclusivePubSink = ep.Position()
			},
		},
		{
			name: "IsConnected",
			fn: func(ep *aeron.ExclusivePublication) {
				if ep.IsConnected() {
					exclusivePubSink = 1
				}
			},
		},
		{
			name: "PositionLimit",
			fn: func(ep *aeron.ExclusivePublication) {
				exclusivePubSink = ep.PositionLimit()
			},
		},
		{
			name: "AvailableWindow",
			fn: func(ep *aeron.ExclusivePublication) {
				exclusivePubSink = ep.AvailableWindow()
			},
		},
		{
			name: "ChannelStatus",
			fn: func(ep *aeron.ExclusivePublication) {
				status, err := ep.ChannelStatus()
				if err != nil {
					panic(err)
				}
				exclusivePubSink = status
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := setupZeroAllocExclusivePublication()
			testutil.AssertZeroAllocs(t, tc.name, 1000, func() {
				tc.fn(ep)
			})
		})
	}
}
