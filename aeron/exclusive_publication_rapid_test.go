//go:build rapid

package aeron_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func newExclusivePublicationNearTermEndForRapid(t *rapid.T) (*aeron.ExclusivePublication, int) {
	margin := 8 * rapid.IntRange(4, 32).Draw(t, "margin")
	cfg := aeron.LogConfigNearTermEndForTest(aeron.DefaultTestExclusivePublicationLogConfig, margin)
	ep := aeron.NewTestExclusivePublicationWithoutTBForRapid(cfg, int64(1)<<62)
	return ep, margin
}

func TestExclusivePublication_Offer_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ep := aeron.NewTestExclusivePublicationWithoutTBForRapid(
			aeron.DefaultTestExclusivePublicationLogConfig(),
			int64(1)<<40,
		)
		maxPayload := ep.MaxPayloadLengthForTest()
		messageCount := rapid.IntRange(1, 32).Draw(t, "messageCount")
		shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)

		var lastPosition int64
		for i := range messageCount {
			length := rapid.IntRange(0, maxPayload).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			position := aeron.OfferRetryForTest(ep, payload)
			require.Positive(t, position)
			if i > 0 {
				require.Greater(t, position, lastPosition)
			}
			lastPosition = position
			aligned := logbuffer.AlignFragmentChunkLength(length)
			frameStart := aeron.FragmentedMessageStartOffsetForTest(position, shift, aligned)

			termIndex := logbuffer.IndexByPosition(position, shift)
			termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameStart))
			require.Equal(t, length+logbuffer.DataHeaderLength, frameLength)
			got := termBuf.Bytes()[frameStart+logbuffer.OffsetFramePayload : frameStart+frameLength]
			require.Equal(t, payload, got)
		}
	})
}

func TestExclusivePublication_TermRotation_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ep, margin := newExclusivePublicationNearTermEndForRapid(t)

		first := ep.Offer(make([]byte, margin))
		require.Equal(t, aeron.AdminAction, first)

		second := aeron.OfferRetryForTest(ep, []byte{1, 2, 3})
		require.Positive(t, second)
		require.NotEqual(t, aeron.BackPressured, second)
	})
}

func TestExclusivePublication_TermRotation_TryClaim_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ep, margin := newExclusivePublicationNearTermEndForRapid(t)

		_, _, position, err := ep.TryClaim(margin)
		require.NoError(t, err)
		require.Equal(t, aeron.AdminAction, position)

		termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(ep, 8)
		require.NoError(t, err)
		require.Positive(t, position)

		termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
		termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("12345678"))
		require.NoError(t, ep.Commit(termIndex, frameOffset))
	})
}

func TestExclusivePublication_Offer_rapid_fragmented(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ep := aeron.NewTestExclusivePublicationWithoutTBForRapid(
			aeron.DefaultTestExclusivePublicationLogConfig(),
			int64(1)<<40,
		)
		maxPayload := ep.MaxPayloadLengthForTest()
		maxMessage := ep.MaxMessageLengthForTest()
		messageCount := rapid.IntRange(1, 8).Draw(t, "messageCount")

		var lastPosition int64
		for i := range messageCount {
			length := rapid.IntRange(maxPayload+1, maxMessage).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			position, _ := requireExclusiveFragmentedOffer(t, ep, payload)
			if i > 0 {
				require.Greater(t, position, lastPosition)
			}
			lastPosition = position
		}
	})
}

func TestExclusivePublication_Offer_lengthRule_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, limit := drawLengthRuleLogConfig(t, aeron.DefaultTestExclusivePublicationLogConfig)
		ep := aeron.NewTestExclusivePublicationWithoutTBForRapid(cfg, limit)

		length, oversized := drawOfferLength(t, ep.MaxPayloadLength(), ep.MaxMessageLength())
		payload := make([]byte, length)
		if oversized {
			offer := func() int64 { return ep.Offer(payload) }
			requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.UnsupportedMessageLength, offer)
			return
		}
		require.NotEqual(t, aeron.UnsupportedMessageLength, ep.Offer(payload), "length=%d", length)
	})
}
