//go:build bench

package harness

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func resetExclusivePublicationTermWithTimer(b *testing.B, ep *aeron.ExclusivePublication) {
	resetTermWithTimer(b, func() {
		aeron.ResetInMemExclusivePublicationTermForBench(ep)
	})
}

// OfferExclusiveWithTermOverflowReset calls ep.Offer once per invocation. On non-positive
// return (including AdminAction at term-end rotation), it stops the bench timer, resets the
// exclusive publication term and writer cache, and restarts the timer. AdminAction is reset
// without spin; cluster ingress helpers in cluster_client.go spin AdminAction outside the
// timer before reset. Contract benches use this shape for steady single-term measurement;
// callers do not retry Offer in the same iteration.
func OfferExclusiveWithTermOverflowReset(
	b *testing.B,
	ep *aeron.ExclusivePublication,
	payload []byte,
) int64 {
	pos := ep.Offer(payload)
	if pos <= 0 {
		resetExclusivePublicationTermWithTimer(b, ep)
	}
	return pos
}

// TryClaimCommitExclusiveWithTermOverflowReset calls ep.TryClaim once per invocation.
// On error, non-positive position (including AdminAction at term-end rotation), or Commit
// failure it stops the bench timer, resets the exclusive publication term and writer
// cache, and restarts the timer. On success it writes payload bytes at the claimed
// frame and commits. Contract benches use this shape for steady single-term measurement;
// callers do not retry TryClaim in the same iteration.
func TryClaimCommitExclusiveWithTermOverflowReset(
	b *testing.B,
	ep *aeron.ExclusivePublication,
	payload []byte,
) (int64, error) {
	termIndex, frameOffset, position, err := ep.TryClaim(len(payload))
	if err == nil && position > 0 {
		termBuf := ep.TermBuffer(termIndex)
		termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
		err = ep.Commit(termIndex, frameOffset)
	}
	if err != nil || position <= 0 {
		resetExclusivePublicationTermWithTimer(b, ep)
	}
	return position, err
}
