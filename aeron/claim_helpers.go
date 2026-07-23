package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// ClaimFillCommit reserves exclusive-publication space, runs fill, and commits.
//
// Tight spin of attempts only. No idle and no Client.DoWork. Callers that need
// transport pacing must wrap with their own duty cycle.
//
// fill receives the term buffer bytes, payload offset (frameOffset +
// OffsetFramePayload), and the successful TryClaim stream position (claim end).
// Return false from fill to Abort the claim and fail closed.
//
// Returns true only after Commit succeeds. Returns false when:
//   - receiver is nil
//   - attempts <= 0
//   - fill is nil
//   - TryClaim returns a non-nil err (invalid length)
//   - all attempts exhaust with retriable codes
//   - Closed / MaxPositionExceeded (or any non-retriable code)
//   - fill returns false (Abort best-effort)
//   - Commit fails (Abort best-effort)
//
// Invoker-only: ExclusivePublication TryClaim/Commit/Abort are single-writer.
func (ep *ExclusivePublication) ClaimFillCommit(
	payloadLength int,
	attempts int,
	fill func(term []byte, payloadOff int, claimPos int64) bool,
) bool {
	if ep == nil || attempts <= 0 || fill == nil {
		return false
	}
	for range attempts {
		termIndex, frameOffset, position, err := ep.TryClaim(payloadLength)
		if err != nil {
			return false
		}
		if position <= 0 {
			if !IsOfferRetriable(position) {
				return false
			}
			continue
		}

		termBuf := ep.TermBuffer(termIndex)
		payloadOff := frameOffset + logbuffer.OffsetFramePayload
		if !fill(termBuf.Bytes(), payloadOff, position) {
			_ = ep.Abort(termIndex, frameOffset)
			return false
		}
		if err := ep.Commit(termIndex, frameOffset); err != nil {
			_ = ep.Abort(termIndex, frameOffset)
			return false
		}
		return true
	}
	return false
}
