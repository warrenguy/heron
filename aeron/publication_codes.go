package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

// Publication return codes mirror Aeron Java Publication / ConcurrentPublication.
//
//   - Values > 0: new stream position after a successful Offer or TryClaim.
//   - NotConnected (-1): no subscriber; retry with idle strategy.
//   - BackPressured (-2): at or past publication limit; retry when subscribers advance.
//   - AdminAction (-3): rotation or metadata catch-up in flight; retry immediately.
//   - Closed (-4): publication closed, or an auxiliary read failed (counter/metadata).
//     PositionLimit returns Closed on close; publicationCounterValueError can force
//     Closed on cold accessors (test hook).
//     AvailableWindow propagates negative codes from PositionLimit and Position.
//     Position maps internal read failures to Closed because it returns int64 only.
//   - MaxPositionExceeded (-5): stream position exhausted; close and add a new publication.
//   - UnsupportedMessageLength (-6): reserved; TryClaim returns a Go error when length
//     exceeds maxPayloadLength. Offer fragments payloads up to maxMessageLength.
//
// Offer and TryClaim read the bound publication-limit CNC slot directly after bind.
// PositionLimit reads the same bound slot without per-call CounterValue validation.
// ChannelStatus calls CountersReader.CounterValue on each read.
//
// publicationCounterValueError test hook can fail those cold accessors; hot publish
// ignores it.
const (
	NotConnected             int64 = -1
	BackPressured            int64 = -2
	AdminAction              int64 = -3
	Closed                   int64 = -4
	MaxPositionExceeded      int64 = -5
	UnsupportedMessageLength int64 = -6
)

func backPressureStatusForLog(
	meta atomic.AtomicBuffer,
	maxPossiblePosition int64,
	position int64,
	requiredSpan int64,
) int64 {
	if position+requiredSpan >= maxPossiblePosition {
		return MaxPositionExceeded
	}
	if logbuffer.IsConnectedVolatile(meta) != 0 {
		return BackPressured
	}
	return NotConnected
}

func backPressureStatus(
	pub *Publication,
	position int64,
	messageLength int64,
) int64 {
	requiredSpan := int64(logbuffer.AlignFragmentChunkLength(int(messageLength)))
	return backPressureStatusForLog(pub.meta, pub.maxPossiblePosition, position, requiredSpan)
}
