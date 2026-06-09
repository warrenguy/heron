package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// Publication return codes mirror Aeron Java Publication / ConcurrentPublication.
//
//   - Values > 0: new stream position after a successful Offer or TryClaim.
//   - NotConnected (-1): no subscriber; retry with idle strategy.
//   - BackPressured (-2): at or past publication limit; retry when subscribers advance.
//   - AdminAction (-3): rotation or metadata catch-up in flight; retry immediately.
//   - Closed (-4): publication closed, or an auxiliary read failed (counter/metadata).
//     PositionLimit, Position, and AvailableWindow map internal read failures to Closed
//     because those methods return int64 only.
//   - MaxPositionExceeded (-5): stream position exhausted; close and add a new publication.
//   - UnsupportedMessageLength (-6): reserved; TryClaim returns a Go error when length
//     exceeds maxPayloadLength. Offer fragments payloads up to maxMessageLength.
const (
	NotConnected             int64 = -1
	BackPressured            int64 = -2
	AdminAction              int64 = -3
	Closed                   int64 = -4
	MaxPositionExceeded      int64 = -5
	UnsupportedMessageLength int64 = -6
)

func backPressureStatus(
	pub *Publication,
	position int64,
	messageLength int64,
) int64 {
	requiredSpan := int64(logbuffer.AlignFragmentChunkLength(int(messageLength)))
	if position+requiredSpan >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}
	if logbuffer.IsConnectedVolatile(pub.meta) != 0 {
		return BackPressured
	}
	return NotConnected
}
