package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// ReservedValueSupplier returns the reserved value for one DATA frame.
// Matches Java ReservedValueSupplier.
//
// Behaviour:
//   - The offer calls the supplier once per DATA frame, never for padding.
//   - The offer calls the supplier after the header, flags and payload
//     stores and before the release store of the frame length.
//   - The offer writes the returned value little-endian at
//     logbuffer.OffsetReservedValue.
//
// frame spans the whole frame, header plus payload. cap(frame) equals
// len(frame). The frame length field still holds the negative in-flight
// length.
//
// Lifetime: frame is valid only during the call. The supplier must not
// retain it.
//
// Concurrency:
//   - The supplier runs on the offering goroutine.
//   - The supplier must not offer on the same publication. On an
//     ExclusivePublication a nested offer reserves the same space.
//   - The supplier must not panic. A panic leaves the frame in flight. On
//     an ExclusivePublication the cached term offset then lags the stored
//     tail, so the handle is unusable.
type ReservedValueSupplier func(frame []byte) int64

// offerPayload is the message of one offer as a list of parts. copyNext
// walks the parts in order.
//
// Lifetime: the value lives on the stack of the public offer method. Callees
// take a pointer and must not retain it. The pointer does not escape, so
// the caller's parts do not escape either.
type offerPayload struct {
	// first is parts[0], or nil when parts is empty. The unfragmented path
	// reads it without a bounds check.
	first []byte

	parts [][]byte

	// length is the message length across all parts.
	length int

	// partIndex is the part that copyNext reads next.
	partIndex int

	// partOffset is the bytes already read from that part.
	partOffset int
}

// newOfferPayload returns the payload over parts. The caller keeps the
// backing array of parts alive for the offer.
func newOfferPayload(parts [][]byte) offerPayload {
	payload := offerPayload{parts: parts}
	for _, part := range parts {
		payload.length += len(part)
	}
	if len(parts) > 0 {
		payload.first = parts[0]
	}
	return payload
}

// copyNext fills dst with the next len(dst) message bytes. len(dst) must
// not exceed the unread message bytes.
func (payload *offerPayload) copyNext(dst []byte) {
	for len(dst) > 0 {
		part := payload.parts[payload.partIndex][payload.partOffset:]
		copied := copy(dst, part)
		dst = dst[copied:]
		if copied < len(part) {
			payload.partOffset += copied
			return
		}
		payload.partIndex++
		payload.partOffset = 0
	}
}

// commitOfferFrame fills frame with the next payload bytes, then publishes
// it. A non-nil supplier sets the reserved value before the release store
// of the frame length. frame spans one DATA frame with its header in
// place. cap(frame) equals len(frame), so the supplier cannot reach the
// frames that follow.
func commitOfferFrame(frame []byte, payload *offerPayload, supplier ReservedValueSupplier) {
	payload.copyNext(frame[logbuffer.DataHeaderLength:])

	header := atomicbuffer.Wrap(frame)
	if supplier != nil {
		header.PutInt64(logbuffer.OffsetReservedValue, supplier(frame))
	}

	//nolint:gosec // G115: a DATA frame is at most MaxUDPPayloadLength, 65504
	header.PutInt32Release(logbuffer.OffsetFrameLength, int32(len(frame)))
}
