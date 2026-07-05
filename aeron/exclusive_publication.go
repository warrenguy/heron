package aeron

import (
	"errors"
	"fmt"
	stdatomic "sync/atomic"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

// ExclusivePublication is a single-writer IPC publisher over a driver-mapped log buffer.
//
// Offer, OfferWithOffset, TryClaim, Commit, and Abort are not thread-safe; concurrent
// calls from multiple goroutines are undefined (Java ExclusivePublication parity).
//
// The offer hot path uses locally cached tail state (activePartitionIndex, termID,
// termOffset, termBeginPosition) initialized at construct; no per-offer meta tail read.
//
// Position reads volatile meta raw tail, not the offer-path cache, so concurrent
// observers are race-safe while the single writer updates cache and meta tail via
// PutInt64Release (Java ExclusivePublication reads cache only).
//
// IsConnected, Close, and limit accessors are safe concurrent with offers from other
// goroutines.
//
// Close tears down this registration via the bound conductor when present.
type ExclusivePublication struct {
	registrationID         int64
	originalRegistrationID int64
	sessionID              int32
	streamID               int32
	channel                string

	log                 *logbuffer.MappedLogBuffer
	meta                atomic.AtomicBuffer
	termBuffers         [logbuffer.PartitionCount]atomic.AtomicBuffer
	termBufferLength    int
	positionBitsToShift int

	initialTermID       int32
	maxPayloadLength    int
	maxMessageLength    int
	maxPossiblePosition int64
	headerWriter        logbuffer.HeaderWriter

	activePartitionIndex int
	termID               int32
	termOffset           int
	termBeginPosition    int64

	counters               *cnc.CountersReader
	publicationLimit       cnc.CounterSlot
	channelStatusCounterID int32

	closed    stdatomic.Uint32
	conductor *conductor.Conductor
}

// RegistrationID returns the driver registration id for this publication.
func (ep *ExclusivePublication) RegistrationID() int64 {
	return ep.registrationID
}

// SessionID returns the session id assigned by the driver.
func (ep *ExclusivePublication) SessionID() int32 {
	return ep.sessionID
}

// StreamID returns the stream id for this publication.
func (ep *ExclusivePublication) StreamID() int32 {
	return ep.streamID
}

// MaxPayloadLength returns the largest payload that fits in one DATA frame
// (publication MTU minus DataHeaderLength).
func (ep *ExclusivePublication) MaxPayloadLength() int {
	return ep.maxPayloadLength
}

// MaxMessageLength returns the largest total message length Offer will accept
// before returning MaxPositionExceeded.
func (ep *ExclusivePublication) MaxMessageLength() int {
	return ep.maxMessageLength
}

// Offer publishes a message from buffer. Fragmented Offer (> maxPayloadLength) is
// unsupported; payloads in (maxPayloadLength, maxMessageLength] return
// UnsupportedMessageLength (-6).
//
// Returns a new stream position (>0) or a negative status code (see publication_codes.go).
//
// See also PositionCodeName, IsOfferRetriable.
func (ep *ExclusivePublication) Offer(buffer []byte) int64 {
	return ep.OfferWithOffset(buffer, 0, len(buffer))
}

// OfferWithOffset publishes length bytes from buffer[offset:offset+length].
//
// Negative length or invalid offset/length bounds panic (Java IllegalArgumentException).
//
// length <= maxPayloadLength uses a single DATA frame. Payloads above maxPayloadLength
// up to maxMessageLength return UnsupportedMessageLength (-6) until fragmented exclusive
// Offer is implemented. length > maxMessageLength returns MaxPositionExceeded (-5).
//
// See also PositionCodeName, IsOfferRetriable.
func (ep *ExclusivePublication) OfferWithOffset(buffer []byte, offset, length int) int64 {
	if ep.IsClosed() {
		return Closed
	}
	validateOfferSliceBounds(offset, length, len(buffer))

	if length > ep.maxMessageLength {
		return MaxPositionExceeded
	}
	if length > ep.maxPayloadLength {
		return UnsupportedMessageLength
	}

	alignedLength := logbuffer.AlignFragmentChunkLength(length)
	if status := ep.checkPublicationLimit(alignedLength); status != 0 {
		return status
	}
	return ep.appendUnfragmentedMessage(buffer, offset, length, alignedLength)
}

// TryClaim reserves aligned space for an unfragmented message.
//
// Caller contract:
//
//  1. If err != nil, length is invalid (negative or exceeds maxPayloadLength);
//     do not retry (Java IllegalArgumentException).
//  2. If position > 0, claim succeeded; fill the reserved frame, then Commit or
//     Abort using termIndex and frameOffset. termIndex pins the partition until
//     BufferClaim consolidates the API; Commit/Abort must use the values returned
//     here, not the currently active term.
//  3. If position < 0, an operational status was returned (publication_codes.go):
//     BackPressured and NotConnected - retry after an idle strategy;
//     AdminAction - retry immediately in a tight loop;
//     Closed and MaxPositionExceeded - stop publishing on this handle.
//
// See also PositionCodeName, IsOfferRetriable.
//
// A typical retry loop returns on err != nil, returns on success or a terminal
// status. For transport-level retries use IsOfferRetriable with DoWork between
// attempts; after BackPressured or NotConnected, pace with an idle strategy.
// Only AdminAction warrants an inner tight spin before returning to the outer loop.
//
// length > maxPayloadLength returns a Go error, not UnsupportedMessageLength (-6).
func (ep *ExclusivePublication) TryClaim(
	length int,
) (termIndex, frameOffset int, position int64, err error) {
	if ep.IsClosed() {
		return 0, 0, Closed, nil
	}
	if length < 0 {
		return 0, 0, 0, fmt.Errorf("aeron: invalid message length=%d", length)
	}
	if length > ep.maxPayloadLength {
		return 0, 0, 0, fmt.Errorf(
			"aeron: claim length %d exceeds maxPayloadLength=%d",
			length,
			ep.maxPayloadLength,
		)
	}

	alignedLength := logbuffer.AlignFragmentChunkLength(length)
	if status := ep.checkPublicationLimit(alignedLength); status != 0 {
		return 0, 0, status, nil
	}

	termIndex, frameOffset, position, status := ep.claim(length, alignedLength)
	if status != 0 {
		return 0, 0, status, nil
	}
	return termIndex, frameOffset, position, nil
}

// Commit publishes an in-flight claim on the termIndex partition from TryClaim.
func (ep *ExclusivePublication) Commit(termIndex, frameOffset int) error {
	return ep.finalizeInFlightClaim(termIndex, frameOffset, false)
}

// Abort retypes an in-flight claim as PAD on the termIndex partition from TryClaim.
func (ep *ExclusivePublication) Abort(termIndex, frameOffset int) error {
	return ep.finalizeInFlightClaim(termIndex, frameOffset, true)
}

// TermBuffer returns the log partition term buffer for in-flight TryClaim writes.
//
// termIndex is the partition index returned by TryClaim.
//
// Valid only until Commit or Abort finalizes the claim on that partition.
//
// Panics when termIndex is out of range.
func (ep *ExclusivePublication) TermBuffer(termIndex int) atomic.AtomicBuffer {
	return ep.termBuffers[termIndex]
}

// Position returns the publisher stream position or Closed (-4) when closed.
//
// Volatile read of the active partition raw tail from log metadata (RawTailVolatile),
// not the offer-path cache. Concurrent callers are safe while the single writer
// advances cache and meta tail via PutInt64Release on Offer/TryClaim.
func (ep *ExclusivePublication) Position() int64 {
	if ep.IsClosed() {
		return Closed
	}

	rawTail, err := logbuffer.RawTailVolatile(ep.meta)
	if err != nil {
		return Closed
	}

	termID := logbuffer.TermID(rawTail)
	termOffset := logbuffer.TermOffset(rawTail, int32(ep.termBufferLength))
	return logbuffer.ComputePosition(
		termID,
		termOffset,
		ep.positionBitsToShift,
		ep.initialTermID,
	)
}

// PositionLimit returns the publication-limit counter or Closed (-4).
func (ep *ExclusivePublication) PositionLimit() int64 {
	if ep.IsClosed() {
		return Closed
	}

	if err := exclusivePublicationCounterValueError(ep, ep.publicationLimit.CounterID()); err != nil {
		return Closed
	}
	return ep.publicationLimit.Get()
}

// IsConnected reports the log metadata is-connected flag (not channel-status counter).
func (ep *ExclusivePublication) IsConnected() bool {
	return !ep.IsClosed() && logbuffer.IsConnectedVolatile(ep.meta) != 0
}

// ChannelStatus reads the driver channel-status counter.
//
// IPC publications omit the counter; NullCounterID reports ChannelEndpointStatusActive.
func (ep *ExclusivePublication) ChannelStatus() (int64, error) {
	if ep.IsClosed() {
		return Closed, errors.New("aeron: exclusive publication is closed")
	}
	if ep.channelStatusCounterID == cnc.NullCounterID {
		return ChannelEndpointStatusActive, nil
	}
	return ep.counterValue(ep.channelStatusCounterID)
}

func (ep *ExclusivePublication) counterValue(counterID int32) (int64, error) {
	if err := exclusivePublicationCounterValueError(ep, counterID); err != nil {
		return 0, err
	}
	return ep.counters.CounterValue(counterID)
}

// AvailableWindow returns positionLimit - position, or a negative status code.
func (ep *ExclusivePublication) AvailableWindow() int64 {
	if ep.IsClosed() {
		return Closed
	}

	positionLimit := ep.PositionLimit()
	if positionLimit < 0 {
		return positionLimit
	}

	position := ep.Position()
	if position < 0 {
		return position
	}
	return positionLimit - position
}

// Close tears down this registration via the bound conductor.
//
// Idempotent: repeated calls return nil. Concurrent Close from multiple
// goroutines is unsupported. After close, Offer and TryClaim return Closed.
func (ep *ExclusivePublication) Close() error {
	if ep.IsClosed() {
		return nil
	}
	if ep.conductor == nil {
		return ep.markClosed()
	}
	return ep.conductor.ClosePublication(ep)
}

func (ep *ExclusivePublication) IsClosed() bool {
	return ep.closed.Load() != 0
}

func (ep *ExclusivePublication) markClosed() error {
	ep.closed.Store(1)
	return nil
}

func (ep *ExclusivePublication) checkPublicationLimit(alignedLength int) int64 {
	limit := ep.publicationLimit.Get()
	position := ep.termBeginPosition + int64(ep.termOffset)
	span := int64(alignedLength)

	if position >= limit {
		return backPressureStatusForLog(ep.meta, ep.maxPossiblePosition, position, span)
	}

	if position+span >= ep.maxPossiblePosition {
		return MaxPositionExceeded
	}

	return 0
}

type exclusiveReservation struct {
	termBuf          atomic.AtomicBuffer
	termID           int32
	startOffset      int
	resultingOffset  int
	reservedPosition int64
	status           int64
}

func (ep *ExclusivePublication) reserveExclusiveSpace(alignedLength int) exclusiveReservation {
	termLength := ep.termBufferLength
	tailOffset := logbuffer.OffsetTermTailCounter(ep.activePartitionIndex)
	termBuf := ep.termBuffers[ep.activePartitionIndex]
	termID := ep.termID
	startOffset := ep.termOffset

	resultingOffset := startOffset + alignedLength
	ep.meta.PutInt64Release(tailOffset, logbuffer.PackRawTail(termID, int32(resultingOffset)))

	reservedPosition := ep.termBeginPosition + int64(resultingOffset)
	if resultingOffset > termLength {
		endStatus := ep.handleEndOfLog(termBuf, termLength, termID, startOffset, reservedPosition)
		if endStatus == MaxPositionExceeded {
			return exclusiveReservation{status: MaxPositionExceeded}
		}
		ep.rotateTerm()
		return exclusiveReservation{status: AdminAction}
	}

	return exclusiveReservation{
		termBuf:          termBuf,
		termID:           termID,
		startOffset:      startOffset,
		resultingOffset:  resultingOffset,
		reservedPosition: reservedPosition,
	}
}

func (ep *ExclusivePublication) appendUnfragmentedMessage(
	buffer []byte,
	srcOffset int,
	length int,
	alignedLength int,
) int64 {
	frameLength := int32(length + logbuffer.DataHeaderLength)

	res := ep.reserveExclusiveSpace(alignedLength)
	if res.status != 0 {
		return res.status
	}

	ep.headerWriter.Write(res.termBuf, res.startOffset, frameLength, res.termID)
	res.termBuf.PutBytes(
		res.startOffset+logbuffer.OffsetFramePayload,
		buffer[srcOffset:srcOffset+length],
	)
	logbuffer.PutFrameLengthOrdered(res.termBuf, res.startOffset, frameLength)

	ep.termOffset = res.resultingOffset
	return res.reservedPosition
}

func (ep *ExclusivePublication) claim(
	length int,
	alignedLength int,
) (termIndex, frameOffset int, position, status int64) {
	frameLength := int32(length + logbuffer.DataHeaderLength)

	res := ep.reserveExclusiveSpace(alignedLength)
	if res.status != 0 {
		return 0, 0, 0, res.status
	}

	ep.headerWriter.Write(res.termBuf, res.startOffset, frameLength, res.termID)
	ep.termOffset = res.resultingOffset
	return ep.activePartitionIndex, res.startOffset, res.reservedPosition, 0
}

func (ep *ExclusivePublication) finalizeInFlightClaim(
	termIndex int,
	frameOffset int,
	pad bool,
) error {
	termBuf, absLength, err := ep.loadInFlightClaim(termIndex, frameOffset)
	if err != nil {
		return err
	}
	if pad {
		logbuffer.PutFrameType(termBuf, frameOffset, logbuffer.HdrTypePad)
	}
	logbuffer.PutFrameLengthOrdered(termBuf, frameOffset, absLength)
	return nil
}

func (ep *ExclusivePublication) loadInFlightClaim(
	termIndex int,
	frameOffset int,
) (atomic.AtomicBuffer, int32, error) {
	if ep.IsClosed() {
		return atomic.AtomicBuffer{}, 0, errors.New("aeron: exclusive publication is closed")
	}
	return loadInFlightClaimFrame(&ep.termBuffers, termIndex, frameOffset)
}

// rotateTerm advances the cached exclusive tail and publishes ordered meta stores.
// Single-writer rotation avoids RotateLog's CAS loop, which is concurrent-publication semantics.
func (ep *ExclusivePublication) rotateTerm() {
	nextIndex := (ep.activePartitionIndex + 1) % logbuffer.PartitionCount
	nextTermID := ep.termID + 1
	ep.activePartitionIndex = nextIndex
	ep.termOffset = 0
	ep.termID = nextTermID
	ep.termBeginPosition += int64(ep.termBufferLength)
	termCount := nextTermID - ep.initialTermID

	_ = logbuffer.PutRawTail(ep.meta, nextIndex, logbuffer.PackRawTail(nextTermID, 0))
	ep.meta.PutInt32Release(logbuffer.OffsetActiveTermCount, termCount)
}

func (ep *ExclusivePublication) handleEndOfLog(
	termBuf atomic.AtomicBuffer,
	termLength int,
	termID int32,
	termOffset int,
	reservedPosition int64,
) int64 {
	if termOffset < termLength {
		paddingLength := int32(termLength - termOffset)
		commitPublicationTermPadding(ep.headerWriter, termBuf, termID, termOffset, paddingLength)
	}

	if reservedPosition >= ep.maxPossiblePosition {
		return MaxPositionExceeded
	}

	return AdminAction
}
