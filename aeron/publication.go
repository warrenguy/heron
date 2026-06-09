package aeron

import (
	"errors"
	"fmt"
	"sync/atomic"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// Publication is a concurrent IPC publisher over a driver-mapped log buffer.
// All methods are safe for concurrent use. Close marks the handle closed only;
// the conductor retains mapped-log lifetime after Close.
type Publication struct {
	registrationID         int64
	originalRegistrationID int64
	sessionID              int32
	streamID               int32
	channel                string

	log                 *logbuffer.MappedLogBuffer
	meta                aeronatomic.AtomicBuffer
	termBuffers         [logbuffer.PartitionCount]aeronatomic.AtomicBuffer
	termBufferLength    int
	positionBitsToShift int

	initialTermID    int32
	maxPayloadLength int
	// Reserved for fragmented offer.
	maxMessageLength    int
	maxPossiblePosition int64
	headerWriter        logbuffer.HeaderWriter

	counters                  cnc.CountersView
	publicationLimitCounterID int32
	channelStatusCounterID    int32

	closed atomic.Uint32
}

// ChannelEndpointStatusActive mirrors Java ChannelEndpointStatus.ACTIVE.
const ChannelEndpointStatusActive int64 = 1

// Offer publishes an unfragmented message from buffer. Returns a new stream
// position (>0) or a negative status code (see publication_codes.go).
func (pub *Publication) Offer(buffer []byte) int64 {
	return pub.OfferWithOffset(buffer, 0, len(buffer))
}

// OfferWithOffset publishes length bytes from buffer[offset:offset+length].
// Negative length or invalid offset/length bounds panic (Java IllegalArgumentException).
// length > maxPayloadLength returns UnsupportedMessageLength (-6).
func (pub *Publication) OfferWithOffset(buffer []byte, offset, length int) int64 {
	if pub.isClosed() {
		return Closed
	}
	validateOfferSliceBounds(offset, length, len(buffer))
	if length > pub.maxPayloadLength {
		return UnsupportedMessageLength
	}
	return pub.reserveAndAppend(buffer, offset, length)
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
// A typical retry loop returns on err != nil, returns on success or a terminal
// status, and spins only while position == AdminAction.
func (pub *Publication) TryClaim(
	length int,
) (termIndex, frameOffset int, position int64, err error) {
	if pub.isClosed() {
		return 0, 0, Closed, nil
	}
	if length < 0 {
		return 0, 0, 0, fmt.Errorf("aeron: invalid message length=%d", length)
	}
	if length > pub.maxPayloadLength {
		return 0, 0, 0, fmt.Errorf(
			"aeron: claim length %d exceeds maxPayloadLength=%d",
			length,
			pub.maxPayloadLength,
		)
	}

	termIndex, frameOffset, position, status := pub.reserveClaim(length)
	if status != 0 {
		return 0, 0, status, nil
	}
	return termIndex, frameOffset, position, nil
}

// Commit publishes an in-flight claim on the termIndex partition from TryClaim.
func (pub *Publication) Commit(termIndex, frameOffset int) error {
	return pub.finalizeInFlightClaim(termIndex, frameOffset, false)
}

// Abort retypes an in-flight claim as PAD on the termIndex partition from TryClaim.
func (pub *Publication) Abort(termIndex, frameOffset int) error {
	return pub.finalizeInFlightClaim(termIndex, frameOffset, true)
}

// Position returns the publisher stream position or Closed (-4) when closed.
// RawTailVolatile drives the read. Its error path is unreachable after valid
// construction because partition index is always in range; the branch maps to
// Closed for defensive parity with other auxiliary int64 accessors.
func (pub *Publication) Position() int64 {
	if pub.isClosed() {
		return Closed
	}

	rawTail, err := logbuffer.RawTailVolatile(pub.meta)
	if err != nil {
		return Closed
	}

	termID := logbuffer.TermID(rawTail)
	termOffset := logbuffer.TermOffset(rawTail, int32(pub.termBufferLength))
	return logbuffer.ComputePosition(
		termID,
		termOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)
}

// PositionLimit returns the publication-limit counter or Closed (-4).
func (pub *Publication) PositionLimit() int64 {
	if pub.isClosed() {
		return Closed
	}

	limit, err := pub.counters.GetCounterValue(pub.publicationLimitCounterID)
	if err != nil {
		return Closed
	}
	return limit
}

// IsConnected reports the log metadata is-connected flag (not channel-status counter).
func (pub *Publication) IsConnected() bool {
	return !pub.isClosed() && logbuffer.IsConnectedVolatile(pub.meta) != 0
}

// ChannelStatus reads the driver channel-status counter. IPC publications omit
// the counter; NullCounterID reports ChannelEndpointStatusActive.
func (pub *Publication) ChannelStatus() (int64, error) {
	if pub.isClosed() {
		return Closed, errors.New("aeron: publication is closed")
	}
	if pub.channelStatusCounterID == cnc.NullCounterID {
		return ChannelEndpointStatusActive, nil
	}
	return pub.counters.GetCounterValue(pub.channelStatusCounterID)
}

// AvailableWindow returns positionLimit - position, or a negative status code.
func (pub *Publication) AvailableWindow() int64 {
	if pub.isClosed() {
		return Closed
	}

	positionLimit := pub.PositionLimit()
	if positionLimit < 0 {
		return positionLimit
	}

	position := pub.Position()
	if position < 0 {
		return position
	}
	return positionLimit - position
}

// Close marks the publication closed. Does not unmap the log buffer.
func (pub *Publication) Close() error {
	pub.closed.Store(1)
	return nil
}

func (pub *Publication) isClosed() bool {
	return pub.closed.Load() != 0
}

func validateOfferSliceBounds(offset, length, bufferLength int) {
	if length < 0 {
		panic(fmt.Sprintf("aeron: invalid message length=%d", length))
	}
	if offset < 0 {
		panic(fmt.Sprintf("aeron: invalid offer offset=%d", offset))
	}
	if offset > bufferLength {
		panic(fmt.Sprintf("aeron: invalid offer offset=%d buffer=%d", offset, bufferLength))
	}
	if length > bufferLength-offset {
		panic(fmt.Sprintf(
			"aeron: invalid offer offset=%d length=%d buffer=%d",
			offset,
			length,
			bufferLength,
		))
	}
}

func validateTermIndex(termIndex int) error {
	if err := logbuffer.ValidateTermIndex(termIndex, "term index"); err != nil {
		return fmt.Errorf("aeron: invalid term index %d", termIndex)
	}
	return nil
}

func validateFrameOffset(termBuf aeronatomic.AtomicBuffer, frameOffset int) error {
	if frameOffset < 0 || frameOffset+logbuffer.DataHeaderLength > termBuf.Capacity() {
		return fmt.Errorf("aeron: invalid frame offset %d", frameOffset)
	}
	return nil
}

func (pub *Publication) finalizeInFlightClaim(
	termIndex int,
	frameOffset int,
	pad bool,
) error {
	termBuf, absLength, err := pub.loadInFlightClaim(termIndex, frameOffset)
	if err != nil {
		return err
	}
	if pad {
		logbuffer.PutFrameType(termBuf, frameOffset, logbuffer.HdrTypePad)
	}
	logbuffer.PutFrameLengthOrdered(termBuf, frameOffset, absLength)
	return nil
}

func (pub *Publication) loadInFlightClaim(
	termIndex int,
	frameOffset int,
) (aeronatomic.AtomicBuffer, int32, error) {
	if pub.isClosed() {
		return aeronatomic.AtomicBuffer{}, 0, errors.New("aeron: publication is closed")
	}
	if err := validateTermIndex(termIndex); err != nil {
		return aeronatomic.AtomicBuffer{}, 0, err
	}

	termBuf := pub.termBuffers[termIndex]
	if err := validateFrameOffset(termBuf, frameOffset); err != nil {
		return aeronatomic.AtomicBuffer{}, 0, err
	}
	frameLength := termBuf.GetInt32Volatile(frameOffset)
	if frameLength >= 0 {
		return aeronatomic.AtomicBuffer{}, 0, errors.New("aeron: claimed space not in flight")
	}
	return termBuf, -frameLength, nil
}

type reserveOutcome struct {
	termIndex   int
	termBuf     aeronatomic.AtomicBuffer
	termOffset  int
	termID      int32
	position    int64
	frameLength int32
	status      int64
}

// reserveMessageSpace checks publication limit, then reserves space via
// fetch-and-add on the active partition tail counter. Read ActiveTermCount and
// partition raw tail separately, not RawTailVolatile, so limit and reservation
// share one term/offset pair (Java ConcurrentPublication ordering). No yield
// between that read and GetAndAddInt64: rotation between them pins the wrong
// partition. Reservation always completes as DATA or PAD on termBuf/termOffset;
// callers retry on AdminAction.
func (pub *Publication) reserveMessageSpace(messageLength int) reserveOutcome {
	limit, err := pub.counters.GetCounterValue(pub.publicationLimitCounterID)
	if err != nil {
		return reserveOutcome{status: Closed}
	}

	frameLength := int32(messageLength + logbuffer.DataHeaderLength)
	alignedLength := logbuffer.AlignFrameLength(int(frameLength))
	termLength := pub.termBufferLength

	termCount := logbuffer.ActiveTermCount(pub.meta)
	termIndex := logbuffer.IndexByTermCount(termCount)
	termBuf := pub.termBuffers[termIndex]
	tailOffset := logbuffer.OffsetTermTailCounter(termIndex)

	rawTail := pub.meta.GetInt64Volatile(tailOffset)
	termID := logbuffer.TermID(rawTail)
	termOffset := int(logbuffer.TermOffset(rawTail, int32(termLength)))

	if termCount != logbuffer.ComputeTermCount(termID, pub.initialTermID) {
		return reserveOutcome{status: AdminAction}
	}

	position := logbuffer.ComputePosition(
		termID,
		int32(termOffset),
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if position+int64(alignedLength) >= pub.maxPossiblePosition {
		return reserveOutcome{status: MaxPositionExceeded}
	}
	if position >= limit {
		return reserveOutcome{status: backPressureStatus(pub, position, int64(alignedLength))}
	}

	rawTail = pub.meta.GetAndAddInt64(tailOffset, int64(alignedLength))
	termID = logbuffer.TermID(rawTail)
	termOffset = int(logbuffer.TermOffset(rawTail, int32(termLength)))

	resultingOffset := termOffset + alignedLength
	newPosition := logbuffer.ComputePosition(
		termID,
		int32(resultingOffset),
		pub.positionBitsToShift,
		pub.initialTermID,
	)

	if resultingOffset > termLength {
		return reserveOutcome{
			termBuf:    termBuf,
			termOffset: termOffset,
			termID:     termID,
			position:   newPosition,
			status:     pub.handleEndOfLog(termBuf, termLength, termID, termOffset, newPosition),
		}
	}

	return reserveOutcome{
		termIndex:   termIndex,
		termBuf:     termBuf,
		termOffset:  termOffset,
		termID:      termID,
		position:    newPosition,
		frameLength: frameLength,
	}
}

func (pub *Publication) reserveWithHeader(length int) (reserveOutcome, bool) {
	outcome := pub.reserveMessageSpace(length)
	if outcome.status != 0 {
		return outcome, false
	}

	pub.headerWriter.Write(outcome.termBuf, outcome.termOffset, outcome.frameLength, outcome.termID)
	return outcome, true
}

func (pub *Publication) reserveAndAppend(buffer []byte, srcOffset, length int) int64 {
	outcome, ok := pub.reserveWithHeader(length)
	if !ok {
		return outcome.status
	}

	outcome.termBuf.PutBytes(
		outcome.termOffset+logbuffer.DataHeaderLength,
		buffer[srcOffset:srcOffset+length],
	)
	logbuffer.PutFrameLengthOrdered(outcome.termBuf, outcome.termOffset, outcome.frameLength)
	return outcome.position
}

func (pub *Publication) reserveClaim(length int) (
	termIndex int,
	frameOffset int,
	position int64,
	status int64,
) {
	outcome, ok := pub.reserveWithHeader(length)
	if !ok {
		return 0, 0, 0, outcome.status
	}
	return outcome.termIndex, outcome.termOffset, outcome.position, 0
}

func (pub *Publication) handleEndOfLog(
	termBuf aeronatomic.AtomicBuffer,
	termLength int,
	termID int32,
	termOffset int,
	position int64,
) int64 {
	if termOffset < termLength {
		paddingLength := int32(termLength - termOffset)
		if paddingLength >= logbuffer.DataHeaderLength {
			// Term-end padding seeds session/stream and default header fields before the
			// PAD type commit; PAD-only writes (as in Abort) would omit them.
			pub.headerWriter.Write(termBuf, termOffset, paddingLength, termID)
			logbuffer.PutFrameType(termBuf, termOffset, logbuffer.HdrTypePad)
			logbuffer.PutFrameLengthOrdered(termBuf, termOffset, paddingLength)
		} else {
			logbuffer.CommitMinimalTermEndPadding(termBuf, termOffset, paddingLength)
		}
	}

	if position >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}

	termCount := logbuffer.ComputeTermCount(termID, pub.initialTermID)
	_ = logbuffer.RotateLog(pub.meta, termCount, termID)
	return AdminAction
}
