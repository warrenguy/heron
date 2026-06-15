package aeron

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// Publication is a concurrent IPC publisher over a driver-mapped log buffer.
//
// Offer, TryClaim, Commit, Abort, and read accessors are safe for concurrent
// use. Close is idempotent but must not be invoked concurrently from multiple
// goroutines.
//
// Close tears down this registration via the bound conductor when present.
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
	// Upper bound for Offer payload length; payloads above maxPayloadLength
	// fragment until this limit, then return MaxPositionExceeded.
	maxMessageLength      int
	maxPossiblePosition   int64
	headerWriter          logbuffer.HeaderWriter
	fragmentedPartitionMu [logbuffer.PartitionCount]sync.Mutex

	counters               *cnc.CountersReader
	publicationLimit       cnc.CounterSlot
	channelStatusCounterID int32

	closed    atomic.Uint32
	conductor *conductor.Conductor
}

// ChannelEndpointStatusActive mirrors Java ChannelEndpointStatus.ACTIVE.
const ChannelEndpointStatusActive int64 = 1

func (pub *Publication) RegistrationID() int64 {
	return pub.registrationID
}

func (pub *Publication) SessionID() int32 {
	return pub.sessionID
}

func (pub *Publication) StreamID() int32 {
	return pub.streamID
}

// MaxPayloadLength returns the largest payload that fits in one DATA frame
// (publication MTU minus DataHeaderLength).
func (pub *Publication) MaxPayloadLength() int {
	return pub.maxPayloadLength
}

// Offer publishes a message from buffer. Unfragmented payloads fit one DATA
// frame; larger payloads up to maxMessageLength split into fragments.
//
// Returns a new stream position (>0) or a negative status code (see publication_codes.go).
func (pub *Publication) Offer(buffer []byte) int64 {
	return pub.OfferWithOffset(buffer, 0, len(buffer))
}

// OfferWithOffset publishes length bytes from buffer[offset:offset+length].
//
// Negative length or invalid offset/length bounds panic (Java IllegalArgumentException).
//
// length <= maxPayloadLength uses a single DATA frame; larger payloads up to
// maxMessageLength split across BEGIN/middle/END fragments (ConcurrentPublication.offer).
//
// Oversized payloads run the shared prelude first. When position < limit,
// length > maxMessageLength returns MaxPositionExceeded (-5).
//
// Prelude may return other negative codes (e.g. BackPressured) before that check.
func (pub *Publication) OfferWithOffset(buffer []byte, offset, length int) int64 {
	if pub.IsClosed() {
		return Closed
	}
	validateOfferSliceBounds(offset, length, len(buffer))
	// Prelude runs inside each append path (same layout as reserveMessageSpace).
	if length <= pub.maxPayloadLength {
		return pub.reserveAndAppend(buffer, offset, length)
	}
	return pub.appendFragmentedMessage(buffer, offset, length)
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
	if pub.IsClosed() {
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
//
// RawTailVolatile drives the read. Its error path is unreachable after valid
// construction because partition index is always in range; the branch maps to
// Closed for defensive parity with other auxiliary int64 accessors.
func (pub *Publication) Position() int64 {
	if pub.IsClosed() {
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
	if pub.IsClosed() {
		return Closed
	}

	if err := publicationCounterValueError(pub, pub.publicationLimit.CounterID()); err != nil {
		return Closed
	}
	return pub.publicationLimit.Get()
}

// IsConnected reports the log metadata is-connected flag (not channel-status counter).
func (pub *Publication) IsConnected() bool {
	return !pub.IsClosed() && logbuffer.IsConnectedVolatile(pub.meta) != 0
}

// ChannelStatus reads the driver channel-status counter.
//
// IPC publications omit the counter; NullCounterID reports ChannelEndpointStatusActive.
func (pub *Publication) ChannelStatus() (int64, error) {
	if pub.IsClosed() {
		return Closed, errors.New("aeron: publication is closed")
	}
	if pub.channelStatusCounterID == cnc.NullCounterID {
		return ChannelEndpointStatusActive, nil
	}
	return pub.counterValue(pub.channelStatusCounterID)
}

func (pub *Publication) counterValue(counterID int32) (int64, error) {
	if err := publicationCounterValueError(pub, counterID); err != nil {
		return 0, err
	}
	return pub.counters.CounterValue(counterID)
}

// AvailableWindow returns positionLimit - position, or a negative status code.
func (pub *Publication) AvailableWindow() int64 {
	if pub.IsClosed() {
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

// Close tears down this registration via the bound conductor.
//
// Idempotent: repeated calls return nil. Concurrent Close from multiple
// goroutines is unsupported. After close, Offer and TryClaim return Closed.
func (pub *Publication) Close() error {
	if pub.IsClosed() {
		return nil
	}
	if pub.conductor == nil {
		return pub.markClosed()
	}
	return pub.conductor.ClosePublication(pub)
}

func (pub *Publication) IsClosed() bool {
	return pub.closed.Load() != 0
}

func (pub *Publication) markClosed() error {
	pub.closed.Store(1)
	return nil
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
	// Claim reservation guarantees the full DataHeaderLength fits before payload access.
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
	if pub.IsClosed() {
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

type offerPrelude struct {
	termIndex  int
	termBuf    aeronatomic.AtomicBuffer
	tailOffset int
	status     int64
}

func (pub *Publication) offerPreludeState() (prelude offerPrelude, position, limit int64) {
	limit = pub.publicationLimit.Get()

	termLength := pub.termBufferLength

	termCount := logbuffer.ActiveTermCount(pub.meta)
	termIndex := logbuffer.IndexByTermCount(termCount)
	termBuf := pub.termBuffers[termIndex]
	tailOffset := logbuffer.OffsetTermTailCounter(termIndex)

	rawTail := pub.meta.GetInt64Volatile(tailOffset)
	termID := logbuffer.TermID(rawTail)
	termOffset := int(logbuffer.TermOffset(rawTail, int32(termLength)))

	if termCount != logbuffer.ComputeTermCount(termID, pub.initialTermID) {
		return offerPrelude{status: AdminAction}, 0, 0
	}

	position = logbuffer.ComputePosition(
		termID,
		int32(termOffset),
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	return offerPrelude{
		termIndex:  termIndex,
		termBuf:    termBuf,
		tailOffset: tailOffset,
	}, position, limit
}

// Java ConcurrentPublication.offer gates fragmented offers on position>=limit
// with backPressureStatus(position, payloadLength); reservationSpan is framed length only.
func (pub *Publication) runOfferPrelude(reservationSpan, messageLength int) offerPrelude {
	prelude, position, limit := pub.offerPreludeState()
	if prelude.status != 0 {
		return prelude
	}
	if position >= limit {
		return offerPrelude{status: backPressureStatus(pub, position, int64(messageLength))}
	}
	if position+int64(reservationSpan) >= pub.maxPossiblePosition {
		return offerPrelude{status: MaxPositionExceeded}
	}
	return prelude
}

func (pub *Publication) reserveTailFromPrelude(
	prelude offerPrelude,
	span int,
	frameLength int32,
) reserveOutcome {
	termLength := pub.termBufferLength
	rawTail := pub.meta.GetAndAddInt64(prelude.tailOffset, int64(span))
	termID := logbuffer.TermID(rawTail)
	termOffset := int(logbuffer.TermOffset(rawTail, int32(termLength)))
	resultingOffset := termOffset + span
	position := logbuffer.ComputePosition(
		termID,
		int32(resultingOffset),
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if resultingOffset > termLength {
		return reserveOutcome{
			termBuf:    prelude.termBuf,
			termOffset: termOffset,
			termID:     termID,
			position:   position,
			status:     pub.handleEndOfLog(prelude.termBuf, termLength, termID, termOffset, position),
		}
	}
	return reserveOutcome{
		termIndex:   prelude.termIndex,
		termBuf:     prelude.termBuf,
		termOffset:  termOffset,
		termID:      termID,
		position:    position,
		frameLength: frameLength,
	}
}

func (pub *Publication) reserveTailAfterPrelude(
	prelude offerPrelude,
	span int,
	frameLength int32,
) reserveOutcome {
	termCount := logbuffer.ActiveTermCount(pub.meta)
	if logbuffer.IndexByTermCount(termCount) != prelude.termIndex {
		return reserveOutcome{status: AdminAction}
	}
	rawTailVolatile := pub.meta.GetInt64Volatile(prelude.tailOffset)
	if logbuffer.ComputeTermCount(logbuffer.TermID(rawTailVolatile), pub.initialTermID) != termCount {
		return reserveOutcome{status: AdminAction}
	}

	return pub.reserveTailFromPrelude(prelude, span, frameLength)
}

// reserveMessageSpace checks publication limit, then reserves space via
// fetch-and-add on the active partition tail counter.
//
// Volatile read order: limit, ActiveTermCount, partition raw tail (Java
// ConcurrentPublication). No yield between tail read and GetAndAddInt64: rotation
// between them pins the wrong partition.
//
// Reservation always completes as DATA or PAD on termBuf/termOffset; callers retry on AdminAction.
//
// Prelude mirrors offerPreludeState but is not delegated to runOfferPrelude: tail read
// and FAA must stay in one function; splitting them adds non-inlinable calls on the
// unfragmented hot path.
func (pub *Publication) reserveMessageSpace(messageLength int) reserveOutcome {
	limit := pub.publicationLimit.Get()

	frameLength := int32(messageLength + logbuffer.DataHeaderLength)
	alignedLength := logbuffer.AlignFragmentChunkLength(messageLength)
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
	if position >= limit {
		return reserveOutcome{status: backPressureStatus(pub, position, int64(messageLength))}
	}
	if position+int64(alignedLength) >= pub.maxPossiblePosition {
		return reserveOutcome{status: MaxPositionExceeded}
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

func (pub *Publication) appendFragmentedMessage(buffer []byte, srcOffset, length int) int64 {
	// Java ConcurrentPublication.appendFragmentedMessage: one FAA for the full
	// framed span via computeFragmentedFrameLength; fragment writes follow in
	// the reserved block without further tail counter updates.
	framedLength := logbuffer.ComputeFragmentedFrameLength(length, pub.maxPayloadLength)
	prelude := pub.runOfferPrelude(framedLength, length)
	if prelude.status != 0 {
		return prelude.status
	}
	// After prelude, before reservation: oversized length returns -5 without FAA.
	if length > pub.maxMessageLength {
		return MaxPositionExceeded
	}

	partitionMu := &pub.fragmentedPartitionMu[prelude.termIndex]
	// Non-blocking: contention returns AdminAction for caller retry, same as rotation.
	if !partitionMu.TryLock() {
		return AdminAction
	}
	defer partitionMu.Unlock()

	outcome := pub.reserveTailAfterPrelude(prelude, framedLength, 0)
	if outcome.status != 0 {
		return outcome.status
	}
	if trackFragmentedReservationBegin(outcome.termIndex, outcome.termOffset) {
		defer trackFragmentedReservationEnd(outcome.termIndex, outcome.termOffset)
	}

	frameOffset := outcome.termOffset
	tailOffset := prelude.tailOffset
	reservationEnd := outcome.termOffset + framedLength
	remaining := length
	fullChunkAligned := logbuffer.AlignFragmentChunkLength(pub.maxPayloadLength)

	// Fragment loop must finish on outcome.termBuf: FAA already advanced the tail
	// for the full framed span; AdminAction mid-loop leaves uncommitted gaps.
	for remaining > 0 {
		// Partition recycle resets raw-tail term ID; pad remainder so tail-advanced
		// space stays contiguous and a new publisher cannot overlap this reservation.
		if logbuffer.TermID(pub.meta.GetInt64Volatile(tailOffset)) != outcome.termID {
			if frameOffset < reservationEnd {
				pub.commitTermPadding(
					outcome.termBuf,
					outcome.termID,
					frameOffset,
					int32(reservationEnd-frameOffset),
				)
			}
			return AdminAction
		}

		bytesToWrite := min(remaining, pub.maxPayloadLength)
		frameLength := int32(bytesToWrite + logbuffer.DataHeaderLength)
		alignedLength := fullChunkAligned
		if bytesToWrite != pub.maxPayloadLength {
			alignedLength = logbuffer.AlignFragmentChunkLength(bytesToWrite)
		}

		var fragFlags uint8
		if remaining == length {
			fragFlags = logbuffer.BeginFlag
		}
		if bytesToWrite == remaining {
			fragFlags |= logbuffer.EndFlag
		}

		trackFragmentedWrite(outcome.termIndex, frameOffset, true)

		pub.headerWriter.WriteWithFlags(
			outcome.termBuf,
			frameOffset,
			frameLength,
			outcome.termID,
			fragFlags,
		)
		payloadOffset := srcOffset + (length - remaining)
		outcome.termBuf.PutBytes(
			frameOffset+logbuffer.OffsetFramePayload,
			buffer[payloadOffset:payloadOffset+bytesToWrite],
		)
		logbuffer.PutFrameLengthOrdered(outcome.termBuf, frameOffset, frameLength)

		trackFragmentedWrite(outcome.termIndex, frameOffset, false)

		frameOffset += alignedLength
		remaining -= bytesToWrite
	}

	return outcome.position
}

func (pub *Publication) commitTermPadding(
	termBuf aeronatomic.AtomicBuffer,
	termID int32,
	fromOffset int,
	paddingLength int32,
) {
	if paddingLength <= 0 {
		return
	}
	if paddingLength >= logbuffer.DataHeaderLength {
		pub.headerWriter.Write(termBuf, fromOffset, paddingLength, termID)
		logbuffer.PutFrameType(termBuf, fromOffset, logbuffer.HdrTypePad)
		logbuffer.PutFrameLengthOrdered(termBuf, fromOffset, paddingLength)
		return
	}
	logbuffer.CommitMinimalTermEndPadding(termBuf, fromOffset, paddingLength)
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
		outcome.termOffset+logbuffer.OffsetFramePayload,
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
		// Term-end padding seeds session/stream and default header fields before the
		// PAD type commit; PAD-only writes (as in Abort) would omit them.
		pub.commitTermPadding(termBuf, termID, termOffset, int32(termLength-termOffset))
	}

	if position >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}

	termCount := logbuffer.ComputeTermCount(termID, pub.initialTermID)
	_ = logbuffer.RotateLog(pub.meta, termCount, termID)
	return AdminAction
}
