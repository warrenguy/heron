package aeron

import (
	"sync/atomic"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
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
	meta                atomicbuffer.AtomicBuffer
	termBuffers         [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer
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

	// activeTermBuffer and activeTailCounter cache the active partition.
	// bindActivePartition sets them and checks the tail counter bounds.
	activeTermBuffer  []byte
	activeTailCounter *int64

	counters               *cnc.CountersReader
	publicationLimit       cnc.CounterSlot
	channelStatusCounterID int32

	closed    atomic.Uint32
	conductor *conductor.Conductor
	client    *Client
}

// RegistrationID returns the correlation id of the add command for this
// publication. Client.ExclusivePublication looks the publication up by this
// id. Close sends this id to the driver.
func (ep *ExclusivePublication) RegistrationID() int64 {
	return ep.registrationID
}

// OriginalRegistrationID returns the registration id of the driver
// publication. Matches Java Publication.originalRegistrationId.
//
// Behaviour:
//   - Each exclusive add creates its own driver publication.
//   - The driver gives that driver publication the add correlation id, so
//     OriginalRegistrationID equals RegistrationID. ExclusivePublication does
//     not check this.
//   - The driver labels the publication limit counter with this id.
func (ep *ExclusivePublication) OriginalRegistrationID() int64 {
	return ep.originalRegistrationID
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

// MaxMessageLength returns the largest message Offer fragments. Offer returns
// UnsupportedMessageLength for a longer message that does not fit one frame.
func (ep *ExclusivePublication) MaxMessageLength() int {
	return ep.maxMessageLength
}

// Offer publishes a message from buffer. Unfragmented payloads fit one DATA
// frame. Larger payloads up to maxMessageLength split into fragments.
//
// Returns a new stream position (>0) or a negative status code (see
// publication_codes.go).
//
// See also PositionCodeName, IsOfferRetriable.
func (ep *ExclusivePublication) Offer(buffer []byte) int64 {
	return ep.OfferWithOffset(buffer, 0, len(buffer))
}

// OfferWithOffset publishes length bytes from buffer[offset:offset+length].
//
// Negative length or invalid offset/length bounds panic (Java
// IllegalArgumentException).
//
// length <= maxPayloadLength uses a single DATA frame. Larger payloads up to
// maxMessageLength split across BEGIN, middle, and END fragments.
//
// Any other length returns UnsupportedMessageLength without a limit read.
//
// See also PositionCodeName, IsOfferRetriable.
func (ep *ExclusivePublication) OfferWithOffset(buffer []byte, offset, length int) int64 {
	// The unfragmented success path stays in this one function and does not
	// call a helper.
	if ep.IsClosed() {
		return Closed
	}

	// The unsigned compares reject negative values too. The panic stays in this
	// function, so the compiler can drop the payload slice checks below.
	if uint(offset) > uint(len(buffer)) || uint(length) > uint(len(buffer[offset:])) {
		panic(invalidOfferSliceMessage(offset, length, len(buffer)))
	}

	if length > ep.maxPayloadLength {
		if length > ep.maxMessageLength {
			return UnsupportedMessageLength
		}
		return ep.appendFragmentedMessage(buffer, offset, length)
	}

	alignedLength := logbuffer.AlignFragmentChunkLength(length)

	limit := ep.publicationLimit.Get()
	position := ep.termBeginPosition + int64(ep.termOffset)
	if position >= limit {
		return backPressureStatusForLog(
			ep.meta,
			ep.maxPossiblePosition,
			position,
			int64(alignedLength),
		)
	}
	if position+int64(alignedLength) >= ep.maxPossiblePosition {
		return MaxPositionExceeded
	}

	termID := ep.termID
	termOffset := ep.termOffset
	resultingOffset := termOffset + alignedLength

	//nolint:gosec // G115: termOffset under TermMaxLength, 1<<30, plus one aligned frame
	rawTail := logbuffer.PackRawTail(termID, int32(resultingOffset))
	atomicbuffer.StoreInt64Release(ep.activeTailCounter, rawTail)

	position = ep.termBeginPosition + int64(resultingOffset)
	if resultingOffset > ep.termBufferLength {
		return ep.rotateAtTermEnd(termOffset, position)
	}

	// One slice check covers every header and payload store below.
	payload := buffer[offset:][:length]
	frame := ep.activeTermBuffer[termOffset : termOffset+logbuffer.DataHeaderLength+len(payload)]

	frameLength := dataFrameLength(length)
	ep.headerWriter.WriteHeader(frame, termOffset, frameLength, termID)
	copy(frame[logbuffer.DataHeaderLength:], payload)
	atomicbuffer.Wrap(frame).PutInt32Release(logbuffer.OffsetFrameLength, frameLength)

	ep.termOffset = resultingOffset
	return position
}

// TryClaim reserves aligned space for an unfragmented message.
//
// Caller contract:
//
//  1. err != nil: length is invalid (negative or exceeds maxPayloadLength).
//     Must not retry (Java IllegalArgumentException).
//  2. position > 0: claim succeeded. Fill the reserved frame, then Commit or
//     Abort with the returned termIndex and frameOffset. Commit/Abort must use
//     these values, not the currently active term. termIndex selects the claim
//     partition until BufferClaim consolidates the API.
//  3. position < 0: operational status (publication_codes.go):
//     - BackPressured, NotConnected: retry after an idle strategy
//     - AdminAction: retry immediately in a tight loop
//     - Closed, MaxPositionExceeded: stop publishing on this handle
//
// See also PositionCodeName, IsOfferRetriable.
//
// Typical retry loop: return on err != nil; return on success or a terminal
// status. For transport-level retries use IsOfferRetriable with DoWork between
// attempts. After BackPressured or NotConnected, pace with an idle strategy.
// Only AdminAction may use an inner tight spin before the outer loop.
//
// length > maxPayloadLength returns a Go error, not UnsupportedMessageLength (-6).
func (ep *ExclusivePublication) TryClaim(
	length int,
) (termIndex, frameOffset int, position int64, err error) {
	// Repeats the OfferWithOffset reservation. A change to one must change the
	// other.
	if ep.IsClosed() {
		return 0, 0, Closed, nil
	}

	// The unsigned compare rejects a negative length too.
	if uint(length) > uint(ep.maxPayloadLength) {
		return 0, 0, 0, invalidClaimLengthError(length, ep.maxPayloadLength)
	}

	alignedLength := logbuffer.AlignFragmentChunkLength(length)

	limit := ep.publicationLimit.Get()
	termBeginPosition := ep.termBeginPosition
	position = termBeginPosition + int64(ep.termOffset)
	if position >= limit {
		status := backPressureStatusForLog(
			ep.meta,
			ep.maxPossiblePosition,
			position,
			int64(alignedLength),
		)
		return 0, 0, status, nil
	}
	if position+int64(alignedLength) >= ep.maxPossiblePosition {
		return 0, 0, MaxPositionExceeded, nil
	}

	termID := ep.termID
	termOffset := ep.termOffset
	resultingOffset := termOffset + alignedLength

	//nolint:gosec // G115: termOffset under TermMaxLength, 1<<30, plus one aligned frame
	rawTail := logbuffer.PackRawTail(termID, int32(resultingOffset))
	atomicbuffer.StoreInt64Release(ep.activeTailCounter, rawTail)

	position = termBeginPosition + int64(resultingOffset)
	if resultingOffset > ep.termBufferLength {
		return 0, 0, ep.rotateAtTermEnd(termOffset, position), nil
	}

	frame := ep.activeTermBuffer[termOffset : termOffset+logbuffer.DataHeaderLength]
	ep.headerWriter.WriteHeader(frame, termOffset, dataFrameLength(length), termID)

	ep.termOffset = resultingOffset
	return ep.activePartitionIndex, termOffset, position, nil
}

// Commit publishes an in-flight claim on the termIndex partition from TryClaim.
func (ep *ExclusivePublication) Commit(termIndex, frameOffset int) error {
	if ep.IsClosed() {
		return errExclusivePublicationClosed
	}

	frame, ok := claimFrame(&ep.termBuffers, termIndex, frameOffset)
	if !ok {
		return claimFrameError(termIndex, frameOffset)
	}
	header := atomicbuffer.Wrap(frame[:logbuffer.DataHeaderLength])

	frameLength := header.GetInt32Volatile(logbuffer.OffsetFrameLength)
	if frameLength >= 0 {
		return errClaimNotInFlight
	}
	header.PutInt32Release(logbuffer.OffsetFrameLength, -frameLength)
	return nil
}

// Abort retypes an in-flight claim as PAD on the termIndex partition from TryClaim.
func (ep *ExclusivePublication) Abort(termIndex, frameOffset int) error {
	if ep.IsClosed() {
		return errExclusivePublicationClosed
	}

	frame, ok := claimFrame(&ep.termBuffers, termIndex, frameOffset)
	if !ok {
		return claimFrameError(termIndex, frameOffset)
	}
	header := atomicbuffer.Wrap(frame[:logbuffer.DataHeaderLength])

	frameLength := header.GetInt32Volatile(logbuffer.OffsetFrameLength)
	if frameLength >= 0 {
		return errClaimNotInFlight
	}
	logbuffer.PutFrameType(header, 0, logbuffer.HdrTypePad)
	header.PutInt32Release(logbuffer.OffsetFrameLength, -frameLength)
	return nil
}

// TermBuffer returns the log partition term buffer for in-flight TryClaim writes.
//
// termIndex is the partition index returned by TryClaim.
//
// Valid only until Commit or Abort finalizes the claim on that partition.
//
// Panics when termIndex is out of range.
func (ep *ExclusivePublication) TermBuffer(termIndex int) atomicbuffer.AtomicBuffer {
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
	termOffset := int(logbuffer.TermOffset(rawTail, ep.termBufferLength))
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
		return Closed, errExclusivePublicationClosed
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

// checkPublicationLimit gates position against the publication limit and max
// position. The back-pressure span is AlignFragmentChunkLength(messageLength).
// The max-position check uses reservationSpan only (aligned frame or full
// framed length). Matches concurrent runOfferPrelude dual-span shape.
//
// The fragmented path calls it. OfferWithOffset and TryClaim repeat the same
// checks inline, so their success paths do not call a helper.
func (ep *ExclusivePublication) checkPublicationLimit(reservationSpan, messageLength int) int64 {
	limit := ep.publicationLimit.Get()
	position := ep.termBeginPosition + int64(ep.termOffset)

	if position >= limit {
		span := int64(logbuffer.AlignFragmentChunkLength(messageLength))
		return backPressureStatusForLog(ep.meta, ep.maxPossiblePosition, position, span)
	}

	if position+int64(reservationSpan) >= ep.maxPossiblePosition {
		return MaxPositionExceeded
	}

	return 0
}

// reserveExclusiveSpace publishes a tail of alignedLength bytes past
// ep.termOffset in the active term. On success the active partition, ep.termID
// and ep.termOffset are unchanged. The caller writes the frame at
// ep.termOffset, then advances ep.termOffset by alignedLength.
//
// The fragmented path calls it. OfferWithOffset and TryClaim repeat the same
// steps inline, so their success paths do not call a helper.
func (ep *ExclusivePublication) reserveExclusiveSpace(alignedLength int) (
	reservedPosition int64,
	status int64,
) {
	startOffset := ep.termOffset

	resultingOffset := startOffset + alignedLength
	//nolint:gosec // G115: startOffset under TermMaxLength, 1<<30, plus a framed 16MB message
	rawTail := logbuffer.PackRawTail(ep.termID, int32(resultingOffset))
	atomicbuffer.StoreInt64Release(ep.activeTailCounter, rawTail)

	reservedPosition = ep.termBeginPosition + int64(resultingOffset)
	if resultingOffset > ep.termBufferLength {
		return 0, ep.rotateAtTermEnd(startOffset, reservedPosition)
	}

	return reservedPosition, 0
}

// rotateAtTermEnd pads the active term from startOffset to its end after a
// reservation past the term end. It returns MaxPositionExceeded when
// reservedPosition reaches the max position. Otherwise it rotates to the next
// term and returns AdminAction.
//
// Every caller rejects position+span >= maxPossiblePosition first, so the
// MaxPositionExceeded branch does not run today. It stays because Java
// ExclusivePublication.newPosition also checks the max position before it
// rotates.
func (ep *ExclusivePublication) rotateAtTermEnd(startOffset int, reservedPosition int64) int64 {
	termBuf := ep.termBuffers[ep.activePartitionIndex]
	endStatus := ep.handleEndOfLog(
		termBuf,
		ep.termBufferLength,
		ep.termID,
		startOffset,
		reservedPosition,
	)
	if endStatus == MaxPositionExceeded {
		return MaxPositionExceeded
	}

	ep.rotateTerm()
	return AdminAction
}

// appendFragmentedMessage reserves the full framed span with one release tail
// store. It then writes the BEGIN, middle, and END DATA frames into that span.
//
// The exclusive writer owns the term, so the loop uses no FAA and no
// mid-loop term ID recheck.
func (ep *ExclusivePublication) appendFragmentedMessage(
	buffer []byte,
	srcOffset int,
	length int,
) int64 {
	framedLength := logbuffer.ComputeFragmentedFrameLength(length, ep.maxPayloadLength)
	if status := ep.checkPublicationLimit(framedLength, length); status != 0 {
		return status
	}

	reservedPosition, status := ep.reserveExclusiveSpace(framedLength)
	if status != 0 {
		return status
	}

	termBuf := ep.termBuffers[ep.activePartitionIndex]
	termID := ep.termID
	frameOffset := ep.termOffset
	remaining := length
	fullChunkAligned := logbuffer.AlignFragmentChunkLength(ep.maxPayloadLength)

	for remaining > 0 {
		chunk := nextFragmentChunk(remaining, length, ep.maxPayloadLength, fullChunkAligned)
		payloadOffset := srcOffset + (length - remaining)

		writeFragmentChunk(
			ep.headerWriter,
			termBuf,
			termID,
			frameOffset,
			chunk,
			buffer[payloadOffset:payloadOffset+chunk.bytesToWrite],
		)

		frameOffset += chunk.alignedLength
		remaining -= chunk.bytesToWrite
	}

	ep.termOffset += framedLength
	return reservedPosition
}

// bindActivePartition makes partition the active partition. It caches the
// partition term buffer and the address of its tail counter. Int64Pointer
// checks the tail counter bounds here, so the offer path stores through the
// address with no bounds check.
func (ep *ExclusivePublication) bindActivePartition(partition int) {
	ep.activePartitionIndex = partition
	ep.activeTermBuffer = ep.termBuffers[partition].Bytes()
	ep.activeTailCounter = ep.meta.Int64Pointer(logbuffer.OffsetTermTailCounter(partition))
}

// rotateTerm advances the cached exclusive tail and publishes ordered meta stores.
// Single-writer rotation avoids RotateLog's CAS loop, which is concurrent-publication semantics.
func (ep *ExclusivePublication) rotateTerm() {
	nextIndex := (ep.activePartitionIndex + 1) % logbuffer.PartitionCount
	nextTermID := ep.termID + 1
	ep.bindActivePartition(nextIndex)
	ep.termOffset = 0
	ep.termID = nextTermID
	ep.termBeginPosition += int64(ep.termBufferLength)
	termCount := nextTermID - ep.initialTermID

	_ = logbuffer.PutRawTail(ep.meta, nextIndex, logbuffer.PackRawTail(nextTermID, 0))
	ep.meta.PutInt32Release(logbuffer.OffsetActiveTermCount, termCount)
}

func (ep *ExclusivePublication) handleEndOfLog(
	termBuf atomicbuffer.AtomicBuffer,
	termLength int,
	termID int32,
	termOffset int,
	reservedPosition int64,
) int64 {
	if termOffset < termLength {
		//nolint:gosec // G115: CheckTermLength caps termLength at TermMaxLength, 1<<30
		paddingLength := int32(termLength - termOffset)
		commitPublicationTermPadding(ep.headerWriter, termBuf, termID, termOffset, paddingLength)
	}

	if reservedPosition >= ep.maxPossiblePosition {
		return MaxPositionExceeded
	}

	return AdminAction
}
