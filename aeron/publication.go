package aeron

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
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
	meta                atomicbuffer.AtomicBuffer
	termBuffers         [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer
	termBufferLength    int
	positionBitsToShift int

	// activeTermCount and tailCounters address fields in meta. The constructor
	// takes them through bounds-checked Int32Pointer and Int64Pointer calls,
	// so the offer path loads through them with no bounds check.
	//
	// The offer path reads activeTermCount with a sync/atomic load and no
	// AcquireFence, like Java getIntVolatile. The fence is not needed:
	//   - The limit read before it, publicationLimit.Get, is a load-acquire,
	//     so it stays ordered before the term count load.
	//   - The term count load is a load-acquire too, so it stays ordered
	//     before the tail load and the FAA.
	//
	// A load-acquire is MOVL on amd64 and an inlined LDARW on arm64.
	activeTermCount *int32
	tailCounters    [logbuffer.PartitionCount]*int64

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
	client    *Client
}

// ChannelEndpointStatusActive mirrors Java ChannelEndpointStatus.ACTIVE.
const ChannelEndpointStatusActive = cnc.ChannelEndpointStatusActive

// RegistrationID returns the correlation id of the add command for this
// publication. Each add gets its own id, even when several adds share one
// driver publication. See OriginalRegistrationID.
//
// Client.Publication looks the publication up by this id. Close sends this
// id to the driver.
func (pub *Publication) RegistrationID() int64 {
	return pub.registrationID
}

// OriginalRegistrationID returns the registration id of the driver
// publication. Matches Java Publication.originalRegistrationId.
//
// Behaviour:
//   - The driver gives a new driver publication the correlation id of the
//     add that created it. For that add, OriginalRegistrationID equals
//     RegistrationID. Publication does not check this.
//   - A later add that joins a live driver publication returns that
//     publication's id. The creating add may belong to another client.
//   - After every add closes, the next add creates a new driver publication
//     with a new id.
//   - The driver labels the publication limit counter with this id.
func (pub *Publication) OriginalRegistrationID() int64 {
	return pub.originalRegistrationID
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

// MaxMessageLength returns the largest total message length Offer will accept
// before returning MaxPositionExceeded (fragmented payloads may span multiple frames).
func (pub *Publication) MaxMessageLength() int {
	return pub.maxMessageLength
}

// Offer publishes a message from buffer. Unfragmented payloads fit one DATA
// frame; larger payloads up to maxMessageLength split into fragments.
//
// Returns a new stream position (>0) or a negative status code (see publication_codes.go).
//
// See also PositionCodeName, IsOfferRetriable.
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
//
// See also PositionCodeName, IsOfferRetriable.
func (pub *Publication) OfferWithOffset(buffer []byte, offset, length int) int64 {
	// The unfragmented success path stays in this one function and calls no
	// helper. reserveFrame states the read order. The tail read and the FAA
	// must stay in one function.
	if pub.IsClosed() {
		return Closed
	}

	// The unsigned compares reject negative values too. The panic stays in this
	// function, so the compiler can drop the payload slice checks below.
	if uint(offset) > uint(len(buffer)) || uint(length) > uint(len(buffer[offset:])) {
		panic(invalidOfferSliceMessage(offset, length, len(buffer)))
	}

	if length > pub.maxPayloadLength {
		return pub.appendFragmentedMessage(buffer, offset, length)
	}

	limit := pub.publicationLimit.Get()

	alignedLength := logbuffer.AlignFragmentChunkLength(length)
	termLength := pub.termBufferLength

	termCount := atomic.LoadInt32(pub.activeTermCount)
	termIndex := logbuffer.IndexByTermCount(termCount)
	tailCounter := pub.tailCounters[termIndex]

	rawTail := atomic.LoadInt64(tailCounter)
	termID := logbuffer.TermID(rawTail)
	termOffset := int(logbuffer.TermOffset(rawTail, termLength))

	if termCount != logbuffer.ComputeTermCount(termID, pub.initialTermID) {
		return AdminAction
	}

	position := logbuffer.ComputePosition(
		termID,
		termOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if position >= limit {
		return backPressureStatus(pub, position, int64(length))
	}
	if position+int64(alignedLength) >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}

	rawTail = atomic.AddInt64(tailCounter, int64(alignedLength)) - int64(alignedLength)
	termID = logbuffer.TermID(rawTail)
	termOffset = int(logbuffer.TermOffset(rawTail, termLength))

	resultingOffset := termOffset + alignedLength
	position = logbuffer.ComputePosition(
		termID,
		resultingOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if resultingOffset > termLength {
		return pub.handleEndOfLog(
			pub.termBuffers[termIndex],
			termLength,
			termID,
			termOffset,
			position,
		)
	}

	// One slice check covers every header and payload store below.
	termBuf := pub.termBuffers[termIndex].Bytes()
	payload := buffer[offset:][:length]
	frame := termBuf[termOffset : termOffset+logbuffer.DataHeaderLength+len(payload)]

	frameLength := dataFrameLength(length)
	pub.headerWriter.WriteHeader(frame, termOffset, frameLength, termID)
	copy(frame[logbuffer.DataHeaderLength:], payload)
	atomicbuffer.Wrap(frame).PutInt32Release(logbuffer.OffsetFrameLength, frameLength)

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

	termIndex, frameOffset, position, status := pub.reserveFrame(length)
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

// TermBuffer returns the log partition term buffer for in-flight TryClaim writes.
//
// termIndex is the partition index returned by TryClaim.
//
// Valid only until Commit or Abort finalizes the claim on that partition.
//
// Panics when termIndex is out of range.
func (pub *Publication) TermBuffer(termIndex int) atomicbuffer.AtomicBuffer {
	return pub.termBuffers[termIndex]
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
	termOffset := int(logbuffer.TermOffset(rawTail, pub.termBufferLength))
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

// invalidOfferSliceMessage returns the panic message for the first invalid
// offer argument. OfferWithOffset calls it only after its bounds compare
// fails, so one of the checks below always holds.
func invalidOfferSliceMessage(offset, length, bufferLength int) string {
	if length < 0 {
		return fmt.Sprintf("aeron: invalid message length=%d", length)
	}
	if offset < 0 {
		return fmt.Sprintf("aeron: invalid offer offset=%d", offset)
	}
	if offset > bufferLength {
		return fmt.Sprintf("aeron: invalid offer offset=%d buffer=%d", offset, bufferLength)
	}
	return fmt.Sprintf(
		"aeron: invalid offer offset=%d length=%d buffer=%d",
		offset,
		length,
		bufferLength,
	)
}

func validateTermIndex(termIndex int) error {
	if err := logbuffer.ValidateTermIndex(termIndex, "term index"); err != nil {
		return fmt.Errorf("aeron: invalid term index %d", termIndex)
	}
	return nil
}

func validateFrameOffset(termBuf atomicbuffer.AtomicBuffer, frameOffset int) error {
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
) (atomicbuffer.AtomicBuffer, int32, error) {
	if pub.IsClosed() {
		return atomicbuffer.AtomicBuffer{}, 0, errors.New("aeron: publication is closed")
	}
	return loadInFlightClaimFrame(&pub.termBuffers, termIndex, frameOffset)
}

// loadInFlightClaimFrame validates a claimed frame and returns its term buffer
// and absolute frame length. Shared by concurrent and exclusive Commit/Abort;
// the closed check stays with each caller for the type-specific error.
func loadInFlightClaimFrame(
	termBuffers *[logbuffer.PartitionCount]atomicbuffer.AtomicBuffer,
	termIndex int,
	frameOffset int,
) (atomicbuffer.AtomicBuffer, int32, error) {
	if err := validateTermIndex(termIndex); err != nil {
		return atomicbuffer.AtomicBuffer{}, 0, err
	}

	termBuf := termBuffers[termIndex]
	if err := validateFrameOffset(termBuf, frameOffset); err != nil {
		return atomicbuffer.AtomicBuffer{}, 0, err
	}
	frameLength := termBuf.GetInt32Volatile(frameOffset)
	if frameLength >= 0 {
		return atomicbuffer.AtomicBuffer{}, 0, errors.New("aeron: claimed space not in flight")
	}
	return termBuf, -frameLength, nil
}

// runOfferPrelude gates a fragmented offer as Java ConcurrentPublication.offer
// does. Back pressure takes messageLength. The max position check takes
// reservationSpan, the framed length.
func (pub *Publication) runOfferPrelude(
	reservationSpan int,
	messageLength int,
) (
	termIndex int,
	tailOffset int,
	status int64,
) {
	limit := pub.publicationLimit.Get()

	termLength := pub.termBufferLength

	termCount := logbuffer.ActiveTermCount(pub.meta)
	termIndex = logbuffer.IndexByTermCount(termCount)
	tailOffset = logbuffer.OffsetTermTailCounter(termIndex)

	rawTail := pub.meta.GetInt64Volatile(tailOffset)
	termID := logbuffer.TermID(rawTail)
	termOffset := int(logbuffer.TermOffset(rawTail, termLength))

	if termCount != logbuffer.ComputeTermCount(termID, pub.initialTermID) {
		return 0, 0, AdminAction
	}

	position := logbuffer.ComputePosition(
		termID,
		termOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if position >= limit {
		return 0, 0, backPressureStatus(pub, position, int64(messageLength))
	}
	if position+int64(reservationSpan) >= pub.maxPossiblePosition {
		return 0, 0, MaxPositionExceeded
	}
	return termIndex, tailOffset, 0
}

// reserveFragmentedSpan rechecks the active term after the prelude, then
// reserves span bytes with one fetch-and-add on the partition tail counter.
func (pub *Publication) reserveFragmentedSpan(
	termIndex int,
	tailOffset int,
	span int,
) (
	termOffset int,
	termID int32,
	position int64,
	status int64,
) {
	termCount := logbuffer.ActiveTermCount(pub.meta)
	if logbuffer.IndexByTermCount(termCount) != termIndex {
		return 0, 0, 0, AdminAction
	}
	rawTailVolatile := pub.meta.GetInt64Volatile(tailOffset)
	if logbuffer.ComputeTermCount(logbuffer.TermID(rawTailVolatile), pub.initialTermID) != termCount {
		return 0, 0, 0, AdminAction
	}

	termLength := pub.termBufferLength
	rawTail := pub.meta.GetAndAddInt64(tailOffset, int64(span))
	termID = logbuffer.TermID(rawTail)
	termOffset = int(logbuffer.TermOffset(rawTail, termLength))
	resultingOffset := termOffset + span
	position = logbuffer.ComputePosition(
		termID,
		resultingOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if resultingOffset > termLength {
		termBuf := pub.termBuffers[termIndex]
		endStatus := pub.handleEndOfLog(termBuf, termLength, termID, termOffset, position)
		return 0, 0, 0, endStatus
	}
	return termOffset, termID, position, 0
}

// reserveFrame checks the publication limit, reserves space with a
// fetch-and-add on the active partition tail counter, then writes the DATA
// frame header at termOffset. The caller fills the payload and commits the
// frame length.
//
// Volatile read order: limit, active term count, partition raw tail, as in
// Java ConcurrentPublication. No yield between the tail read and the FAA. A
// rotation between them reserves on the wrong partition.
//
// Each reservation ends as DATA or PAD at termOffset. Callers retry on
// AdminAction.
//
// The prelude repeats the runOfferPrelude checks inline. The tail read and the
// FAA must stay in one function.
//
// TryClaim calls reserveFrame. OfferWithOffset repeats the same steps inline,
// so its success path calls no helper. A change to one must change the other.
func (pub *Publication) reserveFrame(messageLength int) (
	termIndex int,
	termOffset int,
	position int64,
	status int64,
) {
	limit := pub.publicationLimit.Get()

	alignedLength := logbuffer.AlignFragmentChunkLength(messageLength)
	termLength := pub.termBufferLength

	termCount := atomic.LoadInt32(pub.activeTermCount)
	termIndex = logbuffer.IndexByTermCount(termCount)
	tailCounter := pub.tailCounters[termIndex]

	rawTail := atomic.LoadInt64(tailCounter)
	termID := logbuffer.TermID(rawTail)
	termOffset = int(logbuffer.TermOffset(rawTail, termLength))

	if termCount != logbuffer.ComputeTermCount(termID, pub.initialTermID) {
		return 0, 0, 0, AdminAction
	}

	position = logbuffer.ComputePosition(
		termID,
		termOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)
	if position >= limit {
		return 0, 0, 0, backPressureStatus(pub, position, int64(messageLength))
	}
	if position+int64(alignedLength) >= pub.maxPossiblePosition {
		return 0, 0, 0, MaxPositionExceeded
	}

	rawTail = atomic.AddInt64(tailCounter, int64(alignedLength)) - int64(alignedLength)
	termID = logbuffer.TermID(rawTail)
	termOffset = int(logbuffer.TermOffset(rawTail, termLength))

	resultingOffset := termOffset + alignedLength
	position = logbuffer.ComputePosition(
		termID,
		resultingOffset,
		pub.positionBitsToShift,
		pub.initialTermID,
	)

	termBuf := pub.termBuffers[termIndex]
	if resultingOffset > termLength {
		endStatus := pub.handleEndOfLog(termBuf, termLength, termID, termOffset, position)
		return 0, 0, 0, endStatus
	}

	pub.headerWriter.Write(termBuf, termOffset, dataFrameLength(messageLength), termID)
	return termIndex, termOffset, position, 0
}

func (pub *Publication) appendFragmentedMessage(buffer []byte, srcOffset, length int) int64 {
	// One FAA reserves the full framed span, as in Java
	// ConcurrentPublication.appendFragmentedMessage. The fragment writes do not
	// touch the tail counter again.
	framedLength := logbuffer.ComputeFragmentedFrameLength(length, pub.maxPayloadLength)
	termIndex, tailOffset, status := pub.runOfferPrelude(framedLength, length)
	if status != 0 {
		return status
	}
	// The length check follows the prelude and precedes the FAA. An oversized
	// message reserves nothing.
	if length > pub.maxMessageLength {
		return MaxPositionExceeded
	}

	partitionMu := &pub.fragmentedPartitionMu[termIndex]
	// Contention returns AdminAction. The caller retries, as after a rotation.
	if !partitionMu.TryLock() {
		return AdminAction
	}
	defer partitionMu.Unlock()

	termOffset, termID, position, status := pub.reserveFragmentedSpan(
		termIndex,
		tailOffset,
		framedLength,
	)
	if status != 0 {
		return status
	}
	if trackFragmentedReservationBegin(pub, termIndex, termOffset) {
		defer trackFragmentedReservationEnd(pub, termIndex, termOffset)
	}

	termBuf := pub.termBuffers[termIndex]
	frameOffset := termOffset
	reservationEnd := termOffset + framedLength
	remaining := length
	fullChunkAligned := logbuffer.AlignFragmentChunkLength(pub.maxPayloadLength)

	// The FAA already advanced the tail past the full framed span. The loop
	// must fill that span on termBuf, with frames or padding.
	for remaining > 0 {
		// A partition recycle resets the raw tail term ID. Pad the rest of the
		// reservation so a new publisher cannot overlap it.
		if logbuffer.TermID(pub.meta.GetInt64Volatile(tailOffset)) != termID {
			if frameOffset < reservationEnd {
				pub.commitTermPadding(
					termBuf,
					termID,
					frameOffset,
					//nolint:gosec // G115: at most framedLength, a framed 16MB message, under 1<<30
					int32(reservationEnd-frameOffset),
				)
			}
			return AdminAction
		}

		chunk := nextFragmentChunk(remaining, length, pub.maxPayloadLength, fullChunkAligned)
		payloadOffset := srcOffset + (length - remaining)

		trackFragmentedWrite(pub, termIndex, frameOffset, true)

		writeFragmentChunk(
			pub.headerWriter,
			termBuf,
			termID,
			frameOffset,
			chunk,
			buffer[payloadOffset:payloadOffset+chunk.bytesToWrite],
		)

		trackFragmentedWrite(pub, termIndex, frameOffset, false)

		frameOffset += chunk.alignedLength
		remaining -= chunk.bytesToWrite
	}

	return position
}

func (pub *Publication) commitTermPadding(
	termBuf atomicbuffer.AtomicBuffer,
	termID int32,
	fromOffset int,
	paddingLength int32,
) {
	commitPublicationTermPadding(pub.headerWriter, termBuf, termID, fromOffset, paddingLength)
}

func (pub *Publication) handleEndOfLog(
	termBuf atomicbuffer.AtomicBuffer,
	termLength int,
	termID int32,
	termOffset int,
	position int64,
) int64 {
	if termOffset < termLength {
		// Term-end padding seeds session/stream and default header fields before the
		// PAD type commit; PAD-only writes (as in Abort) would omit them.
		//nolint:gosec // G115: CheckTermLength caps termLength at TermMaxLength, 1<<30
		pub.commitTermPadding(termBuf, termID, termOffset, int32(termLength-termOffset))
	}

	if position >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}

	termCount := logbuffer.ComputeTermCount(termID, pub.initialTermID)
	_ = logbuffer.RotateLog(pub.meta, termCount, termID)
	return AdminAction
}
