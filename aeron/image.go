package aeron

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// Image is a single session/source over a conductor-mapped log buffer.
//
// Image is not thread-safe. Do not call Poll concurrently with Client.DoWork on
// the same client, nor from multiple goroutines. Only one goroutine calls
// Client.DoWork and Image.Poll for a given client. isClosed uses atomic.Bool
// for visibility when close races Poll, but that does not make Poll safe across
// goroutines while DoWork may unmap shared log memory. During unavailable-image
// teardown, Poll on the same client thread may observe isClosed before the
// conductor releases the shared mmap; cross-goroutine Poll during teardown is
// unsupported.
//
// Image does not own the mapped log. Conductor refcounting controls unmap.
// internalClose clears term and metadata views; retained handles must not read
// mapped memory after close or log release.
type Image struct {
	subscription   *Subscription
	correlationID  int64
	sessionID      int32
	streamID       int32
	sourceIdentity string
	logFileName    string

	joinPosition  int64
	subscriberPos SubscriberPosition

	logBuffers  *logbuffer.LogBuffers
	termBuffers [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer
	metaBuffer  atomicbuffer.AtomicBuffer

	initialTermID       int32
	termLengthMask      int32
	positionBitsToShift int
	mtuLength           int32

	header       Header
	errorHandler func(error)

	finalPosition int64

	lastCorruptReportPosition int64

	isClosed atomic.Bool
}

func newImage(
	sub *Subscription,
	event conductor.AvailableImage,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	errorHandler func(error),
) (*Image, error) {
	if sub == nil {
		return nil, errors.New("aeron: subscription is nil")
	}
	if logBuffers == nil {
		return nil, errors.New("aeron: log buffers is nil")
	}
	if countersReader == nil {
		return nil, errors.New("aeron: counters reader is nil")
	}
	if sub.RegistrationID() != event.SubscriptionRegistrationID {
		return nil, fmt.Errorf(
			"aeron: subscription registration %d does not match available image %d",
			sub.RegistrationID(),
			event.SubscriptionRegistrationID,
		)
	}
	if sub.StreamID() != event.StreamID {
		return nil, fmt.Errorf(
			"aeron: subscription stream %d does not match available image %d",
			sub.StreamID(),
			event.StreamID,
		)
	}

	mapped := logBuffers.MappedLogBuffer()
	if mapped == nil {
		return nil, errors.New("aeron: mapped log buffer is nil")
	}

	if err := requireActiveCounter(
		countersReader,
		event.SubscriberPositionID,
		event.SubscriptionRegistrationID,
		cnc.DriverSubscriberPositionTypeID,
		"subscriber position",
	); err != nil {
		return nil, err
	}

	meta, err := logBuffers.MetaDataBuffer()
	if err != nil {
		return nil, err
	}

	metadata, err := logbuffer.ParseMetadataView(meta)
	if err != nil {
		return nil, err
	}

	termLength := mapped.TermLength()
	if int(metadata.TermLength) != termLength {
		return nil, fmt.Errorf(
			"aeron: metadata term length %d does not match mapped log %d",
			metadata.TermLength,
			termLength,
		)
	}
	positionBitsToShift := logbuffer.PositionBitsToShift(termLength)

	termBuffers, err := logBuffers.TermBuffers()
	if err != nil {
		return nil, err
	}

	subscriberPos := newSubscriberPosition(
		event.SubscriberPositionID,
		countersReader.CounterValuesBuffer(),
	)
	joinPosition := subscriberPos.Get()

	image := &Image{
		subscription:        sub,
		correlationID:       event.CorrelationID,
		sessionID:           event.SessionID,
		streamID:            event.StreamID,
		sourceIdentity:      event.SourceIdentity,
		logFileName:         event.LogFileName,
		joinPosition:        joinPosition,
		subscriberPos:       subscriberPos,
		logBuffers:          logBuffers,
		termBuffers:         termBuffers,
		metaBuffer:          meta,
		initialTermID:       metadata.InitialTermID,
		termLengthMask:      int32(termLength) - 1,
		positionBitsToShift: positionBitsToShift,
		mtuLength:           metadata.MTULength,
		errorHandler:        errorHandler,

		lastCorruptReportPosition: -1,
	}
	image.header = Header{
		initialTermID:       metadata.InitialTermID,
		positionBitsToShift: int32(positionBitsToShift),
		context:             image,
		fragmentedFrameLen:  headerFragmentedFrameNotSet,
	}

	return image, nil
}

func (image *Image) CorrelationID() int64 {
	return image.correlationID
}

func (image *Image) SessionID() int32 {
	return image.sessionID
}

func (image *Image) StreamID() int32 {
	return image.streamID
}

func (image *Image) SourceIdentity() string {
	return image.sourceIdentity
}

func (image *Image) LogFileName() string {
	return image.logFileName
}

// TermBufferLength returns the term buffer length of the image log.
//
// Reads the length from the term length mask (termLength - 1). Returns zero on
// a nil receiver.
func (image *Image) TermBufferLength() int32 {
	if image == nil {
		return 0
	}
	return image.termLengthMask + 1
}

// JoinPosition returns the subscriber counter value captured at image construction.
//
// The value does not change when the live counter advances during Poll.
func (image *Image) JoinPosition() int64 {
	return image.joinPosition
}

func (image *Image) Position() int64 {
	if image.isClosed.Load() {
		return image.finalPosition
	}
	return image.subscriberPos.Get()
}

// ReceivedPosition returns the stream position of the end of the contiguous
// committed data the media driver has placed in this image's term buffers.
//
// The scan starts at the subscriber position and stops at the first uncommitted
// frame. It reports the position that repeated unbounded [Image.Poll] calls
// settle at, without consuming a fragment and without advancing the subscriber
// position. A single Poll never crosses a term boundary; one ReceivedPosition
// call scans up to [logbuffer.PartitionCount] terms and stops there, so a term
// ring committed end to end reports the ring rather than looping.
//
// A corrupt or truncated committed frame stops the scan at that frame and is
// not reported. The next Poll over the same offset reports it through the
// subscriber error handler.
//
// A closed image reports its final position, the value [Image.Position] reports.
//
// ReceivedPosition is not thread-safe. Same concurrency contract as
// [Image.Poll]; see the [Image] type godoc.
func (image *Image) ReceivedPosition() int64 {
	if image.isClosed.Load() {
		return image.finalPosition
	}

	position := image.subscriberPos.Get()
	for range logbuffer.PartitionCount {
		termBuffer := image.activeTermBuffer(position)
		capacity := termBuffer.Capacity()
		offset := int(position) & int(image.termLengthMask)

		for offset < capacity {
			advance, ok := receivedFrameAdvance(termBuffer, offset, capacity)
			if !ok {
				return position
			}
			offset += advance
			position += int64(advance)
		}
	}
	return position
}

func (image *Image) SubscriberPositionID() int32 {
	return image.subscriberPos.CounterID()
}

func (image *Image) IsClosed() bool {
	return image.isClosed.Load()
}

func (image *Image) activeTermBuffer(position int64) atomicbuffer.AtomicBuffer {
	termIndex := logbuffer.IndexByPosition(position, image.positionBitsToShift)
	return image.termBuffers[termIndex]
}

func termEndSlackAdvance(
	offset int,
	capacity int,
	frameLength int32,
) (advance int, ok bool) {
	remaining := capacity - offset
	if remaining <= 0 || remaining >= logbuffer.DataHeaderLength {
		return 0, false
	}
	if frameLength > 0 && int(frameLength) != remaining {
		return 0, false
	}
	return remaining, true
}

func paddingAdvance(
	frameOffset int,
	frameLength int32,
	capacity int,
) (advance int, ok bool) {
	if frameLength <= 0 || frameOffset+int(frameLength) > capacity {
		return 0, false
	}
	if frameLength < int32(logbuffer.DataHeaderLength) {
		return 0, false
	}
	// Term-end PAD: committed raw span fills the remaining term bytes (e.g.
	// 40-byte PAD at termLength-40). Advance by frameLength, not alignment.
	if frameOffset+int(frameLength) == capacity {
		return int(frameLength), true
	}
	advance = logbuffer.AlignFrameLength(int(frameLength))
	if frameOffset+advance > capacity {
		return 0, false
	}
	return advance, true
}

// pollConsumePosition maps a term offset back to the subscriber log position for a
// single-term poll scan. Callers must pass initialPosition and initialOffset from
// poll entry; offset must remain within the same term as initialOffset.
func pollConsumePosition(initialPosition int64, initialOffset, offset int) int64 {
	return initialPosition + int64(offset-initialOffset)
}

func dataFrameNextOffset(
	frameOffset int,
	frameLength int32,
	capacity int,
) (nextOffset int, ok bool) {
	if frameLength < int32(logbuffer.DataHeaderLength) {
		return 0, false
	}
	frameLengthInt := int(frameLength)
	if frameOffset+frameLengthInt > capacity {
		return 0, false
	}
	nextOffset = frameOffset + logbuffer.AlignFrameLength(frameLengthInt)
	if nextOffset > capacity {
		return 0, false
	}
	return nextOffset, true
}

// receivedFrameAdvance returns the byte advance across the committed frame at
// offset. Poll's loop is the authority on these rules. The property that compares
// ReceivedPosition to repeated unbounded Poll asserts they agree.
func receivedFrameAdvance(
	termBuffer atomicbuffer.AtomicBuffer,
	offset int,
	capacity int,
) (advance int, ok bool) {
	frameLength := logbuffer.CommittedFrameLengthVolatile(termBuffer, offset)
	if slack, slackOK := termEndSlackAdvance(offset, capacity, frameLength); slackOK {
		return slack, true
	}
	if frameLength <= 0 {
		return 0, false
	}
	if logbuffer.IsPaddingFrameInTerm(termBuffer, offset) {
		return paddingAdvance(offset, frameLength, capacity)
	}
	nextOffset, dataOK := dataFrameNextOffset(offset, frameLength, capacity)
	if !dataOK {
		return 0, false
	}
	return nextOffset - offset, true
}

func (image *Image) reportCorruptCommittedFrame(
	initialPosition int64,
	initialOffset int,
	offset int,
	frameLength int32,
) {
	consumePosition := pollConsumePosition(initialPosition, initialOffset, offset)
	if consumePosition == image.lastCorruptReportPosition {
		return
	}
	image.lastCorruptReportPosition = consumePosition
	image.invokeSubscriberErrorHandler(fmt.Errorf(
		"aeron: corrupt committed frame at term offset %d length %d",
		offset,
		frameLength,
	))
}

func (image *Image) invokeSubscriberErrorHandler(err error) {
	if image.errorHandler == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	image.errorHandler(err)
}

func (image *Image) recoverFragmentHandlerPanic(ok *bool) {
	recovered := recover()
	if recovered == nil {
		return
	}
	*ok = false
	err, isErr := recovered.(error)
	if !isErr {
		err = fmt.Errorf("aeron: fragment handler panic: %v", recovered)
	}
	image.invokeSubscriberErrorHandler(err)
}

func preparePollFragment(
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	frameLength int32,
	header *Header,
) (payloadOffset, payloadLength int) {
	header.SetBuffer(termBuffer)
	header.ResetPollFragmentState()
	header.SetOffset(frameOffset)
	payloadOffset = frameOffset + logbuffer.OffsetFramePayload
	payloadLength = int(frameLength) - logbuffer.DataHeaderLength
	return payloadOffset, payloadLength
}

func (image *Image) invokeFragmentHandler(
	handler FragmentHandler,
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	frameLength int32,
	header *Header,
) (ok bool) {
	payloadOffset, payloadLength := preparePollFragment(
		termBuffer,
		frameOffset,
		frameLength,
		header,
	)
	ok = true
	if image.errorHandler != nil {
		defer image.recoverFragmentHandlerPanic(&ok)
	}

	handler(termBuffer, payloadOffset, payloadLength, header)
	return ok
}

// Poll scans one active log term and delivers up to fragmentLimit committed DATA
// fragments via handler. Padding frames advance the consume position without
// invoking the handler or incrementing the fragment count. A single poll call
// does not span terms; the next poll selects the active term from the updated
// subscriber position.
//
// Poll reads subscriber position with a volatile load at entry and publishes an
// advanced position with SetRelease when bytes were consumed and the image is
// still open. PAD-only scans with zero fragments still advance and store.
// Handler panic does not advance past the failing DATA frame when an error
// handler is configured. Without an error handler, handler panic propagates to
// the caller. Term-end PAD at capacity advances by raw committed padding bytes,
// not alignment.
func (image *Image) Poll(handler FragmentHandler, fragmentLimit int) int {
	if image.isClosed.Load() || handler == nil || fragmentLimit <= 0 {
		return 0
	}

	fragmentsRead := 0
	initialPosition := image.subscriberPos.Get()
	initialOffset := int(initialPosition) & int(image.termLengthMask)
	offset := initialOffset
	termBuffer := image.activeTermBuffer(initialPosition)
	capacity := termBuffer.Capacity()

	header := &image.header

	for fragmentsRead < fragmentLimit && offset < capacity && !image.isClosed.Load() {
		frameLength := logbuffer.CommittedFrameLengthVolatile(termBuffer, offset)
		if advance, ok := termEndSlackAdvance(offset, capacity, frameLength); ok {
			offset += advance
			continue
		}
		if frameLength <= 0 {
			break
		}

		if logbuffer.IsPaddingFrameInTerm(termBuffer, offset) {
			advance, ok := paddingAdvance(offset, frameLength, capacity)
			if !ok {
				image.reportCorruptCommittedFrame(
					initialPosition,
					initialOffset,
					offset,
					frameLength,
				)
				break
			}
			offset += advance
			trackPollAfterPaddingAdvance(image)
			continue
		}

		nextOffset, ok := dataFrameNextOffset(offset, frameLength, capacity)
		if !ok {
			image.reportCorruptCommittedFrame(
				initialPosition,
				initialOffset,
				offset,
				frameLength,
			)
			break
		}

		if !image.invokeFragmentHandler(handler, termBuffer, offset, frameLength, header) {
			break
		}

		offset = nextOffset
		fragmentsRead++
	}

	image.publishPollConsumePosition(initialPosition, initialOffset, offset)
	return fragmentsRead
}

// Shared store for Poll and controlled scan: one SetRelease + corrupt debounce path.
// Returns the computed consume position (whether or not a store ran).
func (image *Image) publishPollConsumePosition(
	initialPosition int64,
	initialOffset int,
	offset int,
) int64 {
	newPosition := pollConsumePosition(initialPosition, initialOffset, offset)
	if newPosition > initialPosition && !image.isClosed.Load() {
		image.subscriberPos.SetRelease(newPosition)
		if image.lastCorruptReportPosition >= 0 && newPosition > image.lastCorruptReportPosition {
			image.lastCorruptReportPosition = -1
		}
	}
	return newPosition
}

func (image *Image) internalClose() {
	if image.isClosed.Load() {
		return
	}
	image.finalPosition = image.subscriberPos.Get()
	image.isClosed.Store(true)
	image.logBuffers = nil
	image.metaBuffer = atomicbuffer.AtomicBuffer{}
	image.termBuffers = [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer{}
	image.header.buffer = atomicbuffer.AtomicBuffer{}
	image.header.offset = 0
}
