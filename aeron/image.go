package aeron

import (
	"errors"
	"fmt"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// Image is a single session/source over a conductor-mapped log buffer.
//
// Image is not thread-safe. Do not call Poll concurrently with Client.DoWork on
// the same client, nor from multiple goroutines. Only one goroutine calls
// Client.DoWork and Image.Poll for a given client.
//
// Image does not own the mapped log. Conductor refcounting controls unmap.
// internalClose clears term and metadata views; retained handles must not read
// mapped memory after close or log release.
// isClosed is a plain bool; safe under the single-thread contract above.
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
	termBuffers [logbuffer.PartitionCount]aeronatomic.AtomicBuffer
	metaBuffer  aeronatomic.AtomicBuffer

	initialTermID       int32
	termLengthMask      int32
	positionBitsToShift int
	mtuLength           int32

	header       Header
	errorHandler func(error)

	finalPosition int64
	isClosed      bool
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

// JoinPosition returns the subscriber counter value captured at image construction.
// The value does not change when the live counter advances during Poll.
func (image *Image) JoinPosition() int64 {
	return image.joinPosition
}

func (image *Image) Position() int64 {
	if image.isClosed {
		return image.finalPosition
	}
	return image.subscriberPos.Get()
}

func (image *Image) SubscriberPositionID() int32 {
	return image.subscriberPos.CounterID()
}

func (image *Image) IsClosed() bool {
	return image.isClosed
}

func (image *Image) Poll(handler FragmentHandler, fragmentLimit int) int {
	if image.isClosed {
		return 0
	}
	// Poll implementation must stop on non-positive committed frameLength and stay
	// within term capacity before invoking handlers or advancing subscriber position.
	return 0
}

func (image *Image) internalClose() {
	if image.isClosed {
		return
	}
	image.finalPosition = image.subscriberPos.Get()
	image.isClosed = true
	image.logBuffers = nil
	image.metaBuffer = aeronatomic.AtomicBuffer{}
	image.termBuffers = [logbuffer.PartitionCount]aeronatomic.AtomicBuffer{}
	image.header.buffer = aeronatomic.AtomicBuffer{}
	image.header.offset = 0
}
