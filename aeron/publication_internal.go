package aeron

import (
	"errors"
	"fmt"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	errPublicationLogBufferNil = errors.New("aeron: publication log buffer is nil")
	errPublicationCountersNil  = errors.New("aeron: publication counters view is nil")
)

func newPublicationFromReady(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
	channel string,
) (*Publication, error) {
	if log == nil {
		return nil, errPublicationLogBufferNil
	}
	if counters == nil {
		return nil, errPublicationCountersNil
	}

	meta, err := log.MetaDataBuffer()
	if err != nil {
		return nil, err
	}

	if err := requireActiveCounter(
		counters,
		ready.PublicationLimitCounterID,
		ready.RegistrationID,
		cnc.DriverPublisherLimitTypeID,
		"publication limit",
	); err != nil {
		return nil, err
	}
	publicationLimit := cnc.NewCounterSlot(
		ready.PublicationLimitCounterID,
		counters.CounterValuesBuffer(),
	)
	if ready.ChannelStatusCounterID != cnc.NullCounterID {
		if err := requireActiveCounter(
			counters,
			ready.ChannelStatusCounterID,
			ready.RegistrationID,
			cnc.DriverSendChannelStatusTypeID,
			"channel status",
		); err != nil {
			return nil, err
		}
	}

	metadata, err := logbuffer.ParseMetadataView(meta)
	if err != nil {
		return nil, err
	}
	if metadata.Type != logbuffer.LogBufferTypeConcurrentPublication {
		return nil, fmt.Errorf(
			"aeron: expected concurrent publication log type %d, got %d",
			logbuffer.LogBufferTypeConcurrentPublication,
			metadata.Type,
		)
	}

	termBufferLength := log.TermLength()
	positionBitsToShift := logbuffer.PositionBitsToShift(termBufferLength)

	pub := &Publication{
		registrationID:         ready.RegistrationID,
		originalRegistrationID: ready.RegistrationID,
		sessionID:              ready.SessionID,
		streamID:               ready.StreamID,
		channel:                channel,
		log:                    log,
		meta:                   meta,
		termBufferLength:       termBufferLength,
		positionBitsToShift:    positionBitsToShift,
		initialTermID:          metadata.InitialTermID,
		maxPayloadLength:       int(metadata.MTULength) - logbuffer.DataHeaderLength,
		maxMessageLength:       logbuffer.ComputeMaxMessageLength(termBufferLength),
		maxPossiblePosition:    int64(termBufferLength) * (1 << 31),
		counters:               counters,
		publicationLimit:       publicationLimit,
		channelStatusCounterID: ready.ChannelStatusCounterID,
	}

	termBuffers, err := log.TermBuffers()
	if err != nil {
		return nil, err
	}
	pub.termBuffers = termBuffers

	pub.headerWriter = logbuffer.NewHeaderWriter(
		logbuffer.DefaultFrameHeaderBuffer(meta),
		ready.SessionID,
		ready.StreamID,
	)

	return pub, nil
}

func commitPublicationTermPadding(
	headerWriter logbuffer.HeaderWriter,
	termBuf aeronatomic.AtomicBuffer,
	termID int32,
	fromOffset int,
	paddingLength int32,
) {
	if paddingLength <= 0 {
		return
	}
	if paddingLength >= logbuffer.DataHeaderLength {
		headerWriter.Write(termBuf, fromOffset, paddingLength, termID)
		logbuffer.PutFrameType(termBuf, fromOffset, logbuffer.HdrTypePad)
		logbuffer.PutFrameLengthOrdered(termBuf, fromOffset, paddingLength)
		return
	}
	logbuffer.CommitMinimalTermEndPadding(termBuf, fromOffset, paddingLength)
}

func requireActiveCounter(
	counters *cnc.CountersReader,
	counterID int32,
	registrationID int64,
	typeID int32,
	label string,
) error {
	if counters.IsCounterActive(counterID, registrationID, typeID) {
		return nil
	}
	return fmt.Errorf(
		"aeron: %s counter %d is not active for registration %d",
		label,
		counterID,
		registrationID,
	)
}
