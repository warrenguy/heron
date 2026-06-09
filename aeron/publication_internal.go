package aeron

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func newPublicationFromReady(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters cnc.CountersView,
	channel string,
) (*Publication, error) {
	if log == nil {
		return nil, errors.New("aeron: publication log buffer is nil")
	}
	if counters == nil {
		return nil, errors.New("aeron: publication counters view is nil")
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

	termBufferLength := log.TermLength()
	positionBitsToShift := logbuffer.PositionBitsToShift(termBufferLength)

	pub := &Publication{
		registrationID:            ready.RegistrationID,
		originalRegistrationID:    ready.RegistrationID,
		sessionID:                 ready.SessionID,
		streamID:                  ready.StreamID,
		channel:                   channel,
		log:                       log,
		meta:                      meta,
		termBufferLength:          termBufferLength,
		positionBitsToShift:       positionBitsToShift,
		initialTermID:             metadata.InitialTermID,
		maxPayloadLength:          int(metadata.MTULength) - logbuffer.DataHeaderLength,
		maxMessageLength:          logbuffer.ComputeMaxMessageLength(termBufferLength),
		maxPossiblePosition:       int64(termBufferLength) * (1 << 31),
		counters:                  counters,
		publicationLimitCounterID: ready.PublicationLimitCounterID,
		channelStatusCounterID:    ready.ChannelStatusCounterID,
	}

	for i := range logbuffer.PartitionCount {
		termBuf, termErr := log.TermBuffer(i)
		if termErr != nil {
			return nil, termErr
		}
		pub.termBuffers[i] = termBuf
	}

	pub.headerWriter = logbuffer.NewHeaderWriter(
		logbuffer.DefaultFrameHeaderBuffer(meta),
		ready.SessionID,
		ready.StreamID,
	)

	return pub, nil
}

func requireActiveCounter(
	counters cnc.CountersView,
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
