package aeron

import (
	"errors"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	errExclusivePublicationLogBufferNil = errors.New("aeron: exclusive publication log buffer is nil")
	errExclusivePublicationCountersNil  = errors.New(
		"aeron: exclusive publication counters view is nil",
	)
)

func newExclusivePublicationFromReady(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
	channel string,
) (*ExclusivePublication, error) {
	if log == nil {
		return nil, errExclusivePublicationLogBufferNil
	}
	if counters == nil {
		return nil, errExclusivePublicationCountersNil
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
		if err := requireAllocatedCounter(
			counters,
			ready.ChannelStatusCounterID,
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
	if err := requireLogBufferType(
		metadata,
		logbuffer.LogBufferTypeExclusivePublication,
		"exclusive publication",
	); err != nil {
		return nil, err
	}

	termBufferLength := log.TermLength()
	positionBitsToShift := logbuffer.PositionBitsToShift(termBufferLength)

	termCount := logbuffer.ActiveTermCount(meta)
	activePartitionIndex := logbuffer.IndexByTermCount(termCount)
	rawTail := meta.GetInt64Volatile(logbuffer.OffsetTermTailCounter(activePartitionIndex))
	termID := logbuffer.TermID(rawTail)
	termOffset := int(logbuffer.TermOffset(rawTail, termBufferLength))
	termBeginPosition := logbuffer.ComputeTermBeginPosition(
		termID,
		positionBitsToShift,
		metadata.InitialTermID,
	)

	termBuffers, err := log.TermBuffers()
	if err != nil {
		return nil, err
	}

	return &ExclusivePublication{
		registrationID:         ready.RegistrationID,
		originalRegistrationID: ready.RegistrationID,
		sessionID:              ready.SessionID,
		streamID:               ready.StreamID,
		channel:                channel,
		log:                    log,
		meta:                   meta,
		termBuffers:            termBuffers,
		termBufferLength:       termBufferLength,
		positionBitsToShift:    positionBitsToShift,
		initialTermID:          metadata.InitialTermID,
		maxPayloadLength:       int(metadata.MTULength) - logbuffer.DataHeaderLength,
		maxMessageLength:       logbuffer.ComputeMaxMessageLength(termBufferLength),
		maxPossiblePosition:    int64(termBufferLength) * (1 << 31),
		headerWriter: logbuffer.NewHeaderWriter(
			logbuffer.DefaultFrameHeaderBuffer(meta),
			ready.SessionID,
			ready.StreamID,
		),
		activePartitionIndex:   activePartitionIndex,
		termID:                 termID,
		termOffset:             termOffset,
		termBeginPosition:      termBeginPosition,
		counters:               counters,
		publicationLimit:       publicationLimit,
		channelStatusCounterID: ready.ChannelStatusCounterID,
	}, nil
}
