//go:build testhooks || bench

// Shared in-memory publication and image fixture internals for unit tests and bench builds.
package aeron

import (
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const (
	inMemPublicationRegistrationID   int64 = 9001
	inMemPublicationLimitCounterID   int32 = 1
	inMemPublicationChannelCounterID int32 = 2
	inMemPublicationSessionID        int32 = 1001
	inMemPublicationStreamID         int32 = 2002
	inMemPublicationInitialTermID    int32 = 7
	inMemPublicationMTULength        int32 = 1408
	inMemPublicationTermLength             = logbuffer.TermMinLength

	inMemSubscriptionRegistrationID  int64 = 8001
	inMemImageCorrelationID          int64 = 8101
	inMemSubscriberPositionCounterID int32 = 3
	inMemIPCChannel                        = "aeron:ipc"
	inMemImageSourceIdentity               = inMemIPCChannel

	inMemPollZeroAllocPayload = "zero-alloc"
)

var (
	inMemPollZeroAllocPayloadBytes = []byte(inMemPollZeroAllocPayload)
	inMemPollZeroAllocFrameBytes   = int64(
		logbuffer.AlignFrameLength(len(inMemPollZeroAllocPayload) + logbuffer.DataHeaderLength),
	)
	inMemMappedLogLength int
)

func init() {
	length, err := logbuffer.ComputeLogLength(inMemPublicationTermLength, logbuffer.PageMinSize)
	if err != nil {
		panic(err)
	}
	inMemMappedLogLength = length
}

type inMemPublicationLogConfig struct {
	InitialTermID   int32
	ActiveTermCount int32
	RawTails        [logbuffer.PartitionCount]int64
	IsConnected     int32
	LogType         uint8
}

func defaultInMemPublicationLogConfig() inMemPublicationLogConfig {
	return inMemPublicationLogConfig{
		InitialTermID:   inMemPublicationInitialTermID,
		ActiveTermCount: 0,
		RawTails: [logbuffer.PartitionCount]int64{
			logbuffer.PackRawTail(inMemPublicationInitialTermID, 0),
			logbuffer.PackRawTail(inMemPublicationInitialTermID-2, 0),
			logbuffer.PackRawTail(inMemPublicationInitialTermID-1, 0),
		},
		IsConnected: 1,
		LogType:     logbuffer.LogBufferTypeConcurrentPublication,
	}
}

func defaultInMemExclusivePublicationLogConfig() inMemPublicationLogConfig {
	cfg := defaultInMemPublicationLogConfig()
	cfg.LogType = logbuffer.LogBufferTypeExclusivePublication
	return cfg
}

func newInMemMappedLog(cfg inMemPublicationLogConfig) *logbuffer.MappedLogBuffer {
	buf := make([]byte, inMemMappedLogLength)
	writeInMemPublicationMetadata(buf, cfg)

	log, err := logbuffer.MapLogBufferFromBytes(buf)
	if err != nil {
		panic(err)
	}
	return log
}

func newInMemPublicationFromLog(
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
) *Publication {
	pub, err := newPublicationFromReady(inMemPublicationBuffersReady(), log, counters, inMemIPCChannel)
	if err != nil {
		panic(err)
	}
	return pub
}

func newInMemExclusivePublicationFromLog(
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
) *ExclusivePublication {
	ep, err := newExclusivePublicationFromReady(
		inMemPublicationBuffersReady(),
		log,
		counters,
		inMemIPCChannel,
	)
	if err != nil {
		panic(err)
	}
	return ep
}

func writeInMemPublicationMetadata(buf []byte, cfg inMemPublicationLogConfig) {
	metaStart := logbuffer.MetaDataOffset(inMemPublicationTermLength)
	meta := atomic.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])

	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(meta, partition, cfg.RawTails[partition])
	}
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, cfg.ActiveTermCount)
	meta.PutInt64(logbuffer.OffsetCorrelationID, inMemPublicationRegistrationID)
	meta.PutInt32(logbuffer.OffsetInitialTermID, cfg.InitialTermID)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	meta.PutInt32(logbuffer.OffsetTermLength, inMemPublicationTermLength)
	meta.PutInt32(logbuffer.OffsetPageSize, logbuffer.PageMinSize)
	meta.PutInt32(logbuffer.OffsetMTULength, inMemPublicationMTULength)
	meta.PutInt32Release(logbuffer.OffsetIsConnected, cfg.IsConnected)
	meta.Bytes()[logbuffer.OffsetLogType] = cfg.LogType

	if err := logbuffer.WriteDataHeaderUnfragmented(
		meta,
		logbuffer.OffsetDefaultFrameHeader,
		inMemPublicationSessionID,
		inMemPublicationStreamID,
		cfg.InitialTermID,
		0,
		logbuffer.Unfragmented,
	); err != nil {
		panic(err)
	}
}

func writeInMemCommittedDataFrame(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	frameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	if err := logbuffer.WriteDataHeaderUnfragmented(
		termBuffer,
		frameOffset,
		inMemPublicationSessionID,
		inMemPublicationStreamID,
		termID,
		termOffset,
		flags,
	); err != nil {
		panic(err)
	}
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)
}

func inMemPublicationBuffersReady() conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		RegistrationID:            inMemPublicationRegistrationID,
		SessionID:                 inMemPublicationSessionID,
		StreamID:                  inMemPublicationStreamID,
		PublicationLimitCounterID: inMemPublicationLimitCounterID,
		ChannelStatusCounterID:    inMemPublicationChannelCounterID,
		LogFileName:               "test.log",
	}
}

func inMemAvailableImage() conductor.AvailableImage {
	return conductor.AvailableImage{
		CorrelationID:              inMemImageCorrelationID,
		SessionID:                  inMemPublicationSessionID,
		StreamID:                   inMemPublicationStreamID,
		SubscriptionRegistrationID: inMemSubscriptionRegistrationID,
		SubscriberPositionID:       inMemSubscriberPositionCounterID,
		LogFileName:                "test-image.log",
		SourceIdentity:             inMemImageSourceIdentity,
	}
}

func inMemCountersReaderWithoutTB(limitPosition int64) *cnc.CountersReader {
	return inMemCountersReaderFromRecords(3, func(meta, values []byte) {
		cnc.WriteCounterRecord(
			meta,
			values,
			inMemPublicationLimitCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverPublisherLimitTypeID,
			inMemPublicationRegistrationID,
			limitPosition,
		)
		err := cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			inMemPublicationChannelCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverSendChannelStatusTypeID,
			inMemIPCChannel,
			inMemPublicationRegistrationID,
			1,
		)
		if err != nil {
			panic(err)
		}
	})
}

func inMemCountersReaderWithSubscriberPosition(
	joinPosition int64,
	registrationID int64,
) *cnc.CountersReader {
	return inMemCountersReaderFromRecords(4, func(meta, values []byte) {
		cnc.WriteCounterRecord(
			meta,
			values,
			inMemSubscriberPositionCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverSubscriberPositionTypeID,
			registrationID,
			joinPosition,
		)
	})
}

func inMemCountersReaderFromRecords(
	counterCount int,
	writeRecords func(meta, values []byte),
) *cnc.CountersReader {
	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)
	writeRecords(meta, values)

	reader, err := cnc.NewCountersReader(meta, values)
	if err != nil {
		panic(err)
	}
	return reader
}

func setInMemImageSubscriberPosition(image *Image, position int64) {
	image.subscriberPos.SetRelease(position)
}
