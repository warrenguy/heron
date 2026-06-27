//go:build bench

package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func newInMemImageFromLog(
	log *logbuffer.MappedLogBuffer,
	joinPosition int64,
) *Image {
	logBuffers, err := logbuffer.NewLogBuffers(log)
	if err != nil {
		panic(err)
	}

	sub := newSubscription(
		inMemSubscriptionRegistrationID,
		inMemPublicationStreamID,
		inMemImageSourceIdentity,
	)
	counters := inMemCountersReaderWithSubscriberPosition(
		joinPosition,
		inMemSubscriptionRegistrationID,
	)

	image, err := newImage(sub, inMemAvailableImage(), logBuffers, counters, nil)
	if err != nil {
		panic(err)
	}
	return image
}

func newInMemZeroAllocPollImage() *Image {
	cfg := defaultInMemPublicationLogConfig()
	log := newInMemMappedLog(cfg)
	termBuffers, err := log.TermBuffers()
	if err != nil {
		panic(err)
	}
	writeInMemCommittedDataFrame(
		termBuffers[0],
		0,
		cfg.InitialTermID,
		0,
		logbuffer.Unfragmented,
		inMemPollZeroAllocPayloadBytes,
	)
	return newInMemImageFromLog(log, 0)
}

func resetInMemPublicationLogState(
	meta atomic.AtomicBuffer,
	termBuffers [logbuffer.PartitionCount]atomic.AtomicBuffer,
	termBufferLength int,
	cfg inMemPublicationLogConfig,
) {
	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(meta, partition, cfg.RawTails[partition])
		termBuf := termBuffers[partition]
		clear(termBuf.Bytes()[:termBufferLength])
	}
	meta.PutInt32(logbuffer.OffsetInitialTermID, cfg.InitialTermID)
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, cfg.ActiveTermCount)
}

func resetInMemPublicationTerm(pub *Publication, cfg inMemPublicationLogConfig) {
	resetInMemPublicationLogState(pub.meta, pub.termBuffers, pub.termBufferLength, cfg)
}

func exclusiveWriterCacheFromConfig(
	cfg inMemPublicationLogConfig,
	termBufferLength int,
	positionBitsToShift int,
) (
	activePartitionIndex int,
	termID int32,
	termOffset int,
	termBeginPosition int64,
) {
	activePartitionIndex = logbuffer.IndexByTermCount(cfg.ActiveTermCount)
	rawTail := cfg.RawTails[activePartitionIndex]
	termID = logbuffer.TermID(rawTail)
	termOffset = int(logbuffer.TermOffset(rawTail, int32(termBufferLength)))
	termBeginPosition = logbuffer.ComputeTermBeginPosition(
		termID,
		positionBitsToShift,
		cfg.InitialTermID,
	)
	return activePartitionIndex, termID, termOffset, termBeginPosition
}

func syncExclusivePublicationWriterCacheFromConfig(
	ep *ExclusivePublication,
	cfg inMemPublicationLogConfig,
) {
	activePartitionIndex, termID, termOffset, termBeginPosition := exclusiveWriterCacheFromConfig(
		cfg,
		ep.termBufferLength,
		ep.positionBitsToShift,
	)
	ep.activePartitionIndex = activePartitionIndex
	ep.termID = termID
	ep.termOffset = termOffset
	ep.termBeginPosition = termBeginPosition
}

func resetInMemExclusivePublicationTerm(ep *ExclusivePublication, cfg inMemPublicationLogConfig) {
	resetInMemPublicationLogState(ep.meta, ep.termBuffers, ep.termBufferLength, cfg)
	syncExclusivePublicationWriterCacheFromConfig(ep, cfg)
}
