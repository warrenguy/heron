//go:build bench

package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
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

func resetInMemPublicationTerm(pub *Publication, cfg inMemPublicationLogConfig) {
	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(pub.meta, partition, cfg.RawTails[partition])
		termBuf := pub.termBuffers[partition]
		clear(termBuf.Bytes()[:pub.termBufferLength])
	}
	pub.meta.PutInt32(logbuffer.OffsetInitialTermID, cfg.InitialTermID)
	pub.meta.PutInt32Release(logbuffer.OffsetActiveTermCount, cfg.ActiveTermCount)
}
