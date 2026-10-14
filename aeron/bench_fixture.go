//go:build bench

package aeron

import (
	"sync"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

// BenchPublicationLimitPosition is the default publication-limit counter seed for offer benches.
// Keeps offer paths from publication-limit back-pressure before term overflow.
const BenchPublicationLimitPosition int64 = 1 << 62

// BenchPublicationConfig holds in-memory publication log metadata for bench fixtures.
type BenchPublicationConfig = inMemPublicationLogConfig

var benchLogConfigs sync.Map

// DefaultBenchPublicationConfig returns defaults matching the in-package test publication.
func DefaultBenchPublicationConfig() BenchPublicationConfig {
	return defaultInMemPublicationLogConfig()
}

// DefaultBenchExclusivePublicationConfig returns defaults for exclusive in-memory bench
// publications, including LogBufferTypeExclusivePublication.
func DefaultBenchExclusivePublicationConfig() BenchPublicationConfig {
	return defaultInMemExclusivePublicationLogConfig()
}

// NewInMemPublicationForBench builds an in-memory publication backed by the test log layout.
// limitPosition seeds the publication-limit counter. The publication owns the mapped log
// for process lifetime; fixtures do not close it.
func NewInMemPublicationForBench(
	cfg BenchPublicationConfig,
	limitPosition int64,
) *Publication {
	return newTrackedBenchPublication(newInMemMappedLog(cfg), cfg, limitPosition)
}

// NewInMemImageForBench builds a poll-path image over sharedLog.
// The image borrows sharedLog for process lifetime; fixtures do not close it.
func NewInMemImageForBench(sharedLog *logbuffer.MappedLogBuffer) *Image {
	if sharedLog == nil {
		panic("aeron: bench image requires non-nil mapped log")
	}
	return newInMemImageFromLog(sharedLog, 0)
}

// NewInMemExclusivePublicationForBench builds an in-memory exclusive publication backed by
// the test log layout. cfg must use LogBufferTypeExclusivePublication; use
// DefaultBenchExclusivePublicationConfig when the default layout is sufficient.
// limitPosition seeds the publication-limit counter. The publication owns the mapped log
// for process lifetime; fixtures do not close it.
func NewInMemExclusivePublicationForBench(
	cfg BenchPublicationConfig,
	limitPosition int64,
) *ExclusivePublication {
	return newTrackedBenchExclusivePublication(newInMemMappedLog(cfg), cfg, limitPosition)
}

// ResetInMemExclusivePublicationTermForBench clears active term data, restores creation
// metadata, and reinitializes the exclusive single-writer cache. Requires ep from
// NewInMemExclusivePublicationForBench; untracked publications panic. Bench fixtures
// assume one publication per benchmark instance; config entries are not removed from the
// internal map.
func ResetInMemExclusivePublicationTermForBench(ep *ExclusivePublication) {
	if ep == nil {
		panic("aeron: bench exclusive publication is nil")
	}
	cfg := benchLogConfigFor(ep, "aeron: bench exclusive publication config is untracked")
	resetInMemExclusivePublicationTerm(ep, cfg)
}

// ResetInMemPublicationTermForBench clears active term data and restores creation metadata.
// Requires pub from NewInMemPublicationForBench or NewInMemPublicationImagePairForBench;
// untracked publications panic. Bench fixtures assume one publication per benchmark
// instance; config entries are not removed from the internal map.
func ResetInMemPublicationTermForBench(pub *Publication) {
	if pub == nil {
		panic("aeron: bench publication is nil")
	}
	cfg := benchLogConfigFor(pub, "aeron: bench publication config is untracked")
	resetInMemPublicationTerm(pub, cfg)
}

// SetImageSubscriberPositionForBench sets the image subscriber-position counter.
func SetImageSubscriberPositionForBench(image *Image, position int64) {
	if image == nil {
		panic("aeron: bench image is nil")
	}
	setInMemImageSubscriberPosition(image, position)
}

// NewInMemPublicationImagePairForBench wires publication and image to one mapped log.
// limitPosition seeds the publication-limit counter. Both handles borrow the mapped log
// for process lifetime; fixtures do not close it.
func NewInMemPublicationImagePairForBench(
	cfg BenchPublicationConfig,
	limitPosition int64,
) (*Publication, *Image) {
	log := newInMemMappedLog(cfg)
	return newTrackedBenchPublication(log, cfg, limitPosition), newInMemImageFromLog(log, 0)
}

// DefaultBenchPollImageForBench returns an image with one committed poll frame pre-seeded.
// The image owns the mapped log for process lifetime; fixtures do not close it.
func DefaultBenchPollImageForBench() *Image {
	return newInMemZeroAllocPollImage()
}

// BenchPollZeroAllocFrameBytes returns the aligned byte size of the default poll frame.
func BenchPollZeroAllocFrameBytes() int64 {
	return inMemPollZeroAllocFrameBytes
}

// OfferRetryForBench spins on AdminAction until Offer returns another value.
func OfferRetryForBench(pub *Publication, payload []byte) int64 {
	if pub == nil {
		panic("aeron: bench publication is nil")
	}
	for {
		pos := pub.Offer(payload)
		if pos != AdminAction {
			return pos
		}
	}
}

func newTrackedBenchPublication(
	log *logbuffer.MappedLogBuffer,
	cfg BenchPublicationConfig,
	limitPosition int64,
) *Publication {
	pub := newInMemPublicationFromLog(log, inMemCountersReaderWithoutTB(limitPosition))
	benchLogConfigs.Store(pub, cfg)
	return pub
}

func newTrackedBenchExclusivePublication(
	log *logbuffer.MappedLogBuffer,
	cfg BenchPublicationConfig,
	limitPosition int64,
) *ExclusivePublication {
	ep := newInMemExclusivePublicationFromLog(log, inMemCountersReaderWithoutTB(limitPosition))
	benchLogConfigs.Store(ep, cfg)
	return ep
}

func benchLogConfigFor(key any, untrackedMsg string) BenchPublicationConfig {
	cfg, ok := benchLogConfigs.Load(key)
	if !ok {
		panic(untrackedMsg)
	}
	return cfg.(BenchPublicationConfig)
}
