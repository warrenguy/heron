//go:build bench

// Bench-only in-memory publication and image fixtures for contract benchmarks.
// Not for production use.
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

var benchPublicationLogConfigs sync.Map

// DefaultBenchPublicationConfig returns defaults matching the in-package test publication.
func DefaultBenchPublicationConfig() BenchPublicationConfig {
	return defaultInMemPublicationLogConfig()
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

// ResetInMemPublicationTermForBench clears active term data and restores creation metadata.
// Requires pub from NewInMemPublicationForBench or NewInMemPublicationImagePairForBench;
// untracked publications panic. Bench fixtures assume one publication per benchmark
// instance; config entries are not removed from the internal map.
func ResetInMemPublicationTermForBench(pub *Publication) {
	if pub == nil {
		panic("aeron: bench publication is nil")
	}
	cfg := benchPublicationConfigFor(pub)
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
	pub := newTrackedBenchPublication(log, cfg, limitPosition)
	image := newInMemImageFromLog(log, 0)
	return pub, image
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
	trackBenchPublicationConfig(pub, cfg)
	return pub
}

func trackBenchPublicationConfig(pub *Publication, cfg BenchPublicationConfig) {
	benchPublicationLogConfigs.Store(pub, cfg)
}

func benchPublicationConfigFor(pub *Publication) BenchPublicationConfig {
	cfg, ok := benchPublicationLogConfigs.Load(pub)
	if !ok {
		panic("aeron: bench publication config is untracked")
	}
	return cfg.(BenchPublicationConfig)
}
