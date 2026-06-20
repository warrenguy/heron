//go:build bench

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func TestResetInMemPublicationTermForBench_preservesCreationConfig(t *testing.T) {
	cfg := DefaultBenchPublicationConfig()
	cfg.InitialTermID = inMemPublicationInitialTermID + 11
	cfg.RawTails[0] = logbuffer.PackRawTail(cfg.InitialTermID, 128)

	pub := NewInMemPublicationForBench(cfg, BenchPublicationLimitPosition)
	require.Equal(t, cfg.InitialTermID, pub.meta.GetInt32(logbuffer.OffsetInitialTermID))

	ResetInMemPublicationTermForBench(pub)

	require.Equal(t, cfg.InitialTermID, pub.meta.GetInt32(logbuffer.OffsetInitialTermID))
	require.Equal(t, cfg.RawTails[0], mustRawTail(t, pub, 0))
	require.Equal(t, int32(0), logbuffer.ActiveTermCount(pub.meta))
}

func TestResetInMemPublicationTermForBench_allowsOfferAfterTermFill(t *testing.T) {
	cfg := DefaultBenchPublicationConfig()
	pub := NewInMemPublicationForBench(cfg, BenchPublicationLimitPosition)
	payload := make([]byte, 64)

	last := fillPublicationUntilNegative(pub, payload)
	require.Negative(t, last)
	require.NotZero(t, pub.termBuffers[0].GetInt32Volatile(0))

	ResetInMemPublicationTermForBench(pub)

	require.Equal(t, cfg.InitialTermID, pub.meta.GetInt32(logbuffer.OffsetInitialTermID))
	require.Equal(t, int32(0), logbuffer.ActiveTermCount(pub.meta))
	for partition := range logbuffer.PartitionCount {
		require.Equal(t, cfg.RawTails[partition], mustRawTail(t, pub, partition))
	}
	requireTermBuffersCleared(t, pub)

	pos := pub.Offer(payload)
	require.Positive(t, pos)
}

func TestResetInMemPublicationTermForBench_pairOfferPollAfterFill(t *testing.T) {
	pub, image := NewInMemPublicationImagePairForBench(
		DefaultBenchPublicationConfig(),
		BenchPublicationLimitPosition,
	)
	payload := []byte{1, 2, 3, 4}

	last := fillPublicationUntilNegative(pub, payload)
	require.Negative(t, last)

	ResetInMemPublicationTermForBench(pub)
	SetImageSubscriberPositionForBench(image, 0)

	pos := pub.Offer(payload)
	require.Positive(t, pos)

	var polledLength int
	polled := image.Poll(func(
		_ atomic.AtomicBuffer,
		_ int,
		length int,
		_ *Header,
	) {
		polledLength = length
	}, 1)
	require.Equal(t, 1, polled)
	require.Equal(t, len(payload), polledLength)
}

func TestBenchFixture_nilInputs(t *testing.T) {
	require.Panics(t, func() {
		ResetInMemPublicationTermForBench(nil)
	})
	require.Panics(t, func() {
		NewInMemImageForBench(nil)
	})
	require.Panics(t, func() {
		SetImageSubscriberPositionForBench(nil, 0)
	})
	require.Panics(t, func() {
		OfferRetryForBench(nil, nil)
	})
}

func TestBenchFixture_untrackedPublicationPanics(t *testing.T) {
	cfg := DefaultBenchPublicationConfig()
	log := newInMemMappedLog(cfg)
	pub := newInMemPublicationFromLog(log, inMemCountersReaderWithoutTB(BenchPublicationLimitPosition))

	require.Panics(t, func() {
		ResetInMemPublicationTermForBench(pub)
	})
}

func fillPublicationUntilNegative(pub *Publication, payload []byte) int64 {
	var last int64
	for range 10_000 {
		last = pub.Offer(payload)
		if last < 0 {
			break
		}
	}
	return last
}

func mustRawTail(t *testing.T, pub *Publication, partition int) int64 {
	t.Helper()

	rawTail, err := logbuffer.GetRawTail(pub.meta, partition)
	require.NoError(t, err)
	return rawTail
}

func requireTermBuffersCleared(t *testing.T, pub *Publication) {
	t.Helper()

	for partition := range logbuffer.PartitionCount {
		termBuf := pub.termBuffers[partition]
		for offset := range pub.termBufferLength {
			if termBuf.Bytes()[offset] != 0 {
				require.Failf(
					t,
					"term buffer not cleared",
					"partition=%d offset=%d",
					partition,
					offset,
				)
			}
		}
	}
}
