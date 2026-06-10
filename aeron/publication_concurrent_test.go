// Concurrent publication stress tests intentionally run under -race in make check.
// client_await_concurrent_test.go excludes -race via //go:build !race.

package aeron_test

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const (
	concurrentPublicationStressDurationDefault = 20 * time.Millisecond
	concurrentPublicationStressDurationRace    = 100 * time.Millisecond
	concurrentPublicationMinSuccessCount       = 4
)

func concurrentPublicationStressDuration() time.Duration {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return concurrentPublicationStressDurationDefault
	}
	for _, setting := range info.Settings {
		if setting.Key == "-race" {
			return concurrentPublicationStressDurationRace
		}
	}
	return concurrentPublicationStressDurationDefault
}

func requireConcurrentStressSuccessCount(tb testing.TB, count, min int64) {
	tb.Helper()
	require.GreaterOrEqual(
		tb,
		count,
		min,
		"stress window too short: got %d successes, need >= %d (duration=%s)",
		count,
		min,
		concurrentPublicationStressDuration(),
	)
}

func runConcurrentPublicationStress(
	tb testing.TB,
	duration time.Duration,
	errs chan error,
	workers ...func(stop <-chan struct{}),
) {
	tb.Helper()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Go(func() {
			worker(stop)
		})
	}
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	select {
	case err := <-errs:
		require.NoError(tb, err)
	default:
	}
}

func spinRotatePublicationLog(pub *aeron.Publication, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
			aeron.RotatePublicationLogForTest(pub, 0, aeron.TestPublicationInitialTermID)
		}
	}
}

func fragmentedStressPayload(pub *aeron.Publication) []byte {
	maxPayload := pub.MaxPayloadLengthForTest()
	payload := make([]byte, maxPayload+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}

func spinOffer(
	pub *aeron.Publication,
	stop <-chan struct{},
	errs chan error,
	successCount *atomic.Int64,
	label string,
	offer func() int64,
) {
	for {
		select {
		case <-stop:
			return
		default:
			position := offer()
			if position == aeron.AdminAction {
				continue
			}
			if position < 0 {
				errs <- fmt.Errorf("%s failed: %d", label, position)
				return
			}
			if position > 0 {
				successCount.Add(1)
			}
		}
	}
}

func spinTryClaimCommit(
	pub *aeron.Publication,
	stop <-chan struct{},
	errs chan error,
	commitCount *atomic.Int64,
) {
	payload := make([]byte, 64)
	for {
		select {
		case <-stop:
			return
		default:
			termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(pub, len(payload))
			if err != nil {
				errs <- err
				return
			}
			if position < 0 {
				errs <- fmt.Errorf("try claim failed: %d", position)
				return
			}
			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
			if err := pub.Commit(termIndex, frameOffset); err != nil {
				errs <- err
				return
			}
			commitCount.Add(1)
		}
	}
}

func requireFragmentedConcurrentOfferClean(tb testing.TB) {
	tb.Helper()
	require.Empty(tb, aeron.ReportFragmentedReservationTrackerForTest())
	require.Empty(tb, aeron.ReportFragmentedWriteTrackerForTest())
}

func TestPublication_Offer_concurrentWithRotation(t *testing.T) {
	pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(256), int64(1)<<62)

	var successCount atomic.Int64
	errs := make(chan error, 2)

	runConcurrentPublicationStress(
		t, concurrentPublicationStressDuration(), errs,
		func(stop <-chan struct{}) {
			payload := make([]byte, 64)
			spinOffer(
				pub, stop, errs, &successCount,
				"offer",
				func() int64 { return pub.Offer(payload) },
			)
		},
		func(stop <-chan struct{}) {
			spinRotatePublicationLog(pub, stop)
		},
	)

	requireConcurrentStressSuccessCount(t, successCount.Load(), 1)
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_Offer_concurrentFragmented(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<62)
	aeron.EnableFragmentedWriteTrackingForTest()
	t.Cleanup(aeron.DisableFragmentedWriteTrackingForTest)

	var successCount atomic.Int64
	errs := make(chan error, 4)
	workers := make([]func(stop <-chan struct{}), 4)
	payload := fragmentedStressPayload(pub)
	for i := range workers {
		workers[i] = func(stop <-chan struct{}) {
			spinOffer(
				pub, stop, errs, &successCount,
				"fragmented offer",
				func() int64 { return aeron.OfferRetryForTest(pub, payload) },
			)
		}
	}

	runConcurrentPublicationStress(t, concurrentPublicationStressDuration(), errs, workers...)

	requireConcurrentStressSuccessCount(t, successCount.Load(), concurrentPublicationMinSuccessCount)
	requireFragmentedConcurrentOfferClean(t)
	requireCommittedFragmentedPayloads(t, pub, payload)
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_Offer_concurrentFragmentedWithRotation(t *testing.T) {
	pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(256), int64(1)<<62)
	aeron.EnableFragmentedWriteTrackingForTest()
	t.Cleanup(aeron.DisableFragmentedWriteTrackingForTest)

	var successCount atomic.Int64
	errs := make(chan error, 3)
	payload := fragmentedStressPayload(pub)
	workers := make([]func(stop <-chan struct{}), 3)
	for i := range 2 {
		workers[i] = func(stop <-chan struct{}) {
			spinOffer(
				pub, stop, errs, &successCount,
				"fragmented offer",
				func() int64 { return aeron.OfferRetryForTest(pub, payload) },
			)
		}
	}
	workers[2] = func(stop <-chan struct{}) {
		spinRotatePublicationLog(pub, stop)
	}

	runConcurrentPublicationStress(t, concurrentPublicationStressDuration(), errs, workers...)

	requireConcurrentStressSuccessCount(t, successCount.Load(), concurrentPublicationMinSuccessCount)
	requireFragmentedConcurrentOfferClean(t)
	requireCommittedFragmentedPayloads(t, pub, payload)
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_TryClaim_concurrentWithRotation(t *testing.T) {
	pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(256), int64(1)<<62)

	var commitCount atomic.Int64
	errs := make(chan error, 2)

	runConcurrentPublicationStress(
		t, concurrentPublicationStressDuration(), errs,
		func(stop <-chan struct{}) {
			spinTryClaimCommit(pub, stop, errs, &commitCount)
		},
		func(stop <-chan struct{}) {
			spinRotatePublicationLog(pub, stop)
		},
	)

	requireConcurrentStressSuccessCount(t, commitCount.Load(), 1)
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}
