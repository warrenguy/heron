// Concurrent publication stress tests intentionally run under -race in make check.

package aeron_test

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
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

var testPublicationPositionShift = logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)

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

// consumedPublication is a publication with a stand-in consumer. position holds
// the consumer position. It starts at the publication position before any
// offer.
type consumedPublication struct {
	pub      *aeron.Publication
	reader   *cnc.CountersReader
	position *atomic.Int64
}

// newConsumedTestPublication returns a publication whose limit starts half a
// term past its initial position. spin moves the limit on. It also enables
// fragmented write tracking. The reservation tracker reports a reservation
// still open in a partition the consumer zeroes.
func newConsumedTestPublication(
	tb testing.TB,
	logCfg aeron.TestPublicationLogConfig,
) consumedPublication {
	tb.Helper()

	reader := aeron.NewTestCountersReaderForTest(tb, 0)
	pub := aeron.NewTestPublicationWithCounters(tb, logCfg, reader)
	position := new(atomic.Int64)
	position.Store(pub.Position())
	writeTestPublicationLimit(tb, reader, position.Load()+aeron.TestPublicationTermLength/2)
	aeron.EnableFragmentedWriteTrackingForTest(pub)
	tb.Cleanup(func() { aeron.DisableFragmentedWriteTrackingForTest(pub) })
	return consumedPublication{pub: pub, reader: reader, position: position}
}

// spin stands in for a subscriber and the driver. It walks
// committed frames from the start position and keeps the publication limit
// half a term ahead. On entry to each term it zeroes the partition of the term
// after it, so writers do not meet stale frames. Before the zeroing it marks
// every reservation still open in that term or a later one as lapped. A
// failed limit write goes to errs.
func (consumed consumedPublication) spin(stop <-chan struct{}, errs chan error) {
	const termLength = aeron.TestPublicationTermLength
	pub := consumed.pub
	shift := testPublicationPositionShift
	meta := pub.MetaDataBufferForTest()
	position := consumed.position.Load()
	for {
		select {
		case <-stop:
			return
		default:
		}

		termCount := int32(position >> shift)
		termIndex := logbuffer.IndexByTermCount(termCount)
		termOffset := int(logbuffer.ComputeTermOffsetFromPosition(position, shift))
		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		frameLength := logbuffer.FrameLengthVolatile(termBuf, termOffset)
		switch {
		case frameLength > 0:
			position += int64(logbuffer.AlignFrameLength(int(frameLength)))
		case isAbandonedTermEnd(meta, termCount, termOffset):
			position = int64(termCount+1) << shift
		default:
			// A writer holds this frame open. Yield so the writer can commit it
			// when GOMAXPROCS is 1.
			runtime.Gosched()
			continue
		}

		if logbuffer.ComputeTermOffsetFromPosition(position, shift) == 0 {
			nextTermCount := termCount + 2
			nextTermIndex := logbuffer.IndexByTermCount(nextTermCount)
			aeron.MarkFragmentedReservationsLappedForTest(pub, nextTermCount)
			aeron.PublicationTermBufferForTest(pub, nextTermIndex).SetBytes(0, termLength, 0)
		}
		consumed.position.Store(position)
		err := consumed.reader.SetCounterValue(
			aeron.TestPublicationLimitCounterID,
			position+termLength/2,
		)
		if err != nil {
			errs <- fmt.Errorf("publication limit write failed: %w", err)
			return
		}
	}
}

// isAbandonedTermEnd reports a term that a racing rotation closed before any
// writer reached its end. A frame never fills the rest of that term. The tail
// also equals the consumer position when the consumer catches up with the
// active term. The check requires a count past this term, so the consumer does
// not skip the active term.
func isAbandonedTermEnd(meta atomicbuffer.AtomicBuffer, termCount int32, termOffset int) bool {
	if logbuffer.ActiveTermCount(meta) <= termCount {
		return false
	}
	rawTail, err := logbuffer.GetRawTail(meta, logbuffer.IndexByTermCount(termCount))
	if err != nil {
		return false
	}
	tailTermCount := logbuffer.ComputeTermCount(
		logbuffer.TermID(rawTail),
		aeron.TestPublicationInitialTermID,
	)
	tailOffset := int(logbuffer.TermOffset(rawTail, aeron.TestPublicationTermLength))
	return tailTermCount == termCount && tailOffset == termOffset
}

func fragmentedStressPayload(pub *aeron.Publication) []byte {
	maxPayload := pub.MaxPayloadLengthForTest()
	payload := patternedPayload(maxPayload + 1)
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
			if position == aeron.BackPressured {
				// Yield so the stand-in consumer can move the limit when
				// GOMAXPROCS is 1.
				runtime.Gosched()
				continue
			}
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
			// Call TryClaim directly so AdminAction rechecks stop. TryClaimRetryForTest
			// may spin on AdminAction without rechecking stop.
			termIndex, frameOffset, position, err := pub.TryClaim(len(payload))
			if err != nil {
				errs <- err
				return
			}
			if position == aeron.AdminAction {
				continue
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
	require.Empty(
		tb,
		aeron.ReportFragmentedReservationsLappedForTest(),
		"reservations still open in a partition the consumer zeroes",
	)
	require.Empty(
		tb,
		aeron.ReportFragmentedReservationTrackerForTest(),
		"reservations share a slot or stay open",
	)
	require.Empty(
		tb,
		aeron.ReportFragmentedWriteTrackerForTest(),
		"fragment writes share a slot or stay open",
	)
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
	consumed := newConsumedTestPublication(t, aeron.DefaultTestPublicationLogConfig())
	pub := consumed.pub

	var successCount atomic.Int64
	errs := make(chan error, 5)
	payload := fragmentedStressPayload(pub)
	offer := func(stop <-chan struct{}) {
		spinOffer(
			pub, stop, errs, &successCount,
			"fragmented offer",
			func() int64 { return pub.Offer(payload) },
		)
	}
	consume := func(stop <-chan struct{}) {
		consumed.spin(stop, errs)
	}

	runConcurrentPublicationStress(
		t, concurrentPublicationStressDuration(), errs,
		offer, offer, offer, offer, consume,
	)

	requireConcurrentStressSuccessCount(t, successCount.Load(), concurrentPublicationMinSuccessCount)
	requireFragmentedConcurrentOfferClean(t)
	requireCommittedFragmentedPayloads(t, pub, payload)
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_OfferVector_concurrentFragmented(t *testing.T) {
	consumed := newConsumedTestPublication(t, aeron.DefaultTestPublicationLogConfig())
	pub := consumed.pub

	var successCount atomic.Int64
	errs := make(chan error, 5)
	payload := fragmentedStressPayload(pub)
	third := len(payload) / 3
	vectors := [][]byte{payload[:third], payload[third : 2*third], payload[2*third:]}
	// Each worker starts on OfferVector and switches form after each success.
	// The first success of any worker is then a vector success.
	offer := func(stop <-chan struct{}) {
		vector := true
		spinOffer(
			pub, stop, errs, &successCount,
			"fragmented offer",
			func() int64 {
				var position int64
				if vector {
					position = pub.OfferVector(vectors, crcReservedValue)
				} else {
					position = pub.Offer(payload)
				}
				if position > 0 {
					vector = !vector
				}
				return position
			},
		)
	}
	consume := func(stop <-chan struct{}) {
		consumed.spin(stop, errs)
	}

	runConcurrentPublicationStress(
		t, concurrentPublicationStressDuration(), errs,
		offer, offer, offer, offer, consume,
	)

	requireConcurrentStressSuccessCount(t, successCount.Load(), concurrentPublicationMinSuccessCount)
	requireFragmentedConcurrentOfferClean(t)
	requireCommittedFragmentedPayloads(t, pub, payload)
	requireCommittedReservedValues(t, pub)
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

// requireCommittedReservedValues asserts the reserved value of each committed
// DATA frame below the partition tails: 0 from Offer, or crcReservedValue
// from OfferVector.
func requireCommittedReservedValues(tb testing.TB, pub *aeron.Publication) {
	tb.Helper()

	for partition := range logbuffer.PartitionCount {
		rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), partition)
		require.NoError(tb, err)

		termEnd := int(logbuffer.TermOffset(rawTail, aeron.TestPublicationTermLength))
		termBuf := aeron.PublicationTermBufferForTest(pub, partition)
		forEachDataFrame(termBuf, termEnd, func(offset, frameLength int) {
			reservedValue := logbuffer.FrameReservedValue(termBuf, offset)
			if reservedValue == 0 {
				return
			}
			frame := termBuf.Bytes()[offset : offset+frameLength]
			label := fmt.Sprintf("partition %d offset %d", partition, offset)
			require.Equal(tb, crcReservedValue(frame), reservedValue, label)
		})
	}
}

func TestPublication_Offer_concurrentFragmentedWithRotation(t *testing.T) {
	consumed := newConsumedTestPublication(t, publicationLogConfigNearTermEnd(256))
	pub := consumed.pub

	var successCount atomic.Int64
	errs := make(chan error, 3)
	payload := fragmentedStressPayload(pub)
	offer := func(stop <-chan struct{}) {
		spinOffer(
			pub, stop, errs, &successCount,
			"fragmented offer",
			func() int64 { return pub.Offer(payload) },
		)
	}
	rotate := func(stop <-chan struct{}) {
		spinRotatePublicationLog(pub, stop)
	}
	consume := func(stop <-chan struct{}) {
		consumed.spin(stop, errs)
	}

	runConcurrentPublicationStress(
		t, concurrentPublicationStressDuration(), errs,
		offer, offer, rotate, consume,
	)

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
