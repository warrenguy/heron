package aeron_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestPublication_Offer_parallel(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<62)

	const goroutines = 8
	const offersPerGoroutine = 32

	var wg sync.WaitGroup
	positions := make([]int64, goroutines*offersPerGoroutine)
	expectedPayloads := make([][2]byte, goroutines*offersPerGoroutine)
	errs := make([]error, goroutines)

	for g := range goroutines {
		wg.Go(func() {
			for i := range offersPerGoroutine {
				idx := g*offersPerGoroutine + i
				expectedPayloads[idx] = [2]byte{byte(g), byte(i)}
				position := aeron.OfferRetryForTest(pub, expectedPayloads[idx][:])
				if position < 0 {
					errs[g] = fmt.Errorf("offer failed: %d", position)
					return
				}
				positions[idx] = position
			}
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	seen := make(map[int64]struct{}, len(positions))
	for idx, position := range positions {
		require.Positive(t, position)
		_, dup := seen[position]
		require.False(t, dup, "duplicate position %d", position)
		seen[position] = struct{}{}

		const payloadLen = 2
		aligned := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
		termOffset := int(logbuffer.ComputeTermOffsetFromPosition(position, shift))
		frameStart := termOffset - aligned

		termIndex := logbuffer.IndexByPosition(position, shift)
		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		require.Equal(
			t,
			int32(payloadLen+logbuffer.DataHeaderLength),
			logbuffer.FrameLengthVolatile(termBuf, frameStart),
		)
		payloadStart := frameStart + logbuffer.DataHeaderLength
		got := termBuf.Bytes()[payloadStart : payloadStart+payloadLen]
		require.Equal(t, expectedPayloads[idx][:], got)
	}

	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_Offer_parallelNearTermEnd(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<62)

	const goroutines = 8
	const offersPerGoroutine = 128

	var wg sync.WaitGroup
	var successCount atomic.Int64
	errs := make([]error, goroutines)

	for g := range goroutines {
		wg.Go(func() {
			payload := [2]byte{byte(g), 0}
			for i := range offersPerGoroutine {
				payload[1] = byte(i)
				position := aeron.OfferRetryForTest(pub, payload[:])
				if position < 0 {
					errs[g] = fmt.Errorf("offer failed: %d", position)
					return
				}
				successCount.Add(1)
			}
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Positive(t, successCount.Load())

	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}
