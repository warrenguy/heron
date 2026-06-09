package aeron_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestPublication_Offer_concurrentWithRotation(t *testing.T) {
	cfg := aeron.DefaultTestPublicationLogConfig()
	termLength := aeron.TestPublicationTermLength
	cfg.RawTails[0] = logbuffer.PackRawTail(
		aeron.TestPublicationInitialTermID,
		int32(termLength-256),
	)

	pub := aeron.NewTestPublication(t, cfg, int64(1)<<62)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var successCount atomic.Int64
	errs := make(chan error, 2)

	wg.Go(func() {
		payload := make([]byte, 64)
		for {
			select {
			case <-stop:
				return
			default:
				position := pub.Offer(payload)
				if position == aeron.AdminAction {
					continue
				}
				if position < 0 {
					errs <- fmt.Errorf("offer failed: %d", position)
					return
				}
				if position > 0 {
					successCount.Add(1)
				}
			}
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				aeron.RotatePublicationLogForTest(pub, 0, aeron.TestPublicationInitialTermID)
			}
		}
	})

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	select {
	case err := <-errs:
		require.NoError(t, err)
	default:
	}

	require.Positive(t, successCount.Load())
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_TryClaim_concurrentWithRotation(t *testing.T) {
	cfg := aeron.DefaultTestPublicationLogConfig()
	termLength := aeron.TestPublicationTermLength
	cfg.RawTails[0] = logbuffer.PackRawTail(
		aeron.TestPublicationInitialTermID,
		int32(termLength-256),
	)

	pub := aeron.NewTestPublication(t, cfg, int64(1)<<62)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var commitCount atomic.Int64
	errs := make(chan error, 2)

	wg.Go(func() {
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
				termBuf.PutBytes(frameOffset+logbuffer.DataHeaderLength, payload)
				if err := pub.Commit(termIndex, frameOffset); err != nil {
					errs <- err
					return
				}
				commitCount.Add(1)
			}
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				aeron.RotatePublicationLogForTest(pub, 0, aeron.TestPublicationInitialTermID)
			}
		}
	})

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	select {
	case err := <-errs:
		require.NoError(t, err)
	default:
	}

	require.Positive(t, commitCount.Load())
	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}
