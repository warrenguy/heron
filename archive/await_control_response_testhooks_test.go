//go:build testhooks

package archive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

func requireControlResponseTimeout(
	tb testing.TB,
	err error,
	wantOperation string,
	wantCorrelationID int64,
	wantTimeout time.Duration,
) {
	tb.Helper()

	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(tb, err, &timeoutErr)
	require.Equal(tb, wantOperation, timeoutErr.Operation)
	require.Equal(tb, wantCorrelationID, timeoutErr.CorrelationID)
	require.Equal(tb, wantTimeout, timeoutErr.Timeout)
}

// runAwaitLoopNeverMatching drives runControlAwaitLoop with a matchFn that never
// completes, for tests exercising the timeout and context exit paths.
func runAwaitLoopNeverMatching(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx Context,
	pollFn func() int,
	timeout time.Duration,
	correlationID int64,
) error {
	return runControlAwaitLoop(
		ctx,
		aeronClient,
		archiveCtx,
		pollFn,
		timeout,
		correlationID,
		ControlResponseAwaitOperation,
		func() (bool, error) { return false, nil },
	)
}

// TestOfferAndAwait_offerRetryDoesNotStarveResponseAwait proves the response await
// starts a fresh ControlResponseTimeout window after the offer succeeds: the offer consumes the
// entire offer-phase budget, yet the already-buffered response is still matched.
func TestOfferAndAwait_offerRetryDoesNotStarveResponseAwait(t *testing.T) {
	const messageTimeout = 100 * time.Millisecond

	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, termBuffer := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)

	correlationID := int64(9100)
	archiveCtx := Context{
		ControlResponseTimeout:   messageTimeout,
		ControlOfferRetryTimeout: messageTimeout,
		IdleStrategy:             NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}

	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		RelevantID:       777,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, responseWire)

	offerCalls := 0
	fields, err := offerAndAwaitControlRequest(
		t.Context(),
		harness.Client,
		archiveCtx,
		poller.Poll,
		poller,
		func() (int64, bool) {
			offerCalls++
			nowNs.Add(messageTimeout.Nanoseconds())
			return 100, true
		},
		controlResponseTestControlSessionID,
		correlationID,
	)
	require.NoError(t, err)
	require.Equal(t, 1, offerCalls)
	require.Equal(t, int64(777), fields.RelevantID)
	require.Equal(t, correlationID, fields.CorrelationID)
}

func TestAwaitControlResponse_timeout(t *testing.T) {
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, _ := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)

	correlationID := int64(9999)
	archiveCtx := Context{
		ControlResponseTimeout:   time.Millisecond,
		ControlOfferRetryTimeout: time.Millisecond,
		IdleStrategy:             NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}

	pollFn := func() int {
		nowNs.Add(int64(time.Millisecond))
		return poller.Poll()
	}

	_, err := AwaitControlResponse(
		t.Context(),
		harness.Client,
		archiveCtx,
		pollFn,
		poller,
		controlResponseTestControlSessionID,
		correlationID,
	)
	requireControlResponseTimeout(
		t,
		err,
		ControlResponseAwaitOperation,
		correlationID,
		time.Millisecond,
	)
}

func TestRunArchiveControlAwaitLoop_nilAeronClient(t *testing.T) {
	err := runAwaitLoopNeverMatching(
		t.Context(),
		nil,
		Context{IdleStrategy: NewDefaultIdleStrategy()},
		func() int { return 0 },
		time.Minute,
		1,
	)
	require.ErrorIs(t, err, errNilControlAeronClient)
}

func TestRunArchiveControlAwaitLoop_deadlineBoundaryPollCount(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	const startNs int64 = 5_000_000_000
	var pollCalls int

	err := runAwaitLoopNeverMatching(
		t.Context(),
		harness.Client,
		Context{
			IdleStrategy: NewDefaultIdleStrategy(),
			AeronContext: aeron.Context{
				NanoClock: func() int64 { return startNs },
			},
		},
		func() int {
			pollCalls++
			return 0
		},
		0,
		4242,
	)
	requireControlResponseTimeout(t, err, ControlResponseAwaitOperation, 4242, 0)
	require.Zero(t, pollCalls, "clock() == deadlineNs must not enter poll loop")
}

func TestRunArchiveControlAwaitLoop_contextCanceled(t *testing.T) {
	t.Run("preCanceled", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		var pollCalls int
		err := runAwaitLoopNeverMatching(
			cancelCtx,
			harness.Client,
			Context{IdleStrategy: NewDefaultIdleStrategy()},
			func() int {
				pollCalls++
				return 0
			},
			time.Minute,
			1,
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, pollCalls)
	})

	t.Run("midLoop", func(t *testing.T) {
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		cancelCtx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var pollCalls int
		err := runAwaitLoopNeverMatching(
			cancelCtx,
			harness.Client,
			Context{
				IdleStrategy: NewDefaultIdleStrategy(),
				AeronContext: aeron.Context{
					NanoClock: func() int64 { return nowNs.Load() },
				},
			},
			func() int {
				pollCalls++
				nowNs.Add(int64(time.Millisecond))
				if pollCalls >= 2 {
					cancel()
				}
				return 0
			},
			time.Minute,
			1,
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, pollCalls)
	})
}

func TestRunArchiveControlAwaitLoop_contextDeadline(t *testing.T) {
	t.Run("preExpired", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		deadlineCtx, cancel := context.WithDeadline(
			t.Context(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		var pollCalls int
		err := runAwaitLoopNeverMatching(
			deadlineCtx,
			harness.Client,
			Context{IdleStrategy: NewDefaultIdleStrategy()},
			func() int {
				pollCalls++
				return 0
			},
			time.Minute,
			1,
		)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, pollCalls)
	})

	t.Run("ctxDeadlineSooner", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		deadlineCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := runAwaitLoopNeverMatching(
			deadlineCtx,
			harness.Client,
			Context{
				IdleStrategy: NewDefaultIdleStrategy(),
				AeronContext: aeron.Context{
					NanoClock: func() int64 { return time.Now().UnixNano() },
				},
			},
			func() int { return 0 },
			time.Minute,
			1,
		)
		elapsed := time.Since(start)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, 50*time.Millisecond)
	})

	t.Run("cfgTimeoutSooner", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		var pollCalls int
		err := runAwaitLoopNeverMatching(
			deadlineCtx,
			harness.Client,
			Context{
				ControlResponseTimeout: timeout,
				IdleStrategy:           NewDefaultIdleStrategy(),
				AeronContext: aeron.Context{
					NanoClock: func() int64 { return nowNs.Load() },
				},
			},
			func() int {
				pollCalls++
				nowNs.Add(stepNs)
				return 0
			},
			timeout,
			77,
		)
		requireControlResponseTimeout(
			t,
			err,
			ControlResponseAwaitOperation,
			77,
			timeout,
		)
		require.Positive(t, pollCalls)
	})
}

func TestOfferAndAwait_offerTimeout(t *testing.T) {
	h := newLifecycleHarness(t)
	h.ctx.ControlOfferRetryTimeout = time.Millisecond
	h.pub.offerResult = aeron.BackPressured

	_, err := offerAndAwaitControlRequest(
		t.Context(),
		h.client,
		h.ctx,
		func() int {
			h.advanceClock()
			return 0
		},
		h.poller,
		func() (int64, bool) {
			return h.proxy.TryKeepAlive(controlResponseTestControlSessionID, 1)
		},
		controlResponseTestControlSessionID,
		1,
	)
	requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
}

func TestOfferAndAwait_offerTerminal(t *testing.T) {
	h := newLifecycleHarness(t)
	h.pub.offerResult = aeron.NotConnected

	_, err := h.controlOps(func() int { return 0 }).startRecording(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
	)
	requireControlTerminal(t, err, aeron.NotConnected)
}

func TestAwaitListRecording_timeout(t *testing.T) {
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	descriptorPoller := NewRecordingDescriptorPoller()
	descriptorPoller.ResetAwait(8888)

	archiveCtx := Context{
		ControlResponseTimeout: time.Millisecond,
		IdleStrategy:           NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}

	pollFn := func() int {
		nowNs.Add(int64(time.Millisecond))
		return 0
	}

	err := AwaitListRecording(
		t.Context(),
		harness.Client,
		archiveCtx,
		pollFn,
		descriptorPoller,
	)
	requireControlResponseTimeout(
		t,
		err,
		ListRecordingDescriptorAwaitOperation,
		8888,
		time.Millisecond,
	)
}
