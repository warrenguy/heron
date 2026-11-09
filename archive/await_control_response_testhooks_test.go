//go:build testhooks

package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/archive/codecs"
	"luas.fast/archive/codecs/archivewire"
	"luas.fast/idlestrategy"
)

func requireControlResponseTimeout(
	tb requireTB,
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

func runAwaitLoopNeverMatching(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	timeout time.Duration,
	correlationID int64,
) error {
	return runControlAwaitLoop(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		nil,
		timeout,
		correlationID,
		ControlResponseAwaitOperation,
		func() (bool, error) { return false, nil },
	)
}

// TestOfferAndAwait_offerRetryDoesNotStarveResponseAwait asserts that the
// response await starts a fresh ControlResponseTimeout window after the offer
// succeeds.
func TestOfferAndAwait_offerRetryDoesNotStarveResponseAwait(t *testing.T) {
	const messageTimeout = 100 * time.Millisecond

	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, termBuffer := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)

	correlationID := int64(9100)
	cfg := Config{
		ControlResponseTimeout:   messageTimeout,
		ControlOfferRetryTimeout: messageTimeout,
		IdleStrategy:             NewDefaultIdleStrategy(),
		AeronConfig: aeron.Config{
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
		cfg,
		poller.Poll,
		nil,
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
	cfg := Config{
		ControlResponseTimeout:   time.Millisecond,
		ControlOfferRetryTimeout: time.Millisecond,
		IdleStrategy:             NewDefaultIdleStrategy(),
		AeronConfig: aeron.Config{
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
		cfg,
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

func TestRunControlAwaitLoop_nilAeronClient(t *testing.T) {
	err := runAwaitLoopNeverMatching(
		t.Context(),
		nil,
		Config{IdleStrategy: NewDefaultIdleStrategy()},
		func() int { return 0 },
		time.Minute,
		1,
	)
	require.ErrorIs(t, err, errNilControlAeronClient)
}

func TestRunControlAwaitLoop_deadlineBoundaryPollCount(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	const startNs int64 = 5_000_000_000
	var pollCalls int

	err := runAwaitLoopNeverMatching(
		t.Context(),
		harness.Client,
		Config{
			IdleStrategy: NewDefaultIdleStrategy(),
			AeronConfig: aeron.Config{
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

func TestRunControlAwaitLoop_contextCanceled(t *testing.T) {
	t.Run("preCanceled", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		var pollCalls int
		err := runAwaitLoopNeverMatching(
			cancelCtx,
			harness.Client,
			Config{IdleStrategy: NewDefaultIdleStrategy()},
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
			Config{
				IdleStrategy: NewDefaultIdleStrategy(),
				AeronConfig: aeron.Config{
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

func TestRunControlAwaitLoop_transportFault(t *testing.T) {
	faultErr := errors.New("transport fault")

	t.Run("fault_ends_loop_before_poll", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		var checks, polls int
		err := runControlAwaitLoop(
			t.Context(),
			harness.Client,
			Config{IdleStrategy: idlestrategy.NoOp},
			func() int {
				polls++
				return 0
			},
			func() error {
				checks++
				if checks >= 2 {
					return faultErr
				}
				return nil
			},
			time.Minute,
			1,
			ControlResponseAwaitOperation,
			func() (bool, error) { return false, nil },
		)
		require.ErrorIs(t, err, faultErr)
		require.Equal(t, 1, polls)
		require.Equal(t, 2, checks)
	})

	t.Run("latched_match_wins", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		matchErr := errors.New("latched match")
		var checks, polls int
		err := runControlAwaitLoop(
			t.Context(),
			harness.Client,
			Config{IdleStrategy: idlestrategy.NoOp},
			func() int {
				polls++
				return 0
			},
			func() error {
				checks++
				return faultErr
			},
			time.Minute,
			1,
			ControlResponseAwaitOperation,
			func() (bool, error) { return true, matchErr },
		)
		require.ErrorIs(t, err, matchErr)
		require.Zero(t, checks)
		require.Zero(t, polls)
	})

	t.Run("context_error_wins", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		ctx, cancel := context.WithCancel(t.Context())
		var polls int
		err := runControlAwaitLoop(
			ctx,
			harness.Client,
			Config{IdleStrategy: idlestrategy.NoOp},
			func() int {
				polls++
				return 0
			},
			func() error {
				cancel()
				return faultErr
			},
			time.Minute,
			1,
			ControlResponseAwaitOperation,
			func() (bool, error) { return false, nil },
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, polls)
	})
}

func TestRunControlAwaitLoop_contextDeadline(t *testing.T) {
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
			Config{IdleStrategy: NewDefaultIdleStrategy()},
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

		err := runAwaitLoopNeverMatching(
			deadlineCtx,
			harness.Client,
			Config{
				IdleStrategy: NewDefaultIdleStrategy(),
				AeronConfig: aeron.Config{
					NanoClock: func() int64 { return time.Now().UnixNano() },
				},
			},
			func() int {
				// Poll must wait for ctx.Done. The wait returns after ctx.Err() is set,
				// before the loop expiry check.
				<-deadlineCtx.Done()
				return 0
			},
			time.Minute,
			1,
		)

		require.ErrorIs(t, err, context.DeadlineExceeded)
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
			Config{
				ControlResponseTimeout: timeout,
				IdleStrategy:           NewDefaultIdleStrategy(),
				AeronConfig: aeron.Config{
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
	h.cfg.ControlOfferRetryTimeout = time.Millisecond
	h.pub.offerResult = aeron.BackPressured

	_, err := offerAndAwaitControlRequest(
		t.Context(),
		h.client,
		h.cfg,
		func() int {
			h.advanceClock()
			return 0
		},
		nil,
		h.poller,
		func() (int64, bool) {
			return h.proxy.TryKeepAlive(controlResponseTestControlSessionID, 1)
		},
		controlResponseTestControlSessionID,
		1,
	)
	requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
}

func TestOfferAndAwait_offerAttemptsExhausted(t *testing.T) {
	h := newLifecycleHarness(t)
	h.pub.offerResult = aeron.BackPressured

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, err := offerAndAwaitControlRequest(
		ctx,
		h.client,
		h.cfg,
		func() int { return 0 },
		nil,
		h.poller,
		func() (int64, bool) {
			return h.proxy.TryKeepAlive(controlResponseTestControlSessionID, 1)
		},
		controlResponseTestControlSessionID,
		1,
	)
	require.Len(t, h.pub.offered, DefaultMessageRetryAttempts)

	var attemptsErr *ControlOfferAttemptsError
	require.ErrorAs(t, err, &attemptsErr)
	require.Equal(t, &ControlOfferAttemptsError{
		Status:   aeron.BackPressured,
		Attempts: DefaultMessageRetryAttempts,
	}, attemptsErr)
}

func TestOfferAndAwait_offerTerminal(t *testing.T) {
	h := newLifecycleHarness(t)
	h.pub.offerResult = aeron.NotConnected

	_, err := h.controlOps(func() int { return 0 }).startRecording(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		archivewire.BooleanType_FALSE,
		[]byte(lifecycleRecordingChannel),
	)
	requireControlTerminal(t, err, aeron.NotConnected)
}

func TestAwaitListRecording_timeout(t *testing.T) {
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	descriptorPoller := NewRecordingDescriptorPoller()
	descriptorPoller.ResetAwait(8888)

	cfg := Config{
		ControlResponseTimeout: time.Millisecond,
		IdleStrategy:           NewDefaultIdleStrategy(),
		AeronConfig: aeron.Config{
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
		cfg,
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

func newTerminatingClientAwait(t testing.TB) (
	*aeron.AsyncConnectTestHarness,
	Config,
	func() int,
) {
	t.Helper()

	harness := aeron.NewAsyncConnectTestHarness(t)
	transmitClientTimeout(t, harness)

	cfg := Config{
		ControlResponseTimeout: time.Millisecond,
		IdleStrategy:           idlestrategy.NoOp,
		AeronConfig: aeron.Config{
			NanoClock: func() int64 { return 5_000_000_000 },
		},
	}
	pollFn := func() int {
		require.Fail(t, "polled after client termination")
		return 0
	}
	return harness, cfg, pollFn
}

func TestAwaitControlResponse_terminatingClient(t *testing.T) {
	harness, cfg, pollFn := newTerminatingClientAwait(t)
	_, err := AwaitControlResponse(
		t.Context(),
		harness.Client,
		cfg,
		pollFn,
		nil,
		1,
		2,
	)
	require.Equal(t, ErrTransportTerminating, err)
}

func TestAwaitListRecording_terminatingClient(t *testing.T) {
	harness, cfg, pollFn := newTerminatingClientAwait(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	descriptorPoller.ResetAwait(8)

	err := AwaitListRecording(
		t.Context(),
		harness.Client,
		cfg,
		pollFn,
		descriptorPoller,
	)
	require.Equal(t, ErrTransportTerminating, err)
}
