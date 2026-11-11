//go:build testhooks

package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
	"github.com/warrenguy/luas/idlestrategy"
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
		awaitChecks{},
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
		awaitChecks{},
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

const (
	notConnectedLoopStartNs       int64 = 5_000_000_000
	notConnectedLoopTimeout             = 10 * time.Nanosecond
	notConnectedLoopCorrelationID int64 = 4711
)

// notConnectedLoop drives runControlAwaitLoop on an idle harness client with a
// test clock. Each subtest moves the clock through pollFn.
type notConnectedLoop struct {
	nowNs          int64
	transportFault func() error
	isConnected    func() bool
	matchFn        func() (bool, error)
}

func (loop *notConnectedLoop) run(
	tb testing.TB,
	ctx context.Context,
	pollFn func() int,
) error {
	tb.Helper()

	harness := aeron.NewAsyncConnectTestHarness(tb)
	loop.nowNs = notConnectedLoopStartNs
	matchFn := loop.matchFn

	if matchFn == nil {
		matchFn = func() (bool, error) { return false, nil }
	}

	return runControlAwaitLoop(
		ctx,
		harness.Client,
		Config{
			IdleStrategy: idlestrategy.NoOp,
			AeronConfig: aeron.Config{
				NanoClock: func() int64 { return loop.nowNs },
			},
		},
		pollFn,
		awaitChecks{transportFault: loop.transportFault, isConnected: loop.isConnected},
		notConnectedLoopTimeout,
		notConnectedLoopCorrelationID,
		ControlResponseAwaitOperation,
		matchFn,
	)
}

func requireControlResponseNotConnected(
	tb testing.TB,
	err error,
	wantOperation string,
	wantCorrelationID int64,
) {
	tb.Helper()

	var notConnectedErr *ControlResponseNotConnectedError
	require.ErrorAs(tb, err, &notConnectedErr)
	require.Equal(tb, wantOperation, notConnectedErr.Operation)
	require.Equal(tb, wantCorrelationID, notConnectedErr.CorrelationID)
}

func TestRunControlAwaitLoop_notConnected(t *testing.T) {
	t.Run("empty_poll_not_connected", func(t *testing.T) {
		var polls, checks int
		loop := &notConnectedLoop{}
		loop.isConnected = func() bool {
			checks++
			return checks < 2
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			loop.nowNs++
			return 0
		})
		requireControlResponseNotConnected(
			t,
			err,
			ControlResponseAwaitOperation,
			notConnectedLoopCorrelationID,
		)
		require.Equal(t, 2, polls)
		require.Equal(t, 2, checks)
	})

	t.Run("busy_poll_skips_check", func(t *testing.T) {
		var polls, checks int
		loop := &notConnectedLoop{}
		loop.isConnected = func() bool {
			checks++
			return false
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			loop.nowNs++

			if polls <= 3 {
				return 1
			}
			return 0
		})
		requireControlResponseNotConnected(
			t,
			err,
			ControlResponseAwaitOperation,
			notConnectedLoopCorrelationID,
		)
		require.Equal(t, 4, polls)
		require.Equal(t, 1, checks)
	})

	t.Run("match_wins", func(t *testing.T) {
		matchErr := errors.New("latched match")
		var polls, checks, matches int
		loop := &notConnectedLoop{
			isConnected: func() bool {
				checks++
				return false
			},
			matchFn: func() (bool, error) {
				matches++
				return matches >= 2, matchErr
			},
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			loop.nowNs++
			return 0
		})
		require.ErrorIs(t, err, matchErr)
		require.Equal(t, 1, polls)
		require.Zero(t, checks)
	})

	t.Run("pre_loop_match_wins", func(t *testing.T) {
		matchErr := errors.New("latched match")
		var polls, checks int
		loop := &notConnectedLoop{
			isConnected: func() bool {
				checks++
				return false
			},
			matchFn: func() (bool, error) { return true, matchErr },
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			return 0
		})
		require.ErrorIs(t, err, matchErr)
		require.Zero(t, polls)
		require.Zero(t, checks)
	})

	t.Run("disconnect_wins_over_deadline", func(t *testing.T) {
		loop := &notConnectedLoop{isConnected: func() bool { return false }}

		err := loop.run(t, t.Context(), func() int {
			loop.nowNs = notConnectedLoopStartNs + notConnectedLoopTimeout.Nanoseconds()
			return 0
		})
		requireControlResponseNotConnected(
			t,
			err,
			ControlResponseAwaitOperation,
			notConnectedLoopCorrelationID,
		)

		var timeoutErr *ControlResponseTimeoutError
		require.NotErrorAs(t, err, &timeoutErr)
	})

	t.Run("context_error_wins", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		loop := &notConnectedLoop{
			isConnected: func() bool {
				cancel()
				return false
			},
		}

		err := loop.run(t, ctx, func() int {
			loop.nowNs++
			return 0
		})
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, ErrNotConnected)
	})

	t.Run("connected_times_out", func(t *testing.T) {
		var polls, emptyPolls, checks int
		loop := &notConnectedLoop{
			isConnected: func() bool {
				checks++
				return true
			},
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			loop.nowNs++

			if polls%2 == 0 {
				return 1
			}

			emptyPolls++
			return 0
		})
		requireControlResponseTimeout(
			t,
			err,
			ControlResponseAwaitOperation,
			notConnectedLoopCorrelationID,
			notConnectedLoopTimeout,
		)
		require.Positive(t, emptyPolls)
		require.Equal(t, emptyPolls, checks)
	})

	t.Run("transport_fault_wins", func(t *testing.T) {
		faultErr := errors.New("transport fault")
		var polls, checks int
		loop := &notConnectedLoop{
			transportFault: func() error { return faultErr },
			isConnected: func() bool {
				checks++
				return false
			},
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			loop.nowNs++
			return 0
		})
		require.ErrorIs(t, err, faultErr)
		require.NotErrorIs(t, err, ErrNotConnected)
		require.Zero(t, polls)
		require.Zero(t, checks)
	})

	t.Run("transport_fault_during_poll_wins", func(t *testing.T) {
		faultErr := errors.New("transport fault")
		var polls int
		loop := &notConnectedLoop{
			transportFault: func() error {
				if polls == 0 {
					return nil
				}
				return faultErr
			},
			isConnected: func() bool { return false },
		}

		err := loop.run(t, t.Context(), func() int {
			polls++
			loop.nowNs++
			return 0
		})
		require.ErrorIs(t, err, faultErr)
		require.NotErrorIs(t, err, ErrNotConnected)
		require.Equal(t, 1, polls)
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
			awaitChecks{transportFault: func() error {
				checks++
				if checks >= 2 {
					return faultErr
				}
				return nil
			}},
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
			awaitChecks{transportFault: func() error {
				checks++
				return faultErr
			}},
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
			awaitChecks{transportFault: func() error {
				cancel()
				return faultErr
			}},
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
		awaitChecks{},
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
		awaitChecks{},
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
