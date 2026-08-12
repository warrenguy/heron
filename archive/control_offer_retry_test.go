//go:build testhooks

package archive

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

type sequentialOfferResultControlPublication struct {
	results []int64
	calls   int
}

func (p *sequentialOfferResultControlPublication) Offer([]byte) int64 {
	if p.calls >= len(p.results) {
		return p.results[len(p.results)-1]
	}
	result := p.results[p.calls]
	p.calls++
	return result
}

func newSequentialOfferResultControlPublication(
	t *testing.T,
	results ...int64,
) *sequentialOfferResultControlPublication {
	t.Helper()

	require.NotEmpty(
		t,
		results,
		"sequentialOfferResultControlPublication requires at least one result",
	)

	return &sequentialOfferResultControlPublication{results: results}
}

func controlOfferRetryHarnessWithSequentialOffers(
	t *testing.T,
	results ...int64,
) (*aeron.AsyncConnectTestHarness, *sequentialOfferResultControlPublication) {
	t.Helper()

	return aeron.NewAsyncConnectTestHarness(t),
		newSequentialOfferResultControlPublication(t, results...)
}

func controlOfferRetryHarnessWithNanoClock(
	t *testing.T,
	startNs int64,
) (*aeron.AsyncConnectTestHarness, *atomic.Int64) {
	t.Helper()

	nowNs := &atomic.Int64{}
	nowNs.Store(startNs)
	return aeron.NewAsyncConnectTestHarness(t), nowNs
}

// recordingControlIdleStrategy counts Idle/Reset invocations (AdminAction + timeout oracle).
//
// totalIdleCalls is cumulative across Reset; idleCalls is the current epoch.
type recordingControlIdleStrategy struct {
	idleCalls      int
	totalIdleCalls int
	resetCalls     int
}

func (r *recordingControlIdleStrategy) Idle(_ int) {
	r.idleCalls++
	r.totalIdleCalls++
}

func (r *recordingControlIdleStrategy) Reset() {
	r.resetCalls++
	r.idleCalls = 0
}

// setupControlOfferRetryDoWorkRecorder installs a per-client DoWork counter.
func setupControlOfferRetryDoWorkRecorder(t *testing.T, client *aeron.Client) *int {
	t.Helper()

	var count int
	SetControlOfferRetryDoWorkRecorderForTest(t, client, func() { count++ })
	return &count
}

func requireControlTerminal(tb testing.TB, err error, wantStatus int64) {
	tb.Helper()

	var terminal *ControlTerminalError
	require.ErrorAs(tb, err, &terminal)
	require.Equal(tb, wantStatus, terminal.Status)
}

func requireControlTimeout(tb testing.TB, err error, wantStatus int64, wantTimeout time.Duration) {
	tb.Helper()

	var timeoutErr *ControlOfferTimeoutError
	require.ErrorAs(tb, err, &timeoutErr)
	require.Equal(tb, wantStatus, timeoutErr.Status)
	require.Equal(tb, wantTimeout, timeoutErr.Timeout)
}

func TestControlOfferUntilPositive_backPressuredRetry(t *testing.T) {
	cases := []struct {
		name   string
		cfg    func(idle *recordingControlIdleStrategy) ControlOfferConfig
		assert func(
			t *testing.T,
			idle *recordingControlIdleStrategy,
			doWorkCount *int,
			seqPub *sequentialOfferResultControlPublication,
		)
	}{
		{
			name: "then_success",
			cfg: func(idle *recordingControlIdleStrategy) ControlOfferConfig {
				return ControlOfferConfig{IdleStrategy: idle}
			},
			assert: func(
				t *testing.T,
				idle *recordingControlIdleStrategy,
				_ *int,
				seqPub *sequentialOfferResultControlPublication,
			) {
				require.Equal(t, 3, seqPub.calls)
				require.Equal(t, 2, idle.totalIdleCalls)
				require.Equal(t, 1, idle.resetCalls)
			},
		},
		{
			name: "do_work_count",
			cfg: func(*recordingControlIdleStrategy) ControlOfferConfig {
				return ControlOfferConfig{}
			},
			assert: func(
				t *testing.T,
				_ *recordingControlIdleStrategy,
				doWorkCount *int,
				_ *sequentialOfferResultControlPublication,
			) {
				require.Equal(t, 5, *doWorkCount)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(
				t,
				aeron.BackPressured,
				aeron.BackPressured,
				100,
			)
			idle := &recordingControlIdleStrategy{}
			doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)

			pos, err := controlOfferUntilPositive(
				t.Context(),
				harness.Client,
				Config{}.Normalize(),
				tc.cfg(idle),
				func() int64 {
					return seqPub.Offer(nil)
				},
			)
			require.NoError(t, err)
			require.Positive(t, pos)
			tc.assert(t, idle, doWorkCount, seqPub)
		})
	}
}

func TestControlOfferUntilPositive_immediateSuccess(t *testing.T) {
	idle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, 100)
	doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)

	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{IdleStrategy: idle},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, seqPub.calls)
	require.Equal(t, 1, *doWorkCount)
	require.Zero(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestControlOfferUntilPositive_pollFnOnRetriable(t *testing.T) {
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	var pollCalls int

	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{
			PollFn: func() int {
				pollCalls++
				return 0
			},
		},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 1, pollCalls)
}

func TestControlOfferUntilPositive_adminActionThenSuccess(t *testing.T) {
	idle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.AdminAction, 100)

	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{IdleStrategy: idle},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 2, seqPub.calls)
	require.Zero(t, idle.totalIdleCalls)
}

func TestControlOfferUntilPositive_terminalNoRetry(t *testing.T) {
	tests := []struct {
		name string
		code int64
	}{
		{name: "closed", code: aeron.Closed},
		{name: "maxPositionExceeded", code: aeron.MaxPositionExceeded},
		{name: "unsupportedMessageLength", code: aeron.UnsupportedMessageLength},
		{name: "notConnected", code: aeron.NotConnected},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, tc.code)

			_, err := controlOfferUntilPositive(
				t.Context(),
				harness.Client,
				Config{}.Normalize(),
				ControlOfferConfig{},
				func() int64 { return seqPub.Offer(nil) },
			)
			requireControlTerminal(t, err, tc.code)
			require.Equal(t, 1, seqPub.calls)
		})
	}
}

// Connect fixtures use a fixed nanoClock that never advances; a real 1ms timeout would
// spin forever.
//
// Advancing time inside offerFn after each BackPressured preserves the timeout oracle
// while still exercising IdleStrategy pacing and defer Reset.
func TestControlOfferUntilPositive_timeout(t *testing.T) {
	t.Run("singleAdvance", func(t *testing.T) {
		idle := &recordingControlIdleStrategy{}
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(int64(time.Millisecond))
			return aeron.BackPressured
		}

		_, err := controlOfferUntilPositive(
			t.Context(),
			harness.Client,
			Config{ControlOfferRetryTimeout: time.Millisecond}.Normalize(),
			ControlOfferConfig{
				IdleStrategy: idle,
				NanoClock:    func() int64 { return nowNs.Load() },
			},
			offerFn,
		)
		requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
		require.Equal(t, 1, offerCalls)
		require.Equal(t, 2, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Zero(t, idle.idleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("multiIteration", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		idle := &recordingControlIdleStrategy{}
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		_, err := controlOfferUntilPositive(
			t.Context(),
			harness.Client,
			Config{ControlOfferRetryTimeout: timeout}.Normalize(),
			ControlOfferConfig{
				IdleStrategy: idle,
				NanoClock:    func() int64 { return nowNs.Load() },
			},
			offerFn,
		)
		requireControlTimeout(t, err, aeron.BackPressured, timeout)
		require.Equal(t, 4, offerCalls)
		require.Equal(t, 8, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})
}

func TestControlOfferUntilPositive_timeoutZeroUsesDefault(t *testing.T) {
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 3_000_000_000)
	step := DefaultControlOfferRetryTimeout.Nanoseconds() / 2
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		nowNs.Add(step)
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{},
		ControlOfferConfig{
			NanoClock: func() int64 { return nowNs.Load() },
		},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, DefaultControlOfferRetryTimeout)
	require.Equal(t, 2, offerCalls)
}

func TestControlOfferUntilPositive_expiredDeadlineAttemptsOnce(t *testing.T) {
	const startNs int64 = 1_000_000_000
	var clockReads int
	clockFn := func() int64 {
		clockReads++
		if clockReads == 1 {
			return startNs
		}
		return startNs + int64(time.Hour)
	}
	harness := aeron.NewAsyncConnectTestHarness(t)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{ControlOfferRetryTimeout: time.Millisecond}.Normalize(),
		ControlOfferConfig{
			NanoClock: clockFn,
		},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
	require.Equal(t, 1, offerCalls)
}

func TestControlOfferUntilPositive_cfgNanoClockOverride(t *testing.T) {
	const startNs int64 = 1_000_000_000
	const frozenNs int64 = 42_000_000_000
	var cfgNowNs atomic.Int64
	cfgNowNs.Store(startNs)

	idle := &recordingControlIdleStrategy{}
	harness := aeron.NewAsyncConnectTestHarness(t)
	doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
	offerFn := func() int64 {
		cfgNowNs.Add(int64(time.Millisecond))
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{
			ControlOfferRetryTimeout: time.Millisecond,
			AeronConfig: aeron.Config{
				NanoClock: func() int64 { return frozenNs },
			},
		}.Normalize(),
		ControlOfferConfig{
			IdleStrategy: idle,
			NanoClock:    func() int64 { return cfgNowNs.Load() },
		},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
	require.Equal(t, 2, *doWorkCount)
	require.Positive(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestControlOfferUntilPositive_sessionConfigFallback(t *testing.T) {
	const timeout = time.Millisecond
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 7_000_000_000)
	sessionIdle := &recordingControlIdleStrategy{}
	stepNs := timeout.Nanoseconds() / 4
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		nowNs.Add(stepNs)
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{
			ControlOfferRetryTimeout: timeout,
			IdleStrategy:             sessionIdle,
			AeronConfig: aeron.Config{
				NanoClock: func() int64 { return nowNs.Load() },
			},
		},
		ControlOfferConfig{},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, timeout)
	require.Equal(t, 4, offerCalls, "empty cfg deadline runs on session AeronConfig.NanoClock")
	require.Equal(t, 4, sessionIdle.totalIdleCalls, "empty cfg paces via session Config.IdleStrategy")
	require.Equal(t, 1, sessionIdle.resetCalls)
}

func TestControlOfferUntilPositive_terminalAfterRetriableRetry(t *testing.T) {
	const startNs int64 = 9_000_000_000
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, startNs)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		if offerCalls == 1 {
			return aeron.BackPressured
		}
		return aeron.NotConnected
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{},
		offerFn,
	)
	requireControlTerminal(t, err, aeron.NotConnected)
	require.Equal(t, 2, offerCalls)
	require.Equal(t, startNs, nowNs.Load(), "returned on terminal code, not deadline expiry")
}

func TestControlOfferUntilPositive_terminalAfterCancel(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)

	cancelCtx, cancel := context.WithCancel(t.Context())
	pos, err := controlOfferUntilPositive(
		cancelCtx,
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{},
		func() int64 {
			cancel()
			return aeron.Closed
		},
	)
	require.Zero(t, pos)
	require.ErrorIs(t, err, context.Canceled)
}

func TestControlOfferUntilPositive_nilGuards(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return 0
	}

	tests := []struct {
		name        string
		aeronClient *aeron.Client
		offerFn     func() int64
		wantErr     error
	}{
		{name: "nilAeronClient", aeronClient: nil, offerFn: offerFn, wantErr: errNilControlAeronClient},
		{name: "nilOfferFn", aeronClient: harness.Client, offerFn: nil, wantErr: errNilControlOfferFn},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, err := controlOfferUntilPositive(
				t.Context(),
				tc.aeronClient,
				Config{}.Normalize(),
				ControlOfferConfig{},
				tc.offerFn,
			)
			require.Zero(t, pos)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
	require.Zero(t, offerCalls)
}

func TestControlOfferUntilPositive_contextCanceled(t *testing.T) {
	t.Run("preCanceled", func(t *testing.T) {
		idle := &recordingControlIdleStrategy{}
		harness := aeron.NewAsyncConnectTestHarness(t)

		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		var offerCalls int
		pos, err := controlOfferUntilPositive(
			cancelCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{IdleStrategy: idle},
			func() int64 {
				offerCalls++
				return 0
			},
		)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("midLoop", func(t *testing.T) {
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)

		cancelCtx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(int64(time.Millisecond))
			if offerCalls >= 2 {
				cancel()
			}
			return aeron.BackPressured
		}

		pos, err := controlOfferUntilPositive(
			cancelCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{},
			offerFn,
		)
		require.Zero(t, pos)
		require.Equal(t, 2, offerCalls)
		require.Equal(t, 4, *doWorkCount)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestControlOfferUntilPositive_contextDeadline(t *testing.T) {
	t.Run("preExpired", func(t *testing.T) {
		idle := &recordingControlIdleStrategy{}
		harness := aeron.NewAsyncConnectTestHarness(t)

		deadlineCtx, cancel := context.WithDeadline(
			t.Context(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		var offerCalls int
		pos, err := controlOfferUntilPositive(
			deadlineCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{IdleStrategy: idle},
			func() int64 {
				offerCalls++
				return aeron.BackPressured
			},
		)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("ctxDeadlineSooner", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)

		deadlineCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
		defer cancel()

		pos, err := controlOfferUntilPositive(
			deadlineCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{
				NanoClock: func() int64 { return time.Now().UnixNano() },
			},
			func() int64 { return aeron.BackPressured },
		)

		require.Zero(t, pos)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("cfgTimeoutSooner", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		_, err := controlOfferUntilPositive(
			deadlineCtx,
			harness.Client,
			Config{ControlOfferRetryTimeout: timeout}.Normalize(),
			ControlOfferConfig{
				NanoClock: func() int64 { return nowNs.Load() },
			},
			offerFn,
		)
		requireControlTimeout(t, err, aeron.BackPressured, timeout)
		require.Equal(t, 4, offerCalls)
	})
}

func TestClient_Config_nilReceiver(t *testing.T) {
	var client *Client
	require.Equal(t, Config{}, client.Config())
}

func TestClient_Config_returnsSessionConfig(t *testing.T) {
	want := Config{ControlOfferRetryTimeout: 3 * time.Second}.Normalize()
	client := newClient(&ConnectResult{Config: want})
	require.Equal(t, want, client.Config())
}

func TestClient_ControlOfferUntilPositive_nilSession(t *testing.T) {
	var client *Client
	var offerCalls int
	pos, err := client.ControlOfferUntilPositive(
		t.Context(),
		ControlOfferConfig{},
		func() int64 {
			offerCalls++
			return 1
		},
	)
	require.ErrorIs(t, err, errNilSession)
	require.Zero(t, pos)
	require.Zero(t, offerCalls)
}

func TestClient_ControlOfferUntilPositive_emptyCfgUsesSessionIdle(t *testing.T) {
	sessionIdle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	archive := newClient(&ConnectResult{
		Aeron: harness.Client,
		Config: Config{
			IdleStrategy: sessionIdle,
		}.Normalize(),
	})

	pos, err := archive.ControlOfferUntilPositive(
		t.Context(),
		ControlOfferConfig{},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 1, sessionIdle.totalIdleCalls, "empty cfg inherits session IdleStrategy")
	require.Equal(t, 1, sessionIdle.resetCalls)
}

func TestClient_ControlOfferUntilPositive_withSessionPoll(t *testing.T) {
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	archive := newClient(&ConnectResult{
		Aeron:  harness.Client,
		Config: Config{}.Normalize(),
	})
	var pollCalls int
	// Inline poll fn substitutes for archive.Poll so poll invocations are countable;
	// production callers pass archive.Poll or an operation-specific poll wrapper.
	pollFn := func() int {
		pollCalls++
		return 0
	}

	pos, err := archive.ControlOfferUntilPositive(
		t.Context(),
		ControlOfferConfig{PollFn: pollFn},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, pollCalls)
}

func TestClient_ControlOfferUntilPositive_closedSession(t *testing.T) {
	t.Run("ownedNilAeronClient", func(t *testing.T) {
		h := newLifecycleHarness(t)
		archive := h.newClient()
		archive.ownsAeronClient = true
		require.NoError(t, archive.Close())
		require.Nil(t, archive.aeronClient)
		require.True(t, archive.IsClosed())
		var offerCalls int

		pos, err := archive.ControlOfferUntilPositive(
			t.Context(),
			ControlOfferConfig{},
			func() int64 {
				offerCalls++
				return 1
			},
		)
		require.ErrorIs(t, err, errNilControlAeronClient)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
	})

	t.Run("borrowedClientTerminal", func(t *testing.T) {
		harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.Closed)
		archive := newClient(&ConnectResult{
			Aeron:           harness.Client,
			OwnsAeronClient: false,
			Config:          Config{}.Normalize(),
		})
		require.NoError(t, archive.Close())
		require.NotNil(t, archive.aeronClient)
		require.True(t, archive.IsClosed())

		pos, err := archive.ControlOfferUntilPositive(
			t.Context(),
			ControlOfferConfig{},
			func() int64 { return seqPub.Offer(nil) },
		)
		requireControlTerminal(t, err, aeron.Closed)
		require.Zero(t, pos)
		require.Equal(t, 1, seqPub.calls)
	})
}
