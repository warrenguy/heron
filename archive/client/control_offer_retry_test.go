//go:build testhooks

package client

import (
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

// setupControlOfferRetryDoWorkRecorder installs a global DoWork counter.
//
// Do not use from t.Parallel() subtests; the recorder is process-wide and unsynchronized.
func setupControlOfferRetryDoWorkRecorder(t *testing.T) *int {
	t.Helper()

	var count int
	SetControlOfferRetryDoWorkRecorderForTest(t, func() { count++ })
	return &count
}

func TestControlRetriableCode_delegatesToIsIngressRetriable(t *testing.T) {
	require.Equal(
		t,
		aeron.IsIngressRetriable(aeron.BackPressured),
		ControlRetriableCode(aeron.BackPressured),
	)
	require.Equal(
		t,
		aeron.IsIngressRetriable(aeron.NotConnected),
		ControlRetriableCode(aeron.NotConnected),
	)
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
			doWorkCount := setupControlOfferRetryDoWorkRecorder(t)

			pos := ControlOfferUntilPositive(
				harness.Client,
				ArchiveContext{}.Normalize(),
				tc.cfg(idle),
				func() int64 {
					return seqPub.Offer(nil)
				},
			)
			require.Positive(t, pos)
			tc.assert(t, idle, doWorkCount, seqPub)
		})
	}
}

func TestControlOfferUntilPositive_immediateSuccess(t *testing.T) {
	idle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, 100)
	doWorkCount := setupControlOfferRetryDoWorkRecorder(t)

	pos := ControlOfferUntilPositive(
		harness.Client,
		ArchiveContext{}.Normalize(),
		ControlOfferConfig{IdleStrategy: idle},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, seqPub.calls)
	require.Equal(t, 1, *doWorkCount)
	require.Zero(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestControlOfferUntilPositive_pollFnOnRetriable(t *testing.T) {
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	var pollCalls int

	pos := ControlOfferUntilPositive(
		harness.Client,
		ArchiveContext{}.Normalize(),
		ControlOfferConfig{
			PollFn: func() int {
				pollCalls++
				return 0
			},
		},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.Positive(t, pos)
	require.Equal(t, 1, pollCalls)
}

func TestControlOfferUntilPositive_adminActionThenSuccess(t *testing.T) {
	idle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.AdminAction, 100)

	pos := ControlOfferUntilPositive(
		harness.Client,
		ArchiveContext{}.Normalize(),
		ControlOfferConfig{IdleStrategy: idle},
		func() int64 { return seqPub.Offer(nil) },
	)
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

			pos := ControlOfferUntilPositive(
				harness.Client,
				ArchiveContext{}.Normalize(),
				ControlOfferConfig{},
				func() int64 { return seqPub.Offer(nil) },
			)
			require.Equal(t, tc.code, pos)
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
	idle := &recordingControlIdleStrategy{}
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	doWorkCount := setupControlOfferRetryDoWorkRecorder(t)
	offerFn := func() int64 {
		nowNs.Add(int64(time.Millisecond))
		return aeron.BackPressured
	}

	pos := ControlOfferUntilPositive(
		harness.Client,
		ArchiveContext{ControlOfferRetryTimeout: time.Millisecond}.Normalize(),
		ControlOfferConfig{
			IdleStrategy: idle,
			NanoClock:    func() int64 { return nowNs.Load() },
		},
		offerFn,
	)
	require.Equal(t, aeron.BackPressured, pos)
	require.Equal(t, 2, *doWorkCount)
	require.Positive(t, idle.totalIdleCalls)
	require.Zero(t, idle.idleCalls)
	require.Equal(t, 1, idle.resetCalls)
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

	pos := ControlOfferUntilPositive(
		harness.Client,
		ArchiveContext{}.Normalize(),
		ControlOfferConfig{
			NanoClock: func() int64 { return nowNs.Load() },
		},
		offerFn,
	)
	require.Equal(t, aeron.BackPressured, pos)
	require.Equal(t, 2, offerCalls)
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
	}{
		{name: "nilAeronClient", aeronClient: nil, offerFn: offerFn},
		{name: "nilOfferFn", aeronClient: harness.Client, offerFn: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				aeron.NotConnected,
				ControlOfferUntilPositive(
					tc.aeronClient,
					ArchiveContext{}.Normalize(),
					ControlOfferConfig{},
					tc.offerFn,
				),
			)
		})
	}
	require.Zero(t, offerCalls)
}
