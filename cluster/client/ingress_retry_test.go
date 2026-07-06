//go:build testhooks

package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// setupIngressRetryDoWorkRecorder installs a global DoWork counter.
//
// Do not use from t.Parallel() subtests; the recorder is process-wide and unsynchronized.
func setupIngressRetryDoWorkRecorder(t *testing.T) *int {
	t.Helper()

	var count int
	SetIngressRetryDoWorkRecorderForTest(t, func() { count++ })
	return &count
}

func clusterWithSequentialIngress(
	t *testing.T,
	results ...int64,
) (*AeronCluster, *sequentialOfferResultIngressPublication) {
	t.Helper()

	var seqPub *sequentialOfferResultIngressPublication
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		seqPub = withSequentialOfferResultIngress(t, result, results...)
	})
	return ac, seqPub
}

// clusterWithAtomicNanoClock wires session.nanoClock to nowNs so tests advance the
// retry deadline from inside offerFn.
func clusterWithAtomicNanoClock(t *testing.T, startNs int64) (*AeronCluster, *atomic.Int64) {
	t.Helper()

	nowNs := &atomic.Int64{}
	nowNs.Store(startNs)
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		withAtomicNanoClock(result, nowNs)
	})
	return ac, nowNs
}

// recordingIdleStrategy counts Idle/Reset invocations (AdminAction + timeout oracle).
//
// totalIdleCalls is cumulative across Reset; idleCalls is the current epoch.
type recordingIdleStrategy struct {
	idleCalls      int
	totalIdleCalls int
	lastWorkCount  int
	resetCalls     int
}

func (r *recordingIdleStrategy) Idle(workCount int) {
	r.idleCalls++
	r.totalIdleCalls++
	r.lastWorkCount = workCount
}

func (r *recordingIdleStrategy) Reset() {
	r.resetCalls++
	r.idleCalls = 0
}

func TestIngressRetriableCode_delegatesToIsIngressRetriable(t *testing.T) {
	require.Equal(
		t,
		aeron.IsIngressRetriable(aeron.BackPressured),
		IngressRetriableCode(aeron.BackPressured),
	)
	require.Equal(
		t,
		aeron.IsIngressRetriable(aeron.NotConnected),
		IngressRetriableCode(aeron.NotConnected),
	)
}

func TestIngressOfferUntilPositive_backPressuredThenSuccess(t *testing.T) {
	idle := &recordingIdleStrategy{}
	ac, seqPub := clusterWithSequentialIngress(
		t,
		aeron.BackPressured,
		aeron.BackPressured,
		100,
	)
	doWorkCount := setupIngressRetryDoWorkRecorder(t)

	pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
		IdleStrategy: idle,
	}, ac.SendKeepAlive)
	require.Positive(t, pos)
	require.Equal(t, 3, seqPub.calls)
	require.Equal(t, 5, *doWorkCount)
	require.Equal(t, 2, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestIngressOfferUntilPositive_immediateSuccess(t *testing.T) {
	idle := &recordingIdleStrategy{}
	ac, seqPub := clusterWithSequentialIngress(t, 100)
	doWorkCount := setupIngressRetryDoWorkRecorder(t)

	pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
		IdleStrategy: idle,
	}, ac.SendKeepAlive)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, seqPub.calls)
	require.Equal(t, 1, *doWorkCount)
	require.Zero(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestIngressOfferUntilPositive_adminActionRetry(t *testing.T) {
	tests := []struct {
		name         string
		results      []int64
		wantSeqCalls int
		wantDoWork   int
	}{
		{
			name:         "singleRetry",
			results:      []int64{aeron.AdminAction, 100},
			wantSeqCalls: 2,
			wantDoWork:   2,
		},
		{
			name:         "multiRetry",
			results:      []int64{aeron.AdminAction, aeron.AdminAction, 100},
			wantSeqCalls: 3,
			wantDoWork:   3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idle := &recordingIdleStrategy{}
			ac, seqPub := clusterWithSequentialIngress(t, tc.results...)
			doWorkCount := setupIngressRetryDoWorkRecorder(t)

			pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
				IdleStrategy: idle,
			}, ac.SendKeepAlive)
			require.Positive(t, pos)
			require.Equal(t, tc.wantSeqCalls, seqPub.calls)
			require.Equal(t, tc.wantDoWork, *doWorkCount)
			require.Zero(t, idle.totalIdleCalls)
			require.Equal(t, 1, idle.resetCalls)
		})
	}
}

func TestIngressOfferUntilPositive_terminalNoRetry(t *testing.T) {
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
			ac, seqPub := clusterWithSequentialIngress(t, tc.code)

			pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{}, ac.SendKeepAlive)
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
func TestIngressOfferUntilPositive_timeout(t *testing.T) {
	t.Run("singleAdvance", func(t *testing.T) {
		idle := &recordingIdleStrategy{}
		ac, nowNs := clusterWithAtomicNanoClock(t, 5_000_000_000)
		doWorkCount := setupIngressRetryDoWorkRecorder(t)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(int64(time.Millisecond))
			return aeron.BackPressured
		}

		pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
			Timeout:      time.Millisecond,
			IdleStrategy: idle,
		}, offerFn)
		require.Equal(t, aeron.BackPressured, pos)
		require.Equal(t, 1, offerCalls)
		require.Equal(t, 2, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Zero(t, idle.idleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("multiIteration", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		idle := &recordingIdleStrategy{}
		ac, nowNs := clusterWithAtomicNanoClock(t, 5_000_000_000)
		doWorkCount := setupIngressRetryDoWorkRecorder(t)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
			Timeout:      timeout,
			IdleStrategy: idle,
		}, offerFn)
		require.Equal(t, aeron.BackPressured, pos)
		require.Equal(t, 4, offerCalls)
		require.Equal(t, 8, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})
}

func TestIngressOfferUntilPositive_cfgNanoClockOverride(t *testing.T) {
	const startNs int64 = 1_000_000_000
	const frozenNs int64 = 42_000_000_000
	var cfgNowNs atomic.Int64
	cfgNowNs.Store(startNs)

	idle := &recordingIdleStrategy{}
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		mutateConnectContext(result, func(ctx *ClusterContext) {
			ctx.AeronContext.NanoClock = func() int64 { return frozenNs }
		})
	})
	doWorkCount := setupIngressRetryDoWorkRecorder(t)
	offerFn := func() int64 {
		cfgNowNs.Add(int64(time.Millisecond))
		return aeron.BackPressured
	}

	pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
		Timeout:      time.Millisecond,
		IdleStrategy: idle,
		NanoClock:    func() int64 { return cfgNowNs.Load() },
	}, offerFn)
	require.Equal(t, aeron.BackPressured, pos)
	require.Equal(t, 2, *doWorkCount)
	require.Positive(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestIngressOfferUntilPositive_timeoutZeroUsesDefault(t *testing.T) {
	step := DefaultIngressOfferTimeout.Nanoseconds() / 2
	ac, nowNs := clusterWithAtomicNanoClock(t, 3_000_000_000)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		nowNs.Add(step)
		return aeron.BackPressured
	}

	pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
		Timeout: 0,
	}, offerFn)
	require.Equal(t, aeron.BackPressured, pos)
	require.Equal(t, 2, offerCalls)
}

func TestIngressOfferUntilPositive_expiredDeadlineAttemptsOnce(t *testing.T) {
	const startNs int64 = 1_000_000_000
	var clockReads int
	clockFn := func() int64 {
		clockReads++
		if clockReads == 1 {
			return startNs
		}
		return startNs + int64(time.Hour)
	}
	ac := newAeronClusterFromConnectFixture(t, nil)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return aeron.BackPressured
	}

	pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
		Timeout:   time.Millisecond,
		NanoClock: clockFn,
	}, offerFn)
	require.Equal(t, aeron.BackPressured, pos)
	require.Equal(t, 1, offerCalls)
}

func TestIngressOfferUntilPositive_nilGuards(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return 0
	}

	tests := []struct {
		name        string
		aeronClient *aeron.Client
		session     *AeronCluster
		offerFn     func() int64
	}{
		{name: "nilAeronClient", aeronClient: nil, session: ac, offerFn: offerFn},
		{name: "nilSession", aeronClient: ac.Aeron(), session: nil, offerFn: offerFn},
		{name: "nilOfferFn", aeronClient: ac.Aeron(), session: ac, offerFn: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				aeron.NotConnected,
				IngressOfferUntilPositive(tc.aeronClient, tc.session, IngressOfferConfig{}, tc.offerFn),
			)
		})
	}
	require.Zero(t, offerCalls)
}

// Simulates post-PollEgress terminal NotConnected on the second offer (BP then NC).
//
// Fixed NanoClock never advances; if the helper kept retrying BackPressured it would
// spin until test timeout rather than return NotConnected.
func TestIngressOfferUntilPositive_terminalAfterRetriableRetry(t *testing.T) {
	const startNs int64 = 9_000_000_000
	ac, nowNs := clusterWithAtomicNanoClock(t, startNs)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		if offerCalls == 1 {
			return aeron.BackPressured
		}
		return aeron.NotConnected
	}

	pos := IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{
		Timeout: DefaultIngressOfferTimeout,
	}, offerFn)
	require.Equal(t, aeron.NotConnected, pos)
	require.Equal(t, 2, offerCalls)
	require.Equal(t, startNs, nowNs.Load(), "returned on terminal code, not deadline expiry")
}

func TestIngressOfferUntilPositive_usesContextIdleStrategy(t *testing.T) {
	tests := []struct {
		name string
		call func(ac *AeronCluster) int64
	}{
		{
			name: "package",
			call: func(ac *AeronCluster) int64 {
				return IngressOfferUntilPositive(ac.Aeron(), ac, IngressOfferConfig{}, ac.SendKeepAlive)
			},
		},
		{
			name: "session",
			call: func(ac *AeronCluster) int64 {
				return ac.IngressOfferUntilPositive(IngressOfferConfig{}, ac.SendKeepAlive)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idle := &recordingIdleStrategy{}
			var seqPub *sequentialOfferResultIngressPublication
			ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
				result.Context.IdleStrategy = idle
				seqPub = withSequentialOfferResultIngress(
					t,
					result,
					aeron.BackPressured,
					aeron.BackPressured,
					100,
				)
			})

			pos := tc.call(ac)

			require.Positive(t, pos)
			require.Equal(t, 3, seqPub.calls)
			require.Equal(t, 2, idle.totalIdleCalls)
			require.Equal(t, 1, idle.resetCalls)
		})
	}
}

func TestSendKeepAliveUntilPositive_delegates(t *testing.T) {
	tests := []struct {
		name string
		call func(ac *AeronCluster) int64
	}{
		{
			name: "package",
			call: func(ac *AeronCluster) int64 {
				return SendKeepAliveUntilPositive(ac.Aeron(), ac, IngressOfferConfig{})
			},
		},
		{
			name: "session",
			call: func(ac *AeronCluster) int64 {
				return ac.SendKeepAliveUntilPositive(IngressOfferConfig{})
			},
		},
		{
			name: "ingressOfferUntilPositive",
			call: func(ac *AeronCluster) int64 {
				return ac.IngressOfferUntilPositive(IngressOfferConfig{}, ac.SendKeepAlive)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac, seqPub := clusterWithSequentialIngress(t, aeron.AdminAction, 100)

			pos := tc.call(ac)
			require.Positive(t, pos)
			require.Equal(t, 2, seqPub.calls)
		})
	}
}

func TestAeronCluster_IngressOfferUntilPositive_closedNilAeron(t *testing.T) {
	t.Run("offerFn", func(t *testing.T) {
		ac := closedOwnedNilAeronCluster(t, nil)
		var offerCalls int

		pos := ac.IngressOfferUntilPositive(IngressOfferConfig{}, func() int64 {
			offerCalls++
			return 1
		})

		require.Equal(t, aeron.NotConnected, pos)
		require.Zero(t, offerCalls)
	})

	t.Run("sendKeepAlive", func(t *testing.T) {
		var seqPub *sequentialOfferResultIngressPublication
		ac := closedOwnedNilAeronCluster(t, func(result *ConnectResult) {
			seqPub = withSequentialOfferResultIngress(t, result, 100)
		})

		callsBefore := seqPub.calls
		pos := ac.SendKeepAliveUntilPositive(IngressOfferConfig{})

		require.Equal(t, aeron.NotConnected, pos)
		require.Equal(t, callsBefore, seqPub.calls)
	})

	t.Run("borrowedClient", func(t *testing.T) {
		var seqPub *sequentialOfferResultIngressPublication
		ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
			seqPub = withSequentialOfferResultIngress(t, result, 100)
		})
		require.NoError(t, ac.Close())
		require.NotNil(t, ac.Aeron())
		require.True(t, ac.IsClosed())

		pos := ac.SendKeepAliveUntilPositive(IngressOfferConfig{})
		require.Equal(t, aeron.Closed, pos)
		require.Equal(t, 1, seqPub.calls)
	})
}
