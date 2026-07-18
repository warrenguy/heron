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

// connectTestHarnessDriver registers an await-loop step observer that completes pending
// driver adds and optional per-step control-response simulation while Connect blocks.
func connectTestHarnessDriver(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	onStep func(ac *AsyncConnect),
) func() *AsyncConnect {
	t.Helper()

	var observedAC atomic.Pointer[AsyncConnect]
	SetConnectAsyncConnectObserverForTest(t, harness.Client, func(ac *AsyncConnect) {
		observedAC.Store(ac)
		registerConnectHarnessStepObserver(t, harness, ac, onStep)
	})
	return func() *AsyncConnect { return observedAC.Load() }
}

func registerConnectHarnessStepObserver(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
	onStep func(ac *AsyncConnect),
) {
	t.Helper()
	if ac == nil {
		return
	}
	SetAwaitConnectStepObserverForTest(t, ac, func() {
		asyncConnectHarnessStep(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
			h.RespondPendingAdds()
		})
		if onStep != nil {
			onStep(ac)
		}
	})
}

func connectConfigForTest(client *aeron.Client) Config {
	cfg := validConnectFixtureConfig()
	cfg.Aeron = client

	// Wall-clock NanoClock so ConnectTimeout can elapse in blocking connect tests.
	cfg.AeronConfig.NanoClock = nil
	return cfg
}

func ownedConnectConfigForTest(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
) Config {
	t.Helper()

	aeronDir := t.TempDir()
	SetConnectConstructionHooksForTest(t, aeronDir, func(Config) (*aeron.Client, error) {
		return harness.Client, nil
	})
	cfg := connectConfigForTest(nil)
	cfg.AeronDir = aeronDir
	cfg.ConnectTimeout = time.Second
	return cfg
}

func requireAsyncConnectClosedAfterConnectFailure(t *testing.T, ac *AsyncConnect) {
	t.Helper()

	require.NotNil(t, ac)
	require.Nil(t, ac.ControlResponseSubscriptionForTest())
	_, pollErr := ac.Poll()
	require.EqualError(t, pollErr, "archive: async connect closed")
}

func TestAwaitArchiveConnect_idleStrategyPacing(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		idle := &recordingControlIdleStrategy{}
		harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config {
			cfg = connectConfigForTest(cfg.Aeron)
			cfg.IdleStrategy = idle
			return cfg
		})
		t.Cleanup(func() { _ = ac.Close() })

		var steps int
		registerConnectHarnessStepObserver(t, harness, ac, func(ac *AsyncConnect) {
			steps++
			if steps < 3 {
				return
			}
			withAsyncConnectControlResponseTerm(t, ac, writeControlResponseOKForConnect)
		})

		result, err := awaitConnect(t.Context(), ac)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		idle := &recordingControlIdleStrategy{}
		harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config {
			cfg = connectConfigForTest(cfg.Aeron)
			cfg.IdleStrategy = idle
			return cfg
		})
		t.Cleanup(func() { _ = ac.Close() })

		registerConnectHarnessStepObserver(t, harness, ac, func(ac *AsyncConnect) {
			if ac.ControlResponseSubscriptionForTest() != nil {
				ac.deadlineNs = ac.nanoClock() - 1
			}
		})

		_, err := awaitConnect(t.Context(), ac)
		require.Error(t, err)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})
}

func TestAwaitArchiveConnect_success(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	t.Cleanup(func() { _ = ac.Close() })

	registerConnectHarnessStepObserver(t, harness, ac, func(ac *AsyncConnect) {
		withAsyncConnectControlResponseTerm(t, ac, writeControlResponseOKForConnect)
	})

	result, err := awaitConnect(t.Context(), ac)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.ControlSubscription)
	require.True(t, result.ControlSubscription.IsConnected())
	require.NotNil(t, result.ControlPublication)
	require.True(t, result.ControlPublication.IsConnected())
}

func TestConnect_canceled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		owned     bool
		preCancel bool
	}{
		{name: "already_canceled", preCancel: true},
		{name: "owned_client_already_canceled", owned: true, preCancel: true},
		{name: "canceled_during_handshake"},
		{name: "owned_client_canceled_during_handshake", owned: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			var cfg Config
			if tc.owned {
				cfg = ownedConnectConfigForTest(t, harness)
			} else {
				cfg = connectConfigForTest(harness.Client)
			}

			cancelCtx, cancel := context.WithCancel(t.Context())
			var onStep func(*AsyncConnect)
			if tc.preCancel {
				cancel()
			} else {
				t.Cleanup(cancel)
				cfg.ConnectTimeout = time.Minute
				var steps int
				onStep = func(*AsyncConnect) {
					steps++
					if steps >= 3 {
						cancel()
					}
				}
			}
			observedAC := connectTestHarnessDriver(t, harness, onStep)

			_, err := Connect(cancelCtx, cfg)
			require.ErrorIs(t, err, context.Canceled)
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())

			// Connect closes the transport client only when it constructed it.
			require.Equal(t, tc.owned, aeron.ClientIsClosedForTest(harness.Client))
		})
	}
}

func TestConnect_contextDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		connectTimeout  time.Duration
		ctxTimeout      time.Duration
		preExpired      bool
		wantErr         error
		wantErrContains string
		maxElapsed      time.Duration
	}{
		{
			name:           "preExpired",
			connectTimeout: time.Minute,
			preExpired:     true,
			wantErr:        context.DeadlineExceeded,
		},
		{
			name:           "ctxDeadlineSooner",
			connectTimeout: time.Minute,
			ctxTimeout:     5 * time.Millisecond,
			wantErr:        context.DeadlineExceeded,
			maxElapsed:     time.Second,
		},
		{
			name:            "connectTimeoutSooner",
			connectTimeout:  5 * time.Millisecond,
			ctxTimeout:      time.Minute,
			wantErrContains: "archive: connect timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			cfg := connectConfigForTest(harness.Client)
			cfg.ConnectTimeout = tc.connectTimeout

			var connectCtx context.Context
			var cancel context.CancelFunc
			if tc.preExpired {
				connectCtx, cancel = context.WithDeadline(
					t.Context(),
					time.Now().Add(-time.Second),
				)
			} else {
				connectCtx, cancel = context.WithTimeout(t.Context(), tc.ctxTimeout)
			}
			defer cancel()

			observedAC := connectTestHarnessDriver(t, harness, nil)

			start := time.Now()
			_, err := Connect(connectCtx, cfg)
			elapsed := time.Since(start)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.ErrorContains(t, err, tc.wantErrContains)
			}
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())

			if tc.maxElapsed > 0 {
				require.Less(t, elapsed, tc.maxElapsed)
			}
		})
	}
}

func TestConnect_invalidConfig(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)

	tests := []struct {
		name    string
		archive Config
		wantErr string
	}{
		{
			name:    "missingAeronDir",
			archive: Config{},
			wantErr: "archive: AeronDir is required when Aeron is not set",
		},
		{
			name: "negativeConnectTimeout",
			archive: Config{
				Aeron:          harness.Client,
				ConnectTimeout: -time.Second,
			},
			wantErr: "archive: ConnectTimeout must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Connect(t.Context(), tc.archive)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestConnect_contextWinsOverConnectTimeoutOnPollError(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client)
	cfg.ConnectTimeout = time.Minute

	connectCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		if ac.ControlResponseSubscriptionForTest() == nil {
			return
		}
		ac.deadlineNs = ac.nanoClock() - 1
		cancel()
	})

	_, err := Connect(connectCtx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "connect timeout")
}

func TestConnect_successWinsOverConcurrentCancel(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client)
	cfg.ConnectTimeout = time.Minute

	connectCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		switch ac.State() {
		case ConnectStatePollResponse:
			withAsyncConnectControlResponseTerm(t, ac, writeControlResponseOKForConnect)
		case ConnectStateConcludeConnect:
			cancel()
		}
	})

	archive, err := Connect(connectCtx, cfg)
	require.NoError(t, err)
	require.NotNil(t, archive)
	t.Cleanup(func() { _ = archive.Close() })

	require.Equal(t, asyncConnectFixtureControlSessionID, archive.ControlSessionID())
}
