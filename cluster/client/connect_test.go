//go:build testhooks

package client

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// connectTestHarnessDriver registers an await-loop step observer that completes pending
// driver adds and optional per-step egress simulation while Connect blocks.
//
// A background goroutine calling DoWork and RespondPendingAdds races the blocking
// thread's Poll under the Aeron client lock.
// The step observer runs harness steps on the same goroutine as awaitClusterConnect.
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
	SetAwaitClusterConnectStepObserverForTest(t, ac, func() {
		asyncConnectHarnessStep(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
			h.RespondPendingAdds()
		})
		if onStep != nil {
			onStep(ac)
		}
	})
}

// connectConfigForTest wraps baseAsyncConnectConfig with fields Connect.Validate requires.
func connectConfigForTest(client *aeron.Client, ingressEndpoints string) Config {
	cfg := baseAsyncConnectConfig(client, ingressEndpoints)
	cfg.EgressHandler = &EgressHandlers{}
	cfg.CredentialsSupplier = NullCredentialsSupplier{}
	return cfg
}

func ownedConnectConfigForTest(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	factory func(Config) (*aeron.Client, error),
) Config {
	t.Helper()

	aeronDir := t.TempDir()
	SetConnectConstructionHooksForTest(t, aeronDir, factory)
	cfg := connectConfigForTest(nil, "")
	cfg.AeronDir = aeronDir
	cfg.MessageTimeout = time.Second
	return cfg
}

func connectConfigWithInvalidEndpoints(cfg Config) Config {
	cfg.IngressChannel = "aeron:udp?ttl=16"
	cfg.IngressEndpoints = "not-valid-endpoints"
	return cfg
}

func requireAsyncConnectClosedAfterConnectFailure(t *testing.T, ac *AsyncConnect) {
	t.Helper()

	require.NotNil(t, ac)
	require.Nil(t, ac.EgressSubscriptionForTest())
	_, pollErr := ac.Poll()
	require.EqualError(t, pollErr, "cluster client: async connect closed")
}

func TestAwaitClusterConnect_idleStrategyPacing(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		idle := &recordingIdleStrategy{}
		harness, ac := setupAsyncConnectWithConfig(t, func(cfg Config) Config {
			cfg = connectConfigForTest(cfg.Aeron, "")
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
			withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
		})

		result, err := awaitClusterConnect(t.Context(), ac)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Positive(t, idle.totalIdleCalls)
		require.Zero(t, idle.lastWorkCount)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		idle := &recordingIdleStrategy{}
		harness, ac := setupAsyncConnectWithConfig(t, func(cfg Config) Config {
			cfg = connectConfigForTest(cfg.Aeron, "")
			cfg.IdleStrategy = idle
			return cfg
		})
		t.Cleanup(func() { _ = ac.Close() })

		registerConnectHarnessStepObserver(t, harness, ac, func(ac *AsyncConnect) {
			if ac.EgressSubscriptionForTest() != nil {
				ac.deadlineNs = ac.nanoClock() - 1
			}
		})

		_, err := awaitClusterConnect(t.Context(), ac)
		require.Error(t, err)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})
}

func TestAwaitClusterConnect_success(t *testing.T) {
	t.Parallel()

	// Uses the step-observer harness driver; see connectTestHarnessDriver.
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	registerConnectHarnessStepObserver(t, harness, ac, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})

	result, err := awaitClusterConnect(t.Context(), ac)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.EgressSubscription)
	require.True(t, result.EgressSubscription.IsConnected())
	require.NotNil(t, result.IngressPublication)
	require.True(t, result.IngressPublication.IsConnected())
}

func TestConnect_canceled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		buildCtx         func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config
		preCancel        bool
		onStep           func(cancel context.CancelFunc) func(*AsyncConnect)
		wantClientClosed bool
	}{
		{
			name: "already_canceled",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				return connectConfigForTest(harness.Client, "")
			},
			preCancel:        true,
			wantClientClosed: false,
		},
		{
			name: "owned_client_already_canceled",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				return ownedConnectConfigForTest(t, harness, func(Config) (*aeron.Client, error) {
					return harness.Client, nil
				})
			},
			preCancel:        true,
			wantClientClosed: true,
		},
		{
			name: "canceled_during_handshake",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := connectConfigForTest(harness.Client, "")
				cfg.MessageTimeout = time.Minute
				return cfg
			},
			onStep: func(cancel context.CancelFunc) func(*AsyncConnect) {
				var steps int
				return func(ac *AsyncConnect) {
					steps++
					if steps >= 3 {
						cancel()
					}
				}
			},
			wantClientClosed: false,
		},
		{
			name: "owned_client_canceled_during_handshake",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := ownedConnectConfigForTest(
					t, harness, func(Config) (*aeron.Client, error) {
						return harness.Client, nil
					},
				)
				cfg.MessageTimeout = time.Minute
				return cfg
			},
			onStep: func(cancel context.CancelFunc) func(*AsyncConnect) {
				var steps int
				return func(ac *AsyncConnect) {
					steps++
					if steps >= 3 {
						cancel()
					}
				}
			},
			wantClientClosed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			cfg := tc.buildCtx(t, harness)

			cancelCtx, cancel := context.WithCancel(t.Context())
			if tc.preCancel {
				cancel()
			} else {
				t.Cleanup(cancel)
			}

			var onStep func(*AsyncConnect)
			if tc.onStep != nil {
				onStep = tc.onStep(cancel)
			}
			observedAC := connectTestHarnessDriver(t, harness, onStep)

			_, err := Connect(cancelCtx, cfg)
			require.ErrorIs(t, err, context.Canceled)
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
			require.Equal(t, tc.wantClientClosed, aeron.ClientIsClosedForTest(harness.Client))
		})
	}
}

func TestConnect_concludeConnect_setsNewLeaderTimeout(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client, "")

	const heartbeatNs = int64(250_000_000)
	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
				writeSessionEventWithHeartbeatForConnect(t, termBuffer, correlation, heartbeatNs)
			},
		)
	})

	session, err := Connect(t.Context(), cfg)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })
	require.Equal(t, 2*time.Duration(heartbeatNs), session.Config().NewLeaderTimeout)
}

func TestConnect_success(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client, "")

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})

	session, err := Connect(t.Context(), cfg)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })

	require.NotZero(t, session.ClusterSessionID())
	require.NotNil(t, session.IngressPublication())
	require.True(t, session.IngressPublication().IsConnected())
	require.NotNil(t, session.EgressSubscription())
	require.True(t, session.EgressSubscription().IsConnected())
}

func TestConnect_success_ownedClient(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := ownedConnectConfigForTest(t, harness, func(Config) (*aeron.Client, error) {
		return harness.Client, nil
	})
	cfg.MessageTimeout = 30 * time.Second

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})

	require.False(t, aeron.ClientIsClosedForTest(harness.Client))

	session, err := Connect(t.Context(), cfg)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })

	require.NotZero(t, session.ClusterSessionID())
	require.NotNil(t, session.IngressPublication())
	require.True(t, session.IngressPublication().IsConnected())
	require.NotNil(t, session.EgressSubscription())
	require.True(t, session.EgressSubscription().IsConnected())
	require.False(t, aeron.ClientIsClosedForTest(harness.Client))

	require.NoError(t, session.Close())
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
}

// ownedHarnessClientFactory returns an owned-client factory. The factory
// installs the connect ErrorHandler on the harness client, then returns that
// client.
func ownedHarnessClientFactory(
	harness *aeron.AsyncConnectTestHarness,
) func(Config) (*aeron.Client, error) {
	return func(cfg Config) (*aeron.Client, error) {
		harness.SetErrorHandler(cfg.AeronConfig.ErrorHandler)
		return harness.Client, nil
	}
}

func transmitClientTimeout(t *testing.T, harness *aeron.AsyncConnectTestHarness) {
	t.Helper()

	require.NoError(t, harness.TransmitClientTimeout())
}

// transmitUnmappableImage broadcasts an available image for sub whose log
// file does not exist. The next harness client DoWork reports the map failure
// and keeps the client running.
func transmitUnmappableImage(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	sub *aeron.Subscription,
) {
	t.Helper()

	event := conductor.AvailableImage{
		CorrelationID:              9001,
		SessionID:                  9002,
		StreamID:                   sub.StreamID(),
		SubscriptionRegistrationID: sub.RegistrationID(),
		LogFileName:                filepath.Join(harness.MediaDriverDir, "missing.logbuffer"),
		SourceIdentity:             "aeron:ipc",
	}
	require.NoError(t, harness.Fixture().Transmit(
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(event),
	))
}

// connectHarnessSession runs Connect over the harness client. Cleanup closes
// the session.
func connectHarnessSession(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	cfg Config,
) *AeronCluster {
	t.Helper()

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})
	session, err := Connect(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func requireTransportFaultClosed(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	session *AeronCluster,
	cause error,
) {
	t.Helper()

	require.True(t, session.IsClosed())
	require.ErrorIs(t, session.TransportFault(), ErrTransportTerminating)
	require.ErrorIs(t, session.TransportFault(), cause)
	require.Nil(t, session.Aeron())
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
}

func requireSessionOpen(t *testing.T, session *AeronCluster) {
	t.Helper()

	require.False(t, session.IsClosed())
	require.NoError(t, session.TransportFault())
}

// TestAeronCluster_ownedClientFaultClosesSession asserts that a fault that
// terminates the connect-owned transport client closes the session on the
// next PollEgress. A per-resource fault and a caller-supplied client leave the
// session open.
func TestAeronCluster_ownedClientFaultClosesSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		callerHandler  bool
		callerSupplied bool
		perResource    bool
		spin           bool
		wantClosed     bool
	}{
		{name: "default_handler_client_timeout", wantClosed: true},
		{name: "caller_handler_client_timeout", callerHandler: true, wantClosed: true},
		{name: "per_resource_error_keeps_session", perResource: true},
		{name: "per_resource_error_caller_handler_keeps_session", callerHandler: true, perResource: true},
		{name: "caller_supplied_client_unchanged", callerHandler: true, callerSupplied: true},
		{name: "spin_once", spin: true, wantClosed: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			var callerErrs []error
			var callerHandler func(error)
			if tc.callerHandler {
				callerHandler = func(err error) { callerErrs = append(callerErrs, err) }
			}
			var cfg Config
			if tc.callerSupplied {
				harness.SetErrorHandler(callerHandler)
				cfg = connectConfigForTest(harness.Client, "")
			} else {
				cfg = ownedConnectConfigForTest(t, harness, ownedHarnessClientFactory(harness))
				cfg.AeronConfig.ErrorHandler = callerHandler
			}
			cfg.MessageTimeout = time.Minute
			session := connectHarnessSession(t, harness, cfg)
			requireSessionOpen(t, session)

			// Phase: the fault reaches the transport client.
			wantCause := conductor.ErrClientTimeout
			if tc.perResource {
				transmitUnmappableImage(t, harness, session.EgressSubscription())
				wantCause = conductor.ErrLogBufferMapFailed
			} else {
				transmitClientTimeout(t, harness)
			}
			if tc.spin {
				require.NotPanics(t, func() { session.SpinOnce() })
			} else {
				require.NotPanics(t, harness.DoWork)
				session.PollEgress()
			}
			if tc.callerHandler {
				require.Len(t, callerErrs, 1)
				require.ErrorIs(t, callerErrs[0], wantCause)
			}

			// Phase: only a terminated owned client closes the session.
			if tc.wantClosed {
				requireTransportFaultClosed(t, harness, session, wantCause)
				return
			}
			requireSessionOpen(t, session)
			require.Same(t, harness.Client, session.Aeron())
			require.False(t, aeron.ClientIsClosedForTest(harness.Client))
			if tc.perResource {
				require.False(t, harness.Client.IsTerminating())
			}
		})
	}
}

// TestConnect_ownedClientFaultFailsConnect asserts that a fault during the
// handshake fails Connect with the cause and closes the owned client.
func TestConnect_ownedClientFaultFailsConnect(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := ownedConnectConfigForTest(t, harness, ownedHarnessClientFactory(harness))
	cfg.MessageTimeout = time.Minute

	var steps int
	observedAC := connectTestHarnessDriver(t, harness, func(*AsyncConnect) {
		steps++
		if steps == 3 {
			transmitClientTimeout(t, harness)
		}
	})

	var (
		session *AeronCluster
		err     error
	)
	require.NotPanics(t, func() { session, err = Connect(t.Context(), cfg) })
	require.Nil(t, session)
	require.ErrorIs(t, err, ErrTransportTerminating)
	require.ErrorIs(t, err, conductor.ErrClientTimeout)
	requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
}

func TestConnect_challengeRoundTrip(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client, "")

	var roundTrip asyncConnectChallengeRoundTripState
	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		roundTrip.onStep(t, ac)
	})

	session, err := Connect(t.Context(), cfg)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })

	require.True(t, roundTrip.challengeWritten)
	require.True(t, roundTrip.challengeResponseOffered)
	require.True(t, roundTrip.sessionEventWritten)
}

func TestConnect_sessionEventAuthenticationRejected(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client, "")

	observedAC := connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		if ac.State() != ConnectStatePollResponse {
			return
		}
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
				fields := defaultSessionEventFields([]byte("bad credentials"))
				fields.CorrelationID = correlation
				fields.Code = clusterwire.EventCode_AUTHENTICATION_REJECTED
				writeUnfragmentedWireToTerm(
					t,
					termBuffer,
					encodeSessionEventWireWithFields(
						t,
						egressTestClusterSessionID,
						fields,
						clusterwire.SessionEventSbeSchemaVersion,
					),
				)
			},
		)
	})

	_, err := Connect(t.Context(), cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "cluster client: authentication rejected")
	requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
}

func TestConnect_timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		buildCtx         func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config
		onStep           func(ac *AsyncConnect)
		wantClientClosed bool
	}{
		{
			name: "injected_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := connectConfigForTest(harness.Client, "")
				cfg.MessageTimeout = 5 * time.Millisecond
				return cfg
			},
			wantClientClosed: false,
		},
		{
			name: "owned_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := ownedConnectConfigForTest(
					t, harness, func(Config) (*aeron.Client, error) {
						return harness.Client, nil
					},
				)
				cfg.MessageTimeout = 5 * time.Millisecond
				return cfg
			},
			wantClientClosed: true,
		},
		{
			name: "handshake_poll_timeout",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := connectConfigForTest(harness.Client, "")
				cfg.MessageTimeout = 5 * time.Millisecond
				return cfg
			},
			onStep: func(ac *AsyncConnect) {
				if ac.EgressSubscriptionForTest() != nil {
					ac.deadlineNs = ac.nanoClock() - 1
				}
			},
			wantClientClosed: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			cfg := tc.buildCtx(t, harness)

			observedAC := connectTestHarnessDriver(t, harness, tc.onStep)

			_, err := Connect(t.Context(), cfg)
			require.Error(t, err)
			require.ErrorContains(t, err, "cluster client: connect timeout")
			require.Equal(t, tc.wantClientClosed, aeron.ClientIsClosedForTest(harness.Client))
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
		})
	}
}

func TestConnect_contextDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		buildCtx        func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config
		connectCtx      func(t *testing.T) (context.Context, context.CancelFunc)
		wantErr         error
		wantErrContains string
	}{
		{
			name: "preExpired",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := connectConfigForTest(harness.Client, "")
				cfg.MessageTimeout = time.Minute
				return cfg
			},
			connectCtx: func(t *testing.T) (context.Context, context.CancelFunc) {
				t.Helper()
				deadlineCtx, cancel := context.WithDeadline(
					t.Context(),
					time.Now().Add(-time.Second),
				)
				return deadlineCtx, cancel
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "ctxDeadlineSooner",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := connectConfigForTest(harness.Client, "")
				cfg.MessageTimeout = time.Minute
				return cfg
			},
			connectCtx: func(t *testing.T) (context.Context, context.CancelFunc) {
				t.Helper()
				return context.WithTimeout(t.Context(), 5*time.Millisecond)
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "messageTimeoutSooner",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := connectConfigForTest(harness.Client, "")
				cfg.MessageTimeout = 5 * time.Millisecond
				return cfg
			},
			connectCtx: func(t *testing.T) (context.Context, context.CancelFunc) {
				t.Helper()
				return context.WithTimeout(t.Context(), time.Minute)
			},
			wantErrContains: "cluster client: connect timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			cfg := tc.buildCtx(t, harness)
			connectCtx, cancel := tc.connectCtx(t)
			defer cancel()

			observedAC := connectTestHarnessDriver(t, harness, nil)

			_, err := Connect(connectCtx, cfg)

			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.wantErrContains != "":
				require.ErrorContains(t, err, tc.wantErrContains)
			default:
				t.Fatal("test case must set wantErr or wantErrContains")
			}
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
		})
	}
}

func TestConnect_successWinsOverConcurrentCancel(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client, "")
	cfg.MessageTimeout = time.Minute

	connectCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		switch ac.State() {
		case ConnectStatePollResponse:
			withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
		case ConnectStateConcludeConnect:
			cancel()
		}
	})

	session, err := Connect(connectCtx, cfg)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })

	require.NotZero(t, session.ClusterSessionID())
}

func TestConnect_validateRejectsMissingEgressHandler(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := baseAsyncConnectConfig(harness.Client, "")
	cfg.CredentialsSupplier = NullCredentialsSupplier{}

	_, err := Connect(t.Context(), cfg)
	require.EqualError(t, err, "cluster client: EgressHandler is required")
}

func TestConnect_newAsyncConnectFailure(t *testing.T) {
	t.Parallel()

	const invalidEndpointsErr = "cluster client: endpoint missing '=' separator: not-valid-endpoints"
	const factoryErr = "cluster client: injected owned client factory failure"

	tests := []struct {
		name             string
		buildCtx         func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config
		wantErr          string
		wantClientClosed bool
	}{
		{
			name: "invalid_endpoints_injected_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				return connectConfigWithInvalidEndpoints(connectConfigForTest(harness.Client, ""))
			},
			wantErr:          invalidEndpointsErr,
			wantClientClosed: false,
		},
		{
			name: "invalid_endpoints_owned_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				cfg := ownedConnectConfigForTest(
					t, harness, func(Config) (*aeron.Client, error) {
						return harness.Client, nil
					},
				)
				return connectConfigWithInvalidEndpoints(cfg)
			},
			wantErr:          invalidEndpointsErr,
			wantClientClosed: true,
		},
		{
			name: "owned_client_factory_error",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) Config {
				t.Helper()
				return ownedConnectConfigForTest(t, harness, func(Config) (*aeron.Client, error) {
					return nil, errors.New(factoryErr)
				})
			},
			wantErr:          factoryErr,
			wantClientClosed: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			cfg := tc.buildCtx(t, harness)

			_, err := Connect(t.Context(), cfg)
			require.EqualError(t, err, tc.wantErr)
			require.Equal(t, tc.wantClientClosed, aeron.ClientIsClosedForTest(harness.Client))
		})
	}
}

func TestConnect_newAeronClusterFailureReleasesResult(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := connectConfigForTest(harness.Client, "")

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})

	var capturedIngress IngressPublication
	var capturedSub *aeron.Subscription
	const injectedErr = "cluster client: injected session construction failure"
	SetNewAeronClusterHookForTest(
		t,
		harness.Client,
		func(result *ConnectResult) (*AeronCluster, error) {
			capturedIngress = result.IngressPublication
			capturedSub = result.EgressSubscription
			return nil, errors.New(injectedErr)
		},
	)

	_, err := Connect(t.Context(), cfg)
	require.EqualError(t, err, injectedErr)
	require.NotNil(t, capturedIngress)
	require.NotNil(t, capturedSub)
	require.True(t, capturedIngress.IsClosed())
	require.True(t, capturedSub.IsClosed())
	require.False(t, aeron.ClientIsClosedForTest(harness.Client))
}

func TestReleaseConnectResult_closesHandles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		ownsAeronClient bool
		withMemberMap   bool
	}{
		{
			name:            "member_ingress_map_owned_client",
			ownsAeronClient: true,
			withMemberMap:   true,
		},
		{
			name:            "ingress_publication_injected_client",
			ownsAeronClient: false,
		},
		{
			name:            "ingress_publication_owned_client",
			ownsAeronClient: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := aeron.NewAsyncConnectTestHarness(t)
			ingressPub := aeron.NewTestExclusivePublicationForHooks(t)
			egressSub := aeron.NewTestSubscriptionForHooks(t)
			result := &ConnectResult{
				IngressPublication: ingressPub,
				EgressSubscription: egressSub,
				Aeron:              harness.Client,
				OwnsAeronClient:    tc.ownsAeronClient,
			}
			if tc.withMemberMap {
				result.MemberIngressMap = MemberIngressMap{
					channelOnlyMemberID: &MemberIngress{
						MemberID:    channelOnlyMemberID,
						Publication: ingressPub,
					},
				}
			}

			releaseConnectResult(result)
			require.True(t, ingressPub.IsClosed())
			require.True(t, egressSub.IsClosed())
			require.Equal(t, tc.ownsAeronClient, aeron.ClientIsClosedForTest(harness.Client))
		})
	}
}
