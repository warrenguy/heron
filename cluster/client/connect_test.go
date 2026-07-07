//go:build testhooks

package client

import (
	"context"
	"errors"
	stdatomic "sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/codecs/gensbe"
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

	var observedAC stdatomic.Pointer[AsyncConnect]
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

// connectContextForTest wraps baseAsyncConnectContext with fields Connect.Validate requires.
func connectContextForTest(client *aeron.Client, ingressEndpoints string) ClusterContext {
	ctx := baseAsyncConnectContext(client, ingressEndpoints)
	ctx.EgressHandler = &EgressHandlers{}
	ctx.CredentialsSupplier = NullCredentialsSupplier{}
	return ctx
}

func ownedConnectContextForTest(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	factory func(ClusterContext) (*aeron.Client, error),
) ClusterContext {
	t.Helper()

	aeronDir := t.TempDir()
	SetConnectConstructionHooksForTest(t, aeronDir, factory)
	ctx := connectContextForTest(nil, "")
	ctx.AeronDir = aeronDir
	ctx.MessageTimeout = time.Second
	return ctx
}

func connectContextWithInvalidEndpoints(ctx ClusterContext) ClusterContext {
	ctx.IngressChannel = "aeron:udp?ttl=16"
	ctx.IngressEndpoints = "not-valid-endpoints"
	return ctx
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
		harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
			ctx = connectContextForTest(ctx.Aeron, "")
			ctx.IdleStrategy = idle
			return ctx
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
		harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
			ctx = connectContextForTest(ctx.Aeron, "")
			ctx.IdleStrategy = idle
			return ctx
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
		buildCtx         func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext
		preCancel        bool
		onStep           func(cancel context.CancelFunc) func(*AsyncConnect)
		wantClientClosed bool
	}{
		{
			name: "already_canceled",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				return connectContextForTest(harness.Client, "")
			},
			preCancel:        true,
			wantClientClosed: false,
		},
		{
			name: "owned_client_already_canceled",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				return ownedConnectContextForTest(t, harness, func(ClusterContext) (*aeron.Client, error) {
					return harness.Client, nil
				})
			},
			preCancel:        true,
			wantClientClosed: true,
		},
		{
			name: "canceled_during_handshake",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := connectContextForTest(harness.Client, "")
				ctx.MessageTimeout = time.Minute
				return ctx
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
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := ownedConnectContextForTest(t, harness, func(ClusterContext) (*aeron.Client, error) {
					return harness.Client, nil
				})
				ctx.MessageTimeout = time.Minute
				return ctx
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
			clusterCtx := tc.buildCtx(t, harness)

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

			_, err := Connect(cancelCtx, clusterCtx)
			require.ErrorIs(t, err, context.Canceled)
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
			require.Equal(t, tc.wantClientClosed, aeron.ClientIsClosedForTest(harness.Client))
		})
	}
}

func TestConnect_concludeConnect_setsNewLeaderTimeout(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := connectContextForTest(harness.Client, "")

	const heartbeatNs = int64(250_000_000)
	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeSessionEventWithHeartbeatForConnect(t, termBuffer, correlation, heartbeatNs)
			},
		)
	})

	session, err := Connect(t.Context(), ctx)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })
	require.Equal(t, 2*time.Duration(heartbeatNs), session.Context().NewLeaderTimeout)
}

func TestConnect_success(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := connectContextForTest(harness.Client, "")

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})

	session, err := Connect(t.Context(), ctx)
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
	ctx := ownedConnectContextForTest(t, harness, func(ClusterContext) (*aeron.Client, error) {
		return harness.Client, nil
	})
	ctx.MessageTimeout = 30 * time.Second

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})

	require.False(t, aeron.ClientIsClosedForTest(harness.Client))

	session, err := Connect(t.Context(), ctx)
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

func TestConnect_challengeRoundTrip(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := connectContextForTest(harness.Client, "")

	var roundTrip asyncConnectChallengeRoundTripState
	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		roundTrip.onStep(t, ac)
	})

	session, err := Connect(t.Context(), ctx)
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
	ctx := connectContextForTest(harness.Client, "")

	observedAC := connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		if ac.State() != ConnectStatePollResponse {
			return
		}
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				fields := defaultSessionEventFields([]byte("bad credentials"))
				fields.CorrelationID = correlation
				fields.Code = gensbe.EventCode_AUTHENTICATION_REJECTED
				writeUnfragmentedWireToTerm(
					t,
					termBuffer,
					encodeSessionEventWireWithFields(
						t,
						egressTestClusterSessionID,
						fields,
						gensbe.SessionEventSbeSchemaVersion,
					),
				)
			},
		)
	})

	_, err := Connect(t.Context(), ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "cluster client: authentication rejected")
	requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
}

func TestConnect_timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		buildCtx         func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext
		onStep           func(ac *AsyncConnect)
		wantClientClosed bool
	}{
		{
			name: "injected_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := connectContextForTest(harness.Client, "")
				ctx.MessageTimeout = 5 * time.Millisecond
				return ctx
			},
			wantClientClosed: false,
		},
		{
			name: "owned_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := ownedConnectContextForTest(t, harness, func(ClusterContext) (*aeron.Client, error) {
					return harness.Client, nil
				})
				ctx.MessageTimeout = 5 * time.Millisecond
				return ctx
			},
			wantClientClosed: true,
		},
		{
			name: "handshake_poll_timeout",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := connectContextForTest(harness.Client, "")
				ctx.MessageTimeout = 5 * time.Millisecond
				return ctx
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
			ctx := tc.buildCtx(t, harness)

			observedAC := connectTestHarnessDriver(t, harness, tc.onStep)

			_, err := Connect(t.Context(), ctx)
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
		buildCtx        func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext
		connectCtx      func(t *testing.T) (context.Context, context.CancelFunc)
		wantErr         error
		wantErrContains string
	}{
		{
			name: "preExpired",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := connectContextForTest(harness.Client, "")
				ctx.MessageTimeout = time.Minute
				return ctx
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
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := connectContextForTest(harness.Client, "")
				ctx.MessageTimeout = time.Minute
				return ctx
			},
			connectCtx: func(t *testing.T) (context.Context, context.CancelFunc) {
				t.Helper()
				return context.WithTimeout(t.Context(), 5*time.Millisecond)
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "messageTimeoutSooner",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := connectContextForTest(harness.Client, "")
				ctx.MessageTimeout = 5 * time.Millisecond
				return ctx
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
			clusterCtx := tc.buildCtx(t, harness)
			connectCtx, cancel := tc.connectCtx(t)
			defer cancel()

			observedAC := connectTestHarnessDriver(t, harness, nil)

			start := time.Now()
			_, err := Connect(connectCtx, clusterCtx)
			elapsed := time.Since(start)

			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.wantErrContains != "":
				require.ErrorContains(t, err, tc.wantErrContains)
			default:
				t.Fatal("test case must set wantErr or wantErrContains")
			}
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())

			if tc.name == "ctxDeadlineSooner" {
				require.Less(t, elapsed, time.Second)
			}
		})
	}
}

func TestConnect_successWinsOverConcurrentCancel(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	clusterCtx := connectContextForTest(harness.Client, "")
	clusterCtx.MessageTimeout = time.Minute

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

	session, err := Connect(connectCtx, clusterCtx)
	require.NoError(t, err)
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })

	require.NotZero(t, session.ClusterSessionID())
}

func TestConnect_validateRejectsMissingEgressHandler(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := baseAsyncConnectContext(harness.Client, "")
	ctx.CredentialsSupplier = NullCredentialsSupplier{}

	_, err := Connect(t.Context(), ctx)
	require.EqualError(t, err, "cluster client: EgressHandler is required")
}

func TestConnect_newAsyncConnectFailure(t *testing.T) {
	t.Parallel()

	const invalidEndpointsErr = "cluster client: endpoint missing '=' separator: not-valid-endpoints"
	const factoryErr = "cluster client: injected owned client factory failure"

	tests := []struct {
		name             string
		buildCtx         func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext
		wantErr          string
		wantClientClosed bool
	}{
		{
			name: "invalid_endpoints_injected_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				return connectContextWithInvalidEndpoints(connectContextForTest(harness.Client, ""))
			},
			wantErr:          invalidEndpointsErr,
			wantClientClosed: false,
		},
		{
			name: "invalid_endpoints_owned_client",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				ctx := ownedConnectContextForTest(t, harness, func(ClusterContext) (*aeron.Client, error) {
					return harness.Client, nil
				})
				return connectContextWithInvalidEndpoints(ctx)
			},
			wantErr:          invalidEndpointsErr,
			wantClientClosed: true,
		},
		{
			name: "owned_client_factory_error",
			buildCtx: func(t *testing.T, harness *aeron.AsyncConnectTestHarness) ClusterContext {
				t.Helper()
				return ownedConnectContextForTest(t, harness, func(ClusterContext) (*aeron.Client, error) {
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
			ctx := tc.buildCtx(t, harness)

			_, err := Connect(t.Context(), ctx)
			require.EqualError(t, err, tc.wantErr)
			require.Equal(t, tc.wantClientClosed, aeron.ClientIsClosedForTest(harness.Client))
		})
	}
}

func TestConnect_newAeronClusterFailureReleasesResult(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := connectContextForTest(harness.Client, "")

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

	_, err := Connect(t.Context(), ctx)
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
