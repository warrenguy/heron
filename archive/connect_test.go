//go:build testhooks

package archive

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
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
	SetConnectConstructionHooksForTest(t, aeronDir, func(cfg Config) (*aeron.Client, error) {
		harness.SetErrorHandler(cfg.AeronConfig.ErrorHandler)
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
			writeConnectHandshakeResponses(t, ac)
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
		writeConnectHandshakeResponses(t, ac)
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

			_, err := Connect(connectCtx, cfg)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.ErrorContains(t, err, tc.wantErrContains)
			}
			requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
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
		case ConnectStateConcludeConnect:
			cancel()
		default:
			writeConnectHandshakeResponses(t, ac)
		}
	})

	archive, err := Connect(connectCtx, cfg)
	require.NoError(t, err)
	require.NotNil(t, archive)
	t.Cleanup(func() { _ = archive.Close() })

	require.Equal(t, asyncConnectFixtureControlSessionID, archive.ControlSessionID())
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
) *Client {
	t.Helper()

	connectTestHarnessDriver(t, harness, func(ac *AsyncConnect) {
		writeConnectHandshakeResponses(t, ac)
	})
	session, err := Connect(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func requireTransportFaultClosed(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	session *Client,
	cause error,
) {
	t.Helper()

	require.True(t, session.IsClosed())
	require.ErrorIs(t, session.TransportFault(), ErrTransportTerminating)
	require.ErrorIs(t, session.TransportFault(), cause)
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
}

func requireSessionOpen(t *testing.T, session *Client) {
	t.Helper()

	require.False(t, session.IsClosed())
	require.NoError(t, session.TransportFault())
}

type faultFixture struct {
	harness    *aeron.AsyncConnectTestHarness
	session    *Client
	callerErrs *[]error
}

// requireAwaitInFlightFault runs call with a client timeout broadcast inside
// its await loop. call must time out and the session must close on the fault.
func requireAwaitInFlightFault(t *testing.T, fixture faultFixture, call func() error) {
	t.Helper()

	// Phase: the offer retry runs the first DoWork. The await loop runs the
	// second. The hook then broadcasts the timeout.
	var doWorkCalls int
	fixture.harness.SetDoWorkAfterHook(func() {
		doWorkCalls++
		if doWorkCalls == 2 {
			transmitClientTimeout(t, fixture.harness)
		}
	})
	callErr := call()
	var timeout *ControlResponseTimeoutError
	require.ErrorAs(t, callErr, &timeout)
	requireTransportFaultClosed(t, fixture.harness, fixture.session, conductor.ErrClientTimeout)
}

// TestClient_ownedClientFaultClosesSession asserts that a fault that
// terminates the connect-owned transport client closes the session on the
// next poll. A per-resource fault and a caller-supplied client leave the
// session open.
func TestClient_ownedClientFaultClosesSession(t *testing.T) {
	t.Parallel()

	const stopStreamID int32 = 1001
	stopChannel := []byte("aeron:ipc")

	tests := []struct {
		name           string
		callerHandler  bool
		callerSupplied bool
		run            func(*testing.T, faultFixture)
	}{
		{
			name: "default_handler_client_timeout",
			run: func(t *testing.T, fixture faultFixture) {
				transmitClientTimeout(t, fixture.harness)
				fixture.harness.DoWork()
				require.Zero(t, fixture.session.Poll())
				requireTransportFaultClosed(t, fixture.harness, fixture.session, conductor.ErrClientTimeout)
			},
		},
		{
			name:          "caller_handler_client_timeout",
			callerHandler: true,
			run: func(t *testing.T, fixture faultFixture) {
				transmitClientTimeout(t, fixture.harness)
				fixture.harness.DoWork()
				require.Len(t, *fixture.callerErrs, 1)
				require.ErrorIs(t, (*fixture.callerErrs)[0], conductor.ErrClientTimeout)
				require.Zero(t, fixture.session.Poll())
				requireTransportFaultClosed(t, fixture.harness, fixture.session, conductor.ErrClientTimeout)
			},
		},
		{
			name: "per_resource_error_keeps_session",
			run: func(t *testing.T, fixture faultFixture) {
				transmitUnmappableImage(t, fixture.harness, fixture.session.controlSubscription)
				fixture.harness.DoWork()
				fixture.session.Poll()
				requireSessionOpen(t, fixture.session)
				require.False(t, fixture.harness.Client.IsTerminating())
			},
		},
		{
			name:          "per_resource_error_caller_handler_keeps_session",
			callerHandler: true,
			run: func(t *testing.T, fixture faultFixture) {
				transmitUnmappableImage(t, fixture.harness, fixture.session.controlSubscription)
				fixture.harness.DoWork()
				require.Len(t, *fixture.callerErrs, 1)
				require.ErrorIs(t, (*fixture.callerErrs)[0], conductor.ErrLogBufferMapFailed)
				fixture.session.Poll()
				requireSessionOpen(t, fixture.session)
				require.False(t, fixture.harness.Client.IsTerminating())
			},
		},
		{
			name:           "caller_supplied_client_unchanged",
			callerHandler:  true,
			callerSupplied: true,
			run: func(t *testing.T, fixture faultFixture) {
				transmitClientTimeout(t, fixture.harness)
				fixture.harness.DoWork()
				require.Len(t, *fixture.callerErrs, 1)
				fixture.session.Poll()
				requireSessionOpen(t, fixture.session)
				require.False(t, aeron.ClientIsClosedForTest(fixture.harness.Client))
			},
		},
		{
			name: "blocking_call_after_fault",
			run: func(t *testing.T, fixture faultFixture) {
				transmitClientTimeout(t, fixture.harness)
				stopErr := fixture.session.StopRecording(t.Context(), stopStreamID, stopChannel)
				var terminal *ControlTerminalError
				require.ErrorAs(t, stopErr, &terminal)
				require.False(t, fixture.session.IsClosed())

				require.Zero(t, fixture.session.Poll())
				requireTransportFaultClosed(t, fixture.harness, fixture.session, conductor.ErrClientTimeout)
			},
		},
		{
			name: "await_in_flight_fault",
			run: func(t *testing.T, fixture faultFixture) {
				requireAwaitInFlightFault(t, fixture, func() error {
					return fixture.session.StopRecording(t.Context(), stopStreamID, stopChannel)
				})
			},
		},
		{
			name: "list_recording_await_in_flight_fault",
			run: func(t *testing.T, fixture faultFixture) {
				requireAwaitInFlightFault(t, fixture, func() error {
					_, err := fixture.session.ListRecording(t.Context(), 1)
					return err
				})
			},
		},
		{
			name: "signal_await_in_flight_fault",
			run: func(t *testing.T, fixture faultFixture) {
				tracker := NewStopSignalTracker(fixture.session.controlSessionID, -1)
				requireAwaitInFlightFault(t, fixture, func() error {
					return fixture.session.StopRecordingAwaitSignal(
						t.Context(),
						stopStreamID,
						stopChannel,
						tracker,
					)
				})
			},
		},
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
				cfg = connectConfigForTest(harness.Client)
			} else {
				cfg = ownedConnectConfigForTest(t, harness)
				cfg.AeronConfig.ErrorHandler = callerHandler
			}
			cfg.ConnectTimeout = time.Minute
			cfg.ControlResponseTimeout = 100 * time.Millisecond
			session := connectHarnessSession(t, harness, cfg)
			requireSessionOpen(t, session)

			tc.run(t, faultFixture{
				harness:    harness,
				session:    session,
				callerErrs: &callerErrs,
			})
		})
	}
}

// TestConnect_ownedClientFaultFailsConnect asserts that a fault during the
// handshake fails Connect with the cause and closes the owned client.
func TestConnect_ownedClientFaultFailsConnect(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := ownedConnectConfigForTest(t, harness)
	cfg.ConnectTimeout = time.Minute

	var steps int
	observedAC := connectTestHarnessDriver(t, harness, func(*AsyncConnect) {
		steps++
		if steps == 3 {
			transmitClientTimeout(t, harness)
		}
	})

	session, err := Connect(t.Context(), cfg)
	require.Nil(t, session)
	require.ErrorIs(t, err, ErrTransportTerminating)
	require.ErrorIs(t, err, conductor.ErrClientTimeout)
	requireAsyncConnectClosedAfterConnectFailure(t, observedAC())
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
}
