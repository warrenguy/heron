package aeron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

const (
	clientAwaitTestCorrelationID int64 = 11
	clientAwaitTestStreamID      int32 = 100
)

func newClientAwaitFixture(
	tb testutil.TB,
	nanoClock func() int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
) (*Client, *testutil.ConductorFixture) {
	tb.Helper()

	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.DriverTimeout = driverTimeout
	config.IdleSleepDuration = idleSleepDuration
	fixture := testutil.NewConductorFixture(tb, config)
	client := NewClientForTestWithDriverProxy(
		fixture.Conductor,
		fixture.DriverProxy,
		fixture.NanoClock,
		config.ClientID,
		config.DriverTimeout,
		config.IdleSleepDuration,
	)
	return client, fixture
}

func requireAwaitTimeout(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	err error,
	correlationID int64,
	timeout time.Duration,
	wantMsg string,
) {
	t.Helper()

	var timeoutErr *AwaitTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, correlationID, timeoutErr.CorrelationID)
	require.Equal(t, timeout, timeoutErr.Timeout)
	require.EqualError(t, err, wantMsg)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func requireActiveCorrelationCleared(
	t *testing.T,
	fixture *testutil.ConductorFixture,
) {
	t.Helper()
	require.Equal(t, conductor.NullCorrelationID, fixture.Conductor.ActiveCorrelationID())
}

func TestClient_NextCorrelationID_monotonic(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		testutil.TestDriverTimeout,
		time.Millisecond,
	)

	first := client.NextCorrelationID()
	second := client.NextCorrelationID()
	third := client.NextCorrelationID()

	require.Less(t, first, second)
	require.Less(t, second, third)
	require.Equal(t, first, fixture.DriverProxy.NextCorrelationID()-3)
}

func TestClient_awaitResponse_timeout_immediate(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Millisecond,
		time.Millisecond,
	)

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		0,
		nil,
	)
	requireAwaitTimeout(
		t,
		fixture,
		err,
		clientAwaitTestCorrelationID,
		0,
		"aeron: await response timeout: correlation=11 timeout=0s",
	)
}

func TestClient_awaitResponse_sharedDeadline_beforeOffer(t *testing.T) {
	const (
		startNs    = int64(1_000)
		timeout    = 500 * time.Nanosecond
		offerDelay = 200 * time.Nanosecond
	)
	deadlineNs := startNs + int64(timeout)
	nanoNs := startNs
	var afterOffer bool
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 {
			if afterOffer {
				return deadlineNs
			}
			return nanoNs
		},
		time.Second,
		time.Microsecond,
	)

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		timeout,
		func() error {
			registration, ok := fixture.Conductor.PendingResult(clientAwaitTestCorrelationID)
			require.True(t, ok)
			require.Equal(t, deadlineNs, registration.DeadlineNs)
			nanoNs = startNs + int64(offerDelay)
			afterOffer = true
			return nil
		},
	)
	requireAwaitTimeout(
		t,
		fixture,
		err,
		clientAwaitTestCorrelationID,
		timeout,
		"aeron: await response timeout: correlation=11 timeout=500ns",
	)
}

func TestClient_awaitResponse_conductorExpireTimeout(t *testing.T) {
	const (
		startNs = int64(1_000)
		timeout = 500 * time.Nanosecond
	)
	deadlineNs := startNs + int64(timeout)
	var nanoReads int
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 {
			nanoReads++
			if nanoReads <= 4 {
				return startNs
			}
			return deadlineNs
		},
		time.Second,
		time.Microsecond,
	)

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		timeout,
		nil,
	)
	require.GreaterOrEqual(t, nanoReads, 5)
	requireAwaitTimeout(
		t,
		fixture,
		err,
		clientAwaitTestCorrelationID,
		timeout,
		"aeron: await response timeout: correlation=11 timeout=500ns",
	)
}

func TestClient_awaitResponse_timeout_afterDeadline(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	var nanoReads int
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 {
			nanoReads++
			if nanoReads < 5 {
				return startNs
			}
			return startNs + timeout.Nanoseconds() + 1
		},
		time.Second,
		time.Microsecond,
	)

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		timeout,
		nil,
	)
	requireAwaitTimeout(
		t,
		fixture,
		err,
		clientAwaitTestCorrelationID,
		timeout,
		"aeron: await response timeout: correlation=11 timeout=5ms",
	)
}

func TestClient_awaitResponse_success(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	require.NoError(
		t,
		fixture.Transmit(
			conductor.OnNextAvailableSessionID,
			conductor.CorrelationAndInt32Bytes(clientAwaitTestCorrelationID, 42),
		),
	)

	registration, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		time.Second,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int32(42), registration.Int32Value)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitResponse_interrupted(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	require.NoError(
		t,
		fixture.Conductor.RegisterPending(
			conductor.PendingKindNextSessionID,
			clientAwaitTestCorrelationID,
			time.Second,
		),
	)
	fixture.Conductor.CancelPending(clientAwaitTestCorrelationID)
	fixture.Conductor.SetReceivedCorrelationIDForTest(clientAwaitTestCorrelationID)

	err := client.PollAwaitForTest(clientAwaitTestCorrelationID)
	require.ErrorIs(t, err, ErrAwaitInterrupted)
	require.EqualError(t, err, "aeron: await response interrupted")
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitResponse_driverRegistrationError(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	driverErr := &conductor.DriverRegistrationError{
		CorrelationID: clientAwaitTestCorrelationID,
		ErrorCode:     11,
		Message:       "registration failed",
	}

	require.NoError(
		t,
		fixture.Conductor.RegisterPending(
			conductor.PendingKindNextSessionID,
			clientAwaitTestCorrelationID,
			time.Second,
		),
	)
	fixture.Conductor.SetActiveCorrelationID(clientAwaitTestCorrelationID)
	require.NoError(
		t,
		fixture.Conductor.CompletePendingForTest(clientAwaitTestCorrelationID, 0, driverErr),
	)

	err := client.PollAwaitForTest(clientAwaitTestCorrelationID)
	var registrationErr *RegistrationError
	require.ErrorAs(t, err, &registrationErr)
	require.Equal(t, clientAwaitTestCorrelationID, registrationErr.CorrelationID)
	require.Equal(t, int32(11), registrationErr.ErrorCode)
	require.Equal(t, "registration failed", registrationErr.Message)
	require.ErrorIs(t, err, driverErr)
	require.EqualError(
		t,
		err,
		"aeron: driver registration error: correlation=11 code=11 message=registration failed",
	)
	_, ok := fixture.Conductor.PendingResult(clientAwaitTestCorrelationID)
	require.False(t, ok)
	fixture.Conductor.SetActiveCorrelationID(conductor.NullCorrelationID)
	requireActiveCorrelationCleared(t, fixture)
}

func pollAndRespondToNextAvailableSessionIDCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	sessionID int32,
	deadline time.Time,
	onCommandSeen func(),
) bool {
	t.Helper()

	for time.Now().Before(deadline) {
		var responded bool
		fixture.Ring.Read(func(msgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) {
			if msgTypeID != conductor.GetNextAvailableSessionID {
				return
			}
			decoded, err := conductor.DecodeCorrelated(buffer, index)
			if err != nil {
				return
			}
			if onCommandSeen != nil {
				onCommandSeen()
			}
			transmitErr := fixture.Transmit(
				conductor.OnNextAvailableSessionID,
				conductor.CorrelationAndInt32Bytes(decoded.CorrelationID, sessionID),
			)
			if transmitErr == nil {
				responded = true
			}
		})
		if responded {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestClient_NextAvailableSessionID_success(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	const wantSessionID int32 = 77

	type result struct {
		sessionID int32
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		sessionID, err := client.NextAvailableSessionID(clientAwaitTestStreamID)
		resultCh <- result{sessionID: sessionID, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	require.True(
		t,
		pollAndRespondToNextAvailableSessionIDCommand(t, fixture, wantSessionID, pollDeadline, nil),
		"timed out waiting for NextAvailableSessionID command on ring",
	)

	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.Equal(t, wantSessionID, got.sessionID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for NextAvailableSessionID client result")
	}
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitResponse_deferredOfferFastResponse(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	const wantSessionID int32 = 42

	registration, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		time.Second,
		func() error {
			return fixture.Transmit(
				conductor.OnNextAvailableSessionID,
				conductor.CorrelationAndInt32Bytes(clientAwaitTestCorrelationID, wantSessionID),
			)
		},
	)
	require.NoError(t, err)
	require.Equal(t, wantSessionID, registration.Int32Value)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAwaitForTest_requiresDoWorkAfterBroadcastCompletion(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	require.NoError(
		t,
		fixture.Conductor.RegisterPending(
			conductor.PendingKindNextSessionID,
			clientAwaitTestCorrelationID,
			time.Second,
		),
	)
	fixture.Conductor.SetActiveCorrelationID(clientAwaitTestCorrelationID)
	require.NoError(
		t,
		fixture.Transmit(
			conductor.OnNextAvailableSessionID,
			conductor.CorrelationAndInt32Bytes(clientAwaitTestCorrelationID, 42),
		),
	)

	err := client.PollAwaitForTest(clientAwaitTestCorrelationID)
	require.NoError(t, err)
	_, ok := fixture.Conductor.PendingResult(clientAwaitTestCorrelationID)
	require.False(t, ok)
}

func TestClient_NextAvailableSessionID_offersCommand(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		0,
		time.Millisecond,
	)

	_, err := client.NextAvailableSessionID(clientAwaitTestStreamID)
	var timeoutErr *AwaitTimeoutError
	require.ErrorAs(t, err, &timeoutErr)

	readCount := fixture.Ring.Read(
		func(msgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) {
			require.Equal(t, conductor.GetNextAvailableSessionID, msgTypeID)
			require.Equal(t, conductor.GetNextAvailableSessionIDBodyLength, length)
		},
	)
	require.Equal(t, 1, readCount)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitResponse_terminationCause(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	client.DoWork()
	nanoNs = testutil.TestWorkNanoNs
	fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		time.Second,
		nil,
	)
	var timeoutErr *DriverTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.ErrorIs(t, timeoutErr.Unwrap(), conductor.ErrDriverShutdown)
	require.EqualError(
		t,
		err,
		"aeron: media driver timeout: conductor: media driver has been shutdown",
	)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitResponse_clientTimeoutTerminationCause(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	client.DoWork()
	require.NoError(
		t,
		fixture.Transmit(
			conductor.OnClientTimeout,
			testutil.ClientTimeoutPayload(testutil.TestClientID),
		),
	)
	nanoNs = testutil.TestWorkNanoNs

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		time.Second,
		nil,
	)
	require.ErrorIs(t, err, conductor.ErrClientTimeout)
	require.EqualError(t, err, "conductor: client timeout from driver")
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitResponse_registerPendingFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(
			t *testing.T,
			client *Client,
			fixture *testutil.ConductorFixture,
			setNanoNs func(int64),
		)
		wantErr error
		wantMsg string
	}{
		{
			name: "conductor terminating",
			setup: func(
				t *testing.T,
				client *Client,
				fixture *testutil.ConductorFixture,
				setNanoNs func(int64),
			) {
				client.DoWork()
				setNanoNs(testutil.TestWorkNanoNs)
				fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
				require.Equal(t, 1, client.DoWork())
				require.True(t, fixture.Conductor.IsTerminating())
			},
			wantErr: ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name: "duplicate pending registration",
			setup: func(
				t *testing.T,
				_ *Client,
				fixture *testutil.ConductorFixture,
				_ func(int64),
			) {
				require.NoError(
					t,
					fixture.Conductor.RegisterPending(
						conductor.PendingKindNextSessionID,
						clientAwaitTestCorrelationID,
						time.Second,
					),
				)
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAwaitFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
			)
			testCase.setup(t, client, fixture, func(value int64) { nanoNs = value })

			_, err := client.AwaitResponseForTest(
				clientAwaitTestCorrelationID,
				conductor.PendingKindNextSessionID,
				time.Second,
				nil,
			)
			require.ErrorIs(t, err, testCase.wantErr)
			require.EqualError(t, err, testCase.wantMsg)
		})
	}
}

func TestClient_closedClientPaths(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	client, _ := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	require.NoError(t, client.Close())

	_, err := client.AwaitResponseForTest(
		clientAwaitTestCorrelationID,
		conductor.PendingKindNextSessionID,
		time.Second,
		nil,
	)
	require.ErrorIs(t, err, ErrClientClosed)

	_, err = client.NextAvailableSessionID(clientAwaitTestStreamID)
	require.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_NextAvailableSessionID_nilDriverProxy(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	_, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)
	client := NewClientForTestWithDriverProxy(
		fixture.Conductor,
		nil,
		fixture.NanoClock,
		testutil.TestClientID,
		time.Second,
		time.Millisecond,
	)

	_, err := client.NextAvailableSessionID(clientAwaitTestStreamID)
	require.ErrorIs(t, err, ErrClientNotReady)
}
