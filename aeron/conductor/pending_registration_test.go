package conductor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	pendingTestCorrelationID      int64 = 7
	pendingTestOtherCorrelationID int64 = 8
	pendingTestTimeout                  = time.Second
)

func TestConductor_RegisterPending_duplicate(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)

	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	err := conductorInstance.RegisterPending(
		conductor.PendingKindNextSessionID,
		pendingTestCorrelationID,
		pendingTestTimeout,
	)
	require.ErrorIs(t, err, conductor.ErrPendingAlreadyRegistered)
	require.EqualError(t, err, "conductor: correlation already pending")
}

func TestConductor_RegisterPending_terminating(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	conductorInstance.SetTerminatingForTest(true)

	err := conductorInstance.RegisterPending(
		conductor.PendingKindNextSessionID,
		pendingTestCorrelationID,
		pendingTestTimeout,
	)
	require.ErrorIs(t, err, conductor.ErrConductorTerminating)
	require.EqualError(t, err, "conductor: conductor is terminating")
}

func TestConductor_PollPending_notReceived(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.False(t, done)
	require.Zero(t, registration)
	require.NoError(t, err)
}

func TestConductor_PollPending_success(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	require.NoError(
		t,
		conductorInstance.CompletePendingForTest(pendingTestCorrelationID, 42, nil),
	)

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.Equal(t, int32(42), registration.Int32Value)
	require.NoError(t, err)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.False(t, ok)
}

func TestConductor_PollPending_driverError(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	driverErr := &conductor.DriverRegistrationError{
		CorrelationID: pendingTestCorrelationID,
		ErrorCode:     11,
		Message:       "registration failed",
	}
	require.NoError(
		t,
		conductorInstance.CompletePendingForTest(pendingTestCorrelationID, 0, driverErr),
	)

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.Equal(t, driverErr, registration.Err)
	require.ErrorIs(t, err, driverErr)
	require.EqualError(
		t,
		err,
		"conductor: driver registration error: correlation=7 code=11 message=registration failed",
	)
}

func TestConductor_PollPending_receivedWithoutRegistration(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	conductorInstance.SetReceivedCorrelationIDForTest(pendingTestCorrelationID)

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.Zero(t, registration)
	require.ErrorIs(t, err, conductor.ErrPendingNotFound)
	require.EqualError(t, err, "conductor: no pending registration for correlation")
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_CancelPending_idempotent(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetReceivedCorrelationIDForTest(pendingTestCorrelationID)

	conductorInstance.CancelPending(pendingTestCorrelationID)
	conductorInstance.CancelPending(pendingTestCorrelationID)
	_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.False(t, ok)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_ClearPending(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			pendingTestOtherCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetReceivedCorrelationIDForTest(pendingTestCorrelationID)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	conductorInstance.ClearPending()

	_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.False(t, ok)
	_, ok = conductorInstance.PendingResult(pendingTestOtherCorrelationID)
	require.False(t, ok)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ActiveCorrelationID())
}

func TestConductor_PollPending_wrongCorrelation(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetReceivedCorrelationIDForTest(pendingTestCorrelationID)

	done, registration, err := conductorInstance.PollPending(pendingTestOtherCorrelationID)
	require.False(t, done)
	require.Zero(t, registration)
	require.NoError(t, err)
	require.Equal(t, pendingTestCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_newConductor_initializesAwaitState(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ActiveCorrelationID())
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_terminateClearsPending(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		1_000,
		(600 * time.Millisecond).Nanoseconds(),
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)

	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(conductor.NullCorrelationID)
	_ = conductorInstance.DoWork()

	require.True(t, conductorInstance.IsTerminating())
	_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.False(t, ok)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ActiveCorrelationID())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, conductorInstance.TerminationCause(), conductor.ErrDriverShutdown)
}

func TestConductor_pendingHandler_wrongKind(t *testing.T) {
	tests := []struct {
		name           string
		registeredKind conductor.PendingKind
		invoke         func(*conductor.Conductor) bool
		wantKind       conductor.PendingKind
		wantInt32Value int32
	}{
		{
			name:           "OnNextAvailableSessionID",
			registeredKind: conductor.PendingKindOperationSuccess,
			invoke: func(conductorInstance *conductor.Conductor) bool {
				return conductorInstance.OnNextAvailableSessionID(pendingTestCorrelationID, 42)
			},
			wantKind:       conductor.PendingKindOperationSuccess,
			wantInt32Value: 0,
		},
		{
			name:           "OnOperationSuccess",
			registeredKind: conductor.PendingKindNextSessionID,
			invoke: func(conductorInstance *conductor.Conductor) bool {
				return conductorInstance.OnOperationSuccess(pendingTestCorrelationID)
			},
			wantKind: conductor.PendingKindNextSessionID,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)
			require.NoError(
				t,
				conductorInstance.RegisterPending(
					testCase.registeredKind,
					pendingTestCorrelationID,
					pendingTestTimeout,
				),
			)

			ok := testCase.invoke(conductorInstance)
			require.False(t, ok)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())

			registration, found := conductorInstance.PendingResult(pendingTestCorrelationID)
			require.True(t, found)
			require.Equal(t, testCase.wantKind, registration.Kind)
			require.Equal(t, testCase.wantInt32Value, registration.Int32Value)
		})
	}
}

func TestConductor_CompletePendingForTest_missingRegistration(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	err := conductorInstance.CompletePendingForTest(pendingTestCorrelationID, 1, nil)
	require.ErrorIs(t, err, conductor.ErrPendingNotFound)
	require.EqualError(t, err, "conductor: no pending registration for correlation")
}

func TestConductor_expirePending_viaDoWork(t *testing.T) {
	const startNs = int64(1_000)
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(t, func(error) {}, 0, &nanoNs),
	)
	nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestOtherCorrelationID,
			500*time.Nanosecond,
		),
	)

	nanoNs = startNs + 500
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	_, ok := conductorInstance.PendingResult(pendingTestOtherCorrelationID)
	require.False(t, ok)
	done, _, err := conductorInstance.PollPending(pendingTestOtherCorrelationID)
	require.False(t, done)
	require.NoError(t, err)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_expirePending_activeAwaitTimeout(t *testing.T) {
	const startNs = int64(1_000)
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(t, func(error) {}, 0, &nanoNs),
	)
	nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			500*time.Nanosecond,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	nanoNs = startNs + 500
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.ErrorIs(t, err, conductor.ErrAwaitTimeout)
	require.EqualError(t, err, "conductor: await response timeout")
}

func TestConductor_expirePending_lateSuccessClearsLatchedTimeout(t *testing.T) {
	const (
		startNs             = int64(1_000)
		nextSessionID int32 = 42
	)
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(t, func(error) {}, 0, &nanoNs),
	)
	nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			500*time.Nanosecond,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	nanoNs = startNs + 500
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.Equal(t, pendingTestCorrelationID, conductorInstance.ReceivedCorrelationID())
	registration, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.True(t, ok)
	require.ErrorIs(t, registration.Err, conductor.ErrAwaitTimeout)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnNextAvailableSessionID,
		conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, nextSessionID),
	)
	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, nextSessionID, registration.Int32Value)
}

func TestConductor_expirePending_sameTickCompletionNotInterrupted(t *testing.T) {
	const (
		startNs             = int64(1_000)
		nextSessionID int32 = 42
	)
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(t, func(error) {}, 0, &nanoNs),
	)
	nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			500*time.Nanosecond,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnNextAvailableSessionID,
		conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, nextSessionID),
	)

	nanoNs = startNs + 500
	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, nextSessionID, registration.Int32Value)
}

func TestConductor_service_resetsReceivedCorrelationIDEachTick(t *testing.T) {
	const nextSessionID int32 = 42
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnNextAvailableSessionID,
		conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, nextSessionID),
	)

	_ = conductorInstance.DoWork()
	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, nextSessionID, registration.Int32Value)

	_ = conductorInstance.DoWork()
	done, _, err = conductorInstance.PollPending(pendingTestCorrelationID)
	require.False(t, done)
	require.NoError(t, err)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}
