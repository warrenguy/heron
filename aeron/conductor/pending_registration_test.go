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

func TestConductor_completeNextSessionID_wrongKind(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)

	ok := conductorInstance.CompleteNextSessionIDForTest(pendingTestCorrelationID, 42)
	require.False(t, ok)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())

	registration, found := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.True(t, found)
	require.Equal(t, conductor.PendingKindOperationSuccess, registration.Kind)
	require.Equal(t, int32(0), registration.Int32Value)
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
