package conductor_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
)

type registerPendingAddErrorCase struct {
	name       string
	setup      func(*conductor.Conductor)
	kind       conductor.PendingKind
	wantErr    error
	wantErrMsg string
}

const (
	pendingTestCorrelationID      int64 = 7
	pendingTestOtherCorrelationID int64 = 8
	pendingTestStreamID           int32 = 7
	pendingTestChannel                  = "aeron:ipc"
	pendingTestTimeout                  = time.Second
)

func registerPendingForTest(
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
) error {
	return conductorInstance.RegisterPending(
		kind,
		pendingTestCorrelationID,
		pendingTestTimeout,
	)
}

func registerPendingAddForTest(
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
) error {
	return registerPendingAddWithCorrelationForTest(
		conductorInstance,
		kind,
		pendingTestCorrelationID,
		pendingTestStreamID,
	)
}

func registerPendingKindWithDeadlineForTest(
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID int64,
	streamID int32,
	channel string,
	timeout time.Duration,
) error {
	if conductor.AddPendingKindForTest(kind) {
		return conductorInstance.RegisterPendingAdd(
			kind,
			correlationID,
			streamID,
			channel,
			timeout,
		)
	}
	return conductorInstance.RegisterPending(kind, correlationID, timeout)
}

func registerPendingAddWithCorrelationForTest(
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID int64,
	streamID int32,
) error {
	return conductorInstance.RegisterPendingAdd(
		kind,
		correlationID,
		streamID,
		pendingTestChannel,
		pendingTestTimeout,
	)
}

func registerTwoPendingKindWithDeadlineForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID0 int64,
	correlationID1 int64,
	streamID int32,
	channel string,
	timeout time.Duration,
) {
	tb.Helper()
	for _, correlationID := range []int64{correlationID0, correlationID1} {
		require.NoError(
			tb,
			registerPendingKindWithDeadlineForTest(
				conductorInstance,
				kind,
				correlationID,
				streamID,
				channel,
				timeout,
			),
		)
	}
}

func registerTwoPendingAddsWithDeadlineForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID0 int64,
	correlationID1 int64,
	timeout time.Duration,
) {
	tb.Helper()
	registerTwoPendingKindWithDeadlineForTest(
		tb,
		conductorInstance,
		kind,
		correlationID0,
		correlationID1,
		pendingTestStreamID,
		pendingTestChannel,
		timeout,
	)
}

func registerPendingKindForTest(
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID int64,
) error {
	return registerPendingKindWithDeadlineForTest(
		conductorInstance,
		kind,
		correlationID,
		pendingTestStreamID,
		pendingTestChannel,
		pendingTestTimeout,
	)
}

type addRemovePendingKindCase struct {
	name string
	kind conductor.PendingKind
	add  bool
}

var addPendingKindCases = []struct {
	name string
	kind conductor.PendingKind
}{
	{name: "addPublication", kind: conductor.PendingKindAddPublication},
	{name: "addSubscription", kind: conductor.PendingKindAddSubscription},
	{name: "addExclusivePublication", kind: conductor.PendingKindAddExclusivePublication},
}

func addRemovePendingKindCases() []addRemovePendingKindCase {
	cases := make([]addRemovePendingKindCase, 0, len(addPendingKindCases)+2)
	for _, kindCase := range addPendingKindCases {
		cases = append(cases, addRemovePendingKindCase{
			name: kindCase.name,
			kind: kindCase.kind,
			add:  true,
		})
	}
	return append(
		cases,
		addRemovePendingKindCase{
			name: "removePublication",
			kind: conductor.PendingKindRemovePublication,
		},
		addRemovePendingKindCase{
			name: "removeSubscription",
			kind: conductor.PendingKindRemoveSubscription,
		},
	)
}

func registerAddRemovePendingKindCase(
	conductorInstance *conductor.Conductor,
	testCase addRemovePendingKindCase,
) error {
	if testCase.add {
		return registerPendingAddForTest(conductorInstance, testCase.kind)
	}
	return registerPendingForTest(conductorInstance, testCase.kind)
}

func invokeNextSessionIDWrongKind(conductorInstance *conductor.Conductor) bool {
	return conductorInstance.NextSessionID(pendingTestCorrelationID, 42)
}

func requireNilPublicationHandle(t *testing.T, registration conductor.PendingRegistration) {
	t.Helper()
	require.Nil(t, registration.Publication)
}

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

func TestConductor_RegisterPending_rejectsAddKinds(t *testing.T) {
	for _, testCase := range addPendingKindCases {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)

			err := registerPendingForTest(conductorInstance, testCase.kind)
			require.ErrorIs(t, err, conductor.ErrAddPendingKindRequiresAddAPI)
			require.EqualError(
				t,
				err,
				"conductor: add pending kind requires RegisterPendingAdd",
			)
		})
	}
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
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
	require.False(t, ok)
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

	t.Run("addSubscriptionPreInsert", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		require.NoError(
			t,
			registerPendingKindForTest(
				conductorInstance,
				conductor.PendingKindAddSubscription,
				goldenSubscriptionReadyCorrelationID,
			),
		)
		require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

		conductorInstance.ClearPending()

		require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
		require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
		_, ok := conductorInstance.SubscriptionByRegID(goldenSubscriptionReadyCorrelationID)
		require.False(t, ok)
	})

	publicationClearPendingCases := []struct {
		name  string
		setup func(testing.TB, *conductor.Conductor)
		regID int64
	}{
		{
			name:  "addPublicationAfterReady",
			setup: completeGoldenPublicationReadyForTest,
			regID: goldenPublicationBuffersReadyEvent.CorrelationID,
		},
		{
			name:  "addExclusivePublicationAfterReady",
			setup: completeGoldenExclusivePublicationReadyForTest,
			regID: goldenExclusivePublicationBuffersReadyEvent.CorrelationID,
		},
	}
	for _, testCase := range publicationClearPendingCases {
		t.Run(testCase.name, func(t *testing.T) {
			lifecycle := &recordingResourceLifecycle{}
			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
			testCase.setup(t, conductorInstance)
			require.Equal(t, 1, conductorInstance.PublicationCountForTest())

			conductorInstance.ClearPending()

			require.Equal(t, 0, conductorInstance.PublicationCountForTest())
			require.Equal(t, 1, lifecycle.retirePublicationCalls)
			_, ok := conductorInstance.PublicationByRegID(testCase.regID)
			require.False(t, ok)
		})
	}
}

func TestConductor_PollPending_mapCompletionWhenMarkMismatch(t *testing.T) {
	const (
		correlationID0       int64 = 101
		correlationID1       int64 = 102
		correlationIDSuccess int64 = 201
		correlationIDFail    int64 = 202
	)

	tests := []struct {
		name         string
		kind         conductor.PendingKind
		correlation0 int64
		correlation1 int64
		complete     func(*testing.T, *conductor.Conductor)
		assertFirst  func(*testing.T, conductor.PendingRegistration)
		assertSecond func(*testing.T, conductor.PendingRegistration, error)
	}{
		{
			name:         "successAndSuccess",
			kind:         conductor.PendingKindAddSubscription,
			correlation0: correlationID0,
			correlation1: correlationID1,
			complete: func(_ *testing.T, conductorInstance *conductor.Conductor) {
				conductorInstance.SubscriptionReadyForTest(
					goldenSubscriptionReadyWithCorrelation(correlationID0),
				)
				conductorInstance.SubscriptionReadyForTest(
					goldenSubscriptionReadyWithCorrelation(correlationID1),
				)
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.NoError(t, err)
				require.True(t, registration.Ready)
			},
		},
		{
			name:         "successAndError",
			kind:         conductor.PendingKindAddPublication,
			correlation0: correlationIDSuccess,
			correlation1: correlationIDFail,
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				conductorInstance.PublicationReadyForTest(
					goldenPublicationBuffersReadyWithCorrelation(correlationIDSuccess),
				)
				require.True(
					t,
					conductorInstance.DriverError(conductor.ErrorResponse{
						OffendingCommandCorrelationID: correlationIDFail,
						ErrorCode:                     11,
						ErrorMessage:                  "add publication rejected",
					}),
				)
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.Error(t, err)
				require.False(t, registration.Ready)
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(t, registration.Err, &driverErr)
				require.Equal(t, correlationIDFail, driverErr.CorrelationID)
				require.Equal(t, int32(11), driverErr.ErrorCode)
			},
		},
		{
			name:         "publicationZeroCorrelationAndSuccess",
			kind:         conductor.PendingKindAddPublication,
			correlation0: 0,
			correlation1: correlationID1,
			complete: func(_ *testing.T, conductorInstance *conductor.Conductor) {
				conductorInstance.PublicationReadyForTest(
					goldenPublicationBuffersReadyWithCorrelation(0),
				)
				conductorInstance.PublicationReadyForTest(
					goldenPublicationBuffersReadyWithCorrelation(correlationID1),
				)
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.NoError(t, err)
				require.True(t, registration.Ready)
			},
		},
		{
			name:         "counterReadyAndCounterReady",
			kind:         conductor.PendingKindAddCounter,
			correlation0: correlationID0,
			correlation1: correlationID1,
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.True(t, conductorInstance.CounterReady(correlationID0, 3))
				require.True(t, conductorInstance.CounterReady(correlationID1, 4))
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
				require.Equal(t, int32(3), registration.Int32Value)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.NoError(t, err)
				require.True(t, registration.Ready)
				require.Equal(t, int32(4), registration.Int32Value)
			},
		},
		{
			name:         "operationSuccessAndError",
			kind:         conductor.PendingKindOperationSuccess,
			correlation0: correlationIDSuccess,
			correlation1: correlationIDFail,
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.True(t, conductorInstance.OperationSucceeded(correlationIDSuccess))
				require.True(
					t,
					conductorInstance.DriverError(conductor.ErrorResponse{
						OffendingCommandCorrelationID: correlationIDFail,
						ErrorCode:                     11,
						ErrorMessage:                  "add destination rejected",
					}),
				)
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.False(t, registration.Ready)
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(t, err, &driverErr)
				require.Equal(t, correlationIDFail, driverErr.CorrelationID)
				require.Equal(t, int32(11), driverErr.ErrorCode)
			},
		},
		{
			name:         "nextSessionIDAndError",
			kind:         conductor.PendingKindNextSessionID,
			correlation0: correlationIDSuccess,
			correlation1: correlationIDFail,
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.True(t, conductorInstance.NextSessionID(correlationIDSuccess, 42))
				require.True(
					t,
					conductorInstance.DriverError(conductor.ErrorResponse{
						OffendingCommandCorrelationID: correlationIDFail,
						ErrorCode:                     11,
						ErrorMessage:                  "next session id rejected",
					}),
				)
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
				require.Equal(t, int32(42), registration.Int32Value)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.False(t, registration.Ready)
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(t, err, &driverErr)
				require.Equal(t, correlationIDFail, driverErr.CorrelationID)
				require.Equal(t, int32(11), driverErr.ErrorCode)
			},
		},
		{
			name:         "removePublicationAndSuccess",
			kind:         conductor.PendingKindRemovePublication,
			correlation0: correlationID0,
			correlation1: correlationID1,
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.True(t, conductorInstance.OperationSucceeded(correlationID0))
				require.True(t, conductorInstance.OperationSucceeded(correlationID1))
			},
			assertFirst: func(t *testing.T, registration conductor.PendingRegistration) {
				require.True(t, registration.Ready)
			},
			assertSecond: func(t *testing.T, registration conductor.PendingRegistration, err error) {
				require.NoError(t, err)
				require.True(t, registration.Ready)
			},
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
			registerTwoPendingAddsWithDeadlineForTest(
				t,
				conductorInstance,
				testCase.kind,
				testCase.correlation0,
				testCase.correlation1,
				pendingTestTimeout,
			)
			testCase.complete(t, conductorInstance)
			require.Equal(t, testCase.correlation1, conductorInstance.ReceivedCorrelationID())

			done0, registration0, err := conductorInstance.PollPending(testCase.correlation0)
			require.True(t, done0)
			require.NoError(t, err)
			testCase.assertFirst(t, registration0)
			require.Equal(t, testCase.correlation1, conductorInstance.ReceivedCorrelationID())

			done1, registration1, err := conductorInstance.PollPending(testCase.correlation1)
			require.True(t, done1)
			testCase.assertSecond(t, registration1, err)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
		})
	}
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
	type wrongKindTestCase struct {
		name           string
		register       func(*conductor.Conductor) error
		invoke         func(*conductor.Conductor) bool
		wantKind       conductor.PendingKind
		wantInt32Value int32
	}

	tests := []wrongKindTestCase{
		{
			name: "OnNextAvailableSessionID",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindOperationSuccess)
			},
			invoke:         invokeNextSessionIDWrongKind,
			wantKind:       conductor.PendingKindOperationSuccess,
			wantInt32Value: 0,
		},
		{
			name: "OnOperationSuccess",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindNextSessionID)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				return conductorInstance.OperationSucceeded(pendingTestCorrelationID)
			},
			wantKind: conductor.PendingKindNextSessionID,
		},
		{
			name: "OnPublicationReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddSubscription)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.PublicationReadyForTest(
					goldenPublicationBuffersReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindAddSubscription,
		},
		{
			name: "OnSubscriptionReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddPublication)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.SubscriptionReadyForTest(
					goldenSubscriptionReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindAddPublication,
		},
		{
			name: "OnSubscriptionReady_removePublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemovePublication)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.SubscriptionReadyForTest(
					goldenSubscriptionReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindRemovePublication,
		},
		{
			name: "OnPublicationReady_removeSubscription",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemoveSubscription)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.PublicationReadyForTest(
					goldenPublicationBuffersReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindRemoveSubscription,
		},
		{
			name: "OnExclusivePublicationReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddSubscription)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.ExclusivePublicationReadyForTest(
					goldenExclusivePublicationBuffersReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindAddSubscription,
		},
		{
			name: "OnExclusivePublicationReady_addPublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddPublication)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.ExclusivePublicationReadyForTest(
					goldenExclusivePublicationBuffersReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindAddPublication,
		},
		{
			name: "OnPublicationReady_addExclusivePublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(
					conductorInstance,
					conductor.PendingKindAddExclusivePublication,
				)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.PublicationReadyForTest(
					goldenPublicationBuffersReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindAddExclusivePublication,
		},
		{
			name: "OnSubscriptionReady_addExclusivePublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(
					conductorInstance,
					conductor.PendingKindAddExclusivePublication,
				)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				conductorInstance.SubscriptionReadyForTest(
					goldenSubscriptionReadyWithCorrelation(pendingTestCorrelationID),
				)
				return false
			},
			wantKind: conductor.PendingKindAddExclusivePublication,
		},
		{
			name: "OnCounterReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindNextSessionID)
			},
			invoke: func(conductorInstance *conductor.Conductor) bool {
				return conductorInstance.CounterReady(pendingTestCorrelationID, 3)
			},
			wantKind: conductor.PendingKindNextSessionID,
		},
	}

	for _, testCase := range addRemovePendingKindCases() {
		kindCase := testCase
		tests = append(tests, wrongKindTestCase{
			name: "OnNextAvailableSessionID_" + kindCase.name,
			register: func(conductorInstance *conductor.Conductor) error {
				return registerAddRemovePendingKindCase(conductorInstance, kindCase)
			},
			invoke:   invokeNextSessionIDWrongKind,
			wantKind: kindCase.kind,
		})
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)
			require.NoError(t, testCase.register(conductorInstance))

			ok := testCase.invoke(conductorInstance)
			require.False(t, ok)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())

			done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
			require.False(t, done)
			require.NoError(t, err)

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

func TestConductor_CompletePendingForTest_missingRegistrationOnError(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	driverErr := errors.New("conductor: driver rejected registration")
	err := conductorInstance.CompletePendingForTest(pendingTestCorrelationID, 0, driverErr)
	require.ErrorIs(t, err, conductor.ErrPendingNotFound)
	require.EqualError(t, err, "conductor: no pending registration for correlation")
}

func TestConductor_expirePending_preservesSucceededUnpolledAdd_multiPending(t *testing.T) {
	const (
		correlationID0 int64 = 101
		correlationID1 int64 = 102
		startNs              = int64(1_000)
		expireNs             = 500 * time.Nanosecond
	)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	registerTwoPendingAddsWithDeadlineForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
		correlationID0,
		correlationID1,
		expireNs,
	)
	conductorInstance.SubscriptionReadyForTest(
		goldenSubscriptionReadyWithCorrelation(correlationID0),
	)

	*nanoNs = startNs + int64(expireNs)
	conductorInstance.SetActiveCorrelationID(correlationID1)
	_ = conductorInstance.DoWork()

	registration, ok := conductorInstance.PendingResult(correlationID0)
	require.True(t, ok)
	require.NoError(t, registration.Err)
	require.True(t, registration.Ready)

	done, polledRegistration, err := conductorInstance.PollPending(correlationID0)
	require.True(t, done)
	require.NoError(t, err)
	require.True(t, polledRegistration.Ready)
	_, ok = conductorInstance.PendingResult(correlationID0)
	require.False(t, ok)

	requirePendingAddExpiredForTest(t, conductorInstance, correlationID1)
}

func TestConductor_expirePending_preservesDriverError_multiPending(t *testing.T) {
	const (
		correlationIDSuccess int64 = 101
		correlationIDFail    int64 = 102
		startNs                    = int64(1_000)
	)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	registerTwoPendingAddsWithDeadlineForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
		correlationIDSuccess,
		correlationIDFail,
		pendingTestTimeout,
	)
	require.True(
		t,
		conductorInstance.DriverError(conductor.ErrorResponse{
			OffendingCommandCorrelationID: correlationIDFail,
			ErrorCode:                     11,
			ErrorMessage:                  "add subscription rejected",
		}),
	)

	*nanoNs = startNs + 500
	_ = conductorInstance.DoWork()

	registration, ok := conductorInstance.PendingResult(correlationIDFail)
	require.True(t, ok)
	var driverErr *conductor.DriverRegistrationError
	require.ErrorAs(t, registration.Err, &driverErr)
	require.Equal(t, correlationIDFail, driverErr.CorrelationID)
	require.Equal(t, int32(11), driverErr.ErrorCode)
	require.NotErrorIs(t, registration.Err, conductor.ErrAwaitTimeout)

	done, polledRegistration, err := conductorInstance.PollPending(correlationIDFail)
	require.True(t, done)
	require.ErrorAs(t, polledRegistration.Err, &driverErr)
	require.ErrorAs(t, err, &driverErr)
	require.NotErrorIs(t, err, conductor.ErrAwaitTimeout)
	_, ok = conductorInstance.PendingResult(correlationIDFail)
	require.False(t, ok)

	_, ok = conductorInstance.PendingResult(correlationIDSuccess)
	require.True(t, ok)
}

func TestConductor_expirePending_viaDoWork(t *testing.T) {
	const startNs = int64(1_000)
	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestOtherCorrelationID,
			500*time.Nanosecond,
		),
	)

	*nanoNs = startNs + 500
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
	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			500*time.Nanosecond,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	*nanoNs = startNs + 500
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.ErrorIs(t, err, conductor.ErrAwaitTimeout)
	require.EqualError(t, err, "conductor: await response timeout")
}

func TestConductor_expirePending_lateSuccessClearsMarkedTimeout(t *testing.T) {
	const (
		startNs             = int64(1_000)
		nextSessionID int32 = 42
	)
	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			500*time.Nanosecond,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	*nanoNs = startNs + 500
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
	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
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

	*nanoNs = startNs + 500
	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, nextSessionID, registration.Int32Value)
}

func TestConductor_expirePending_unpolledSuccessNotTimedOut(t *testing.T) {
	const (
		startNs             = int64(1_000)
		nextSessionID int32 = 42
	)
	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindNextSessionID,
			pendingTestCorrelationID,
			500*time.Nanosecond,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	// Phase 1: NextSessionID marks the active entry.
	require.True(t, conductorInstance.NextSessionID(pendingTestCorrelationID, nextSessionID))
	require.Equal(t, pendingTestCorrelationID, conductorInstance.ReceivedCorrelationID())

	// Phase 2: the deadline tick clears the mark before ExpireBefore runs.
	*nanoNs = startNs + 500
	_ = conductorInstance.DoWork()

	// Phase 3: PollPending still returns the success.
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

func TestConductor_RegisterPendingAdd_errors(t *testing.T) {
	tests := []registerPendingAddErrorCase{
		{
			name: "duplicate",
			setup: func(conductorInstance *conductor.Conductor) {
				require.NoError(
					t,
					registerPendingAddForTest(conductorInstance, conductor.PendingKindAddPublication),
				)
			},
			kind:       conductor.PendingKindAddPublication,
			wantErr:    conductor.ErrPendingAlreadyRegistered,
			wantErrMsg: "conductor: correlation already pending",
		},
		{
			name: "terminating",
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.SetTerminatingForTest(true)
			},
			kind:       conductor.PendingKindAddSubscription,
			wantErr:    conductor.ErrConductorTerminating,
			wantErrMsg: "conductor: conductor is terminating",
		},
	}

	invalidAddKindCases := []struct {
		name string
		kind conductor.PendingKind
	}{
		{name: "invalidKind", kind: conductor.PendingKindRemovePublication},
		{name: "nextSessionID", kind: conductor.PendingKindNextSessionID},
		{name: "operationSuccess", kind: conductor.PendingKindOperationSuccess},
		{name: "removeSubscription", kind: conductor.PendingKindRemoveSubscription},
		{name: "addCounter", kind: conductor.PendingKindAddCounter},
		{name: "destination", kind: conductor.PendingKindDestination},
	}
	for _, invalidCase := range invalidAddKindCases {
		tests = append(tests, registerPendingAddErrorCase{
			name:       invalidCase.name,
			kind:       invalidCase.kind,
			wantErr:    conductor.ErrNonAddPendingKindRequiresRegisterPending,
			wantErrMsg: "conductor: pending kind requires RegisterPending",
		})
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)
			if testCase.setup != nil {
				testCase.setup(conductorInstance)
			}

			err := registerPendingAddForTest(conductorInstance, testCase.kind)
			require.ErrorIs(t, err, testCase.wantErr)
			require.EqualError(t, err, testCase.wantErrMsg)
		})
	}
}

func TestConductor_OperationSucceeded_unknownCorrelation(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	ok := conductorInstance.OperationSucceeded(pendingTestCorrelationID)
	require.False(t, ok)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_PollPending_addReady(t *testing.T) {
	tests := []struct {
		name          string
		kind          conductor.PendingKind
		correlationID int64
		streamID      int32
		signalReady   func(*conductor.Conductor)
		wantStreamID  int32
		wantHandleNil func(t *testing.T, registration conductor.PendingRegistration)
	}{
		{
			name:          "publication",
			kind:          conductor.PendingKindAddPublication,
			correlationID: goldenPublicationReadyCorrelationID,
			streamID:      goldenPublicationBuffersReadyEvent.StreamID,
			signalReady: func(conductorInstance *conductor.Conductor) {
				conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
			},
			wantStreamID:  goldenPublicationBuffersReadyEvent.StreamID,
			wantHandleNil: requireNilPublicationHandle,
		},
		{
			name:          "exclusivePublication",
			kind:          conductor.PendingKindAddExclusivePublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			streamID:      goldenExclusivePublicationBuffersReadyEvent.StreamID,
			signalReady: func(conductorInstance *conductor.Conductor) {
				conductorInstance.ExclusivePublicationReadyForTest(
					goldenExclusivePublicationBuffersReadyEvent,
				)
			},
			wantStreamID:  goldenExclusivePublicationBuffersReadyEvent.StreamID,
			wantHandleNil: requireNilPublicationHandle,
		},
		{
			name:          "subscription",
			kind:          conductor.PendingKindAddSubscription,
			correlationID: goldenSubscriptionReadyCorrelationID,
			streamID:      pendingTestStreamID,
			signalReady: func(conductorInstance *conductor.Conductor) {
				conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)
			},
			wantStreamID: pendingTestStreamID,
			wantHandleNil: func(t *testing.T, registration conductor.PendingRegistration) {
				require.Nil(t, registration.Subscription)
			},
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
				registerPendingAddWithCorrelationForTest(
					conductorInstance,
					testCase.kind,
					testCase.correlationID,
					testCase.streamID,
				),
			)
			testCase.signalReady(conductorInstance)

			done, registration, err := conductorInstance.PollPending(testCase.correlationID)
			require.True(t, done)
			require.NoError(t, err)
			require.Equal(t, testCase.kind, registration.Kind)
			require.True(t, registration.Ready)
			require.Equal(t, testCase.wantStreamID, registration.StreamID)
			require.Equal(t, pendingTestChannel, registration.Channel)
			testCase.wantHandleNil(t, registration)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			_, ok := conductorInstance.PendingResult(testCase.correlationID)
			require.False(t, ok)
		})
	}
}

func TestConductor_PollPending_onError(t *testing.T) {
	tests := []struct {
		name         string
		register     func(*conductor.Conductor) error
		errorCode    int32
		errorMessage string
	}{
		{
			name: "addPublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddPublication)
			},
			errorCode:    11,
			errorMessage: "add publication failed",
		},
		{
			name: "addExclusivePublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(
					conductorInstance,
					conductor.PendingKindAddExclusivePublication,
				)
			},
			errorCode:    15,
			errorMessage: "add exclusive publication failed",
		},
		{
			name: "addSubscription",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddSubscription)
			},
			errorCode:    12,
			errorMessage: "add subscription failed",
		},
		{
			name: "removePublication",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemovePublication)
			},
			errorCode:    13,
			errorMessage: "remove publication failed",
		},
		{
			name: "removeSubscription",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemoveSubscription)
			},
			errorCode:    14,
			errorMessage: "remove subscription failed",
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
			require.NoError(t, testCase.register(conductorInstance))
			require.True(
				t,
				conductorInstance.DriverError(conductor.ErrorResponse{
					OffendingCommandCorrelationID: pendingTestCorrelationID,
					ErrorCode:                     testCase.errorCode,
					ErrorMessage:                  testCase.errorMessage,
				}),
			)

			done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
			require.True(t, done)
			var driverErr *conductor.DriverRegistrationError
			require.ErrorAs(t, registration.Err, &driverErr)
			require.ErrorAs(t, err, &driverErr)
			require.Equal(t, pendingTestCorrelationID, driverErr.CorrelationID)
			require.Equal(t, testCase.errorCode, driverErr.ErrorCode)
			require.Equal(t, testCase.errorMessage, driverErr.Message)
			wantErr := (&conductor.DriverRegistrationError{
				CorrelationID: pendingTestCorrelationID,
				ErrorCode:     testCase.errorCode,
				Message:       testCase.errorMessage,
			}).Error()
			require.EqualError(t, err, wantErr)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
			require.False(t, ok)
		})
	}
}

func TestConductor_OperationSucceeded_addKindsAckOnly(t *testing.T) {
	for _, testCase := range addPendingKindCases {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)
			require.NoError(t, registerPendingAddForTest(conductorInstance, testCase.kind))

			ok := conductorInstance.OperationSucceeded(pendingTestCorrelationID)
			require.True(t, ok)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())

			_, found := conductorInstance.PendingResult(pendingTestCorrelationID)
			require.True(t, found)

			done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
			require.False(t, done)
			require.NoError(t, err)
		})
	}
}

func TestConductor_OperationSucceeded_removeKinds(t *testing.T) {
	tests := []struct {
		name string
		kind conductor.PendingKind
	}{
		{name: "removePublication", kind: conductor.PendingKindRemovePublication},
		{name: "removeSubscription", kind: conductor.PendingKindRemoveSubscription},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)
			require.NoError(t, registerPendingForTest(conductorInstance, testCase.kind))

			ok := conductorInstance.OperationSucceeded(pendingTestCorrelationID)
			require.True(t, ok)

			done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
			require.True(t, done)
			require.NoError(t, err)
			require.NoError(t, registration.Err)
			require.Equal(t, testCase.kind, registration.Kind)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			_, ok = conductorInstance.PendingResult(pendingTestCorrelationID)
			require.False(t, ok)
		})
	}
}

func TestConductor_OperationSucceeded_destination(t *testing.T) {
	tests := []struct {
		name     string
		complete func(*testing.T, *conductor.Conductor)
		wantErr  bool
	}{
		{
			name: "success",
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.True(t, conductorInstance.OperationSucceeded(pendingTestCorrelationID))
			},
		},
		{
			name: "driverError",
			complete: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.True(t, conductorInstance.DriverError(conductor.ErrorResponse{
					OffendingCommandCorrelationID: pendingTestCorrelationID,
					ErrorCode:                     3,
					ErrorMessage:                  "unknown publication",
				}))
			},
			wantErr: true,
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
				registerPendingForTest(conductorInstance, conductor.PendingKindDestination),
			)

			testCase.complete(t, conductorInstance)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			registration, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
			require.True(t, ok)
			require.Equal(t, !testCase.wantErr, registration.Ready)
			require.Equal(t, testCase.wantErr, registration.Err != nil)

			_ = conductorInstance.DoWork()
			done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
			require.True(t, done)
			require.Equal(t, conductor.PendingKindDestination, registration.Kind)
			if testCase.wantErr {
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(t, err, &driverErr)
				require.Equal(t, int32(3), driverErr.ErrorCode)
			} else {
				require.NoError(t, err)
			}
			_, ok = conductorInstance.PendingResult(pendingTestCorrelationID)
			require.False(t, ok)
		})
	}
}

// TestConductor_destinationCompletion_survivesForeignMark delivers a
// destination success and a foreign driver error before one PollPending. One
// DoWork reads one broadcast record, so the test calls the handlers directly.
func TestConductor_destinationCompletion_survivesForeignMark(t *testing.T) {
	const (
		destinationCorrelationID int64 = 301
		addCorrelationID         int64 = 302
	)
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(t, registerPendingKindForTest(
		conductorInstance,
		conductor.PendingKindDestination,
		destinationCorrelationID,
	))
	require.NoError(t, registerPendingKindForTest(
		conductorInstance,
		conductor.PendingKindAddPublication,
		addCorrelationID,
	))
	conductorInstance.SetActiveCorrelationID(destinationCorrelationID)

	require.True(t, conductorInstance.OperationSucceeded(destinationCorrelationID))
	require.True(t, conductorInstance.DriverError(conductor.ErrorResponse{
		OffendingCommandCorrelationID: addCorrelationID,
		ErrorCode:                     11,
		ErrorMessage:                  "add publication rejected",
	}))
	require.Equal(t, addCorrelationID, conductorInstance.ReceivedCorrelationID())

	done, registration, err := conductorInstance.PollPending(destinationCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.True(t, registration.Ready)
	require.Equal(t, addCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_addPending_ackThenReady(t *testing.T) {
	tests := []struct {
		name         string
		kind         conductor.PendingKind
		correlation  int64
		readyHandler func(*conductor.Conductor)
	}{
		{
			name:        "addPublication",
			kind:        conductor.PendingKindAddPublication,
			correlation: goldenPublicationReadyCorrelationID,
			readyHandler: func(conductorInstance *conductor.Conductor) {
				conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
			},
		},
		{
			name:        "addSubscription",
			kind:        conductor.PendingKindAddSubscription,
			correlation: goldenSubscriptionReadyCorrelationID,
			readyHandler: func(conductorInstance *conductor.Conductor) {
				conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)
			},
		},
		{
			name:        "addExclusivePublication",
			kind:        conductor.PendingKindAddExclusivePublication,
			correlation: goldenExclusivePublicationReadyCorrelationID,
			readyHandler: func(conductorInstance *conductor.Conductor) {
				conductorInstance.ExclusivePublicationReadyForTest(
					goldenExclusivePublicationBuffersReadyEvent,
				)
			},
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
				registerPendingKindForTest(conductorInstance, testCase.kind, testCase.correlation),
			)

			ok := conductorInstance.OperationSucceeded(testCase.correlation)
			require.True(t, ok)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			_, found := conductorInstance.PendingResult(testCase.correlation)
			require.True(t, found)

			testCase.readyHandler(conductorInstance)

			done, registration, err := conductorInstance.PollPending(testCase.correlation)
			require.True(t, done)
			require.NoError(t, err)
			require.Equal(t, testCase.kind, registration.Kind)
			require.True(t, registration.Ready)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			_, ok = conductorInstance.PendingResult(testCase.correlation)
			require.False(t, ok)
		})
	}
}

const (
	nonAddWrongKindPollDoneReason = "handleCorrelatedEvent sets receivedCorrelationID for " +
		"non-add pendings before the ready handler runs; PollPending observes the match " +
		"even though completePendingIfKind rejected the wrong kind"
)

func TestConductor_correlatedReady_markPolicy(t *testing.T) {
	publicationReadyForPending := goldenPublicationBuffersReadyWithCorrelation(
		pendingTestCorrelationID,
	)
	publicationReadyBytes := conductor.PublicationBuffersReadyBytes(publicationReadyForPending)

	tests := []struct {
		name                      string
		register                  func(*conductor.Conductor) error
		correlationID             int64
		activeCorrelationID       int64
		wireEvent                 int
		eventBytes                []byte
		wantReceivedCorrelationID int64
		wantPollDone              bool
		wantPollDoneReason        string
		wantPollKind              conductor.PendingKind
	}{
		{
			name: "nonAddOperationSuccess_marksOnCorrelatedReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindOperationSuccess)
			},
			correlationID:             pendingTestCorrelationID,
			activeCorrelationID:       pendingTestCorrelationID,
			wireEvent:                 conductor.OnPublicationReady,
			eventBytes:                publicationReadyBytes,
			wantReceivedCorrelationID: pendingTestCorrelationID,
			wantPollDone:              true,
			wantPollDoneReason:        nonAddWrongKindPollDoneReason,
			wantPollKind:              conductor.PendingKindOperationSuccess,
		},
		{
			name: "nonAddRemovePublication_marksOnCorrelatedReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemovePublication)
			},
			correlationID:             pendingTestCorrelationID,
			activeCorrelationID:       pendingTestCorrelationID,
			wireEvent:                 conductor.OnPublicationReady,
			eventBytes:                publicationReadyBytes,
			wantReceivedCorrelationID: pendingTestCorrelationID,
			wantPollDone:              true,
			wantPollDoneReason:        nonAddWrongKindPollDoneReason,
			wantPollKind:              conductor.PendingKindRemovePublication,
		},
		{
			name: "addPublication_doesNotMarkOnWrongKindReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingKindForTest(
					conductorInstance,
					conductor.PendingKindAddPublication,
					goldenSubscriptionReadyCorrelationID,
				)
			},
			correlationID:             goldenSubscriptionReadyCorrelationID,
			activeCorrelationID:       goldenSubscriptionReadyCorrelationID,
			wireEvent:                 conductor.OnSubscriptionReady,
			eventBytes:                conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
			wantReceivedCorrelationID: conductor.NullCorrelationID,
			wantPollDone:              false,
		},
		{
			name: "addSubscription_doesNotMarkOnWrongKindReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingKindForTest(
					conductorInstance,
					conductor.PendingKindAddSubscription,
					goldenPublicationReadyCorrelationID,
				)
			},
			correlationID:       goldenPublicationReadyCorrelationID,
			activeCorrelationID: goldenPublicationReadyCorrelationID,
			wireEvent:           conductor.OnPublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenPublicationBuffersReadyEvent,
			),
			wantReceivedCorrelationID: conductor.NullCorrelationID,
			wantPollDone:              false,
		},
		{
			name: "addExclusivePublication_doesNotMarkOnWrongKindReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingKindForTest(
					conductorInstance,
					conductor.PendingKindAddExclusivePublication,
					goldenPublicationReadyCorrelationID,
				)
			},
			correlationID:       goldenPublicationReadyCorrelationID,
			activeCorrelationID: goldenPublicationReadyCorrelationID,
			wireEvent:           conductor.OnPublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenPublicationBuffersReadyEvent,
			),
			wantReceivedCorrelationID: conductor.NullCorrelationID,
			wantPollDone:              false,
		},
		{
			name: "addPublication_doesNotMarkOnExclusiveReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingKindForTest(
					conductorInstance,
					conductor.PendingKindAddPublication,
					goldenExclusivePublicationReadyCorrelationID,
				)
			},
			correlationID:       goldenExclusivePublicationReadyCorrelationID,
			activeCorrelationID: goldenExclusivePublicationReadyCorrelationID,
			wireEvent:           conductor.OnExclusivePublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenExclusivePublicationBuffersReadyEvent,
			),
			wantReceivedCorrelationID: conductor.NullCorrelationID,
			wantPollDone:              false,
		},
		{
			name: "addExclusivePublication_doesNotMarkOnSubscriptionReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingKindForTest(
					conductorInstance,
					conductor.PendingKindAddExclusivePublication,
					goldenSubscriptionReadyCorrelationID,
				)
			},
			correlationID:             goldenSubscriptionReadyCorrelationID,
			activeCorrelationID:       goldenSubscriptionReadyCorrelationID,
			wireEvent:                 conductor.OnSubscriptionReady,
			eventBytes:                conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
			wantReceivedCorrelationID: conductor.NullCorrelationID,
			wantPollDone:              false,
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
			require.NoError(t, testCase.register(conductorInstance))
			conductorInstance.SetActiveCorrelationID(testCase.activeCorrelationID)

			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				testCase.wireEvent,
				testCase.eventBytes,
			)
			_ = conductorInstance.DoWork()

			require.Equal(
				t,
				testCase.wantReceivedCorrelationID,
				conductorInstance.ReceivedCorrelationID(),
			)
			_, ok := conductorInstance.PendingResult(testCase.correlationID)
			require.True(t, ok, "pending must remain registered before PollPending")

			done, registration, err := conductorInstance.PollPending(testCase.correlationID)
			require.Equal(t, testCase.wantPollDone, done, testCase.wantPollDoneReason)
			require.NoError(t, err)
			if testCase.wantPollDone {
				require.Equal(t, testCase.wantPollKind, registration.Kind)
				require.Equal(t, testCase.correlationID, registration.CorrelationID)
				require.False(t, registration.Ready)
			} else {
				require.Zero(t, registration)
				_, ok = conductorInstance.PendingResult(testCase.correlationID)
				require.True(t, ok, "add pending stays registered when wrong-kind ready does not complete it")
			}
		})
	}
}

func TestConductor_addReadyWrongKindViaDoWork(t *testing.T) {
	tests := []struct {
		name          string
		kind          conductor.PendingKind
		correlationID int64
		wireEvent     int
		eventBytes    []byte
	}{
		{
			name:          "addPublicationReadyWrongKind",
			kind:          conductor.PendingKindAddSubscription,
			correlationID: goldenPublicationReadyCorrelationID,
			wireEvent:     conductor.OnPublicationReady,
			eventBytes:    conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
		},
		{
			name:          "addSubscriptionReadyWrongKind",
			kind:          conductor.PendingKindAddPublication,
			correlationID: goldenSubscriptionReadyCorrelationID,
			wireEvent:     conductor.OnSubscriptionReady,
			eventBytes:    conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
		},
		{
			name:          "addExclusivePublicationReadyWrongKind",
			kind:          conductor.PendingKindAddPublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			wireEvent:     conductor.OnExclusivePublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenExclusivePublicationBuffersReadyEvent,
			),
		},
		{
			name:          "addPublicationReadyWrongExclusiveKind",
			kind:          conductor.PendingKindAddExclusivePublication,
			correlationID: goldenPublicationReadyCorrelationID,
			wireEvent:     conductor.OnPublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenPublicationBuffersReadyEvent,
			),
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
				registerPendingKindForTest(
					conductorInstance,
					testCase.kind,
					testCase.correlationID,
				),
			)
			conductorInstance.SetActiveCorrelationID(testCase.correlationID)

			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				testCase.wireEvent,
				testCase.eventBytes,
			)
			_ = conductorInstance.DoWork()

			done, registration, err := conductorInstance.PollPending(testCase.correlationID)
			require.False(t, done)
			require.Zero(t, registration)
			require.NoError(t, err)
			_, ok := conductorInstance.PendingResult(testCase.correlationID)
			require.True(t, ok)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
		})
	}
}

func TestConductor_addPending_ackThenReadyViaDoWork(t *testing.T) {
	tests := []struct {
		name          string
		kind          conductor.PendingKind
		correlationID int64
		transmitAck   func(t *testing.T, adapter *conductor.DriverEventsAdapter)
		transmitReady func(t *testing.T, adapter *conductor.DriverEventsAdapter)
	}{
		{
			name:          "addPublication",
			kind:          conductor.PendingKindAddPublication,
			correlationID: goldenPublicationReadyCorrelationID,
			transmitAck: func(t *testing.T, adapter *conductor.DriverEventsAdapter) {
				t.Helper()
				transmitConductorEvent(
					t,
					adapter,
					conductor.OnOperationSuccess,
					conductor.OperationSuccessBytes(goldenPublicationReadyCorrelationID),
				)
			},
			transmitReady: func(t *testing.T, adapter *conductor.DriverEventsAdapter) {
				t.Helper()
				transmitConductorEvent(
					t,
					adapter,
					conductor.OnPublicationReady,
					conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
				)
			},
		},
		{
			name:          "addSubscription",
			kind:          conductor.PendingKindAddSubscription,
			correlationID: goldenSubscriptionReadyCorrelationID,
			transmitAck: func(t *testing.T, adapter *conductor.DriverEventsAdapter) {
				t.Helper()
				transmitConductorEvent(
					t,
					adapter,
					conductor.OnOperationSuccess,
					conductor.OperationSuccessBytes(goldenSubscriptionReadyCorrelationID),
				)
			},
			transmitReady: func(t *testing.T, adapter *conductor.DriverEventsAdapter) {
				t.Helper()
				transmitConductorEvent(
					t,
					adapter,
					conductor.OnSubscriptionReady,
					conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
				)
			},
		},
		{
			name:          "addExclusivePublication",
			kind:          conductor.PendingKindAddExclusivePublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			transmitAck: func(t *testing.T, adapter *conductor.DriverEventsAdapter) {
				t.Helper()
				transmitConductorEvent(
					t,
					adapter,
					conductor.OnOperationSuccess,
					conductor.OperationSuccessBytes(goldenExclusivePublicationReadyCorrelationID),
				)
			},
			transmitReady: func(t *testing.T, adapter *conductor.DriverEventsAdapter) {
				t.Helper()
				transmitConductorEvent(
					t,
					adapter,
					conductor.OnExclusivePublicationReady,
					conductor.PublicationBuffersReadyBytes(
						goldenExclusivePublicationBuffersReadyEvent,
					),
				)
			},
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
				registerPendingKindForTest(
					conductorInstance,
					testCase.kind,
					testCase.correlationID,
				),
			)
			conductorInstance.SetActiveCorrelationID(testCase.correlationID)

			adapter := conductorInstance.EventsAdapterForTest()
			testCase.transmitAck(t, adapter)
			_ = conductorInstance.DoWork()

			done, _, err := conductorInstance.PollPending(testCase.correlationID)
			require.False(t, done)
			require.NoError(t, err)
			_, found := conductorInstance.PendingResult(testCase.correlationID)
			require.True(t, found)

			testCase.transmitReady(t, adapter)
			_ = conductorInstance.DoWork()

			done, registration, err := conductorInstance.PollPending(testCase.correlationID)
			require.True(t, done)
			require.NoError(t, err)
			require.Equal(t, testCase.kind, registration.Kind)
			require.True(t, registration.Ready)
		})
	}
}

func TestConductor_expirePending_kinds(t *testing.T) {
	const expireNs = 500 * time.Nanosecond

	tests := []struct {
		name          string
		kind          conductor.PendingKind
		active        bool
		correlationID int64
	}{
		{
			name:          "addPublication_inactive",
			kind:          conductor.PendingKindAddPublication,
			active:        false,
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "addSubscription_inactive",
			kind:          conductor.PendingKindAddSubscription,
			active:        false,
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "addExclusivePublication_inactive",
			kind:          conductor.PendingKindAddExclusivePublication,
			active:        false,
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "addPublication_active",
			kind:          conductor.PendingKindAddPublication,
			active:        true,
			correlationID: pendingTestCorrelationID,
		},
		{
			name:          "addSubscription_active",
			kind:          conductor.PendingKindAddSubscription,
			active:        true,
			correlationID: pendingTestCorrelationID,
		},
		{
			name:          "addExclusivePublication_active",
			kind:          conductor.PendingKindAddExclusivePublication,
			active:        true,
			correlationID: pendingTestCorrelationID,
		},
		{
			name:          "removePublication_inactive",
			kind:          conductor.PendingKindRemovePublication,
			active:        false,
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "removeSubscription_inactive",
			kind:          conductor.PendingKindRemoveSubscription,
			active:        false,
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "removePublication_active",
			kind:          conductor.PendingKindRemovePublication,
			active:        true,
			correlationID: pendingTestCorrelationID,
		},
		{
			name:          "removeSubscription_active",
			kind:          conductor.PendingKindRemoveSubscription,
			active:        true,
			correlationID: pendingTestCorrelationID,
		},
		{
			name:          "destination_inactive",
			kind:          conductor.PendingKindDestination,
			active:        false,
			correlationID: pendingTestOtherCorrelationID,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			const startNs = int64(1_000)
			conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
			*nanoNs = startNs
			require.NoError(
				t,
				registerPendingKindWithDeadlineForTest(
					conductorInstance,
					testCase.kind,
					testCase.correlationID,
					pendingTestStreamID,
					pendingTestChannel,
					expireNs,
				),
			)
			if testCase.active {
				conductorInstance.SetActiveCorrelationID(testCase.correlationID)
			}

			*nanoNs = startNs + 500
			workCount := conductorInstance.DoWork()
			require.Equal(t, 1, workCount)

			readyCompleted := conductor.AddPendingKindForTest(testCase.kind) ||
				testCase.kind == conductor.PendingKindDestination
			if testCase.active || readyCompleted {
				requirePendingAddExpiredForTest(t, conductorInstance, testCase.correlationID)
				return
			}

			_, ok := conductorInstance.PendingResult(testCase.correlationID)
			require.False(t, ok)
			done, _, err := conductorInstance.PollPending(testCase.correlationID)
			require.False(t, done)
			require.NoError(t, err)
		})
	}
}

func TestConductor_expirePending_addSubscriptionSameTickReady(t *testing.T) {
	const startNs = int64(1_000)
	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = startNs
	require.NoError(
		t,
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindAddSubscription,
			goldenSubscriptionReadyCorrelationID,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenSubscriptionReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	*nanoNs = startNs + 500
	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(goldenSubscriptionReadyCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, conductor.PendingKindAddSubscription, registration.Kind)
	require.True(t, registration.Ready)
}

func TestConductor_rollbackAddPendingResources_nonAddKind_silent(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingResourceLifecycle{}
	config := conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0})
	config.ResourceLifecycle = lifecycle
	conductorInstance := newConductorForTest(t, config)

	completeGoldenPublicationReadyForTest(t, conductorInstance)
	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
	)
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	nonAddKinds := []struct {
		name          string
		kind          conductor.PendingKind
		correlationID int64
	}{
		{
			name:          "removePublication",
			kind:          conductor.PendingKindRemovePublication,
			correlationID: pendingTestCorrelationID,
		},
		{
			name:          "removeSubscription",
			kind:          conductor.PendingKindRemoveSubscription,
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "nextSessionID",
			kind:          conductor.PendingKindNextSessionID,
			correlationID: pendingTestCorrelationID,
		},
	}
	for _, testCase := range nonAddKinds {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance.RollbackAddPendingResourcesForTest(conductor.PendingRegistration{
				CorrelationID: testCase.correlationID,
				Kind:          testCase.kind,
			})
		})
	}

	require.Equal(t, 1, conductorInstance.PublicationCountForTest())
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 0, lifecycle.retirePublicationCalls)
	require.Equal(t, 0, lifecycle.retireSubscriptionCalls)
	_, ok := conductorInstance.PublicationByRegID(
		goldenPublicationBuffersReadyEvent.CorrelationID,
	)
	require.True(t, ok)
	_, ok = conductorInstance.SubscriptionByRegID(goldenSubscriptionReadyCorrelationID)
	require.True(t, ok)
	require.Empty(t, *gotErrors)
}

// TestConductor_rollbackAddPendingResources_publicationKeyedByCorrelation rolls
// back one add while the golden add holds the same driver registration id.
func TestConductor_rollbackAddPendingResources_publicationKeyedByCorrelation(t *testing.T) {
	tests := []struct {
		name            string
		correlationID   int64
		ready           bool
		wantRetireCalls int
	}{
		{
			name:            "sharedDriverIDReadyRemoved",
			correlationID:   pendingTestOtherCorrelationID,
			ready:           true,
			wantRetireCalls: 1,
		},
		{
			name:            "sharedDriverIDReadyAtZeroCorrelationRemoved",
			correlationID:   0,
			ready:           true,
			wantRetireCalls: 1,
		},
		{
			name:            "notReadyLeavesOtherPublication",
			correlationID:   0,
			ready:           false,
			wantRetireCalls: 0,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			lifecycle := &recordingResourceLifecycle{}
			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
			registry := conductor.LogBuffersRegistryForTest(conductorInstance)
			completeGoldenPublicationReadyForTest(t, conductorInstance)

			registration := conductor.PendingRegistration{
				CorrelationID: testCase.correlationID,
				Kind:          conductor.PendingKindAddPublication,
			}
			if testCase.ready {
				event := goldenPublicationBuffersReadyWithCorrelation(testCase.correlationID)
				require.NoError(t, registerPendingAddWithCorrelationForTest(
					conductorInstance,
					conductor.PendingKindAddPublication,
					testCase.correlationID,
					event.StreamID,
				))
				conductorInstance.PublicationReadyForTest(event)
				require.Equal(t, 2, conductorInstance.PublicationCountForTest())
				done, polled, err := conductorInstance.PollPending(testCase.correlationID)
				require.True(t, done)
				require.NoError(t, err)
				require.True(t, polled.Ready)
				registration = polled
			}
			refCountBefore := conductor.LogBuffersRegistryRefCountForTest(
				registry,
				goldenPublicationReadyLogFileName,
			)

			conductorInstance.RollbackAddPendingResourcesForTest(registration)

			require.Equal(t, 1, conductorInstance.PublicationCountForTest())
			require.Equal(t, testCase.wantRetireCalls, lifecycle.retirePublicationCalls)
			requireGoldenLogRefCountForTest(
				t,
				registry,
				goldenPublicationReadyLogFileName,
				refCountBefore-int32(testCase.wantRetireCalls),
			)
			_, ok := conductorInstance.PublicationByRegID(testCase.correlationID)
			require.False(t, ok)
			_, ok = conductorInstance.PublicationByRegID(goldenPublicationReadyCorrelationID)
			require.True(t, ok)
		})
	}
}

func TestConductor_DriverError_negative(t *testing.T) {
	tests := []struct {
		name          string
		register      func(*conductor.Conductor) error
		correlationID int64
	}{
		{
			name: "addPublication_wrongCorrelation",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(conductorInstance, conductor.PendingKindAddPublication)
			},
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "addPublication_missingPending",
			correlationID: pendingTestCorrelationID,
		},
		{
			name: "addExclusivePublication_wrongCorrelation",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddForTest(
					conductorInstance,
					conductor.PendingKindAddExclusivePublication,
				)
			},
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "addExclusivePublication_missingPending",
			correlationID: pendingTestCorrelationID,
		},
		{
			name: "removePublication_wrongCorrelation",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemovePublication)
			},
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "removePublication_missingPending",
			correlationID: pendingTestCorrelationID,
		},
		{
			name: "removeSubscription_wrongCorrelation",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingForTest(conductorInstance, conductor.PendingKindRemoveSubscription)
			},
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "removeSubscription_missingPending",
			correlationID: pendingTestCorrelationID,
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
			if testCase.register != nil {
				require.NoError(t, testCase.register(conductorInstance))
			}

			ok := conductorInstance.DriverError(conductor.ErrorResponse{
				OffendingCommandCorrelationID: testCase.correlationID,
				ErrorCode:                     11,
				ErrorMessage:                  "failed",
			})
			require.False(t, ok)
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
		})
	}
}
