package conductor_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/record"
)

const (
	conductorTestClientID            int64 = 42
	conductorTestDriverTimeout             = 10 * time.Second
	conductorTestKeepAliveInterval         = 500 * time.Millisecond
	conductorTestIdleSleepDuration         = 16 * time.Millisecond
	conductorTestInterServiceTimeout       = time.Second
	// unknownDriverEventTypeID matches no driver event, so the adapter marks
	// the record unhandled.
	unknownDriverEventTypeID = 0xFFFF
)

var conductorTestWorkNanoNs = (600 * time.Millisecond).Nanoseconds()

func collectConductorErrors() (func(error), *[]error) {
	var gotErrors []error
	return func(err error) {
		gotErrors = append(gotErrors, err)
	}, &gotErrors
}

func TestConductor_staleDriverKeepalive(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		errorHandler,
		20_000,
		conductorTestWorkNanoNs,
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	var staleErr *conductor.DriverKeepaliveStaleError
	require.ErrorAs(t, (*gotErrors)[0], &staleErr)
	require.EqualError(
		t,
		(*gotErrors)[0],
		"conductor: media driver keepalive stale: age=19000ms timeout=10000ms",
	)
}

func TestConductor_unpublishedDriverHeartbeatNotStale(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		errorHandler,
		20_000,
		conductorTestWorkNanoNs,
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(0)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.False(t, conductorInstance.IsTerminating())
	require.Empty(t, *gotErrors)
}

func TestConductor_driverShutdownHeartbeat(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		errorHandler,
		1_000,
		conductorTestWorkNanoNs,
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(conductor.NullCorrelationID)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrDriverShutdown)
	require.EqualError(t, (*gotErrors)[0], "conductor: media driver has been shutdown")
}

func TestConductor_heartbeatCounterUpdate(t *testing.T) {
	epochMs := int64(1_700_000_000_123)
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		epochMs,
		conductorTestWorkNanoNs,
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	got, err := conductorInstance.CountersReaderForTest().CounterValue(0)
	require.NoError(t, err)
	require.Equal(t, epochMs, got)
}

func TestConductor_unregisteredClientSkipsHeartbeatCounter(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		1_700_000_000_123,
		conductorTestWorkNanoNs,
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.False(t, conductorInstance.IsTerminating())
}

func TestConductor_DoWork_completesActivePending_nextAvailableSessionID(t *testing.T) {
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

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnNextAvailableSessionID,
		conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, nextSessionID),
	)

	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, nextSessionID, registration.Int32Value)
}

func TestConductor_DoWork_completesActivePending_operationSuccess(t *testing.T) {
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
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnOperationSuccess,
		conductor.OperationSuccessBytes(pendingTestCorrelationID),
	)

	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, conductor.PendingKindOperationSuccess, registration.Kind)
}

func TestConductor_DoWork_completesActivePending_errorEvent(t *testing.T) {
	const errorCode int32 = 11
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
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnError,
		conductor.ErrorResponseBytes(pendingTestCorrelationID, errorCode, "registration failed"),
	)

	_ = conductorInstance.DoWork()

	done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
	require.True(t, done)
	var driverErr *conductor.DriverRegistrationError
	require.ErrorAs(t, err, &driverErr)
	require.Equal(t, pendingTestCorrelationID, driverErr.CorrelationID)
	require.Equal(t, errorCode, driverErr.ErrorCode)
	require.Equal(t, "registration failed", driverErr.Message)
	require.EqualError(
		t,
		err,
		"conductor: driver registration error: correlation=7 code=11 message=registration failed",
	)
}

func TestConductor_DoWork_completesActivePending_publicationReady(t *testing.T) {
	testConductorDoWorkCompletesActivePendingPublicationReady(
		t,
		conductor.PendingKindAddPublication,
		goldenPublicationReadyCorrelationID,
		99,
		conductor.OnPublicationReady,
		goldenPublicationBuffersReadyEvent,
	)
}

func TestConductor_DoWork_completesActivePending_exclusivePublicationReady(t *testing.T) {
	testConductorDoWorkCompletesActivePendingPublicationReady(
		t,
		conductor.PendingKindAddExclusivePublication,
		goldenExclusivePublicationReadyCorrelationID,
		goldenExclusivePublicationBuffersReadyEvent.StreamID,
		conductor.OnExclusivePublicationReady,
		goldenExclusivePublicationBuffersReadyEvent,
	)
}

func TestConductor_DoWork_completesActivePending_subscriptionReady(t *testing.T) {
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
			conductor.PendingKindAddSubscription,
			goldenSubscriptionReadyCorrelationID,
			pendingTestStreamID,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenSubscriptionReadyCorrelationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(goldenSubscriptionReadyCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, conductor.PendingKindAddSubscription, registration.Kind)
	require.True(t, registration.Ready)
	require.Equal(t, int32(7), registration.StreamID)
	require.Equal(t, "aeron:ipc", registration.Channel)
	require.Nil(t, registration.Subscription)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	_, ok := conductorInstance.PendingResult(goldenSubscriptionReadyCorrelationID)
	require.False(t, ok)
}

func TestConductor_handlePublicationReadyViaDoWork(t *testing.T) {
	testConductorOnReadyViaDoWorkWrongKind(
		t,
		goldenPublicationReadyCorrelationID,
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)
}

func TestConductor_handleExclusivePublicationReadyViaDoWork(t *testing.T) {
	testConductorOnReadyViaDoWorkWrongKind(
		t,
		goldenExclusivePublicationReadyCorrelationID,
		conductor.OnExclusivePublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenExclusivePublicationBuffersReadyEvent),
	)
}

func TestConductor_oneBroadcastEventPerDoWork(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0}),
	)
	adapter := conductorInstance.EventsAdapterForTest()

	transmitConductorEvent(
		t,
		adapter,
		conductor.OnError,
		conductor.ErrorResponseBytes(99, 1, "first"),
	)
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnError,
		conductor.ErrorResponseBytes(100, 2, "second"),
	)

	for step := range 2 {
		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.Empty(t, *gotErrors)
		require.Equal(t, int64(step+1), adapter.UnhandledEventCount())
	}

	receiver := adapter.CopyReceiverForTest().BroadcastReceiver()
	require.False(t, receiver.ReceiveNext())
}

func TestConductor_handleSubscriptionReadyViaDoWork(t *testing.T) {
	testConductorOnReadyViaDoWorkWrongKind(
		t,
		goldenSubscriptionReadyCorrelationID,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)
}

func TestConductor_oneGatedReadyEventPerDoWork(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	for _, correlationID := range []int64{
		goldenPublicationReadyCorrelationID,
		goldenSubscriptionReadyCorrelationID,
	} {
		require.NoError(
			t,
			registerPendingKindForTest(
				conductorInstance,
				conductor.PendingKindOperationSuccess,
				correlationID,
			),
		)
	}
	conductorInstance.SetActiveCorrelationID(goldenPublicationReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	require.Equal(
		t,
		goldenPublicationReadyCorrelationID,
		conductorInstance.ReceivedCorrelationID(),
	)

	workCount = conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	require.Equal(
		t,
		goldenSubscriptionReadyCorrelationID,
		conductorInstance.ReceivedCorrelationID(),
	)
	require.False(t, adapter.CopyReceiverForTest().BroadcastReceiver().ReceiveNext())
}

func TestConductor_handleClientTimeout(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, pub, sub, beforeTypeIDs := newConductorWithLiveResourcesAtWorkNano(
		t,
		errorHandler,
		0,
		0,
	)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrClientTimeout)
	require.EqualError(t, (*gotErrors)[0], "conductor: client timeout from driver")
	var closedErr *conductor.HeartbeatCounterClosedError
	require.NotErrorAs(t, (*gotErrors)[0], &closedErr)
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
}

func TestConductor_terminateWithError_retireFailure_reportsThenTerminates(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	config := conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0})
	config.ResourceLifecycle = realCreateMarkRecordingLifecycleWithRetireErrs(
		errRetirePublication,
		errRetireSubscription,
	)
	conductorInstance := newConductorForTest(t, config)
	scope := installCountingHandlerScope(conductorInstance)
	pub := insertRealPublicationForCloseTest(t, conductorInstance)
	sub := insertRealSubscriptionForCloseTest(t, conductorInstance)
	beforeTypeIDs := testutil.CommittedRecordTypeIDs(conductorInstance.DriverProxyForTest().Ring())

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.ErrorIs(t, conductorInstance.TerminationCause(), conductor.ErrClientTimeout)
	require.Len(t, *gotErrors, 3)
	require.ErrorIs(t, (*gotErrors)[0], errRetirePublication)
	require.ErrorIs(t, (*gotErrors)[1], errRetireSubscription)
	require.ErrorIs(t, (*gotErrors)[2], conductor.ErrClientTimeout)
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
	requireHandlerScope(t, scope, 1, 1)
}

func TestConductor_serviceIntervalExceeded(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, pub, sub, beforeTypeIDs := newConductorWithLiveResourcesAtWorkNano(
		t,
		errorHandler,
		0,
		(2 * time.Second).Nanoseconds(),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrConductorServiceTimeout)
	require.EqualError(t, (*gotErrors)[0], "conductor: service interval exceeded")
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
}

func TestConductor_serviceIntervalSkipsLivenessOnSameTick(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, pub, sub, beforeTypeIDs := newConductorWithLiveResourcesAtWorkNano(
		t,
		errorHandler,
		20_000,
		(2 * time.Second).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrConductorServiceTimeout)
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
}

func TestConductor_heartbeatCounterClosed(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, pub, sub, beforeTypeIDs, nanoNs := newConductorWithLiveResourcesMutableNano(
		t,
		errorHandler,
		testutil.TestEpochMs,
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)

	*nanoNs = conductorTestWorkNanoNs
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Empty(t, *gotErrors)
	require.False(t, conductorInstance.IsTerminating())

	setReclaimedHeartbeatCountersReader(t, conductorInstance)

	*nanoNs = conductorTestWorkNanoNs + conductorTestKeepAliveInterval.Nanoseconds() + 1
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, conductorInstance.TerminationCause(), conductor.ErrClientTimeout)
	assertClientTimeoutProbeCause(t, (*gotErrors)[0], 0)
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
}

func TestConductor_serviceIntervalExceeded_heartbeatRecordState(t *testing.T) {
	tests := []struct {
		name    string
		reclaim bool
		wantErr error
		wantMsg string
	}{
		{
			name:    "reclaimed",
			reclaim: true,
			wantErr: conductor.ErrClientTimeout,
		},
		{
			name:    "active",
			wantErr: conductor.ErrConductorServiceTimeout,
			wantMsg: "conductor: service interval exceeded",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			errorHandler, gotErrors := collectConductorErrors()
			conductorInstance, pub, sub, beforeTypeIDs, nanoNs := newConductorWithLiveResourcesMutableNano(
				t,
				errorHandler,
				testutil.TestEpochMs,
				withHeartbeatCounter(t, conductorTestClientID, 0),
			)

			*nanoNs = conductorTestWorkNanoNs
			require.Equal(t, 1, conductorInstance.DoWork())
			require.Empty(t, *gotErrors)
			require.False(t, conductorInstance.IsTerminating())

			if testCase.reclaim {
				setReclaimedHeartbeatCountersReader(t, conductorInstance)
			}

			*nanoNs = conductorTestWorkNanoNs + conductorTestInterServiceTimeout.Nanoseconds() + 1
			workCount := conductorInstance.DoWork()
			require.Equal(t, 1, workCount)
			require.True(t, conductorInstance.IsTerminating())
			require.Len(t, *gotErrors, 1)
			require.ErrorIs(t, conductorInstance.TerminationCause(), testCase.wantErr)
			if testCase.reclaim {
				assertClientTimeoutProbeCause(t, (*gotErrors)[0], 0)
			} else {
				require.ErrorIs(t, (*gotErrors)[0], testCase.wantErr)
				require.EqualError(t, (*gotErrors)[0], testCase.wantMsg)
				var closedErr *conductor.HeartbeatCounterClosedError
				require.NotErrorAs(t, (*gotErrors)[0], &closedErr)
			}
			assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
		})
	}
}

func TestConductor_unregisteredClientDetectsStaleDriverEachIdleTick(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(
			t,
			errorHandler,
			1_700_000_000_123,
			&nanoNs,
		),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_700_000_000_000)

	nanoNs = conductorTestWorkNanoNs
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Empty(t, *gotErrors)

	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)
	nanoNs = (620 * time.Millisecond).Nanoseconds()

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	var staleErr *conductor.DriverKeepaliveStaleError
	require.ErrorAs(t, (*gotErrors)[0], &staleErr)
}

func TestConductor_driverTimeoutBoundary(t *testing.T) {
	driverHeartbeatMs := int64(1_000_000)
	timeoutMs := conductorTestDriverTimeout.Milliseconds()

	t.Run("at boundary stays alive", func(t *testing.T) {
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			func(error) {},
			driverHeartbeatMs+timeoutMs,
			conductorTestWorkNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.False(t, conductorInstance.IsTerminating())
	})

	t.Run("one millisecond past boundary terminates", func(t *testing.T) {
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			driverHeartbeatMs+timeoutMs+1,
			conductorTestWorkNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.True(t, conductorInstance.IsTerminating())
		require.Len(t, *gotErrors, 1)
		var staleErr *conductor.DriverKeepaliveStaleError
		require.ErrorAs(t, (*gotErrors)[0], &staleErr)
		require.Equal(t, timeoutMs+1, staleErr.AgeMs)
	})
}

func TestConductor_doWorkAfterTerminatingReturnsZero(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		errorHandler,
		20_000,
		conductorTestWorkNanoNs,
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)

	require.Zero(t, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
}

func TestConductor_duplicateClientTimeoutIgnored_conductorLevel(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0}),
	)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)

	require.Zero(t, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
}

func TestConductor_broadcastLappedDuringDoWork(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0}),
	)
	scriptBroadcastLap(t, conductorInstance.EventsAdapterForTest())

	// Phase: the lap reaches ErrorHandler and the conductor keeps running.
	require.Equal(t, 1, conductorInstance.DoWork())
	require.False(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.Same(t, conductor.ErrBroadcastLapped, (*gotErrors)[0])

	// Phase: the latest record reaches the adapter.
	adapter := conductorInstance.EventsAdapterForTest()
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, int64(1), adapter.UnhandledEventCount())

	// Phase: the next event after the lap reaches the adapter.
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID+1),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.False(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}

func TestConductor_broadcastLappedWithReclaimedHeartbeatCounter(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorWithHeartbeatForProbeTest(t, errorHandler)
	setReclaimedHeartbeatCountersReader(t, conductorInstance)
	scriptBroadcastLap(t, conductorInstance.EventsAdapterForTest())

	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 2)
	require.Same(t, conductor.ErrBroadcastLapped, (*gotErrors)[0])
	assertClientTimeoutProbeCause(t, (*gotErrors)[1], 0)
	require.ErrorIs(t, conductorInstance.TerminationCause(), conductor.ErrClientTimeout)

	require.Zero(t, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 2)
}

func TestConductor_broadcastMessageTooLongTerminates(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0}),
	)
	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID+1),
	)
	adapter.CopyReceiverForTest().
		BroadcastReceiver().
		Buffer().
		PutInt32(record.LengthOffset(firstRecordOffset), tornRecordLength)

	// Phase: a record above the broadcast bounds terminates the conductor.
	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrBroadcastMessageTooLong)
	require.ErrorIs(t, conductorInstance.TerminationCause(), conductor.ErrBroadcastMessageTooLong)

	require.Zero(t, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
}

func TestConductor_broadcastLapped_reclaimedHeartbeatCounter_timeoutChecksClaimFirst(
	t *testing.T,
) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		errorHandler,
		testutil.TestEpochMs,
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)
	conductorInstance.SetHeartbeatCounterIDForTest(0)
	setReclaimedHeartbeatCountersReader(t, conductorInstance)
	adapter := conductorInstance.EventsAdapterForTest()
	scriptBroadcastLap(t, adapter)

	*nanoNs = conductorTestInterServiceTimeout.Nanoseconds() + 1
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	assertClientTimeoutProbeCause(t, (*gotErrors)[0], 0)
}

func TestConductor_unknownRegistrationError_probesHeartbeatCounter(t *testing.T) {
	const errorMessage = "unknown registration"

	t.Run("correlatedUnknownSubscriptionReclaimed", func(t *testing.T) {
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorWithHeartbeatForProbeTest(t, errorHandler)
		setReclaimedHeartbeatCountersReader(t, conductorInstance)
		registerUnknownRegistrationPending(t, conductorInstance)
		adapter := conductorInstance.EventsAdapterForTest()
		transmitUnknownRegistrationError(
			t,
			adapter,
			conductor.ErrorCodeUnknownSubscription,
			errorMessage,
		)

		require.Equal(t, 1, conductorInstance.DoWork())
		require.Zero(t, adapter.UnhandledEventCount())
		require.True(t, conductorInstance.IsTerminating())
		require.Len(t, *gotErrors, 1)
		assertClientTimeoutProbeCause(t, (*gotErrors)[0], 0)
		require.ErrorIs(
			t,
			conductorInstance.TerminationCause(),
			conductor.ErrClientTimeout,
		)
		done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
		require.False(t, done)
		require.NoError(t, err)
		_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
		require.False(t, ok)

		require.Zero(t, conductorInstance.DoWork())
		require.Len(t, *gotErrors, 1)
		require.True(t, conductorInstance.IsTerminating())
	})

	t.Run("correlatedUnknownSubscriptionActive", func(t *testing.T) {
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorWithHeartbeatForProbeTest(t, errorHandler)
		registerUnknownRegistrationPending(t, conductorInstance)
		adapter := conductorInstance.EventsAdapterForTest()
		transmitUnknownRegistrationError(
			t,
			adapter,
			conductor.ErrorCodeUnknownSubscription,
			errorMessage,
		)

		require.Equal(t, 1, conductorInstance.DoWork())
		require.Zero(t, adapter.UnhandledEventCount())
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
		done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
		require.True(t, done)
		var driverErr *conductor.DriverRegistrationError
		require.ErrorAs(t, err, &driverErr)
		require.Equal(t, pendingTestCorrelationID, driverErr.CorrelationID)
		require.Equal(t, conductor.ErrorCodeUnknownSubscription, driverErr.ErrorCode)
		require.Equal(t, errorMessage, driverErr.Message)
	})

	t.Run("uncorrelatedUnknownSubscriptionReclaimed", func(t *testing.T) {
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorWithHeartbeatForProbeTest(t, errorHandler)
		setReclaimedHeartbeatCountersReader(t, conductorInstance)
		adapter := conductorInstance.EventsAdapterForTest()
		transmitUnknownRegistrationError(
			t,
			adapter,
			conductor.ErrorCodeUnknownSubscription,
			errorMessage,
		)

		require.Equal(t, 1, conductorInstance.DoWork())
		require.Equal(t, int64(1), adapter.UnhandledEventCount())
		require.True(t, conductorInstance.IsTerminating())
		require.Len(t, *gotErrors, 1)
		assertClientTimeoutProbeCause(t, (*gotErrors)[0], 0)
		require.ErrorIs(
			t,
			conductorInstance.TerminationCause(),
			conductor.ErrClientTimeout,
		)

		require.Zero(t, conductorInstance.DoWork())
		require.Len(t, *gotErrors, 1)
		require.True(t, conductorInstance.IsTerminating())
	})

	t.Run("unknownSubscriptionNeverFound", func(t *testing.T) {
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			testutil.TestEpochMs,
			0,
		)
		adapter := conductorInstance.EventsAdapterForTest()
		transmitUnknownRegistrationError(
			t,
			adapter,
			conductor.ErrorCodeUnknownSubscription,
			errorMessage,
		)

		require.Equal(t, 1, conductorInstance.DoWork())
		require.Equal(t, int64(1), adapter.UnhandledEventCount())
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
	})

	t.Run("otherErrorCodeReclaimed", func(t *testing.T) {
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorWithHeartbeatForProbeTest(t, errorHandler)
		setReclaimedHeartbeatCountersReader(t, conductorInstance)
		registerUnknownRegistrationPending(t, conductorInstance)
		adapter := conductorInstance.EventsAdapterForTest()
		transmitUnknownRegistrationError(t, adapter, 11, errorMessage)

		require.Equal(t, 1, conductorInstance.DoWork())
		require.Zero(t, adapter.UnhandledEventCount())
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
		done, _, err := conductorInstance.PollPending(pendingTestCorrelationID)
		require.True(t, done)
		var driverErr *conductor.DriverRegistrationError
		require.ErrorAs(t, err, &driverErr)
		require.Equal(t, pendingTestCorrelationID, driverErr.CorrelationID)
		require.Equal(t, int32(11), driverErr.ErrorCode)
		require.Equal(t, errorMessage, driverErr.Message)
	})
}

func TestConductor_livenessProbeRequested_clearsAfterOneConsume(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorWithHeartbeatForProbeTest(t, errorHandler)
	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnError,
		conductor.ErrorResponseBytes(
			pendingTestCorrelationID,
			conductor.ErrorCodeUnknownSubscription,
			"unknown subscription",
		),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.False(t, conductorInstance.IsTerminating())
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
	require.Empty(t, *gotErrors)

	setReclaimedHeartbeatCountersReader(t, conductorInstance)
	require.Zero(t, conductorInstance.DoWork())
	require.False(t, conductorInstance.IsTerminating())
	require.Empty(t, *gotErrors)
}

func TestConductor_terminatesBeforeReceiveOnSameTick(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		errorHandler,
		20_000,
		conductorTestWorkNanoNs,
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnError,
		[]byte{0x01},
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	var staleErr *conductor.DriverKeepaliveStaleError
	require.ErrorAs(t, (*gotErrors)[0], &staleErr)
}

func TestConductor_idleDoWorkNoProgress(t *testing.T) {
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, func(error) {}, fixedClocks{nanoNs: 0}),
	)

	require.Zero(t, conductorInstance.DoWork())
	require.False(t, conductorInstance.IsTerminating())
}

func TestConductor_serviceTimestampSeeding(t *testing.T) {
	tests := []struct {
		name            string
		seed            bool
		advanceDuration time.Duration
		wantWorkCount   int
		wantTerminating bool
		wantErr         error
	}{
		{
			name:            "seeded avoids false service timeout",
			seed:            true,
			advanceDuration: conductorTestKeepAliveInterval + time.Nanosecond,
			wantWorkCount:   1,
			wantTerminating: false,
		},
		{
			name:            "constructor seed avoids false service timeout",
			seed:            false,
			wantWorkCount:   0,
			wantTerminating: false,
		},
		{
			name:            "service timeout after interServiceTimeout",
			seed:            false,
			advanceDuration: conductorTestInterServiceTimeout + time.Nanosecond,
			wantWorkCount:   1,
			wantTerminating: true,
			wantErr:         conductor.ErrConductorServiceTimeout,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			errorHandler, gotErrors := collectConductorErrors()
			nanoNs := (2 * time.Second).Nanoseconds()
			conductorInstance := newConductorForTest(
				t,
				conductorTestConfigMutableNano(
					t,
					errorHandler,
					1_700_000_000_123,
					&nanoNs,
				),
			)
			if testCase.seed {
				conductorInstance.SeedServiceTimestamps(nanoNs)
			}

			nanoNs += testCase.advanceDuration.Nanoseconds()

			workCount := conductorInstance.DoWork()
			require.Equal(t, testCase.wantWorkCount, workCount)
			require.Equal(t, testCase.wantTerminating, conductorInstance.IsTerminating())
			if testCase.wantErr == nil {
				require.Empty(t, *gotErrors)
				return
			}
			require.Len(t, *gotErrors, 1)
			require.ErrorIs(t, (*gotErrors)[0], testCase.wantErr)
		})
	}
}

func TestConductor_setCounterValueErrorDoesNotAdvanceKeepaliveTimer(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		errorHandler,
		1_700_000_000_123,
		conductorTestWorkNanoNs,
	)
	conductorInstance.SetHeartbeatCounterIDForTest(99)

	workCount := conductorInstance.StoreClientHeartbeatForTest(
		conductorTestWorkNanoNs,
		1_700_000_000_123,
	)
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.EqualError(
		t,
		(*gotErrors)[0],
		"cnc: counter id 99 out of range: maxCounterId=0",
	)
	require.Zero(t, conductorInstance.TimeOfLastKeepAliveNsForTest())
}

type fixedClocks struct {
	epochMs int64
	nanoNs  int64
}

func conductorTestConfig(
	tb testing.TB,
	errorHandler func(error),
	clocks fixedClocks,
) conductor.Config {
	tb.Helper()
	return conductorTestConfigMutableNano(tb, errorHandler, clocks.epochMs, &clocks.nanoNs)
}

// conductorTestConfigMutableNano leaves ResourceLifecycle nil; Close* then drops
// map entries only. Tests that need retire, linger, or mark-closed inject lifecycle
// via newConductorWithResourceLifecycleForTest or newConductorWithResourceLingerForTest.
func conductorTestConfigMutableNano(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	nanoNs *int64,
) conductor.Config {
	tb.Helper()
	return conductor.Config{
		ClientID:            conductorTestClientID,
		DriverTimeout:       conductorTestDriverTimeout,
		KeepAliveInterval:   conductorTestKeepAliveInterval,
		IdleSleepDuration:   conductorTestIdleSleepDuration,
		InterServiceTimeout: conductorTestInterServiceTimeout,
		EpochClock: func() int64 {
			return epochMs
		},
		NanoClock: func() int64 {
			return *nanoNs
		},
		ErrorHandler: errorHandler,
	}
}

type conductorTestOption func(tb testing.TB, meta, values []byte)

func registerUnknownRegistrationPending(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()

	require.NoError(
		tb,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			pendingTestCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)
}

func transmitUnknownRegistrationError(
	t *testing.T,
	adapter *conductor.DriverEventsAdapter,
	errorCode int32,
	errorMessage string,
) {
	t.Helper()

	transmitConductorEvent(
		t,
		adapter,
		conductor.OnError,
		conductor.ErrorResponseBytes(
			pendingTestCorrelationID,
			errorCode,
			errorMessage,
		),
	)
}

func newConductorWithHeartbeatForProbeTest(
	tb testing.TB,
	errorHandler func(error),
) *conductor.Conductor {
	tb.Helper()

	conductorInstance := newConductorForTestAtWorkNano(
		tb,
		errorHandler,
		testutil.TestEpochMs,
		0,
		withHeartbeatCounter(tb, conductorTestClientID, 0),
	)
	conductorInstance.SetHeartbeatCounterIDForTest(0)
	return conductorInstance
}

func assertClientTimeoutProbeCause(tb testing.TB, got error, counterID int32) {
	tb.Helper()

	require.ErrorIs(tb, got, conductor.ErrClientTimeout)
	var closedErr *conductor.HeartbeatCounterClosedError
	require.ErrorAs(tb, got, &closedErr)
	require.Equal(tb, counterID, closedErr.CounterID)
	require.EqualError(
		tb,
		got,
		(&conductor.HeartbeatCounterClosedError{CounterID: counterID}).Error(),
	)
}

func withHeartbeatCounter(_ testing.TB, clientID int64, counterID int32) conductorTestOption {
	return func(tb testing.TB, meta, values []byte) {
		tb.Helper()
		cnc.WriteCounterRecord(
			meta,
			values,
			counterID,
			cnc.CounterRecordAllocated,
			cnc.ClientHeartbeatTypeID,
			clientID,
			0,
		)
	}
}

func setReclaimedHeartbeatCountersReader(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	cnc.WriteCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordReclaimed,
		cnc.ClientHeartbeatTypeID,
		conductorTestClientID,
		0,
	)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	conductorInstance.SetCountersReaderForTest(reader)
}

func newConductorForTest(
	tb testing.TB,
	config conductor.Config,
	options ...conductorTestOption,
) *conductor.Conductor {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	for _, option := range options {
		option(tb, meta, values)
	}

	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)

	return conductor.NewConductorForTest(tb, config, countersReader)
}

func newConductorForTestAtWorkNano(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	workNanoNs int64,
	options ...conductorTestOption,
) *conductor.Conductor {
	tb.Helper()

	conductorInstance, nanoNs := newConductorForMutableNanoTest(tb, errorHandler, epochMs, options...)
	*nanoNs = workNanoNs
	return conductorInstance
}

func newConductorForMutableNanoTest(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	options ...conductorTestOption,
) (*conductor.Conductor, *int64) {
	tb.Helper()

	var nanoNs int64
	conductorInstance := newConductorForTest(
		tb,
		conductorTestConfigMutableNano(tb, errorHandler, epochMs, &nanoNs),
		options...,
	)
	return conductorInstance, &nanoNs
}

func newConductorWithLiveResourcesMutableNano(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	options ...conductorTestOption,
) (
	*conductor.Conductor,
	*aeron.Publication,
	*aeron.Subscription,
	[]int,
	*int64,
) {
	tb.Helper()

	var nanoNs int64
	config := conductorTestConfigMutableNano(tb, errorHandler, epochMs, &nanoNs)
	config.ResourceLifecycle = aeron.ConductorResourceLifecycle
	conductorInstance := newConductorForTest(tb, config, options...)
	pub, sub := seedLivePublicationAndSubscription(tb, conductorInstance)
	beforeTypeIDs := testutil.CommittedRecordTypeIDs(conductorInstance.DriverProxyForTest().Ring())
	return conductorInstance, pub, sub, beforeTypeIDs, &nanoNs
}

func newConductorWithLiveResourcesAtWorkNano(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	workNanoNs int64,
) (*conductor.Conductor, *aeron.Publication, *aeron.Subscription, []int) {
	tb.Helper()

	conductorInstance, pub, sub, beforeTypeIDs, nanoNs := newConductorWithLiveResourcesMutableNano(
		tb,
		errorHandler,
		epochMs,
	)
	*nanoNs = workNanoNs
	return conductorInstance, pub, sub, beforeTypeIDs
}

func testConductorDoWorkCompletesActivePendingPublicationReady(
	t *testing.T,
	kind conductor.PendingKind,
	correlationID int64,
	registerStreamID int32,
	wireEvent int,
	event conductor.PublicationBuffersReady,
) {
	t.Helper()

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
			kind,
			correlationID,
			registerStreamID,
		),
	)
	conductorInstance.SetActiveCorrelationID(correlationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		wireEvent,
		conductor.PublicationBuffersReadyBytes(event),
	)

	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(correlationID)
	require.True(t, done)
	require.NoError(t, err)
	require.Equal(t, kind, registration.Kind)
	require.True(t, registration.Ready)
	require.Equal(t, event.StreamID, registration.StreamID)
	require.Equal(t, "aeron:ipc", registration.Channel)
	require.Nil(t, registration.Publication)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	_, ok := conductorInstance.PendingResult(correlationID)
	require.False(t, ok)
}

func testConductorOnReadyViaDoWorkWrongKind(
	t *testing.T,
	correlationID int64,
	wireEvent int,
	eventBytes []byte,
) {
	t.Helper()

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
			conductor.PendingKindOperationSuccess,
			correlationID,
		),
	)
	conductorInstance.SetActiveCorrelationID(correlationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		wireEvent,
		eventBytes,
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.Equal(t, correlationID, conductorInstance.ReceivedCorrelationID())
}

func scriptBroadcastLap(tb testing.TB, adapter *conductor.DriverEventsAdapter) {
	tb.Helper()

	copyReceiver := adapter.CopyReceiverForTest()
	buf := copyReceiver.BroadcastReceiver().Buffer()
	capacity := copyReceiver.BroadcastReceiver().Capacity()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)

	// An unknown type id keeps the latest record, which the receive after
	// the lap delivers, out of every event handler.
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	fillBroadcastUntilLap(tb, tx, unknownDriverEventTypeID, payload, capacity)
	rewindBroadcastTailForLap(tb, buf, capacity)
}

func fillBroadcastUntilLap(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	msgTypeID int,
	payload []byte,
	capacity int,
) {
	tb.Helper()

	for range 3*capacity/16 + 1 {
		err := tx.Transmit(msgTypeID, payload)
		require.NoError(tb, err)
	}
}

func rewindBroadcastTailForLap(
	tb testing.TB,
	buf atomicbuffer.AtomicBuffer,
	capacity int,
) {
	tb.Helper()

	actualLatest := buf.GetInt64Volatile(broadcast.LatestIndex(capacity))
	buf.PutInt64Release(broadcast.LatestIndex(capacity), 0)
	buf.PutInt64Release(broadcast.LatestIndex(capacity), actualLatest)
}

func TestNewConductor_requiresResourceLifecycleWithImageLifecycle(t *testing.T) {
	t.Parallel()

	conductorInstance, err := conductor.NewConductor(
		nil,
		nil,
		nil,
		conductor.Config{
			ImageLifecycle: &recordingImageLifecycle{},
		},
	)
	require.Nil(t, conductorInstance)
	require.ErrorIs(t, err, conductor.ErrResourceLifecycleRequired)
	require.EqualError(
		t,
		err,
		"conductor: ResourceLifecycle is required when ImageLifecycle is set",
	)
}

func TestNewConductor_rejectsNegativeResourceLingerNs(t *testing.T) {
	t.Parallel()

	conductorInstance, err := conductor.NewConductor(
		nil,
		nil,
		nil,
		conductor.Config{
			ResourceLingerNs: -1,
		},
	)
	require.Nil(t, conductorInstance)
	require.ErrorIs(t, err, conductor.ErrNegativeResourceLingerNs)
	require.EqualError(t, err, "conductor: ResourceLingerNs must not be negative")
}

type countingHandlerScope struct {
	depth  int
	enters int
	exits  int
	nested int
}

func (scope *countingHandlerScope) EnterHandler() bool {
	if scope.depth != 0 {
		scope.nested++
		return false
	}
	scope.depth++
	scope.enters++
	return true
}

func (scope *countingHandlerScope) ExitHandler() {
	scope.exits++
	scope.depth--
}

func requireHandlerScope(t *testing.T, scope *countingHandlerScope, enters, nested int) {
	t.Helper()

	require.Equal(t, enters, scope.enters)
	require.Equal(t, nested, scope.nested)
	require.Equal(t, enters, scope.exits)
	require.Zero(t, scope.depth)
}

func installCountingHandlerScope(conductorInstance *conductor.Conductor) *countingHandlerScope {
	scope := &countingHandlerScope{}
	conductorInstance.SetHandlerScopeForTest(scope)
	return scope
}

func newHandlerScopeImageConductor(
	t *testing.T,
	lifecycle *recordingImageLifecycle,
	errorHandler func(error),
	trigger func(*testing.T, *conductor.Conductor),
) (*conductor.Conductor, *countingHandlerScope) {
	t.Helper()

	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, errorHandler)
	scope := installCountingHandlerScope(conductorInstance)
	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)
	trigger(t, conductorInstance)
	return conductorInstance, scope
}

func newHandlerScopeImageLogConductor(
	t *testing.T,
	mediaDriverDir string,
	logFileName string,
) (*conductor.Conductor, *countingHandlerScope, *[]error) {
	t.Helper()

	lifecycle := &recordingImageLifecycle{addImageOK: true}
	errorHandler, gotErrors := collectConductorErrors()
	config := conductorTestConfig(t, errorHandler, fixedClocks{})
	config.ImageLifecycle = lifecycle
	config.MediaDriverDir = mediaDriverDir
	conductorInstance := newConductorForTest(t, config)
	scope := installCountingHandlerScope(conductorInstance)
	registerGoldenImageSubscription(conductorInstance)

	event := goldenAvailableImageEvent
	event.LogFileName = logFileName
	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(event),
	)
	return conductorInstance, scope, gotErrors
}

func newHandlerScopeResourceConductor(
	t *testing.T,
	lifecycle conductor.ResourceLifecycle,
) (*conductor.Conductor, *countingHandlerScope, *[]error) {
	t.Helper()

	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
	conductorInstance.SetErrorHandlerForTest(errorHandler)
	return conductorInstance, installCountingHandlerScope(conductorInstance), gotErrors
}

func TestConductor_handlerScope_invokerPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "availableImageHandler", run: testHandlerScopeAvailableImage},
		{name: "unavailableImageHandler", run: testHandlerScopeUnavailableImage},
		{name: "imageHandlerPanicReported", run: testHandlerScopeImagePanicReported},
		{name: "imageHandlerPanicErrorHandlerPanics", run: testHandlerScopeImagePanicErrorHandlerPanics},
		{name: "logOutsideMediaDriverDir", run: testHandlerScopeLogOutsideDir},
		{name: "missingLogFile", run: testHandlerScopeMissingLogFile},
		{name: "publicationErrorTwoMatches", run: testHandlerScopePublicationErrorTwoMatches},
		{name: "terminateWithErrorNoResources", run: testHandlerScopeTerminateNoResources},
		{name: "rollbackAddPendingRetireError", run: testHandlerScopeRollbackRetire},
		{name: "closeAllResourcesRetireError", run: testHandlerScopeCloseAllRetire},
		{name: "closeAllResourcesOfferError", run: testHandlerScopeCloseAllOffer},
		{name: "closePublicationRetireError", run: testHandlerScopeClosePublicationRetire},
		{name: "closePublicationOfferError", run: testHandlerScopeClosePublicationOffer},
		{name: "closeSubscriptionRetireError", run: testHandlerScopeCloseSubscriptionRetire},
		{name: "closeSubscriptionOfferError", run: testHandlerScopeCloseSubscriptionOffer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testHandlerScopeAvailableImage(t *testing.T) {
	var calls int
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	lifecycle.availableImageHandler = func(conductor.ImageHandle) { calls++ }
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, scope := newHandlerScopeImageConductor(
		t,
		lifecycle,
		errorHandler,
		triggerGoldenAvailableImage,
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, calls)
	require.Empty(t, *gotErrors)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeUnavailableImage(t *testing.T) {
	var calls int
	lifecycle := &recordingImageLifecycle{
		addImageOK:         true,
		removedLogFileName: goldenAvailableImageEvent.LogFileName,
	}
	lifecycle.unavailableImageHandler = func(conductor.ImageHandle) { calls++ }
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, scope := newHandlerScopeImageConductor(
		t,
		lifecycle,
		errorHandler,
		triggerGoldenUnavailableImage,
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, calls)
	require.Empty(t, *gotErrors)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeImagePanicReported(t *testing.T) {
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	lifecycle.availableImageHandler = func(conductor.ImageHandle) {
		panic(errors.New("handler boom"))
	}
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance, scope := newHandlerScopeImageConductor(
		t,
		lifecycle,
		errorHandler,
		triggerGoldenAvailableImage,
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.EqualError(t, (*gotErrors)[0], "handler boom")
	requireHandlerScope(t, scope, 1, 1)
}

func testHandlerScopeImagePanicErrorHandlerPanics(t *testing.T) {
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	lifecycle.availableImageHandler = func(conductor.ImageHandle) {
		panic(errors.New("handler boom"))
	}
	conductorInstance, scope := newHandlerScopeImageConductor(
		t,
		lifecycle,
		func(error) {
			panic("error handler boom")
		},
		triggerGoldenAvailableImage,
	)

	require.Panics(t, func() {
		_ = conductorInstance.DoWork()
	})
	requireHandlerScope(t, scope, 1, 1)
}

func testHandlerScopeLogOutsideDir(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeImageLogConductor(
		t,
		t.TempDir(),
		filepath.Join(t.TempDir(), "image.log"),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrLogFileOutsideMediaDriverDir)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeMissingLogFile(t *testing.T) {
	dir := t.TempDir()
	conductorInstance, scope, gotErrors := newHandlerScopeImageLogConductor(
		t,
		dir,
		filepath.Join(dir, "missing.log"),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrLogBufferMapFailed)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopePublicationErrorTwoMatches(t *testing.T) {
	var calls int
	errorHandler, gotErrors := collectConductorErrors()
	config := conductorTestConfig(t, errorHandler, fixedClocks{})
	config.PublicationErrorFrameHandler = func(conductor.PublicationError) { calls++ }
	conductorInstance := newConductorForTest(t, config)
	scope := installCountingHandlerScope(conductorInstance)
	frameID := goldenPublicationErrorEvent.RegistrationID
	conductorInstance.InsertPublicationForTest(
		10,
		stubPublication{registrationID: 10, originalRegistrationID: frameID},
	)
	conductorInstance.InsertPublicationForTest(
		11,
		stubPublication{registrationID: 11, originalRegistrationID: frameID},
	)
	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnPublicationError,
		conductor.PublicationErrorBytes(goldenPublicationErrorEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 2, calls)
	require.Empty(t, *gotErrors)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeTerminateNoResources(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(t, conductorTestConfig(t, errorHandler, fixedClocks{}))
	scope := installCountingHandlerScope(conductorInstance)
	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrClientTimeout)
	requireHandlerScope(t, scope, 1, 1)
}

func testHandlerScopeRollbackRetire(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{
			retirePublicationErr: errRetirePublication,
		},
	)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.RollbackAddPendingResourcesForTest(conductor.PendingRegistration{
		Kind:          conductor.PendingKindAddPublication,
		CorrelationID: 42,
	})

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], errRetirePublication)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeCloseAllRetire(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{
			retirePublicationErr: errRetirePublication,
		},
	)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.CloseAllResources()

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], errRetirePublication)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeCloseAllOffer(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{},
	)
	seedFullDriverRingForCloseTest(t, conductorInstance)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.CloseAllResources()

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrRingInsufficientCapacity)
	requireHandlerScope(t, scope, 1, 0)
}

func testHandlerScopeClosePublicationRetire(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{
			retirePublicationErr: errRetirePublication,
		},
	)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	require.NoError(t, conductorInstance.ClosePublication(stubPublication{registrationID: 42}))

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], errRetirePublication)
	requireHandlerScope(t, scope, 0, 0)
}

func testHandlerScopeClosePublicationOffer(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{},
	)
	seedFullDriverRingForCloseTest(t, conductorInstance)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	require.NoError(t, conductorInstance.ClosePublication(stubPublication{registrationID: 42}))

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrRingInsufficientCapacity)
	requireHandlerScope(t, scope, 0, 0)
}

func testHandlerScopeCloseSubscriptionRetire(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{
			retireSubscriptionErr: errRetireSubscription,
		},
	)
	sub := &stubResourceSubscription{registrationID: 22}
	conductorInstance.InsertSubscriptionForTest(22, sub)
	require.NoError(t, conductorInstance.CloseSubscription(sub))

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], errRetireSubscription)
	requireHandlerScope(t, scope, 0, 0)
}

func testHandlerScopeCloseSubscriptionOffer(t *testing.T) {
	conductorInstance, scope, gotErrors := newHandlerScopeResourceConductor(
		t,
		&recordingResourceLifecycle{},
	)
	seedFullDriverRingForCloseTest(t, conductorInstance)
	sub := &stubResourceSubscription{registrationID: 22}
	conductorInstance.InsertSubscriptionForTest(22, sub)
	require.NoError(t, conductorInstance.CloseSubscription(sub))

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrRingInsufficientCapacity)
	requireHandlerScope(t, scope, 0, 0)
}
