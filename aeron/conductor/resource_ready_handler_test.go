package conductor_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var errStubPublicationBuild = errors.New("conductor_test: publication build failed")

type stubResourcePublication struct {
	registrationID int64
}

func (publication stubResourcePublication) RegistrationID() int64 {
	return publication.registrationID
}

type stubResourceSubscription struct {
	registrationID         int64
	streamID               int32
	channelStatusCounterID int32
}

func (subscription *stubResourceSubscription) RegistrationID() int64 {
	return subscription.registrationID
}

func (subscription *stubResourceSubscription) StreamID() int32 {
	return subscription.streamID
}

type recordingResourceLifecycle struct {
	newPublicationErr        error
	applySubscriptionErr     error
	retireSubscriptionErr    error
	retirePublicationErr     error
	newPublicationCalls      int
	newPreSubscriptionCalls  int
	applySubscriptionCalls   int
	retireSubscriptionCalls  int
	retirePublicationCalls   int
	lastAppliedChannelStatus int32
}

func (lifecycle *recordingResourceLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	lifecycle.newPublicationCalls++
	if lifecycle.newPublicationErr != nil {
		return nil, lifecycle.newPublicationErr
	}
	return stubResourcePublication{registrationID: event.RegistrationID}, nil
}

func (lifecycle *recordingResourceLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	_ string,
) conductor.SubscriptionHandle {
	lifecycle.newPreSubscriptionCalls++
	return &stubResourceSubscription{
		registrationID: registrationID,
		streamID:       streamID,
	}
}

func (lifecycle *recordingResourceLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	lifecycle.applySubscriptionCalls++
	if lifecycle.applySubscriptionErr != nil {
		return lifecycle.applySubscriptionErr
	}
	stubSub, ok := sub.(*stubResourceSubscription)
	if !ok {
		return errors.New("conductor_test: unexpected subscription handle type")
	}
	stubSub.channelStatusCounterID = event.ChannelStatusCounterID
	lifecycle.lastAppliedChannelStatus = event.ChannelStatusCounterID
	return nil
}

func (lifecycle *recordingResourceLifecycle) RetireSubscription(
	_ conductor.SubscriptionHandle,
) ([]string, error) {
	lifecycle.retireSubscriptionCalls++
	if lifecycle.retireSubscriptionErr != nil {
		return nil, lifecycle.retireSubscriptionErr
	}
	return nil, nil
}

func (lifecycle *recordingResourceLifecycle) RetirePublication(
	_ conductor.PublicationHandle,
) (string, error) {
	lifecycle.retirePublicationCalls++
	if lifecycle.retirePublicationErr != nil {
		return "", lifecycle.retirePublicationErr
	}
	return goldenPublicationReadyLogFileName, nil
}

func newConductorWithResourceLifecycleForTest(
	tb testing.TB,
	lifecycle conductor.ResourceLifecycle,
) *conductor.Conductor {
	tb.Helper()

	config := conductorTestConfig(tb, func(error) {}, fixedClocks{})
	config.ResourceLifecycle = lifecycle
	return newConductorForTest(tb, config)
}

func newConductorWithResourceLifecycleMutableNanoForTest(
	tb testing.TB,
	epochMs int64,
	nanoNs *int64,
) (*conductor.Conductor, *recordingResourceLifecycle) {
	tb.Helper()

	lifecycle := &recordingResourceLifecycle{}
	config := conductorTestConfigMutableNano(tb, func(error) {}, epochMs, nanoNs)
	config.ResourceLifecycle = lifecycle
	return newConductorForTest(tb, config), lifecycle
}

func registerGoldenAddPendingForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
) {
	tb.Helper()

	var (
		correlationID int64
		streamID      int32
	)
	switch kind {
	case conductor.PendingKindAddSubscription:
		correlationID = goldenSubscriptionReadyCorrelationID
		streamID = pendingTestStreamID
	case conductor.PendingKindAddPublication:
		correlationID = goldenPublicationReadyCorrelationID
		streamID = goldenPublicationBuffersReadyEvent.StreamID
	default:
		tb.Fatalf("unsupported pending kind %v", kind)
	}
	require.NoError(
		tb,
		registerPendingAddWithCorrelationForTest(
			conductorInstance,
			kind,
			correlationID,
			streamID,
		),
	)
}

func completeGoldenPublicationReadyForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()
	seedGoldenPublicationLogBuffers(tb, conductorInstance)
	registerGoldenAddPendingForTest(
		tb,
		conductorInstance,
		conductor.PendingKindAddPublication,
	)
	conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
}

func newConductorWithLockDepthLifecycleForTest(
	tb testing.TB,
) (*conductor.Conductor, *lockDepthResourceLifecycle) {
	tb.Helper()

	depth := &lockDepthResourceLifecycle{
		recordingResourceLifecycle: &recordingResourceLifecycle{},
	}
	config := conductorTestConfig(tb, func(error) {}, fixedClocks{})
	config.ResourceLifecycle = depth
	conductorInstance := newConductorForTest(tb, config)
	depth.conductor = conductorInstance
	return conductorInstance, depth
}

func TestConductor_handlePublicationReady_insertsPublicationWithLifecycle(t *testing.T) {
	lifecycle := &recordingResourceLifecycle{}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
	seedGoldenPublicationLogBuffers(t, conductorInstance)

	require.NoError(
		t,
		registerPendingAddWithCorrelationForTest(
			conductorInstance,
			conductor.PendingKindAddPublication,
			goldenPublicationReadyCorrelationID,
			goldenPublicationBuffersReadyEvent.StreamID,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenPublicationReadyCorrelationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)
	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(goldenPublicationReadyCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.NotNil(t, registration.Publication)
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())
	require.Equal(t, 1, lifecycle.newPublicationCalls)

	got, ok := conductorInstance.PublicationByRegID(goldenPublicationBuffersReadyEvent.RegistrationID)
	require.True(t, ok)
	require.Equal(t, goldenPublicationBuffersReadyEvent.RegistrationID, got.RegistrationID())

	ready, latched := conductor.LastPublicationReadyForTest(conductorInstance)
	require.True(t, latched)
	require.Equal(t, goldenPublicationBuffersReadyEvent, ready)
}

func TestConductor_handleSubscriptionReady_updatesPreInsertedSubscription(t *testing.T) {
	lifecycle := &recordingResourceLifecycle{}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)

	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
	)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.newPreSubscriptionCalls)

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
	require.NotNil(t, registration.Subscription)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.applySubscriptionCalls)
	require.Equal(
		t,
		goldenSubscriptionReadyEvent.ChannelStatusCounterID,
		lifecycle.lastAppliedChannelStatus,
	)

	ready, latched := conductor.LastSubscriptionReadyForTest(conductorInstance)
	require.True(t, latched)
	require.Equal(t, goldenSubscriptionReadyEvent, ready)
}

func TestConductor_RegisterPendingAdd_nilLifecycle_skipsPreInsert(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(t, func(error) {}, 0, 0)
	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
	)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
}

func TestConductor_preInsertSubscription_idempotent(t *testing.T) {
	lifecycle := &recordingResourceLifecycle{}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)

	conductorInstance.PreInsertSubscriptionForTest(
		goldenSubscriptionReadyCorrelationID,
		pendingTestStreamID,
		pendingTestChannel,
	)
	conductorInstance.PreInsertSubscriptionForTest(
		goldenSubscriptionReadyCorrelationID,
		pendingTestStreamID,
		pendingTestChannel,
	)

	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.newPreSubscriptionCalls)
}

func TestConductor_handlePublicationReady_failurePaths(t *testing.T) {
	tests := []struct {
		name          string
		seedLog       bool
		buildErr      error
		wantErr       error
		wantErrString string
	}{
		{
			name:          "acquireLogBuffersFailure",
			seedLog:       false,
			wantErrString: "conductor: failed to map log buffer",
		},
		{
			name:     "newPublicationFailure",
			seedLog:  true,
			buildErr: errStubPublicationBuild,
			wantErr:  errStubPublicationBuild,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			lifecycle := &recordingResourceLifecycle{newPublicationErr: testCase.buildErr}
			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
			registry := conductor.LogBuffersRegistryForTest(conductorInstance)
			requireGoldenPublicationLogRefCount := func(want int32) {
				require.Equal(
					t,
					want,
					conductor.LogBuffersRegistryRefCountForTest(
						registry,
						goldenPublicationReadyLogFileName,
					),
				)
			}
			if testCase.seedLog {
				seedGoldenPublicationLogBuffers(t, conductorInstance)
				requireGoldenPublicationLogRefCount(1)
			}

			require.NoError(
				t,
				registerPendingAddWithCorrelationForTest(
					conductorInstance,
					conductor.PendingKindAddPublication,
					goldenPublicationReadyCorrelationID,
					goldenPublicationBuffersReadyEvent.StreamID,
				),
			)
			conductorInstance.SetActiveCorrelationID(goldenPublicationReadyCorrelationID)
			conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)

			done, _, err := conductorInstance.PollPending(goldenPublicationReadyCorrelationID)
			require.True(t, done)
			if testCase.wantErr != nil {
				require.ErrorIs(t, err, testCase.wantErr)
			}
			if testCase.wantErrString != "" {
				require.EqualError(t, err, testCase.wantErrString)
			}
			require.Equal(t, 0, conductorInstance.PublicationCountForTest())
			if testCase.seedLog {
				requireGoldenPublicationLogRefCount(1)
			}
		})
	}
}

func TestConductor_handleSubscriptionReady_applyFailure_failsPendingAndCleansMap(t *testing.T) {
	errApply := errors.New("conductor_test: apply subscription ready failed")
	lifecycle := &recordingResourceLifecycle{applySubscriptionErr: errApply}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)

	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
	)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)

	done, _, err := conductorInstance.PollPending(goldenSubscriptionReadyCorrelationID)
	require.True(t, done)
	require.ErrorIs(t, err, errApply)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
}

func TestConductor_addSubscriptionPending_teardownRollsBackPreInsert(t *testing.T) {
	t.Run("driverError", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddSubscription,
		)
		require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

		ok := conductorInstance.DriverError(conductor.ErrorResponse{
			OffendingCommandCorrelationID: goldenSubscriptionReadyCorrelationID,
			ErrorCode:                     11,
			ErrorMessage:                  "rejected",
		})
		require.True(t, ok)
		require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
		require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
	})

	t.Run("cancelPending", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddSubscription,
		)
		conductorInstance.CancelPending(goldenSubscriptionReadyCorrelationID)
		require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
		require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
	})

	t.Run("cancelPendingAfterReady", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddSubscription,
		)
		conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)
		require.Equal(t, goldenSubscriptionReadyCorrelationID, conductorInstance.ReceivedCorrelationID())

		conductorInstance.CancelPending(goldenSubscriptionReadyCorrelationID)

		require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
		require.Equal(t, 0, lifecycle.retireSubscriptionCalls)
		_, ok := conductorInstance.SubscriptionByRegID(goldenSubscriptionReadyCorrelationID)
		require.True(t, ok)
		require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
		_, ok = conductorInstance.PendingResult(goldenSubscriptionReadyCorrelationID)
		require.False(t, ok)
	})

	expireCases := []struct {
		name          string
		correlationID int64
		setActive     bool
	}{
		{
			name:          "inactiveExpire",
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "activeExpire",
			correlationID: goldenSubscriptionReadyCorrelationID,
			setActive:     true,
		},
	}
	for _, testCase := range expireCases {
		t.Run(testCase.name, func(t *testing.T) {
			const startNs = int64(1_000)
			nanoNs := startNs
			conductorInstance, lifecycle := newConductorWithResourceLifecycleMutableNanoForTest(
				t,
				0,
				&nanoNs,
			)
			require.NoError(
				t,
				conductorInstance.RegisterPendingAdd(
					conductor.PendingKindAddSubscription,
					testCase.correlationID,
					pendingTestStreamID,
					pendingTestChannel,
					500*time.Nanosecond,
				),
			)
			if testCase.setActive {
				conductorInstance.SetActiveCorrelationID(testCase.correlationID)
			}
			require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

			nanoNs = startNs + 500
			_ = conductorInstance.DoWork()
			require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
			require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
		})
	}
}

func TestConductor_inactiveExpire_dropsLateSubscriptionReady(t *testing.T) {
	const startNs = int64(1_000)
	nanoNs := startNs
	conductorInstance, lifecycle := newConductorWithResourceLifecycleMutableNanoForTest(
		t,
		0,
		&nanoNs,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPendingAdd(
			conductor.PendingKindAddSubscription,
			pendingTestOtherCorrelationID,
			pendingTestStreamID,
			pendingTestChannel,
			500*time.Nanosecond,
		),
	)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	nanoNs = startNs + 500
	_ = conductorInstance.DoWork()
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(
			goldenSubscriptionReadyWithCorrelation(pendingTestOtherCorrelationID),
		),
	)
	_ = conductorInstance.DoWork()

	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	_, ok := conductorInstance.PendingResult(pendingTestOtherCorrelationID)
	require.False(t, ok)
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_handleSubscriptionReady_fallbackInsertWithoutPreInsert(t *testing.T) {
	lifecycle := &recordingResourceLifecycle{}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)

	require.NoError(
		t,
		conductorInstance.RegisterPendingEntryForTest(
			conductor.PendingRegistration{
				CorrelationID: goldenSubscriptionReadyCorrelationID,
				Kind:          conductor.PendingKindAddSubscription,
				StreamID:      pendingTestStreamID,
				Channel:       pendingTestChannel,
				DeadlineNs:    time.Second.Nanoseconds(),
			},
			true,
		),
	)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())

	conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)

	done, registration, err := conductorInstance.PollPending(goldenSubscriptionReadyCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.NotNil(t, registration.Subscription)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.newPreSubscriptionCalls)
}

func TestConductor_readyHandlers_useInternalLookupUnderWriteLock(t *testing.T) {
	t.Run("publicationReady", func(t *testing.T) {
		conductorInstance, lifecycleWithDepth := newConductorWithLockDepthLifecycleForTest(t)
		seedGoldenPublicationLogBuffers(t, conductorInstance)

		require.NoError(
			t,
			registerPendingAddWithCorrelationForTest(
				conductorInstance,
				conductor.PendingKindAddPublication,
				goldenPublicationReadyCorrelationID,
				goldenPublicationBuffersReadyEvent.StreamID,
			),
		)

		conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
		require.Equal(t, int32(1), lifecycleWithDepth.depthDuringNewPublication)
	})

	t.Run("subscriptionReady", func(t *testing.T) {
		conductorInstance, lifecycleWithDepth := newConductorWithLockDepthLifecycleForTest(t)
		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddSubscription,
		)

		conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)
		require.Equal(t, int32(1), lifecycleWithDepth.depthDuringApplySubscription)
	})

	t.Run("preInsert", func(t *testing.T) {
		conductorInstance, lifecycleWithDepth := newConductorWithLockDepthLifecycleForTest(t)

		conductorInstance.PreInsertSubscriptionForTest(
			goldenSubscriptionReadyCorrelationID,
			pendingTestStreamID,
			pendingTestChannel,
		)
		require.Equal(t, int32(1), lifecycleWithDepth.depthDuringNewPreSubscription)
	})
}

type lockDepthResourceLifecycle struct {
	*recordingResourceLifecycle
	conductor                     *conductor.Conductor
	depthDuringNewPublication     int32
	depthDuringApplySubscription  int32
	depthDuringNewPreSubscription int32
}

func (lifecycle *lockDepthResourceLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	lifecycle.depthDuringNewPublication = lifecycle.conductor.ResourceWriteLockDepthForTest()
	return lifecycle.recordingResourceLifecycle.NewPublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
}

func (lifecycle *lockDepthResourceLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) conductor.SubscriptionHandle {
	lifecycle.depthDuringNewPreSubscription = lifecycle.conductor.ResourceWriteLockDepthForTest()
	return lifecycle.recordingResourceLifecycle.NewPreSubscription(
		registrationID,
		streamID,
		channel,
	)
}

func (lifecycle *lockDepthResourceLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	lifecycle.depthDuringApplySubscription = lifecycle.conductor.ResourceWriteLockDepthForTest()
	return lifecycle.recordingResourceLifecycle.ApplySubscriptionReady(sub, event)
}

func TestConductor_wrongKindReady_doesNotLatchStaging(t *testing.T) {
	tests := []struct {
		name              string
		register          func(*conductor.Conductor) error
		wireEvent         int
		eventBytes        []byte
		assertPub         bool
		assertSub         bool
		wantSubCount      int
		wantPubCount      int
		wantCorrelationID int64
		pollCorrelationID int64
	}{
		{
			name: "addPublicationWrongSubscriptionReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddWithCorrelationForTest(
					conductorInstance,
					conductor.PendingKindAddPublication,
					goldenSubscriptionReadyCorrelationID,
					pendingTestStreamID,
				)
			},
			wireEvent:         conductor.OnSubscriptionReady,
			eventBytes:        conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
			assertSub:         true,
			wantPubCount:      0,
			wantSubCount:      0,
			wantCorrelationID: conductor.NullCorrelationID,
			pollCorrelationID: goldenSubscriptionReadyCorrelationID,
		},
		{
			name: "addSubscriptionWrongPublicationReady",
			register: func(conductorInstance *conductor.Conductor) error {
				return registerPendingAddWithCorrelationForTest(
					conductorInstance,
					conductor.PendingKindAddSubscription,
					goldenPublicationReadyCorrelationID,
					goldenPublicationBuffersReadyEvent.StreamID,
				)
			},
			wireEvent: conductor.OnPublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenPublicationBuffersReadyEvent,
			),
			assertPub:         true,
			wantPubCount:      0,
			wantSubCount:      1,
			wantCorrelationID: conductor.NullCorrelationID,
			pollCorrelationID: goldenPublicationReadyCorrelationID,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			lifecycle := &recordingResourceLifecycle{}
			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
			require.NoError(t, testCase.register(conductorInstance))

			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				testCase.wireEvent,
				testCase.eventBytes,
			)
			_ = conductorInstance.DoWork()

			_, pubLatched := conductor.LastPublicationReadyForTest(conductorInstance)
			_, subLatched := conductor.LastSubscriptionReadyForTest(conductorInstance)
			if testCase.assertPub {
				require.False(t, pubLatched)
			}
			if testCase.assertSub {
				require.False(t, subLatched)
			}
			require.Equal(t, testCase.wantPubCount, conductorInstance.PublicationCountForTest())
			require.Equal(t, testCase.wantSubCount, conductorInstance.SubscriptionCountForTest())
			require.Equal(t, testCase.wantCorrelationID, conductorInstance.ReceivedCorrelationID())

			done, _, err := conductorInstance.PollPending(testCase.pollCorrelationID)
			require.False(t, done)
			require.NoError(t, err)
		})
	}
}

func TestConductor_adapterWrongKindRemoveReadyViaDoWork(t *testing.T) {
	tests := []struct {
		name       string
		kind       conductor.PendingKind
		wireEvent  int
		eventBytes []byte
	}{
		{
			name:      "removePublication_subscriptionReady",
			kind:      conductor.PendingKindRemovePublication,
			wireEvent: conductor.OnSubscriptionReady,
			eventBytes: conductor.SubscriptionReadyBytes(
				goldenSubscriptionReadyWithCorrelation(pendingTestCorrelationID),
			),
		},
		{
			name:      "removeSubscription_publicationReady",
			kind:      conductor.PendingKindRemoveSubscription,
			wireEvent: conductor.OnPublicationReady,
			eventBytes: conductor.PublicationBuffersReadyBytes(
				goldenPublicationBuffersReadyWithCorrelation(pendingTestCorrelationID),
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
			require.NoError(t, registerPendingForTest(conductorInstance, testCase.kind))
			conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)

			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				testCase.wireEvent,
				testCase.eventBytes,
			)
			_ = conductorInstance.DoWork()

			require.Equal(t, pendingTestCorrelationID, conductorInstance.ReceivedCorrelationID())
			done, registration, err := conductorInstance.PollPending(pendingTestCorrelationID)
			require.True(t, done, nonAddWrongKindLatchPollDoneReason)
			require.NoError(t, err)
			require.Equal(t, testCase.kind, registration.Kind)
			require.Zero(t, registration.RegistrationID)
		})
	}
}

func TestConductor_addPublicationPending_teardownRollsBackReadyInsert(t *testing.T) {
	t.Run("driverError", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddPublication,
		)
		require.Equal(t, 0, conductorInstance.PublicationCountForTest())

		ok := conductorInstance.DriverError(conductor.ErrorResponse{
			OffendingCommandCorrelationID: goldenPublicationReadyCorrelationID,
			ErrorCode:                     11,
			ErrorMessage:                  "rejected",
		})
		require.True(t, ok)
		require.Equal(t, 0, conductorInstance.PublicationCountForTest())
		require.Equal(t, 0, lifecycle.retirePublicationCalls)
	})

	t.Run("cancelPending", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddPublication,
		)
		conductorInstance.CancelPending(goldenPublicationReadyCorrelationID)
		require.Equal(t, 0, conductorInstance.PublicationCountForTest())
		require.Equal(t, 0, lifecycle.retirePublicationCalls)
	})

	t.Run("cancelPendingAfterReady", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		completeGoldenPublicationReadyForTest(t, conductorInstance)
		require.Equal(t, 1, conductorInstance.PublicationCountForTest())
		require.Equal(
			t,
			goldenPublicationReadyCorrelationID,
			conductorInstance.ReceivedCorrelationID(),
		)

		conductorInstance.CancelPending(goldenPublicationReadyCorrelationID)

		require.Equal(t, 1, conductorInstance.PublicationCountForTest())
		require.Equal(t, 0, lifecycle.retirePublicationCalls)
		_, ok := conductorInstance.PublicationByRegID(
			goldenPublicationBuffersReadyEvent.RegistrationID,
		)
		require.True(t, ok)
		require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
		_, ok = conductorInstance.PendingResult(goldenPublicationReadyCorrelationID)
		require.False(t, ok)
	})

	t.Run("clearPendingAfterReady", func(t *testing.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		completeGoldenPublicationReadyForTest(t, conductorInstance)
		require.Equal(t, 1, conductorInstance.PublicationCountForTest())

		conductorInstance.ClearPending()

		require.Equal(t, 0, conductorInstance.PublicationCountForTest())
		require.Equal(t, 1, lifecycle.retirePublicationCalls)
		_, ok := conductorInstance.PublicationByRegID(
			goldenPublicationBuffersReadyEvent.RegistrationID,
		)
		require.False(t, ok)
	})

	expireCases := []struct {
		name          string
		correlationID int64
		setActive     bool
	}{
		{
			name:          "inactiveExpire",
			correlationID: pendingTestOtherCorrelationID,
		},
		{
			name:          "activeExpire",
			correlationID: goldenPublicationReadyCorrelationID,
			setActive:     true,
		},
	}
	for _, testCase := range expireCases {
		t.Run(testCase.name, func(t *testing.T) {
			const startNs = int64(1_000)
			nanoNs := startNs
			conductorInstance, lifecycle := newConductorWithResourceLifecycleMutableNanoForTest(
				t,
				0,
				&nanoNs,
			)
			require.NoError(
				t,
				conductorInstance.RegisterPendingAdd(
					conductor.PendingKindAddPublication,
					testCase.correlationID,
					goldenPublicationBuffersReadyEvent.StreamID,
					pendingTestChannel,
					500*time.Nanosecond,
				),
			)
			if testCase.setActive {
				conductorInstance.SetActiveCorrelationID(testCase.correlationID)
			}
			require.Equal(t, 0, conductorInstance.PublicationCountForTest())

			nanoNs = startNs + 500
			_ = conductorInstance.DoWork()
			require.Equal(t, 0, conductorInstance.PublicationCountForTest())
			require.Equal(t, 0, lifecycle.retirePublicationCalls)
		})
	}
}

func TestConductor_removeInsertedPublication_retireFailureReportsAndRemovesMap(t *testing.T) {
	errRetire := errors.New("conductor_test: retire publication failed")
	errorHandler, gotErrors := collectConductorErrors()
	config := conductorTestConfig(t, errorHandler, fixedClocks{})
	config.ResourceLifecycle = &recordingResourceLifecycle{retirePublicationErr: errRetire}
	conductorInstance := newConductorForTest(t, config)

	completeGoldenPublicationReadyForTest(t, conductorInstance)
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())

	conductorInstance.ClearPending()

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], errRetire)
	require.Equal(t, 0, conductorInstance.PublicationCountForTest())
	_, ok := conductorInstance.PublicationByRegID(
		goldenPublicationBuffersReadyEvent.RegistrationID,
	)
	require.False(t, ok)
}

func TestConductor_removePreInsertedSubscription_retireFailureReportsAndRemovesMap(t *testing.T) {
	errRetire := errors.New("conductor_test: retire subscription failed")
	errorHandler, gotErrors := collectConductorErrors()
	config := conductorTestConfig(t, errorHandler, fixedClocks{})
	config.ResourceLifecycle = &recordingResourceLifecycle{retireSubscriptionErr: errRetire}
	conductorInstance := newConductorForTest(t, config)

	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddSubscription,
	)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	conductorInstance.CancelPending(goldenSubscriptionReadyCorrelationID)

	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], errRetire)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	_, ok := conductorInstance.SubscriptionByRegID(goldenSubscriptionReadyCorrelationID)
	require.False(t, ok)
}

func seedGoldenPublicationLogBuffers(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()
	seedLogBuffersForTest(tb, conductorInstance, goldenPublicationReadyLogFileName)
}
