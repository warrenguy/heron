package conductor_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	errStubPublicationBuild = errors.New("conductor_test: publication build failed")
	errStubBindPublication  = errors.New("conductor_test: bind publication failed")
	errStubBindSubscription = errors.New("conductor_test: bind subscription failed")
	errRetirePublication    = errors.New("conductor_test: retire publication failed")
	errRetireSubscription   = errors.New("conductor_test: retire subscription failed")
)

const pendingAddExpireTimeout = 500 * time.Nanosecond

type noopCloseBindLifecycle struct{}

func (noopCloseBindLifecycle) BindPublication(
	_ conductor.PublicationHandle,
	_ *conductor.Conductor,
) error {
	return nil
}

func (noopCloseBindLifecycle) BindSubscription(
	_ conductor.SubscriptionHandle,
	_ *conductor.Conductor,
) error {
	return nil
}

func (noopCloseBindLifecycle) MarkPublicationClosed(_ conductor.PublicationHandle) {}

func (noopCloseBindLifecycle) MarkSubscriptionClosed(_ conductor.SubscriptionHandle) {}

type stubResourcePublication struct {
	registrationID int64
	registryKey    string
}

func (publication stubResourcePublication) RegistrationID() int64 {
	return publication.registrationID
}

func stubResourcePublicationFromEvent(
	event conductor.PublicationBuffersReady,
) stubResourcePublication {
	return stubResourcePublication{
		registrationID: event.CorrelationID,
		registryKey:    event.LogFileName,
	}
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
	noopCloseBindLifecycle

	newPublicationErr            error
	newExclusivePublicationErr   error
	bindPublicationErr           error
	bindSubscriptionErr          error
	applySubscriptionErr         error
	retireSubscriptionErr        error
	retirePublicationErr         error
	newPublicationCalls          int
	newExclusivePublicationCalls int
	newPreSubscriptionCalls      int
	bindPublicationCalls         int
	bindSubscriptionCalls        int
	applySubscriptionCalls       int
	retireSubscriptionCalls      int
	retirePublicationCalls       int
	lastAppliedChannelStatus     int32

	// registrationIDOffset shifts stub publication ids away from the ready
	// correlation id.
	registrationIDOffset int64
}

func (lifecycle *recordingResourceLifecycle) BindPublication(
	_ conductor.PublicationHandle,
	_ *conductor.Conductor,
) error {
	lifecycle.bindPublicationCalls++
	if lifecycle.bindPublicationErr != nil {
		return lifecycle.bindPublicationErr
	}
	return nil
}

func (lifecycle *recordingResourceLifecycle) BindSubscription(
	_ conductor.SubscriptionHandle,
	_ *conductor.Conductor,
) error {
	lifecycle.bindSubscriptionCalls++
	if lifecycle.bindSubscriptionErr != nil {
		return lifecycle.bindSubscriptionErr
	}
	return nil
}

func (lifecycle *recordingResourceLifecycle) recordStubPublication(
	event conductor.PublicationBuffersReady,
	exclusive bool,
) (conductor.PublicationHandle, error) {
	if exclusive {
		lifecycle.newExclusivePublicationCalls++
		if lifecycle.newExclusivePublicationErr != nil {
			return nil, lifecycle.newExclusivePublicationErr
		}
	} else {
		lifecycle.newPublicationCalls++
		if lifecycle.newPublicationErr != nil {
			return nil, lifecycle.newPublicationErr
		}
	}
	publication := stubResourcePublicationFromEvent(event)
	publication.registrationID += lifecycle.registrationIDOffset
	return publication, nil
}

func (lifecycle *recordingResourceLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	return lifecycle.recordStubPublication(event, false)
}

func (lifecycle *recordingResourceLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	return lifecycle.recordStubPublication(event, true)
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
	pub conductor.PublicationHandle,
) (string, error) {
	lifecycle.retirePublicationCalls++
	if lifecycle.retirePublicationErr != nil {
		return "", lifecycle.retirePublicationErr
	}
	switch p := pub.(type) {
	case *aeron.Publication, *aeron.ExclusivePublication:
		return aeron.ConductorResourceLifecycle.RetirePublication(pub)
	case stubResourcePublication:
		return p.registryKey, nil
	default:
		return goldenPublicationReadyLogFileName, nil
	}
}

type absentPublicationLifecycle struct {
	recordingResourceLifecycle

	returnTypedNil bool
}

func (lifecycle *absentPublicationLifecycle) absentPublicationHandle(
	exclusive bool,
) (conductor.PublicationHandle, error) {
	if exclusive {
		lifecycle.newExclusivePublicationCalls++
		if lifecycle.returnTypedNil {
			return (*aeron.ExclusivePublication)(nil), nil
		}
	} else {
		lifecycle.newPublicationCalls++
		if lifecycle.returnTypedNil {
			return (*aeron.Publication)(nil), nil
		}
	}
	return nil, nil
}

func (lifecycle *absentPublicationLifecycle) NewPublication(
	_ conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	return lifecycle.absentPublicationHandle(false)
}

func (lifecycle *absentPublicationLifecycle) NewExclusivePublication(
	_ conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	return lifecycle.absentPublicationHandle(true)
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
	case conductor.PendingKindAddExclusivePublication:
		correlationID = goldenExclusivePublicationReadyCorrelationID
		streamID = goldenExclusivePublicationBuffersReadyEvent.StreamID
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
	completeGoldenPublicationKindReadyForTest(
		tb,
		conductorInstance,
		conductor.PendingKindAddPublication,
	)
}

func completeGoldenExclusivePublicationReadyForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()
	completeGoldenPublicationKindReadyForTest(
		tb,
		conductorInstance,
		conductor.PendingKindAddExclusivePublication,
	)
}

func completeGoldenPublicationKindReadyForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
) {
	tb.Helper()

	switch kind {
	case conductor.PendingKindAddPublication:
		seedGoldenPublicationLogBuffers(tb, conductorInstance)
		registerGoldenAddPendingForTest(tb, conductorInstance, kind)
		conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
	case conductor.PendingKindAddExclusivePublication:
		seedGoldenExclusivePublicationLogBuffers(tb, conductorInstance)
		seedPublicationCountersForTest(tb, conductorInstance, goldenExclusivePublicationBuffersReadyEvent)
		registerGoldenAddPendingForTest(tb, conductorInstance, kind)
		conductorInstance.ExclusivePublicationReadyForTest(goldenExclusivePublicationBuffersReadyEvent)
	default:
		tb.Fatalf("unsupported publication pending kind %v", kind)
	}
}

func requireGoldenPublicationKindLogRefCountForTest(
	tb testing.TB,
	registry *conductor.LogBuffersRegistry,
	kind conductor.PendingKind,
	want int32,
) {
	tb.Helper()
	requireGoldenLogRefCountForTest(
		tb,
		registry,
		goldenPublicationLogFileNameForKind(kind),
		want,
	)
}

func requireGoldenLogRefCountForTest(
	tb testing.TB,
	registry *conductor.LogBuffersRegistry,
	logFileName string,
	want int32,
) {
	tb.Helper()
	require.Equal(
		tb,
		want,
		conductor.LogBuffersRegistryRefCountForTest(registry, logFileName),
	)
}

func goldenPublicationLogFileNameForKind(kind conductor.PendingKind) string {
	switch kind {
	case conductor.PendingKindAddPublication:
		return goldenPublicationReadyLogFileName
	case conductor.PendingKindAddExclusivePublication:
		return goldenExclusivePublicationReadyLogFileName
	default:
		panic("conductor_test: unsupported publication pending kind")
	}
}

type expirePendingAddCase struct {
	name          string
	correlationID int64
	setActive     bool
}

func requirePendingAddExpiredResultForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	correlationID int64,
) {
	tb.Helper()

	registration, ok := conductorInstance.PendingResult(correlationID)
	require.True(tb, ok)
	require.ErrorIs(tb, registration.Err, conductor.ErrAwaitTimeout)
}

func requirePendingAddExpiredForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	correlationID int64,
) {
	tb.Helper()

	requirePendingAddExpiredResultForTest(tb, conductorInstance, correlationID)
	done, _, pollErr := conductorInstance.PollPending(correlationID)
	require.True(tb, done)
	require.ErrorIs(tb, pollErr, conductor.ErrAwaitTimeout)
}

func setupInactiveExpireSubscriptionPendingForTest(
	tb testing.TB,
) (*conductor.Conductor, *recordingResourceLifecycle, *int64, int64) {
	tb.Helper()

	const startNs = int64(1_000)
	nanoNs := startNs
	conductorInstance, lifecycle := newConductorWithResourceLifecycleMutableNanoForTest(
		tb,
		0,
		&nanoNs,
	)
	require.NoError(
		tb,
		conductorInstance.RegisterPendingAdd(
			conductor.PendingKindAddSubscription,
			pendingTestOtherCorrelationID,
			pendingTestStreamID,
			pendingTestChannel,
			pendingAddExpireTimeout,
		),
	)
	require.Equal(tb, 1, conductorInstance.SubscriptionCountForTest())
	return conductorInstance, lifecycle, &nanoNs, startNs
}

func advancePendingAddExpireForTest(
	nanoNs *int64,
	startNs int64,
	conductorInstance *conductor.Conductor,
) {
	*nanoNs = startNs + int64(pendingAddExpireTimeout)
	_ = conductorInstance.DoWork()
}

func transmitLateSubscriptionReadyForTest(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	correlationID int64,
) {
	t.Helper()

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(
			goldenSubscriptionReadyWithCorrelation(correlationID),
		),
	)
	_ = conductorInstance.DoWork()
}

func runExpirePendingAddCases(
	t *testing.T,
	kind conductor.PendingKind,
	streamID int32,
	activeCorrelationID int64,
	assertPreExpire func(testing.TB, *conductor.Conductor),
	assertPostExpire func(testing.TB, *conductor.Conductor, *recordingResourceLifecycle),
) {
	t.Helper()

	cases := []expirePendingAddCase{
		{name: "inactiveExpire", correlationID: pendingTestOtherCorrelationID},
		{name: "activeExpire", correlationID: activeCorrelationID, setActive: true},
	}
	for _, testCase := range cases {
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
					kind,
					testCase.correlationID,
					streamID,
					pendingTestChannel,
					pendingAddExpireTimeout,
				),
			)
			if testCase.setActive {
				conductorInstance.SetActiveCorrelationID(testCase.correlationID)
			}
			assertPreExpire(t, conductorInstance)

			advancePendingAddExpireForTest(&nanoNs, startNs, conductorInstance)
			requirePendingAddExpiredForTest(t, conductorInstance, testCase.correlationID)

			assertPostExpire(t, conductorInstance, lifecycle)
		})
	}
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

	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddPublication,
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

	got, ok := conductorInstance.PublicationByRegID(goldenPublicationBuffersReadyEvent.CorrelationID)
	require.True(t, ok)
	require.Equal(t, goldenPublicationBuffersReadyEvent.CorrelationID, got.RegistrationID())
}

func TestConductor_handleExclusivePublicationReady_insertsExclusivePublication(t *testing.T) {
	lifecycle := &productionBindStubLifecycle{}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
	seedGoldenExclusivePublicationLogBuffers(t, conductorInstance)
	seedPublicationCountersForTest(t, conductorInstance, goldenExclusivePublicationBuffersReadyEvent)

	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddExclusivePublication,
	)
	conductorInstance.SetActiveCorrelationID(goldenExclusivePublicationReadyCorrelationID)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnExclusivePublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenExclusivePublicationBuffersReadyEvent),
	)
	_ = conductorInstance.DoWork()

	done, registration, err := conductorInstance.PollPending(
		goldenExclusivePublicationReadyCorrelationID,
	)
	require.True(t, done)
	require.NoError(t, err)
	ep, ok := registration.Publication.(*aeron.ExclusivePublication)
	require.True(t, ok)
	require.NotNil(t, ep)
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())

	got, ok := conductorInstance.PublicationByRegID(
		goldenExclusivePublicationBuffersReadyEvent.CorrelationID,
	)
	require.True(t, ok)
	gotEP, ok := got.(*aeron.ExclusivePublication)
	require.True(t, ok)
	require.Same(t, ep, gotEP)
	require.Equal(t, int64(0), gotEP.Position())
}

func TestConductor_handlePublicationReady_wrongPendingKind_noComplete(t *testing.T) {
	tests := []struct {
		name              string
		registerKind      conductor.PendingKind
		wireEvent         int
		makeEventBytes    func() []byte
		activeCorrelation int64
		pollCorrelationID int64
		wantLifecycleCall func(*recordingResourceLifecycle) int
	}{
		{
			name:         "exclusiveReadyWithAddPublicationPending",
			registerKind: conductor.PendingKindAddPublication,
			wireEvent:    conductor.OnExclusivePublicationReady,
			makeEventBytes: func() []byte {
				event := goldenExclusivePublicationBuffersReadyEvent
				event.CorrelationID = goldenPublicationReadyCorrelationID
				return conductor.PublicationBuffersReadyBytes(event)
			},
			activeCorrelation: goldenPublicationReadyCorrelationID,
			pollCorrelationID: goldenPublicationReadyCorrelationID,
			wantLifecycleCall: func(lifecycle *recordingResourceLifecycle) int {
				return lifecycle.newExclusivePublicationCalls
			},
		},
		{
			name:         "publicationReadyWithAddExclusivePublicationPending",
			registerKind: conductor.PendingKindAddExclusivePublication,
			wireEvent:    conductor.OnPublicationReady,
			makeEventBytes: func() []byte {
				event := goldenPublicationBuffersReadyEvent
				event.CorrelationID = goldenExclusivePublicationReadyCorrelationID
				return conductor.PublicationBuffersReadyBytes(event)
			},
			activeCorrelation: goldenExclusivePublicationReadyCorrelationID,
			pollCorrelationID: goldenExclusivePublicationReadyCorrelationID,
			wantLifecycleCall: func(lifecycle *recordingResourceLifecycle) int {
				return lifecycle.newPublicationCalls
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			lifecycle := &recordingResourceLifecycle{}
			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
			seedGoldenPublicationLogBuffers(t, conductorInstance)

			registerGoldenAddPendingForTest(t, conductorInstance, testCase.registerKind)
			conductorInstance.SetActiveCorrelationID(testCase.activeCorrelation)

			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				testCase.wireEvent,
				testCase.makeEventBytes(),
			)
			_ = conductorInstance.DoWork()

			done, _, err := conductorInstance.PollPending(testCase.pollCorrelationID)
			require.False(t, done)
			require.NoError(t, err)
			require.Equal(t, 0, conductorInstance.PublicationCountForTest())
			require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
			require.Zero(t, testCase.wantLifecycleCall(lifecycle))
		})
	}
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

	require.NoError(t, conductorInstance.PreInsertSubscriptionForTest(
		goldenSubscriptionReadyCorrelationID,
		pendingTestStreamID,
		pendingTestChannel,
	))
	require.NoError(t, conductorInstance.PreInsertSubscriptionForTest(
		goldenSubscriptionReadyCorrelationID,
		pendingTestStreamID,
		pendingTestChannel,
	))

	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.newPreSubscriptionCalls)
}

type trackableStubPublication struct {
	registrationID int64
	registryKey    string
	closed         bool
}

func (publication *trackableStubPublication) RegistrationID() int64 {
	return publication.registrationID
}

type trackableStubSubscription struct {
	registrationID int64
	streamID       int32
	closed         bool
}

func (subscription *trackableStubSubscription) RegistrationID() int64 {
	return subscription.registrationID
}

func (subscription *trackableStubSubscription) StreamID() int32 {
	return subscription.streamID
}

type bindFailureContractLifecycle struct {
	bindPublicationErr          error
	bindSubscriptionErr         error
	retirePublicationErr        error
	retireSubscriptionErr       error
	lastPublication             *trackableStubPublication
	lastSubscription            *trackableStubSubscription
	retirePublicationCalls      int
	retireSubscriptionCalls     int
	markPublicationClosedCalls  int
	markSubscriptionClosedCalls int
}

func (lifecycle *bindFailureContractLifecycle) trackablePublicationFromEvent(
	event conductor.PublicationBuffersReady,
) *trackableStubPublication {
	lifecycle.lastPublication = &trackableStubPublication{
		registrationID: event.CorrelationID,
		registryKey:    event.LogFileName,
	}
	return lifecycle.lastPublication
}

func (lifecycle *bindFailureContractLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	return lifecycle.trackablePublicationFromEvent(event), nil
}

func (lifecycle *bindFailureContractLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	return lifecycle.NewPublication(event, logBuffers, countersReader, channel)
}

func (lifecycle *bindFailureContractLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	_ string,
) conductor.SubscriptionHandle {
	lifecycle.lastSubscription = &trackableStubSubscription{
		registrationID: registrationID,
		streamID:       streamID,
	}
	return lifecycle.lastSubscription
}

func (lifecycle *bindFailureContractLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	_ conductor.SubscriptionReady,
) error {
	_, ok := sub.(*trackableStubSubscription)
	if !ok {
		return errors.New("conductor_test: unexpected subscription handle type")
	}
	return nil
}

func (lifecycle *bindFailureContractLifecycle) BindPublication(
	_ conductor.PublicationHandle,
	_ *conductor.Conductor,
) error {
	return lifecycle.bindPublicationErr
}

func (lifecycle *bindFailureContractLifecycle) BindSubscription(
	_ conductor.SubscriptionHandle,
	_ *conductor.Conductor,
) error {
	return lifecycle.bindSubscriptionErr
}

func (lifecycle *bindFailureContractLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	lifecycle.retirePublicationCalls++
	if lifecycle.retirePublicationErr != nil {
		return "", lifecycle.retirePublicationErr
	}
	trackablePub, ok := pub.(*trackableStubPublication)
	if !ok {
		return "", errors.New("conductor_test: unexpected publication handle type")
	}
	trackablePub.closed = true
	return trackablePub.registryKey, nil
}

func (lifecycle *bindFailureContractLifecycle) RetireSubscription(
	sub conductor.SubscriptionHandle,
) ([]string, error) {
	lifecycle.retireSubscriptionCalls++
	if lifecycle.retireSubscriptionErr != nil {
		return nil, lifecycle.retireSubscriptionErr
	}
	trackableSub, ok := sub.(*trackableStubSubscription)
	if !ok {
		return nil, errors.New("conductor_test: unexpected subscription handle type")
	}
	trackableSub.closed = true
	return nil, nil
}

func (lifecycle *bindFailureContractLifecycle) MarkPublicationClosed(
	pub conductor.PublicationHandle,
) {
	lifecycle.markPublicationClosedCalls++
	trackablePub, ok := pub.(*trackableStubPublication)
	if !ok {
		return
	}
	trackablePub.closed = true
}

func (lifecycle *bindFailureContractLifecycle) MarkSubscriptionClosed(
	sub conductor.SubscriptionHandle,
) {
	lifecycle.markSubscriptionClosedCalls++
	trackableSub, ok := sub.(*trackableStubSubscription)
	if !ok {
		return
	}
	trackableSub.closed = true
}

type productionBindStubLifecycle struct {
	bindPublicationErr       error
	bindSubscriptionErr      error
	lastPublication          *aeron.Publication
	lastExclusivePublication *aeron.ExclusivePublication
	lastSubscription         *aeron.Subscription
}

func (lifecycle *productionBindStubLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	pub, err := aeron.ConductorResourceLifecycle.NewPublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
	if err != nil {
		return nil, err
	}
	lifecycle.lastPublication, _ = pub.(*aeron.Publication)
	return pub, nil
}

func (lifecycle *productionBindStubLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	pub, err := aeron.ConductorResourceLifecycle.NewExclusivePublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
	if err != nil {
		return nil, err
	}
	lifecycle.lastExclusivePublication, _ = pub.(*aeron.ExclusivePublication)
	return pub, nil
}

func (lifecycle *productionBindStubLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) conductor.SubscriptionHandle {
	sub := aeron.ConductorResourceLifecycle.NewPreSubscription(
		registrationID,
		streamID,
		channel,
	)
	lifecycle.lastSubscription, _ = sub.(*aeron.Subscription)
	return sub
}

func (lifecycle *productionBindStubLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	return aeron.ConductorResourceLifecycle.ApplySubscriptionReady(sub, event)
}

func (lifecycle *productionBindStubLifecycle) RetireSubscription(
	sub conductor.SubscriptionHandle,
) ([]string, error) {
	return aeron.ConductorResourceLifecycle.RetireSubscription(sub)
}

func (lifecycle *productionBindStubLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	return aeron.ConductorResourceLifecycle.RetirePublication(pub)
}

func (lifecycle *productionBindStubLifecycle) BindPublication(
	pub conductor.PublicationHandle,
	clientConductor *conductor.Conductor,
) error {
	if lifecycle.bindPublicationErr != nil {
		return lifecycle.bindPublicationErr
	}
	return aeron.ConductorResourceLifecycle.BindPublication(pub, clientConductor)
}

func (lifecycle *productionBindStubLifecycle) BindSubscription(
	sub conductor.SubscriptionHandle,
	clientConductor *conductor.Conductor,
) error {
	if lifecycle.bindSubscriptionErr != nil {
		return lifecycle.bindSubscriptionErr
	}
	return aeron.ConductorResourceLifecycle.BindSubscription(sub, clientConductor)
}

func (lifecycle *productionBindStubLifecycle) MarkPublicationClosed(
	pub conductor.PublicationHandle,
) {
	aeron.ConductorResourceLifecycle.MarkPublicationClosed(pub)
}

func (lifecycle *productionBindStubLifecycle) MarkSubscriptionClosed(
	sub conductor.SubscriptionHandle,
) {
	aeron.ConductorResourceLifecycle.MarkSubscriptionClosed(sub)
}

type retireBeforeLogReleaseLifecycle struct {
	bindFailureContractLifecycle
	tb               testing.TB
	registry         *conductor.LogBuffersRegistry
	logFileName      string
	refCountAtRetire int32
}

func (lifecycle *retireBeforeLogReleaseLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	if lifecycle.registry != nil && lifecycle.logFileName != "" {
		require.Equal(
			lifecycle.tb,
			lifecycle.refCountAtRetire,
			conductor.LogBuffersRegistryRefCountForTest(
				lifecycle.registry,
				lifecycle.logFileName,
			),
			"log acquire must remain until retire runs during bind-failure rollback",
		)
	}
	return lifecycle.bindFailureContractLifecycle.RetirePublication(pub)
}

func registerSubscriptionAddPendingEntryForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()

	require.NoError(
		tb,
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
}

type bindFailureTeardownKind int

const (
	bindFailureTeardownPublicationReady bindFailureTeardownKind = iota
	bindFailureTeardownExclusivePublicationReady
	bindFailureTeardownSubscriptionPreInsert
	bindFailureTeardownSubscriptionReady
)

type publicationBindFailureCase struct {
	pendingKind      conductor.PendingKind
	correlationID    int64
	seed             func(tb testing.TB, conductorInstance *conductor.Conductor, useProduction bool)
	signalReady      func(conductorInstance *conductor.Conductor)
	assertProduction func(tb testing.TB, productionLifecycle *productionBindStubLifecycle)
}

func publicationBindFailureCaseForKind(
	kind bindFailureTeardownKind,
) publicationBindFailureCase {
	switch kind {
	case bindFailureTeardownPublicationReady:
		return publicationBindFailureCase{
			pendingKind:   conductor.PendingKindAddPublication,
			correlationID: goldenPublicationReadyCorrelationID,
			seed: func(tb testing.TB, conductorInstance *conductor.Conductor, useProduction bool) {
				seedGoldenPublicationLogBuffers(tb, conductorInstance)
				if useProduction {
					seedGoldenPublicationCountersForTest(tb, conductorInstance)
				}
			},
			signalReady: func(conductorInstance *conductor.Conductor) {
				conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
			},
			assertProduction: func(tb testing.TB, productionLifecycle *productionBindStubLifecycle) {
				require.NotNil(tb, productionLifecycle.lastPublication)
				require.True(tb, productionLifecycle.lastPublication.IsClosed())
			},
		}
	case bindFailureTeardownExclusivePublicationReady:
		return publicationBindFailureCase{
			pendingKind:   conductor.PendingKindAddExclusivePublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			seed: func(tb testing.TB, conductorInstance *conductor.Conductor, useProduction bool) {
				seedGoldenExclusivePublicationLogBuffers(tb, conductorInstance)
				if useProduction {
					seedPublicationCountersForTest(
						tb,
						conductorInstance,
						goldenExclusivePublicationBuffersReadyEvent,
					)
				}
			},
			signalReady: func(conductorInstance *conductor.Conductor) {
				conductorInstance.ExclusivePublicationReadyForTest(
					goldenExclusivePublicationBuffersReadyEvent,
				)
			},
			assertProduction: func(tb testing.TB, productionLifecycle *productionBindStubLifecycle) {
				require.NotNil(tb, productionLifecycle.lastExclusivePublication)
				require.True(tb, productionLifecycle.lastExclusivePublication.IsClosed())
			},
		}
	default:
		panic("conductor_test: unsupported publication bind failure kind")
	}
}

func TestConductor_bindFailure_teardownContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		kind                  bindFailureTeardownKind
		useProduction         bool
		retirePublicationErr  error
		retireSubscriptionErr error
	}{
		{name: "publication_ready_stub", kind: bindFailureTeardownPublicationReady},
		{
			name:          "publication_ready_production",
			kind:          bindFailureTeardownPublicationReady,
			useProduction: true,
		},
		{
			name:                 "publication_ready_retireFails_marksClosed",
			kind:                 bindFailureTeardownPublicationReady,
			retirePublicationErr: errRetirePublication,
		},
		{name: "exclusive_publication_ready_stub", kind: bindFailureTeardownExclusivePublicationReady},
		{
			name:          "exclusive_publication_ready_production",
			kind:          bindFailureTeardownExclusivePublicationReady,
			useProduction: true,
		},
		{
			name:                 "exclusive_publication_ready_retireFails_marksClosed",
			kind:                 bindFailureTeardownExclusivePublicationReady,
			retirePublicationErr: errRetirePublication,
		},
		{name: "subscription_preInsert_stub", kind: bindFailureTeardownSubscriptionPreInsert},
		{
			name:          "subscription_preInsert_production",
			kind:          bindFailureTeardownSubscriptionPreInsert,
			useProduction: true,
		},
		{name: "subscription_ready_stub", kind: bindFailureTeardownSubscriptionReady},
		{
			name:          "subscription_ready_production",
			kind:          bindFailureTeardownSubscriptionReady,
			useProduction: true,
		},
		{
			name:                  "subscription_preInsert_retireFails_marksClosed",
			kind:                  bindFailureTeardownSubscriptionPreInsert,
			retireSubscriptionErr: errRetireSubscription,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var (
				contractLifecycle   *bindFailureContractLifecycle
				productionLifecycle *productionBindStubLifecycle
				lifecycle           conductor.ResourceLifecycle
			)
			if testCase.useProduction {
				productionLifecycle = &productionBindStubLifecycle{
					bindPublicationErr:  errStubBindPublication,
					bindSubscriptionErr: errStubBindSubscription,
				}
				lifecycle = productionLifecycle
			} else {
				contractLifecycle = &bindFailureContractLifecycle{
					bindPublicationErr:    errStubBindPublication,
					bindSubscriptionErr:   errStubBindSubscription,
					retirePublicationErr:  testCase.retirePublicationErr,
					retireSubscriptionErr: testCase.retireSubscriptionErr,
				}
				lifecycle = contractLifecycle
			}

			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)

			switch testCase.kind {
			case bindFailureTeardownPublicationReady, bindFailureTeardownExclusivePublicationReady:
				pubCase := publicationBindFailureCaseForKind(testCase.kind)
				pubCase.seed(t, conductorInstance, testCase.useProduction)
				registerGoldenAddPendingForTest(t, conductorInstance, pubCase.pendingKind)
				pubCase.signalReady(conductorInstance)

				done, registration, err := conductorInstance.PollPending(pubCase.correlationID)
				require.True(t, done)
				require.ErrorIs(t, err, errStubBindPublication)
				require.False(t, registration.Ready)
				require.Equal(t, 0, conductorInstance.PublicationCountForTest())

				if testCase.useProduction {
					pubCase.assertProduction(t, productionLifecycle)
					return
				}

				require.NotNil(t, contractLifecycle.lastPublication)
				require.True(t, contractLifecycle.lastPublication.closed)
				require.Equal(t, 1, contractLifecycle.retirePublicationCalls)
				if testCase.retirePublicationErr != nil {
					require.Equal(t, 1, contractLifecycle.markPublicationClosedCalls)
				} else {
					require.Equal(t, 0, contractLifecycle.markPublicationClosedCalls)
					registry := conductor.LogBuffersRegistryForTest(conductorInstance)
					requireGoldenPublicationKindLogRefCountForTest(
						t,
						registry,
						pubCase.pendingKind,
						1,
					)
				}

			case bindFailureTeardownSubscriptionPreInsert:
				err := registerPendingAddWithCorrelationForTest(
					conductorInstance,
					conductor.PendingKindAddSubscription,
					goldenSubscriptionReadyCorrelationID,
					pendingTestStreamID,
				)
				require.ErrorIs(t, err, errStubBindSubscription)
				require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())

				if testCase.useProduction {
					require.NotNil(t, productionLifecycle.lastSubscription)
					require.True(t, productionLifecycle.lastSubscription.IsClosed())
					return
				}

				require.NotNil(t, contractLifecycle.lastSubscription)
				require.True(t, contractLifecycle.lastSubscription.closed)
				require.Equal(t, 1, contractLifecycle.retireSubscriptionCalls)
				if testCase.retireSubscriptionErr != nil {
					require.Equal(t, 1, contractLifecycle.markSubscriptionClosedCalls)
				} else {
					require.Equal(t, 0, contractLifecycle.markSubscriptionClosedCalls)
				}

			case bindFailureTeardownSubscriptionReady:
				registerSubscriptionAddPendingEntryForTest(t, conductorInstance)
				conductorInstance.SubscriptionReadyForTest(goldenSubscriptionReadyEvent)

				done, registration, err := conductorInstance.PollPending(
					goldenSubscriptionReadyCorrelationID,
				)
				require.True(t, done)
				require.ErrorIs(t, err, errStubBindSubscription)
				require.False(t, registration.Ready)
				require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())

				if testCase.useProduction {
					require.NotNil(t, productionLifecycle.lastSubscription)
					require.True(t, productionLifecycle.lastSubscription.IsClosed())
					return
				}

				require.NotNil(t, contractLifecycle.lastSubscription)
				require.True(t, contractLifecycle.lastSubscription.closed)
				require.Equal(t, 1, contractLifecycle.retireSubscriptionCalls)
				require.Equal(t, 0, contractLifecycle.markSubscriptionClosedCalls)
			}
		})
	}
}

func TestConductor_bindFailure_retireBeforeLogRelease(t *testing.T) {
	t.Parallel()

	lifecycle := &retireBeforeLogReleaseLifecycle{
		bindFailureContractLifecycle: bindFailureContractLifecycle{
			bindPublicationErr: errStubBindPublication,
		},
		tb:               t,
		logFileName:      goldenPublicationReadyLogFileName,
		refCountAtRetire: 2,
	}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	lifecycle.registry = registry
	seedGoldenPublicationLogBuffers(t, conductorInstance)
	registerGoldenAddPendingForTest(
		t,
		conductorInstance,
		conductor.PendingKindAddPublication,
	)
	conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)

	done, _, err := conductorInstance.PollPending(goldenPublicationReadyCorrelationID)
	require.True(t, done)
	require.ErrorIs(t, err, errStubBindPublication)
	require.NotNil(t, lifecycle.lastPublication)
	require.True(t, lifecycle.lastPublication.closed)
	requireGoldenPublicationKindLogRefCountForTest(
		t,
		registry,
		conductor.PendingKindAddPublication,
		1,
	)
}

func TestConductor_handlePublicationReady_failurePaths(t *testing.T) {
	tests := []struct {
		name          string
		pendingKind   conductor.PendingKind
		correlationID int64
		setBuildErr   func(*recordingResourceLifecycle, error)
		seedLog       func(testing.TB, *conductor.Conductor) // nil to skip seeding
		signalReady   func(*conductor.Conductor)
		buildErr      error
		wantErr       error
		wantErrString string
	}{
		{
			name:          "exclusive_acquireLogBuffersFailure",
			pendingKind:   conductor.PendingKindAddExclusivePublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			setBuildErr: func(l *recordingResourceLifecycle, err error) {
				l.newExclusivePublicationErr = err
			},
			signalReady: func(c *conductor.Conductor) {
				c.ExclusivePublicationReadyForTest(goldenExclusivePublicationBuffersReadyEvent)
			},
			wantErrString: "conductor: failed to map log buffer",
		},
		{
			name:          "exclusive_newExclusivePublicationFailure",
			pendingKind:   conductor.PendingKindAddExclusivePublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			setBuildErr: func(l *recordingResourceLifecycle, err error) {
				l.newExclusivePublicationErr = err
			},
			seedLog: seedGoldenExclusivePublicationLogBuffers,
			signalReady: func(c *conductor.Conductor) {
				c.ExclusivePublicationReadyForTest(goldenExclusivePublicationBuffersReadyEvent)
			},
			buildErr: errStubPublicationBuild,
			wantErr:  errStubPublicationBuild,
		},
		{
			name:          "publication_acquireLogBuffersFailure",
			pendingKind:   conductor.PendingKindAddPublication,
			correlationID: goldenPublicationReadyCorrelationID,
			setBuildErr: func(l *recordingResourceLifecycle, err error) {
				l.newPublicationErr = err
			},
			signalReady: func(c *conductor.Conductor) {
				c.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
			},
			wantErrString: "conductor: failed to map log buffer",
		},
		{
			name:          "publication_newPublicationFailure",
			pendingKind:   conductor.PendingKindAddPublication,
			correlationID: goldenPublicationReadyCorrelationID,
			setBuildErr: func(l *recordingResourceLifecycle, err error) {
				l.newPublicationErr = err
			},
			seedLog: seedGoldenPublicationLogBuffers,
			signalReady: func(c *conductor.Conductor) {
				c.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
			},
			buildErr: errStubPublicationBuild,
			wantErr:  errStubPublicationBuild,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			lifecycle := &recordingResourceLifecycle{}
			testCase.setBuildErr(lifecycle, testCase.buildErr)

			conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
			registry := conductor.LogBuffersRegistryForTest(conductorInstance)
			if testCase.seedLog != nil {
				testCase.seedLog(t, conductorInstance)
				requireGoldenLogRefCountForTest(
					t,
					registry,
					goldenPublicationLogFileNameForKind(testCase.pendingKind),
					1,
				)
			}

			registerGoldenAddPendingForTest(t, conductorInstance, testCase.pendingKind)
			conductorInstance.SetActiveCorrelationID(testCase.correlationID)
			testCase.signalReady(conductorInstance)

			done, registration, err := conductorInstance.PollPending(testCase.correlationID)
			require.True(t, done)
			if testCase.wantErr != nil {
				require.ErrorIs(t, err, testCase.wantErr)
			}
			if testCase.wantErrString != "" {
				require.EqualError(t, err, testCase.wantErrString)
			}
			require.False(t, registration.Ready)
			require.Equal(t, 0, conductorInstance.PublicationCountForTest())
			if testCase.seedLog != nil {
				requireGoldenLogRefCountForTest(
					t,
					registry,
					goldenPublicationLogFileNameForKind(testCase.pendingKind),
					1,
				)
			}
		})
	}
}

func TestConductor_handlePublicationReady_rejectsInvalidPublicationHandle(t *testing.T) {
	t.Parallel()

	kinds := []struct {
		name          string
		pendingKind   conductor.PendingKind
		correlationID int64
		seedLog       func(testing.TB, *conductor.Conductor)
		signalReady   func(*conductor.Conductor)
	}{
		{
			name:          "publication",
			pendingKind:   conductor.PendingKindAddPublication,
			correlationID: goldenPublicationReadyCorrelationID,
			seedLog:       seedGoldenPublicationLogBuffers,
			signalReady: func(c *conductor.Conductor) {
				c.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
			},
		},
		{
			name:          "exclusive",
			pendingKind:   conductor.PendingKindAddExclusivePublication,
			correlationID: goldenExclusivePublicationReadyCorrelationID,
			seedLog:       seedGoldenExclusivePublicationLogBuffers,
			signalReady: func(c *conductor.Conductor) {
				c.ExclusivePublicationReadyForTest(goldenExclusivePublicationBuffersReadyEvent)
			},
		},
	}

	const absentHandleErr = "conductor: resource lifecycle returned nil publication handle"

	handles := []struct {
		name         string
		newLifecycle func() conductor.ResourceLifecycle
		wantErr      func(correlationID int64) string
	}{
		{
			name:         "interfaceNil",
			newLifecycle: func() conductor.ResourceLifecycle { return &absentPublicationLifecycle{} },
			wantErr:      func(int64) string { return absentHandleErr },
		},
		{
			name: "typedNil",
			newLifecycle: func() conductor.ResourceLifecycle {
				return &absentPublicationLifecycle{returnTypedNil: true}
			},
			wantErr: func(int64) string { return absentHandleErr },
		},
		{
			name: "mismatchedRegistrationID",
			newLifecycle: func() conductor.ResourceLifecycle {
				return &recordingResourceLifecycle{registrationIDOffset: 1}
			},
			wantErr: func(correlationID int64) string {
				return fmt.Sprintf(
					"conductor: publication handle registration id %d, want %d",
					correlationID+1,
					correlationID,
				)
			},
		},
	}

	for _, kind := range kinds {
		for _, handle := range handles {
			t.Run(kind.name+"_"+handle.name, func(t *testing.T) {
				t.Parallel()

				conductorInstance := newConductorWithResourceLifecycleForTest(t, handle.newLifecycle())
				registry := conductor.LogBuffersRegistryForTest(conductorInstance)
				kind.seedLog(t, conductorInstance)
				requireGoldenPublicationKindLogRefCountForTest(t, registry, kind.pendingKind, 1)

				registerGoldenAddPendingForTest(t, conductorInstance, kind.pendingKind)
				conductorInstance.SetActiveCorrelationID(kind.correlationID)
				kind.signalReady(conductorInstance)

				done, registration, err := conductorInstance.PollPending(kind.correlationID)
				require.True(t, done)
				require.EqualError(t, err, handle.wantErr(kind.correlationID))
				require.False(t, registration.Ready)
				require.Equal(t, 0, conductorInstance.PublicationCountForTest())
				requireGoldenPublicationKindLogRefCountForTest(t, registry, kind.pendingKind, 1)
			})
		}
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

	runExpirePendingAddCases(
		t,
		conductor.PendingKindAddSubscription,
		pendingTestStreamID,
		goldenSubscriptionReadyCorrelationID,
		func(tb testing.TB, conductorInstance *conductor.Conductor) {
			require.Equal(tb, 1, conductorInstance.SubscriptionCountForTest())
		},
		func(
			tb testing.TB,
			conductorInstance *conductor.Conductor,
			lifecycle *recordingResourceLifecycle,
		) {
			require.Equal(tb, 0, conductorInstance.SubscriptionCountForTest())
			require.Equal(tb, 1, lifecycle.retireSubscriptionCalls)
		},
	)
}

func TestConductor_inactiveExpire_dropsLateSubscriptionReadyBeforePoll(t *testing.T) {
	conductorInstance, lifecycle, nanoNs, startNs := setupInactiveExpireSubscriptionPendingForTest(t)

	advancePendingAddExpireForTest(nanoNs, startNs, conductorInstance)

	requirePendingAddExpiredResultForTest(t, conductorInstance, pendingTestOtherCorrelationID)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 0, lifecycle.retireSubscriptionCalls)

	transmitLateSubscriptionReadyForTest(t, conductorInstance, pendingTestOtherCorrelationID)

	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 0, lifecycle.retireSubscriptionCalls)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())

	done, _, pollErr := conductorInstance.PollPending(pendingTestOtherCorrelationID)
	require.True(t, done)
	require.EqualError(t, pollErr, "conductor: await response timeout")

	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	_, ok := conductorInstance.PendingResult(pendingTestOtherCorrelationID)
	require.False(t, ok)
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
}

func TestConductor_inactiveExpire_dropsLateSubscriptionReady(t *testing.T) {
	conductorInstance, lifecycle, nanoNs, startNs := setupInactiveExpireSubscriptionPendingForTest(t)

	advancePendingAddExpireForTest(nanoNs, startNs, conductorInstance)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 0, lifecycle.retireSubscriptionCalls)
	requirePendingAddExpiredForTest(t, conductorInstance, pendingTestOtherCorrelationID)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)

	transmitLateSubscriptionReadyForTest(t, conductorInstance, pendingTestOtherCorrelationID)

	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	_, ok := conductorInstance.PendingResult(pendingTestOtherCorrelationID)
	require.False(t, ok)
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestConductor_handleSubscriptionReady_fallbackInsertWithoutPreInsert(t *testing.T) {
	lifecycle := &recordingResourceLifecycle{}
	conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)

	registerSubscriptionAddPendingEntryForTest(t, conductorInstance)
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

		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddPublication,
		)

		conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)
		require.Equal(t, int32(1), lifecycleWithDepth.depthDuringNewPublication)
	})

	t.Run("exclusivePublicationReady", func(t *testing.T) {
		conductorInstance, lifecycleWithDepth := newConductorWithLockDepthLifecycleForTest(t)
		seedGoldenExclusivePublicationLogBuffers(t, conductorInstance)

		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddExclusivePublication,
		)

		conductorInstance.ExclusivePublicationReadyForTest(
			goldenExclusivePublicationBuffersReadyEvent,
		)
		require.Equal(t, int32(1), lifecycleWithDepth.depthDuringNewExclusivePublication)
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

		require.NoError(t, conductorInstance.PreInsertSubscriptionForTest(
			goldenSubscriptionReadyCorrelationID,
			pendingTestStreamID,
			pendingTestChannel,
		))
		require.Equal(t, int32(1), lifecycleWithDepth.depthDuringNewPreSubscription)
	})
}

type lockDepthResourceLifecycle struct {
	*recordingResourceLifecycle
	conductor                          *conductor.Conductor
	depthDuringNewPublication          int32
	depthDuringNewExclusivePublication int32
	depthDuringApplySubscription       int32
	depthDuringNewPreSubscription      int32
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

func (lifecycle *lockDepthResourceLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	lifecycle.depthDuringNewExclusivePublication = lifecycle.conductor.ResourceWriteLockDepthForTest()
	return lifecycle.recordingResourceLifecycle.NewExclusivePublication(
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

func TestConductor_wrongKindReady_doesNotInsertResource(t *testing.T) {
	tests := []struct {
		name              string
		register          func(*conductor.Conductor) error
		wireEvent         int
		eventBytes        []byte
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
			require.True(t, done, nonAddWrongKindPollDoneReason)
			require.NoError(t, err)
			require.Equal(t, testCase.kind, registration.Kind)
			require.False(t, registration.Ready)
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
			goldenPublicationBuffersReadyEvent.CorrelationID,
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
			goldenPublicationBuffersReadyEvent.CorrelationID,
		)
		require.False(t, ok)
	})

	for _, expireCase := range []struct {
		kind                conductor.PendingKind
		streamID            int32
		activeCorrelationID int64
	}{
		{
			kind:                conductor.PendingKindAddPublication,
			streamID:            goldenPublicationBuffersReadyEvent.StreamID,
			activeCorrelationID: goldenPublicationReadyCorrelationID,
		},
		{
			kind:                conductor.PendingKindAddExclusivePublication,
			streamID:            goldenExclusivePublicationBuffersReadyEvent.StreamID,
			activeCorrelationID: goldenExclusivePublicationReadyCorrelationID,
		},
	} {
		runExpirePendingAddCases(
			t,
			expireCase.kind,
			expireCase.streamID,
			expireCase.activeCorrelationID,
			func(tb testing.TB, conductorInstance *conductor.Conductor) {
				require.Equal(tb, 0, conductorInstance.PublicationCountForTest())
			},
			func(
				tb testing.TB,
				conductorInstance *conductor.Conductor,
				lifecycle *recordingResourceLifecycle,
			) {
				require.Equal(tb, 0, conductorInstance.PublicationCountForTest())
				require.Equal(tb, 0, lifecycle.retirePublicationCalls)
			},
		)
	}
}

func realCreateMarkRecordingLifecycleWithRetireErrs(
	retirePublicationErr error,
	retireSubscriptionErr error,
) conductor.ResourceLifecycle {
	return &realCreateMarkRecordingLifecycle{
		realMarkRecordingLifecycle: realMarkRecordingLifecycle{
			recordingResourceLifecycle: recordingResourceLifecycle{
				retirePublicationErr:  retirePublicationErr,
				retireSubscriptionErr: retireSubscriptionErr,
			},
		},
	}
}

func realCreateMarkRecordingLifecycleWithRetirePublicationErr(
	errRetire error,
) conductor.ResourceLifecycle {
	return realCreateMarkRecordingLifecycleWithRetireErrs(errRetire, nil)
}

func realCreateMarkRecordingLifecycleWithRetireSubscriptionErr(
	errRetire error,
) conductor.ResourceLifecycle {
	return realCreateMarkRecordingLifecycleWithRetireErrs(nil, errRetire)
}

func TestConductor_rollbackAddPendingResources_retireFailure_marksClosedAndRemovesMap(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		lifecycle  func(errRetire error) conductor.ResourceLifecycle
		setup      func(testing.TB, *conductor.Conductor) any
		teardown   func(*conductor.Conductor)
		wantErr    error
		count      func(*conductor.Conductor) int
		isClosed   func(any) bool
		assertGone func(testing.TB, *conductor.Conductor)
	}{
		{
			name:      "publication",
			lifecycle: realCreateMarkRecordingLifecycleWithRetirePublicationErr,
			setup: func(tb testing.TB, conductorInstance *conductor.Conductor) any {
				seedGoldenPublicationCountersForTest(tb, conductorInstance)
				completeGoldenPublicationReadyForTest(tb, conductorInstance)
				pub, ok := conductorInstance.PublicationByRegID(
					goldenPublicationBuffersReadyEvent.CorrelationID,
				)
				require.True(tb, ok)
				return pub
			},
			teardown: (*conductor.Conductor).ClearPending,
			wantErr:  errRetirePublication,
			count: func(conductorInstance *conductor.Conductor) int {
				return conductorInstance.PublicationCountForTest()
			},
			isClosed: func(handle any) bool {
				return handle.(*aeron.Publication).IsClosed()
			},
			assertGone: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				_, ok := conductorInstance.PublicationByRegID(
					goldenPublicationBuffersReadyEvent.CorrelationID,
				)
				require.False(tb, ok)
			},
		},
		{
			name:      "exclusivePublication",
			lifecycle: realCreateMarkRecordingLifecycleWithRetirePublicationErr,
			setup: func(tb testing.TB, conductorInstance *conductor.Conductor) any {
				completeGoldenExclusivePublicationReadyForTest(tb, conductorInstance)
				pub, ok := conductorInstance.PublicationByRegID(
					goldenExclusivePublicationBuffersReadyEvent.CorrelationID,
				)
				require.True(tb, ok)
				return pub
			},
			teardown: (*conductor.Conductor).ClearPending,
			wantErr:  errRetirePublication,
			count: func(conductorInstance *conductor.Conductor) int {
				return conductorInstance.PublicationCountForTest()
			},
			isClosed: func(handle any) bool {
				return handle.(*aeron.ExclusivePublication).IsClosed()
			},
			assertGone: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				_, ok := conductorInstance.PublicationByRegID(
					goldenExclusivePublicationBuffersReadyEvent.CorrelationID,
				)
				require.False(tb, ok)
			},
		},
		{
			name:      "subscription",
			lifecycle: realCreateMarkRecordingLifecycleWithRetireSubscriptionErr,
			setup: func(tb testing.TB, conductorInstance *conductor.Conductor) any {
				registerGoldenAddPendingForTest(
					tb,
					conductorInstance,
					conductor.PendingKindAddSubscription,
				)
				sub, ok := conductorInstance.SubscriptionByRegID(
					goldenSubscriptionReadyCorrelationID,
				)
				require.True(tb, ok)
				return sub
			},
			teardown: func(conductorInstance *conductor.Conductor) {
				conductorInstance.CancelPending(goldenSubscriptionReadyCorrelationID)
			},
			wantErr: errRetireSubscription,
			count: func(conductorInstance *conductor.Conductor) int {
				return conductorInstance.SubscriptionCountForTest()
			},
			isClosed: func(handle any) bool {
				return handle.(*aeron.Subscription).IsClosed()
			},
			assertGone: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				_, ok := conductorInstance.SubscriptionByRegID(
					goldenSubscriptionReadyCorrelationID,
				)
				require.False(tb, ok)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			errorHandler, gotErrors := collectConductorErrors()
			config := conductorTestConfig(t, errorHandler, fixedClocks{})
			config.ResourceLifecycle = testCase.lifecycle(testCase.wantErr)
			conductorInstance := newConductorForTest(t, config)

			handle := testCase.setup(t, conductorInstance)
			testCase.teardown(conductorInstance)

			require.Len(t, *gotErrors, 1)
			require.ErrorIs(t, (*gotErrors)[0], testCase.wantErr)
			require.Equal(t, 0, testCase.count(conductorInstance))
			require.True(t, testCase.isClosed(handle))
			testCase.assertGone(t, conductorInstance)
		})
	}
}

func TestConductor_removeResource_retireFailureReportsAndRemovesMap(t *testing.T) {
	tests := []struct {
		name          string
		lifecycle     *recordingResourceLifecycle
		setup         func(testing.TB, *conductor.Conductor)
		teardown      func(*conductor.Conductor)
		wantErr       error
		assertRemoved func(testing.TB, *conductor.Conductor)
	}{
		{
			name:      "publication",
			lifecycle: &recordingResourceLifecycle{retirePublicationErr: errRetirePublication},
			setup: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				completeGoldenPublicationReadyForTest(tb, conductorInstance)
				require.Equal(tb, 1, conductorInstance.PublicationCountForTest())
			},
			teardown: (*conductor.Conductor).ClearPending,
			wantErr:  errRetirePublication,
			assertRemoved: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				require.Equal(tb, 0, conductorInstance.PublicationCountForTest())
				_, ok := conductorInstance.PublicationByRegID(
					goldenPublicationBuffersReadyEvent.CorrelationID,
				)
				require.False(tb, ok)
			},
		},
		{
			name:      "subscription",
			lifecycle: &recordingResourceLifecycle{retireSubscriptionErr: errRetireSubscription},
			setup: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				registerGoldenAddPendingForTest(
					tb,
					conductorInstance,
					conductor.PendingKindAddSubscription,
				)
				require.Equal(tb, 1, conductorInstance.SubscriptionCountForTest())
			},
			teardown: func(conductorInstance *conductor.Conductor) {
				conductorInstance.CancelPending(goldenSubscriptionReadyCorrelationID)
			},
			wantErr: errRetireSubscription,
			assertRemoved: func(tb testing.TB, conductorInstance *conductor.Conductor) {
				require.Equal(tb, 0, conductorInstance.SubscriptionCountForTest())
				_, ok := conductorInstance.SubscriptionByRegID(goldenSubscriptionReadyCorrelationID)
				require.False(tb, ok)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			errorHandler, gotErrors := collectConductorErrors()
			config := conductorTestConfig(t, errorHandler, fixedClocks{})
			config.ResourceLifecycle = testCase.lifecycle
			conductorInstance := newConductorForTest(t, config)

			testCase.setup(t, conductorInstance)
			testCase.teardown(conductorInstance)

			require.Len(t, *gotErrors, 1)
			require.ErrorIs(t, (*gotErrors)[0], testCase.wantErr)
			testCase.assertRemoved(t, conductorInstance)
		})
	}
}

func seedGoldenPublicationLogBuffers(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()
	seedLogBuffersForTest(tb, conductorInstance, goldenPublicationReadyLogFileName)
}

func seedGoldenExclusivePublicationLogBuffers(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()
	seedSyntheticLogBuffersForTest(
		tb,
		conductorInstance,
		goldenExclusivePublicationReadyLogFileName,
		true,
	)
}

func seedPublicationCountersForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	event conductor.PublicationBuffersReady,
) {
	tb.Helper()

	const counterCount = 12
	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)
	cnc.WriteCounterRecord(
		meta,
		values,
		event.PublicationLimitCounterID,
		cnc.CounterRecordAllocated,
		cnc.DriverPublisherLimitTypeID,
		event.RegistrationID,
		1<<40,
	)
	require.NoError(tb, cnc.WriteChannelStatusCounterRecord(
		meta,
		values,
		event.ChannelStatusCounterID,
		cnc.CounterRecordAllocated,
		cnc.DriverSendChannelStatusTypeID,
		pendingTestChannel,
		event.RegistrationID,
		1,
	))
	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	conductorInstance.SetCountersReaderForTest(countersReader)
}

func seedGoldenPublicationCountersForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()

	seedPublicationCountersForTest(
		tb,
		conductorInstance,
		goldenPublicationBuffersReadyEvent,
	)
}
