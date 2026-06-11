package conductor_test

import (
	"errors"
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
	noopCloseBindLifecycle

	newPublicationErr        error
	bindPublicationErr       error
	bindSubscriptionErr      error
	applySubscriptionErr     error
	retireSubscriptionErr    error
	retirePublicationErr     error
	newPublicationCalls      int
	newPreSubscriptionCalls  int
	bindPublicationCalls     int
	bindSubscriptionCalls    int
	applySubscriptionCalls   int
	retireSubscriptionCalls  int
	retirePublicationCalls   int
	lastAppliedChannelStatus int32
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

func requireGoldenPublicationLogRefCountForTest(
	tb testing.TB,
	registry *conductor.LogBuffersRegistry,
	want int32,
) {
	tb.Helper()
	require.Equal(
		tb,
		want,
		conductor.LogBuffersRegistryRefCountForTest(
			registry,
			goldenPublicationReadyLogFileName,
		),
	)
}

type expirePendingAddCase struct {
	name          string
	correlationID int64
	setActive     bool
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

			nanoNs = startNs + int64(pendingAddExpireTimeout)
			_ = conductorInstance.DoWork()
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

	got, ok := conductorInstance.PublicationByRegID(goldenPublicationBuffersReadyEvent.RegistrationID)
	require.True(t, ok)
	require.Equal(t, goldenPublicationBuffersReadyEvent.RegistrationID, got.RegistrationID())
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

func (lifecycle *bindFailureContractLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ string,
) (conductor.PublicationHandle, error) {
	lifecycle.lastPublication = &trackableStubPublication{
		registrationID: event.RegistrationID,
	}
	return lifecycle.lastPublication, nil
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
	return goldenPublicationReadyLogFileName, nil
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
	bindPublicationErr  error
	bindSubscriptionErr error
	lastPublication     *aeron.Publication
	lastSubscription    *aeron.Subscription
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
	bindFailureTeardownSubscriptionPreInsert
	bindFailureTeardownSubscriptionReady
)

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
			case bindFailureTeardownPublicationReady:
				seedGoldenPublicationLogBuffers(t, conductorInstance)
				if testCase.useProduction {
					seedGoldenPublicationCountersForTest(t, conductorInstance)
				}
				registerGoldenAddPendingForTest(
					t,
					conductorInstance,
					conductor.PendingKindAddPublication,
				)
				conductorInstance.PublicationReadyForTest(goldenPublicationBuffersReadyEvent)

				done, _, err := conductorInstance.PollPending(
					goldenPublicationReadyCorrelationID,
				)
				require.True(t, done)
				require.ErrorIs(t, err, errStubBindPublication)
				require.Equal(t, 0, conductorInstance.PublicationCountForTest())

				if testCase.useProduction {
					require.NotNil(t, productionLifecycle.lastPublication)
					require.True(t, productionLifecycle.lastPublication.IsClosed())
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
					requireGoldenPublicationLogRefCountForTest(t, registry, 1)
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

				done, _, err := conductorInstance.PollPending(
					goldenSubscriptionReadyCorrelationID,
				)
				require.True(t, done)
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
	requireGoldenPublicationLogRefCountForTest(t, registry, 1)
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
			if testCase.seedLog {
				seedGoldenPublicationLogBuffers(t, conductorInstance)
				requireGoldenPublicationLogRefCountForTest(t, registry, 1)
			}

			registerGoldenAddPendingForTest(
				t,
				conductorInstance,
				conductor.PendingKindAddPublication,
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
				requireGoldenPublicationLogRefCountForTest(t, registry, 1)
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
			pendingAddExpireTimeout,
		),
	)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	nanoNs = startNs + int64(pendingAddExpireTimeout)
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

		registerGoldenAddPendingForTest(
			t,
			conductorInstance,
			conductor.PendingKindAddPublication,
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

	runExpirePendingAddCases(
		t,
		conductor.PendingKindAddPublication,
		goldenPublicationBuffersReadyEvent.StreamID,
		goldenPublicationReadyCorrelationID,
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
			name: "publication",
			lifecycle: func(errRetire error) conductor.ResourceLifecycle {
				return &realCreateMarkRecordingLifecycle{
					realMarkRecordingLifecycle: realMarkRecordingLifecycle{
						recordingResourceLifecycle: recordingResourceLifecycle{
							retirePublicationErr: errRetire,
						},
					},
				}
			},
			setup: func(tb testing.TB, conductorInstance *conductor.Conductor) any {
				seedGoldenPublicationCountersForTest(tb, conductorInstance)
				completeGoldenPublicationReadyForTest(tb, conductorInstance)
				pub, ok := conductorInstance.PublicationByRegID(
					goldenPublicationBuffersReadyEvent.RegistrationID,
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
					goldenPublicationBuffersReadyEvent.RegistrationID,
				)
				require.False(tb, ok)
			},
		},
		{
			name: "subscription",
			lifecycle: func(errRetire error) conductor.ResourceLifecycle {
				return &realCreateMarkRecordingLifecycle{
					realMarkRecordingLifecycle: realMarkRecordingLifecycle{
						recordingResourceLifecycle: recordingResourceLifecycle{
							retireSubscriptionErr: errRetire,
						},
					},
				}
			},
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
					goldenPublicationBuffersReadyEvent.RegistrationID,
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
