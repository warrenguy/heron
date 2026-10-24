package conductor_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/ringbuffer"
)

type lingerPublicationLifecycle struct {
	recordingResourceLifecycle
	registryKey string
}

func (lifecycle *lingerPublicationLifecycle) RetirePublication(
	_ conductor.PublicationHandle,
) (string, error) {
	lifecycle.retirePublicationCalls++
	if lifecycle.retirePublicationErr != nil {
		return "", lifecycle.retirePublicationErr
	}
	return lifecycle.registryKey, nil
}

type lingerSubscriptionLifecycle struct {
	recordingResourceLifecycle
	registryKeys []string
}

func (lifecycle *lingerSubscriptionLifecycle) RetireSubscription(
	_ conductor.SubscriptionHandle,
) ([]string, error) {
	lifecycle.retireSubscriptionCalls++
	if lifecycle.retireSubscriptionErr != nil {
		return nil, lifecycle.retireSubscriptionErr
	}
	return lifecycle.registryKeys, nil
}

type retireBeforeRemoveLifecycle struct {
	recordingResourceLifecycle
	tb   testing.TB
	ring *ringbuffer.ManyToOneRingBuffer
}

func (lifecycle *retireBeforeRemoveLifecycle) assertNoRemoveOffered(message string) {
	require.Empty(
		lifecycle.tb,
		testutil.CommittedRecordTypeIDs(lifecycle.ring),
		message,
	)
}

func (lifecycle *retireBeforeRemoveLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	lifecycle.assertNoRemoveOffered("REMOVE_PUBLICATION must not be offered before retire")
	return lifecycle.recordingResourceLifecycle.RetirePublication(pub)
}

func (lifecycle *retireBeforeRemoveLifecycle) RetireSubscription(
	sub conductor.SubscriptionHandle,
) ([]string, error) {
	lifecycle.assertNoRemoveOffered("REMOVE_SUBSCRIPTION must not be offered before retire")
	return lifecycle.recordingResourceLifecycle.RetireSubscription(sub)
}

type closeOrderResourceLifecycle struct {
	recordingResourceLifecycle
	closeOrder []string
}

func (lifecycle *closeOrderResourceLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	lifecycle.closeOrder = append(lifecycle.closeOrder, "publication")
	return lifecycle.recordingResourceLifecycle.RetirePublication(pub)
}

func (lifecycle *closeOrderResourceLifecycle) RetireSubscription(
	sub conductor.SubscriptionHandle,
) ([]string, error) {
	lifecycle.closeOrder = append(lifecycle.closeOrder, "subscription")
	return lifecycle.recordingResourceLifecycle.RetireSubscription(sub)
}

type realMarkRecordingLifecycle struct {
	recordingResourceLifecycle
}

func (lifecycle *realMarkRecordingLifecycle) MarkPublicationClosed(
	pub conductor.PublicationHandle,
) {
	aeron.ConductorResourceLifecycle.MarkPublicationClosed(pub)
}

func (lifecycle *realMarkRecordingLifecycle) MarkSubscriptionClosed(
	sub conductor.SubscriptionHandle,
) {
	aeron.ConductorResourceLifecycle.MarkSubscriptionClosed(sub)
}

type realCreateMarkRecordingLifecycle struct {
	realMarkRecordingLifecycle
}

func (lifecycle *realCreateMarkRecordingLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	return aeron.ConductorResourceLifecycle.NewPublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
}

func (lifecycle *realCreateMarkRecordingLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	return aeron.ConductorResourceLifecycle.NewExclusivePublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
}

func (lifecycle *realCreateMarkRecordingLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) conductor.SubscriptionHandle {
	return aeron.ConductorResourceLifecycle.NewPreSubscription(
		registrationID,
		streamID,
		channel,
	)
}

func (lifecycle *realCreateMarkRecordingLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	return aeron.ConductorResourceLifecycle.ApplySubscriptionReady(sub, event)
}

func (lifecycle *realCreateMarkRecordingLifecycle) BindPublication(
	pub conductor.PublicationHandle,
	clientConductor *conductor.Conductor,
) error {
	return aeron.ConductorResourceLifecycle.BindPublication(pub, clientConductor)
}

func (lifecycle *realCreateMarkRecordingLifecycle) BindSubscription(
	sub conductor.SubscriptionHandle,
	clientConductor *conductor.Conductor,
) error {
	return aeron.ConductorResourceLifecycle.BindSubscription(sub, clientConductor)
}

func newConductorWithCloseLingerForTest(
	tb testing.TB,
	lingerNs int64,
	lifecycle conductor.ResourceLifecycle,
	errorHandlers ...func(error),
) (*conductor.Conductor, *int64) {
	tb.Helper()

	errorHandler := func(error) {}
	if len(errorHandlers) > 0 {
		errorHandler = errorHandlers[0]
	}

	var nanoNs int64
	config := conductorTestConfigMutableNano(tb, errorHandler, resourceMapTestEpochMs, &nanoNs)
	config.CloseLingerNs = lingerNs
	config.ResourceLifecycle = lifecycle
	conductorInstance := newConductorForTest(tb, config)
	return conductorInstance, &nanoNs
}

func newConductorWithImageCloseLingerForTest(
	tb testing.TB,
	lingerNs int64,
	lifecycle *recordingImageLifecycle,
) (*conductor.Conductor, *int64) {
	tb.Helper()

	var nanoNs int64
	config := conductorTestConfigMutableNano(tb, func(error) {}, resourceMapTestEpochMs, &nanoNs)
	config.ImageLifecycle = lifecycle
	config.MediaDriverDir = "/"
	config.CloseLingerNs = lingerNs
	return newConductorForTest(tb, config), &nanoNs
}

func insertPublicationForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	pub conductor.PublicationHandle,
) {
	tb.Helper()

	conductorInstance.InsertPublicationForTest(pub.RegistrationID(), pub)
}

func insertRealPublicationForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) *aeron.Publication {
	tb.Helper()

	pub := aeron.NewTestPublicationForHooks(tb)
	insertPublicationForCloseTest(tb, conductorInstance, pub)
	return pub
}

func insertRealExclusivePublicationForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) *aeron.ExclusivePublication {
	tb.Helper()

	pub := aeron.NewTestExclusivePublicationForHooks(tb)
	insertPublicationForCloseTest(tb, conductorInstance, pub)
	return pub
}

func closePublicationForCloseTest(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	handle any,
) error {
	t.Helper()

	return conductorInstance.ClosePublication(handle.(conductor.PublicationHandle))
}

func closeSubscriptionForCloseTest(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	handle any,
) error {
	t.Helper()

	return conductorInstance.CloseSubscription(handle.(conductor.SubscriptionHandle))
}

func removePublicationForCloseTest(
	conductorInstance *conductor.Conductor,
	handle any,
) {
	conductorInstance.RemovePublicationForTest(
		handle.(conductor.PublicationHandle).RegistrationID(),
	)
}

func removeSubscriptionForCloseTest(
	conductorInstance *conductor.Conductor,
	handle any,
) {
	conductorInstance.RemoveSubscriptionForTest(
		handle.(conductor.SubscriptionHandle).RegistrationID(),
	)
}

func forceClosePublicationForCloseTest(
	conductorInstance *conductor.Conductor,
	handle any,
) {
	conductorInstance.ForceClosePublicationForTest(handle.(conductor.PublicationHandle))
}

func forceCloseSubscriptionForCloseTest(
	conductorInstance *conductor.Conductor,
	handle any,
) {
	conductorInstance.ForceCloseSubscriptionForTest(handle.(conductor.SubscriptionHandle))
}

func insertFileBackedRealPublicationForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	logPath string,
) *aeron.Publication {
	tb.Helper()

	pub := aeron.NewFileBackedPublicationForHooks(tb, logPath)

	_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(tb, err)

	insertPublicationForCloseTest(tb, conductorInstance, pub)
	return pub
}

func insertRealSubscriptionForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) *aeron.Subscription {
	tb.Helper()

	sub := aeron.NewTestSubscriptionForHooks(tb)
	conductorInstance.InsertSubscriptionForTest(sub.RegistrationID(), sub)
	return sub
}

func seedLivePublicationAndSubscription(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) (*aeron.Publication, *aeron.Subscription) {
	tb.Helper()

	pub := insertRealPublicationForCloseTest(tb, conductorInstance)
	sub := insertRealSubscriptionForCloseTest(tb, conductorInstance)
	return pub, sub
}

func seedFullDriverRingForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
) {
	tb.Helper()

	ring := conductorInstance.DriverProxyForTest().Ring()
	buffer := ring.Buffer()
	capacity := ring.Capacity()
	head := int64(0)
	tail := int64(capacity)
	buffer.PutInt64Release(ringbuffer.HeadIndex(capacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(capacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(capacity), head)
}

func assertDrainLingerRetainedThenSwept(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	nanoNs *int64,
	startNs, lingerNs int64,
) {
	t.Helper()

	*nanoNs = startNs + lingerNs - 1
	conductorInstance.DrainLingeringLogBuffers()
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	*nanoNs = startNs + lingerNs
	require.Equal(t, 1, conductorInstance.DrainLingeringLogBuffers())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func assertMapsEmptyAndRing(
	tb testutil.TB,
	conductorInstance *conductor.Conductor,
	wantTypeIDs []int,
	msg string,
) {
	tb.Helper()

	require.Equal(
		tb,
		0,
		conductorInstance.PublicationCountForTest(),
		"publication map must be empty",
	)
	require.Equal(
		tb,
		0,
		conductorInstance.SubscriptionCountForTest(),
		"subscription map must be empty",
	)
	require.Equal(
		tb,
		wantTypeIDs,
		testutil.CommittedRecordTypeIDs(conductorInstance.DriverProxyForTest().Ring()),
		msg,
	)
}

func assertForceClosedResources(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	pub *aeron.Publication,
	sub *aeron.Subscription,
	beforeTypeIDs []int,
) {
	tb.Helper()

	assertMapsEmptyAndRing(
		tb,
		conductorInstance,
		beforeTypeIDs,
		"force close must not offer REMOVE",
	)
	require.True(tb, pub.IsClosed(), "publication must report closed")
	require.True(tb, sub.IsClosed(), "subscription must report closed")
}

func TestConductor_CloseResource_retireBeforeRemoveOffer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		removeTypeID int
		setup        func(conductorInstance *conductor.Conductor)
		close        func(t *testing.T, conductorInstance *conductor.Conductor)
	}{
		{
			name:         "publication",
			removeTypeID: conductor.RemovePublication,
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
			},
			close: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.NoError(t, conductorInstance.ClosePublication(stubPublication{registrationID: 42}))
			},
		},
		{
			name:         "subscription",
			removeTypeID: conductor.RemoveSubscription,
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
			},
			close: func(t *testing.T, conductorInstance *conductor.Conductor) {
				sub := &stubResourceSubscription{registrationID: 22}
				require.NoError(t, conductorInstance.CloseSubscription(sub))
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			lifecycle := &retireBeforeRemoveLifecycle{}
			conductorInstance, _ := newConductorWithCloseLingerForTest(t, 0, lifecycle)
			ring := conductorInstance.DriverProxyForTest().Ring()
			lifecycle.tb = t
			lifecycle.ring = ring

			testCase.setup(conductorInstance)
			testCase.close(t, conductorInstance)

			require.Equal(t, []int{testCase.removeTypeID}, testutil.CommittedRecordTypeIDs(ring))
		})
	}
}

func TestConductor_ClosePublication_realPublication_positiveLinger_retainsMmap(t *testing.T) {
	t.Parallel()

	const startNs int64 = 21_000_000_000
	const lingerNs int64 = int64(time.Second)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "real-pub-close.log")

	conductorInstance, nanoNs := newConductorWithCloseLingerForTest(
		t,
		lingerNs,
		aeron.ConductorResourceLifecycle,
	)
	*nanoNs = startNs

	pub := insertFileBackedRealPublicationForCloseTest(t, conductorInstance, logPath)
	require.NoError(t, conductorInstance.ClosePublication(pub))

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)
	require.Equal(t, 0, conductorInstance.PublicationCountForTest())
	require.True(t, pub.IsClosed())

	assertDrainLingerRetainedThenSwept(
		t, conductorInstance, registry, logPath, nanoNs, startNs, lingerNs,
	)
}

func TestConductor_ClosePublication_positiveLinger_retainsMmap(t *testing.T) {
	t.Parallel()

	const startNs int64 = 20_000_000_000
	const lingerNs int64 = int64(time.Second)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "pub-close.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	lifecycle := &lingerPublicationLifecycle{registryKey: logPath}
	conductorInstance, nanoNs := newConductorWithCloseLingerForTest(t, lingerNs, lifecycle)
	*nanoNs = startNs

	_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)

	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	require.NoError(t, conductorInstance.ClosePublication(stubPublication{registrationID: 42}))

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)
	require.Equal(t, 0, conductorInstance.PublicationCountForTest())
	require.Equal(t, 1, lifecycle.retirePublicationCalls)

	assertDrainLingerRetainedThenSwept(
		t, conductorInstance, registry, logPath, nanoNs, startNs, lingerNs,
	)
}

func TestConductor_CloseSubscription_positiveLinger_retainsMmap(t *testing.T) {
	t.Parallel()

	const startNs int64 = 22_000_000_000
	const lingerNs int64 = int64(time.Second)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "sub-close.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	lifecycle := &lingerSubscriptionLifecycle{registryKeys: []string{logPath}}
	conductorInstance, nanoNs := newConductorWithCloseLingerForTest(t, lingerNs, lifecycle)
	*nanoNs = startNs

	_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)
	conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
	sub := &stubResourceSubscription{registrationID: 22}
	require.NoError(t, conductorInstance.CloseSubscription(sub))

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	require.Equal(t, 1, lifecycle.retireSubscriptionCalls)

	assertDrainLingerRetainedThenSwept(
		t, conductorInstance, registry, logPath, nanoNs, startNs, lingerNs,
	)
}

func TestConductor_CloseAllResources_publicationsBeforeSubscriptions(t *testing.T) {
	t.Parallel()

	lifecycle := &closeOrderResourceLifecycle{}
	conductorInstance, _ := newConductorWithCloseLingerForTest(t, 0, lifecycle)

	conductorInstance.InsertPublicationForTest(11, stubPublication{registrationID: 11})
	conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})

	conductorInstance.CloseAllResources()

	require.Equal(
		t,
		[]string{"publication", "subscription"},
		lifecycle.closeOrder,
	)
	require.Equal(t, 0, conductorInstance.PublicationCountForTest())
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
}

func TestConductor_CloseAllResources_removeOffersBeforeClientClose(t *testing.T) {
	t.Parallel()

	lifecycle := &recordingResourceLifecycle{}
	conductorInstance, _ := newConductorWithCloseLingerForTest(t, 0, lifecycle)
	ring := conductorInstance.DriverProxyForTest().Ring()

	conductorInstance.InsertPublicationForTest(11, stubPublication{registrationID: 11})
	conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
	conductorInstance.CloseAllResources()

	typeIDs := testutil.CommittedRecordTypeIDs(ring)
	require.Equal(
		t,
		[]int{conductor.RemovePublication, conductor.RemoveSubscription},
		typeIDs,
	)
}

func TestConductor_forceCloseResources_retiresWithoutRemoveOffer(t *testing.T) {
	t.Parallel()

	const startNs int64 = 23_000_000_000
	const lingerNs int64 = int64(time.Second)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "force-close.log")

	conductorInstance, nanoNs := newConductorWithCloseLingerForTest(
		t,
		lingerNs,
		aeron.ConductorResourceLifecycle,
	)
	*nanoNs = startNs

	pub := insertFileBackedRealPublicationForCloseTest(t, conductorInstance, logPath)
	sub := insertRealSubscriptionForCloseTest(t, conductorInstance)
	ring := conductorInstance.DriverProxyForTest().Ring()
	beforeTypeIDs := testutil.CommittedRecordTypeIDs(ring)

	require.False(t, pub.IsClosed())
	require.False(t, sub.IsClosed())
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	conductorInstance.ForceCloseResourcesForTest()
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)

	conductorInstance.ForceCloseResourcesForTest()
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)

	conductorInstance.CloseAllResources()
	assertForceClosedResources(t, conductorInstance, pub, sub, beforeTypeIDs)
}

func TestConductor_forceCloseResources_nilLifecycle_removesMapsWithoutRemoveOffer(t *testing.T) {
	t.Parallel()

	conductorInstance := newConductorForTest(t, conductorTestConfig(t, func(error) {}, fixedClocks{}))
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
	beforeTypeIDs := testutil.CommittedRecordTypeIDs(conductorInstance.DriverProxyForTest().Ring())

	require.Equal(t, 1, conductorInstance.PublicationCountForTest())
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	conductorInstance.ForceCloseResourcesForTest()
	assertMapsEmptyAndRing(
		t,
		conductorInstance,
		beforeTypeIDs,
		"force close must not offer REMOVE",
	)

	conductorInstance.ForceCloseResourcesForTest()
	assertMapsEmptyAndRing(
		t,
		conductorInstance,
		beforeTypeIDs,
		"second force close must not offer REMOVE",
	)

	conductorInstance.CloseAllResources()
	assertMapsEmptyAndRing(
		t,
		conductorInstance,
		beforeTypeIDs,
		"CloseAllResources after force close must not offer REMOVE",
	)
}

// resourceMapMissCase drives the map-miss tests for both the public Close*
// path and the force-close path.
type resourceMapMissCase struct {
	name       string
	insert     func(t *testing.T, conductorInstance *conductor.Conductor) any
	remove     func(conductorInstance *conductor.Conductor, handle any)
	close      func(t *testing.T, conductorInstance *conductor.Conductor, handle any) error
	forceClose func(conductorInstance *conductor.Conductor, handle any)
	count      func(conductorInstance *conductor.Conductor) int
	closed     func(handle any) bool
}

func resourceMapMissCases() []resourceMapMissCase {
	return []resourceMapMissCase{
		{
			name: "publication",
			insert: func(t *testing.T, conductorInstance *conductor.Conductor) any {
				return insertRealPublicationForCloseTest(t, conductorInstance)
			},
			remove:     removePublicationForCloseTest,
			close:      closePublicationForCloseTest,
			forceClose: forceClosePublicationForCloseTest,
			count:      (*conductor.Conductor).PublicationCountForTest,
			closed: func(handle any) bool {
				return handle.(*aeron.Publication).IsClosed()
			},
		},
		{
			name: "exclusivePublication",
			insert: func(t *testing.T, conductorInstance *conductor.Conductor) any {
				return insertRealExclusivePublicationForCloseTest(t, conductorInstance)
			},
			remove:     removePublicationForCloseTest,
			close:      closePublicationForCloseTest,
			forceClose: forceClosePublicationForCloseTest,
			count:      (*conductor.Conductor).PublicationCountForTest,
			closed: func(handle any) bool {
				return handle.(*aeron.ExclusivePublication).IsClosed()
			},
		},
		{
			name: "subscription",
			insert: func(t *testing.T, conductorInstance *conductor.Conductor) any {
				return insertRealSubscriptionForCloseTest(t, conductorInstance)
			},
			remove:     removeSubscriptionForCloseTest,
			close:      closeSubscriptionForCloseTest,
			forceClose: forceCloseSubscriptionForCloseTest,
			count:      (*conductor.Conductor).SubscriptionCountForTest,
			closed: func(handle any) bool {
				return handle.(*aeron.Subscription).IsClosed()
			},
		},
	}
}

func TestConductor_forceCloseResources_mapMiss_marksClosed(t *testing.T) {
	t.Parallel()

	for _, testCase := range resourceMapMissCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			conductorInstance := newConductorWithResourceLifecycleForTest(
				t,
				aeron.ConductorResourceLifecycle,
			)
			beforeTypeIDs := testutil.CommittedRecordTypeIDs(
				conductorInstance.DriverProxyForTest().Ring(),
			)

			handle := testCase.insert(t, conductorInstance)
			testCase.remove(conductorInstance, handle)
			require.False(t, testCase.closed(handle))
			require.Equal(t, 0, testCase.count(conductorInstance))

			testCase.forceClose(conductorInstance, handle)
			require.True(t, testCase.closed(handle))
			require.Equal(t, 0, testCase.count(conductorInstance))
			require.Equal(
				t,
				beforeTypeIDs,
				testutil.CommittedRecordTypeIDs(conductorInstance.DriverProxyForTest().Ring()),
				"map-miss force close must not offer REMOVE",
			)
		})
	}
}

func TestConductor_CloseResource_zeroLinger_immediateUnmap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		logFile   string
		lifecycle func(logPath string) conductor.ResourceLifecycle
		register  func(conductorInstance *conductor.Conductor)
		close     func(t *testing.T, conductorInstance *conductor.Conductor)
	}{
		{
			name:    "publication",
			logFile: "pub-zero-linger.log",
			lifecycle: func(logPath string) conductor.ResourceLifecycle {
				return &lingerPublicationLifecycle{registryKey: logPath}
			},
			register: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
			},
			close: func(t *testing.T, conductorInstance *conductor.Conductor) {
				require.NoError(t, conductorInstance.ClosePublication(stubPublication{registrationID: 42}))
			},
		},
		{
			name:    "subscription",
			logFile: "sub-zero-linger.log",
			lifecycle: func(logPath string) conductor.ResourceLifecycle {
				return &lingerSubscriptionLifecycle{registryKeys: []string{logPath}}
			},
			register: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
			},
			close: func(t *testing.T, conductorInstance *conductor.Conductor) {
				sub := &stubResourceSubscription{registrationID: 22}
				require.NoError(t, conductorInstance.CloseSubscription(sub))
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			logPath := filepath.Join(dir, testCase.logFile)
			writeMinimalLogFileForRegistryTest(t, logPath)

			conductorInstance, _ := newConductorWithCloseLingerForTest(
				t,
				0,
				testCase.lifecycle(logPath),
			)

			_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
			require.NoError(t, err)
			testCase.register(conductorInstance)
			testCase.close(t, conductorInstance)

			registry := conductor.LogBuffersRegistryForTest(conductorInstance)
			require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
			require.False(t, registry.HasScheduledLinger())
		})
	}
}

func TestConductor_CloseResource_retireFailure_marksClosedAndRemovesMap(t *testing.T) {
	t.Parallel()

	publicationFound := func(conductorInstance *conductor.Conductor, registrationID int64) bool {
		_, ok := conductorInstance.PublicationByRegID(registrationID)
		return ok
	}

	tests := []struct {
		name      string
		errRetire error
		lifecycle func(error) conductor.ResourceLifecycle
		insert    func(t *testing.T, conductorInstance *conductor.Conductor) any
		close     func(t *testing.T, conductorInstance *conductor.Conductor, handle any) error
		count     func(conductorInstance *conductor.Conductor) int
		isClosed  func(handle any) bool
		lookup    func(conductorInstance *conductor.Conductor, registrationID int64) bool
	}{
		{
			name:      "publication",
			errRetire: errRetirePublication,
			lifecycle: realCreateMarkRecordingLifecycleWithRetirePublicationErr,
			insert: func(t *testing.T, conductorInstance *conductor.Conductor) any {
				return insertRealPublicationForCloseTest(t, conductorInstance)
			},
			close: closePublicationForCloseTest,
			count: (*conductor.Conductor).PublicationCountForTest,
			isClosed: func(handle any) bool {
				return handle.(*aeron.Publication).IsClosed()
			},
			lookup: publicationFound,
		},
		{
			name:      "exclusivePublication",
			errRetire: errRetirePublication,
			lifecycle: realCreateMarkRecordingLifecycleWithRetirePublicationErr,
			insert: func(t *testing.T, conductorInstance *conductor.Conductor) any {
				return insertRealExclusivePublicationForCloseTest(t, conductorInstance)
			},
			close: closePublicationForCloseTest,
			count: (*conductor.Conductor).PublicationCountForTest,
			isClosed: func(handle any) bool {
				return handle.(*aeron.ExclusivePublication).IsClosed()
			},
			lookup: publicationFound,
		},
		{
			name:      "subscription",
			errRetire: errRetireSubscription,
			lifecycle: realCreateMarkRecordingLifecycleWithRetireSubscriptionErr,
			insert: func(t *testing.T, conductorInstance *conductor.Conductor) any {
				return insertRealSubscriptionForCloseTest(t, conductorInstance)
			},
			close: func(
				t *testing.T,
				conductorInstance *conductor.Conductor,
				handle any,
			) error {
				return conductorInstance.CloseSubscription(handle.(conductor.SubscriptionHandle))
			},
			count: func(conductorInstance *conductor.Conductor) int {
				return conductorInstance.SubscriptionCountForTest()
			},
			isClosed: func(handle any) bool {
				return handle.(*aeron.Subscription).IsClosed()
			},
			lookup: func(conductorInstance *conductor.Conductor, registrationID int64) bool {
				_, ok := conductorInstance.SubscriptionByRegID(registrationID)
				return ok
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var (
				conductorInstance *conductor.Conductor
				handle            any
				registrationID    int64
				gotErrors         []error
				foundAtReport     bool
				closedAtReport    bool
			)
			errorHandler := func(err error) {
				gotErrors = append(gotErrors, err)
				foundAtReport = testCase.lookup(conductorInstance, registrationID)
				closedAtReport = testCase.isClosed(handle)
			}
			config := conductorTestConfig(t, errorHandler, fixedClocks{})
			config.ResourceLifecycle = testCase.lifecycle(testCase.errRetire)
			conductorInstance = newConductorForTest(t, config)

			handle = testCase.insert(t, conductorInstance)
			registrationID = handle.(interface{ RegistrationID() int64 }).RegistrationID()
			var closeErr error
			testutil.RequireReturnsWithin(t, 2*time.Second, func() {
				closeErr = testCase.close(t, conductorInstance, handle)
			})
			require.NoError(t, closeErr)

			require.Len(t, gotErrors, 1)
			require.ErrorIs(t, gotErrors[0], testCase.errRetire)
			require.False(t, foundAtReport)
			require.True(t, closedAtReport)
			require.Equal(t, 0, testCase.count(conductorInstance))
			require.True(t, testCase.isClosed(handle))
		})
	}
}

func TestConductor_CloseResource_idempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                        string
		removeTypeID                int
		wantRetirePublicationCalls  int
		wantRetireSubscriptionCalls int
		setup                       func(conductorInstance *conductor.Conductor)
		closeTwice                  func(t *testing.T, conductorInstance *conductor.Conductor)
	}{
		{
			name:                       "publication",
			removeTypeID:               conductor.RemovePublication,
			wantRetirePublicationCalls: 1,
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
			},
			closeTwice: func(t *testing.T, conductorInstance *conductor.Conductor) {
				pub := stubPublication{registrationID: 42}
				require.NoError(t, conductorInstance.ClosePublication(pub))
				require.NoError(t, conductorInstance.ClosePublication(pub))
			},
		},
		{
			name:                        "subscription",
			removeTypeID:                conductor.RemoveSubscription,
			wantRetireSubscriptionCalls: 1,
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
			},
			closeTwice: func(t *testing.T, conductorInstance *conductor.Conductor) {
				sub := &stubResourceSubscription{registrationID: 22}
				require.NoError(t, conductorInstance.CloseSubscription(sub))
				require.NoError(t, conductorInstance.CloseSubscription(sub))
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			lifecycle := &recordingResourceLifecycle{}
			conductorInstance, _ := newConductorWithCloseLingerForTest(t, 0, lifecycle)
			ring := conductorInstance.DriverProxyForTest().Ring()

			testCase.setup(conductorInstance)
			testCase.closeTwice(t, conductorInstance)

			require.Equal(t, testCase.wantRetirePublicationCalls, lifecycle.retirePublicationCalls)
			require.Equal(t, testCase.wantRetireSubscriptionCalls, lifecycle.retireSubscriptionCalls)
			require.Equal(t, []int{testCase.removeTypeID}, testutil.CommittedRecordTypeIDs(ring))
		})
	}
}

func TestConductor_CloseResource_mapMiss_marksClosed(t *testing.T) {
	t.Parallel()

	for _, testCase := range resourceMapMissCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			config := conductorTestConfig(t, func(error) {}, fixedClocks{})
			config.ResourceLifecycle = aeron.ConductorResourceLifecycle
			conductorInstance := newConductorForTest(t, config)

			handle := testCase.insert(t, conductorInstance)
			testCase.remove(conductorInstance, handle)

			require.NoError(t, testCase.close(t, conductorInstance, handle))
			require.True(t, testCase.closed(handle))
		})
	}
}

func TestConductor_CloseResource_removeOfferFailure_reportsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(conductorInstance *conductor.Conductor)
		close func(t *testing.T, conductorInstance *conductor.Conductor) error
		count func(conductorInstance *conductor.Conductor) int
	}{
		{
			name: "publication",
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
			},
			close: func(t *testing.T, conductorInstance *conductor.Conductor) error {
				return conductorInstance.ClosePublication(stubPublication{registrationID: 42})
			},
			count: func(conductorInstance *conductor.Conductor) int {
				return conductorInstance.PublicationCountForTest()
			},
		},
		{
			name: "subscription",
			setup: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertSubscriptionForTest(22, &stubResourceSubscription{registrationID: 22})
			},
			close: func(t *testing.T, conductorInstance *conductor.Conductor) error {
				sub := &stubResourceSubscription{registrationID: 22}
				return conductorInstance.CloseSubscription(sub)
			},
			count: func(conductorInstance *conductor.Conductor) int {
				return conductorInstance.SubscriptionCountForTest()
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			errorHandler, gotErrors := collectConductorErrors()
			conductorInstance, _ := newConductorWithCloseLingerForTest(
				t,
				0,
				&recordingResourceLifecycle{},
				errorHandler,
			)
			seedFullDriverRingForCloseTest(t, conductorInstance)
			testCase.setup(conductorInstance)

			require.NoError(t, testCase.close(t, conductorInstance))

			require.Len(t, *gotErrors, 1)
			require.ErrorIs(t, (*gotErrors)[0], conductor.ErrRingInsufficientCapacity)
			require.Equal(t, 0, testCase.count(conductorInstance))
		})
	}
}

func TestConductor_onUnavailableImage_positiveLinger_retainsMmap(t *testing.T) {
	t.Parallel()

	const startNs int64 = 21_000_000_000
	const lingerNs int64 = int64(time.Second)

	lifecycle := &recordingImageLifecycle{
		hasImage:           true,
		removedLogFileName: goldenAvailableImageEvent.LogFileName,
	}
	conductorInstance, nanoNs := newConductorWithImageCloseLingerForTest(t, lingerNs, lifecycle)
	conductorInstance.SeedServiceTimestamps(startNs)
	*nanoNs = startNs

	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)

	triggerGoldenUnavailableImage(t, conductorInstance)
	conductorInstance.DoWork()

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	logPath := goldenAvailableImageEvent.LogFileName
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)

	assertDrainLingerRetainedThenSwept(
		t, conductorInstance, registry, logPath, nanoNs, startNs, lingerNs,
	)
}
