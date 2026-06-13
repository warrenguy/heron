package aeron_test

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

const clientCloseLingerNs = int64(50_000_000)

func newClientCloseTestClient(
	fixture *testutil.ConductorFixture,
	nanoClock func() int64,
	idleSleep time.Duration,
) *aeron.Client {
	return aeron.NewClientForTestWithDriverProxy(
		fixture.Conductor,
		fixture.DriverProxy,
		nanoClock,
		testutil.TestClientID,
		testutil.TestDriverTimeout,
		idleSleep,
	)
}

func indexOfRecordType(typeIDs []int, typeID int) int {
	for index, id := range typeIDs {
		if id == typeID {
			return index
		}
	}
	return -1
}

func requireRecordComesBefore(t *testing.T, typeIDs []int, earlierTypeID, laterTypeID int) {
	t.Helper()

	earlierIndex := indexOfRecordType(typeIDs, earlierTypeID)
	require.NotEqual(t, -1, earlierIndex)
	laterIndex := indexOfRecordType(typeIDs, laterTypeID)
	require.NotEqual(t, -1, laterIndex)
	require.Less(t, earlierIndex, laterIndex)
}

func closeClientAsync(client *aeron.Client) <-chan error {
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- client.Close()
	}()
	return closeDone
}

func waitForClientClose(t *testing.T, closeDone <-chan error, timeoutMsg string) {
	t.Helper()

	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(2 * time.Second):
		t.Fatal(timeoutMsg)
	}
}

func assertClientCloseIsIdempotent(t *testing.T, client *aeron.Client) {
	t.Helper()

	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())
	require.False(t, client.IsTerminating())
}

func waitForClientCloseWithAdvancingClock(
	t *testing.T,
	closeDone <-chan error,
	nanoNs *atomic.Int64,
	lingerNs int64,
) {
	t.Helper()

	var closeErr error
	advanceDeadline := time.After(2 * time.Second)
advanceLoop:
	for {
		select {
		case closeErr = <-closeDone:
			break advanceLoop
		case <-advanceDeadline:
			t.Fatal("client close did not finish during linger drain")
		default:
			nanoNs.Add(lingerNs / 5)
			time.Sleep(time.Microsecond)
		}
	}
	require.NoError(t, closeErr)
}

func setupClientCloseRealPublicationLingerTest(
	t *testing.T,
	logFileName string,
	lingerNs int64,
	nanoClock func() int64,
) (*testutil.ConductorFixture, *aeron.Client, *aeron.Publication, string) {
	t.Helper()

	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.CloseLingerNs = lingerNs
	config.ResourceLifecycle = aeron.ConductorResourceLifecycle
	fixture := testutil.NewConductorFixture(t, config)

	logPath := filepath.Join(t.TempDir(), logFileName)
	pub := aeron.NewFileBackedPublicationBoundForCloseTest(t, fixture.Conductor, logPath)

	client := newClientCloseTestClient(fixture, nanoClock, time.Microsecond)
	return fixture, client, pub, logPath
}

func assertDrainLingerRetainedThenSwept(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	logPath string,
	nanoNs *atomic.Int64,
	startNs, lingerNs int64,
) {
	t.Helper()

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)

	nanoNs.Store(startNs + lingerNs - 1)
	require.Zero(t, fixture.Conductor.DrainLingeringLogBuffers())
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	nanoNs.Store(startNs + lingerNs)
	require.Equal(t, 1, fixture.Conductor.DrainLingeringLogBuffers())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func setupClientCloseLingerTest(
	t *testing.T,
	logFileName string,
	lingerNs int64,
	nanoClock func() int64,
) (*testutil.ConductorFixture, *aeron.Client) {
	t.Helper()

	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.CloseLingerNs = lingerNs
	dir := t.TempDir()
	logPath := filepath.Join(dir, logFileName)
	conductor.WriteMinimalLogFileForTest(t, logPath)
	config.ResourceLifecycle = aeron.NewLingerStubPublicationLifecycle(logPath)
	fixture := testutil.NewConductorFixture(t, config)

	_, err := conductor.AcquireLogBuffersForTest(fixture.Conductor, logPath)
	require.NoError(t, err)
	aeron.InsertPublicationForTest(fixture.Conductor, 11)

	client := newClientCloseTestClient(fixture, nanoClock, time.Microsecond)
	return fixture, client
}

func TestClient_nilGuards(t *testing.T) {
	var client *aeron.Client

	require.Zero(t, client.DoWork())
	require.False(t, client.IsTerminating())
	require.NoError(t, client.Close())
	require.Zero(t, client.ClientId())
}

func TestClient_closeBehavior(t *testing.T) {
	t.Run("emptyClient", func(t *testing.T) {
		client := &aeron.Client{}

		require.False(t, client.IsTerminating())
		assertClientCloseIsIdempotent(t, client)
	})

	t.Run("conductorBackedClient", func(t *testing.T) {
		nanoNs := testutil.TestStartNanoNs
		fixture := testutil.NewConductorFixture(t, testutil.DefaultConductorConfig(&nanoNs))
		client := aeron.NewClientForTest(fixture.Conductor, func() int64 { return nanoNs })

		assertClientCloseIsIdempotent(t, client)
	})
}

func TestClient_accessorGuardsAfterClose(t *testing.T) {
	accessorCases := []struct {
		name   string
		client func(t *testing.T) *aeron.Client
	}{
		{
			name: "closedClient",
			client: func(t *testing.T) *aeron.Client {
				client := aeron.NewAccessorTestClient(t)
				require.NoError(t, client.Close())
				return client
			},
		},
		{
			name: "nilClient",
			client: func(t *testing.T) *aeron.Client {
				// Nil accessors share mappedCncFile guard with TestClient_nilGuards.
				return nil
			},
		},
	}
	accessorCalls := []struct {
		name string
		call func(client *aeron.Client) error
	}{
		{
			name: "CncMetadata",
			call: func(client *aeron.Client) error {
				_, err := client.CncMetadata()
				return err
			},
		},
		{
			name: "CountersReader",
			call: func(client *aeron.Client) error {
				_, err := client.CountersReader()
				return err
			},
		},
		{
			name: "ErrorLogBuffer",
			call: func(client *aeron.Client) error {
				_, err := client.ErrorLogBuffer()
				return err
			},
		},
		{
			name: "AddPublication",
			call: func(client *aeron.Client) error {
				_, err := client.AddPublication(1001, "aeron:ipc")
				return err
			},
		},
		{
			name: "AddSubscription",
			call: func(client *aeron.Client) error {
				_, err := client.AddSubscription(1001, "aeron:ipc")
				return err
			},
		},
	}

	lookupCases := []struct {
		name string
		call func(client *aeron.Client) bool
	}{
		{
			name: "Publication",
			call: func(client *aeron.Client) bool {
				_, ok := client.Publication(1)
				return ok
			},
		},
		{
			name: "Subscription",
			call: func(client *aeron.Client) bool {
				_, ok := client.Subscription(1)
				return ok
			},
		},
	}

	for _, accessorCase := range accessorCases {
		t.Run(accessorCase.name, func(t *testing.T) {
			client := accessorCase.client(t)
			for _, accessorCall := range accessorCalls {
				t.Run(accessorCall.name, func(t *testing.T) {
					err := accessorCall.call(client)
					if accessorCase.name == "nilClient" &&
						(accessorCall.name == "AddPublication" ||
							accessorCall.name == "AddSubscription") {
						require.ErrorIs(t, err, aeron.ErrClientNotReady)
						return
					}
					require.ErrorIs(t, err, aeron.ErrClientClosed)
				})
			}
			for _, lookupCase := range lookupCases {
				t.Run(lookupCase.name, func(t *testing.T) {
					require.False(t, lookupCase.call(client))
				})
			}
		})
	}

	t.Run("retainedCountersReader", func(t *testing.T) {
		client := aeron.NewAccessorTestClient(t)

		retained, err := client.CountersReader()
		require.NoError(t, err)
		require.NotNil(t, retained)

		_, err = retained.CounterValue(0)
		require.NoError(t, err)

		require.NoError(t, client.Close())

		_, err = client.CountersReader()
		require.ErrorIs(t, err, aeron.ErrClientClosed)

		_, err = retained.CounterValue(1)
		require.EqualError(t, err, "cnc: counter id 1 out of range: maxCounterId=0")
	})
}

func TestClient_conductorServiceTimeoutAfterInterServiceTimeout(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	fixture := testutil.NewConductorFixture(t, testutil.DefaultConductorConfig(&nanoNs))

	nanoNs += testutil.TestInterServiceTimeout.Nanoseconds() + 1

	require.Equal(t, 1, fixture.Conductor.DoWork())
	require.True(t, fixture.Conductor.IsTerminating())
}

func TestClient_Close_drainsPublicationBeforeClientClose(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	fixture := testutil.NewConductorFixture(t, testutil.DefaultConductorConfig(&nanoNs))

	aeron.InsertPublicationForTest(fixture.Conductor, 11)

	client := newClientCloseTestClient(
		fixture,
		func() int64 { return nanoNs },
		testutil.TestIdleSleepDuration,
	)
	require.NoError(t, client.Close())

	typeIDs := testutil.CommittedRecordTypeIDs(fixture.Ring)
	require.Contains(t, typeIDs, conductor.RemovePublication)
	requireRecordComesBefore(t, typeIDs, conductor.RemovePublication, conductor.ClientClose)
	require.Equal(t, 0, client.PublicationCountForTest())
}

func TestClient_Close_drainsSubscriptionBeforeClientClose(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	fixture := testutil.NewConductorFixture(t, testutil.DefaultConductorConfig(&nanoNs))

	sub := aeron.NewSubscriptionForTest(22, 1001, "aeron:ipc")
	conductor.RegisterSubscriptionForTest(fixture.Conductor, 22, sub)

	client := newClientCloseTestClient(
		fixture,
		func() int64 { return nanoNs },
		testutil.TestIdleSleepDuration,
	)
	require.NoError(t, client.Close())

	typeIDs := testutil.CommittedRecordTypeIDs(fixture.Ring)
	require.Contains(t, typeIDs, conductor.RemoveSubscription)
	requireRecordComesBefore(t, typeIDs, conductor.RemoveSubscription, conductor.ClientClose)
	require.Equal(t, 0, client.SubscriptionCountForTest())
}

func TestClient_Close_lingerDrainRunsConductorDoWork(t *testing.T) {
	const startNs = int64(30_000_000_000)

	var nanoNs atomic.Int64
	nanoNs.Store(startNs)
	nanoClock := nanoNs.Load

	fixture, client := setupClientCloseLingerTest(
		t, "linger-dowork.log", clientCloseLingerNs, nanoClock,
	)

	var doWorkLingerSweeps atomic.Int32
	fixture.Conductor.SetLingerSweepBlockerForTest(func() {
		doWorkLingerSweeps.Add(1)
	})
	t.Cleanup(func() { fixture.Conductor.SetLingerSweepBlockerForTest(nil) })

	closeDone := closeClientAsync(client)
	lingerScheduled := false
	var sweepsAtLingerStart int32
	advanceDeadline := time.After(2 * time.Second)
advanceLoop:
	for {
		select {
		case closeErr := <-closeDone:
			require.NoError(t, closeErr)
			require.True(t, lingerScheduled, "linger was not scheduled during close")
			require.Greater(
				t,
				doWorkLingerSweeps.Load(),
				sweepsAtLingerStart,
				"conductor DoWork did not run during linger drain",
			)
			break advanceLoop
		case <-advanceDeadline:
			t.Fatal("client close did not finish during linger drain")
		default:
			registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
			if registry.HasScheduledLinger() {
				if !lingerScheduled {
					lingerScheduled = true
					sweepsAtLingerStart = doWorkLingerSweeps.Load()
				}
				nanoNs.Add(clientCloseLingerNs / 5)
			}
			time.Sleep(time.Microsecond)
		}
	}
}

func TestClient_Close_realPublication_lingerDrainBeforeClientClose(t *testing.T) {
	const startNs = int64(31_000_000_000)

	var nanoNs atomic.Int64
	nanoNs.Store(startNs)
	nanoClock := nanoNs.Load

	fixture, client, _, _ := setupClientCloseRealPublicationLingerTest(
		t, "client-real-linger.log", clientCloseLingerNs, nanoClock,
	)
	waitForClientCloseWithAdvancingClock(
		t, closeClientAsync(client), &nanoNs, clientCloseLingerNs,
	)

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.False(t, registry.HasScheduledLinger())

	typeIDs := testutil.CommittedRecordTypeIDs(fixture.Ring)
	require.Contains(t, typeIDs, conductor.RemovePublication)
	requireRecordComesBefore(t, typeIDs, conductor.RemovePublication, conductor.ClientClose)
	require.Equal(t, 0, client.PublicationCountForTest())
}

func TestClient_Close_realPublication_closePublicationLingerRetainedThenSwept(t *testing.T) {
	const startNs = int64(32_000_000_000)

	var nanoNs atomic.Int64
	nanoNs.Store(startNs)
	nanoClock := nanoNs.Load

	fixture, client, pub, logPath := setupClientCloseRealPublicationLingerTest(
		t, "client-real-pub-close.log", clientCloseLingerNs, nanoClock,
	)

	require.NoError(t, pub.Close())
	require.True(t, pub.IsClosed())
	require.Equal(t, 0, client.PublicationCountForTest())

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.True(t, registry.HasScheduledLinger())
	require.Equal(
		t,
		startNs+clientCloseLingerNs,
		conductor.LingerDeadlineNsForTest(registry, logPath),
	)

	assertDrainLingerRetainedThenSwept(
		t, fixture, logPath, &nanoNs, startNs, clientCloseLingerNs,
	)
	require.False(t, registry.HasScheduledLinger())

	require.NoError(t, client.Close())
}

func TestClient_Close_lingerDrainBeforeClientClose(t *testing.T) {
	const startNs = int64(30_000_000_000)

	var nanoNs atomic.Int64
	nanoNs.Store(startNs)
	nanoClock := nanoNs.Load

	fixture, client := setupClientCloseLingerTest(
		t, "client-linger.log", clientCloseLingerNs, nanoClock,
	)
	waitForClientCloseWithAdvancingClock(
		t, closeClientAsync(client), &nanoNs, clientCloseLingerNs,
	)

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.False(t, registry.HasScheduledLinger())

	typeIDs := testutil.CommittedRecordTypeIDs(fixture.Ring)
	require.Contains(t, typeIDs, conductor.RemovePublication)
	requireRecordComesBefore(t, typeIDs, conductor.RemovePublication, conductor.ClientClose)
}

func TestClient_Close_frozenNanoClock_bestEffortLingerDrain(t *testing.T) {
	const startNs = int64(40_000_000_000)

	nanoClock := func() int64 { return startNs }

	fixture, client := setupClientCloseLingerTest(
		t, "frozen-linger.log", clientCloseLingerNs, nanoClock,
	)
	waitForClientClose(t, closeClientAsync(client), "client close hung with frozen NanoClock")

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.True(t, registry.HasScheduledLinger())
}

func TestClient_delayedFirstDoWorkReSeedsAfterConstructorSeed(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	fixture := testutil.NewConductorFixture(t, testutil.DefaultConductorConfig(&nanoNs))

	nanoNs += testutil.TestInterServiceTimeout.Nanoseconds() + 1

	client := aeron.NewClientForTest(
		fixture.Conductor,
		func() int64 { return nanoNs },
	)
	client.DoWork()
	require.False(t, client.IsTerminating())
}
