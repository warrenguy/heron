package aeron_test

import (
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

const testResourceLingerNs = int64(50 * time.Millisecond)

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

func requireRecordOrder(t *testing.T, typeIDs []int, orderedTypeIDs ...int) {
	t.Helper()

	for i := 0; i < len(orderedTypeIDs)-1; i++ {
		earlierIndex := slices.Index(typeIDs, orderedTypeIDs[i])
		require.NotEqual(t, -1, earlierIndex)
		laterIndex := slices.Index(typeIDs, orderedTypeIDs[i+1])
		require.NotEqual(t, -1, laterIndex)
		require.Less(t, earlierIndex, laterIndex)
	}
}

func setupClientCloseDrainTest(
	t *testing.T,
	withPublication, withSubscription bool,
) (*testutil.ConductorFixture, *aeron.Client) {
	t.Helper()

	nanoNs := testutil.TestStartNanoNs
	fixture := testutil.NewConductorFixture(t, testutil.DefaultConductorConfig(&nanoNs))

	if withPublication {
		aeron.InsertPublicationForTest(fixture.Conductor, 11)
	}
	if withSubscription {
		sub := aeron.NewSubscriptionForTest(22, 1001, "aeron:ipc")
		conductor.RegisterSubscriptionForTest(fixture.Conductor, 22, sub)
	}

	return fixture, newClientCloseTestClient(
		fixture,
		func() int64 { return nanoNs },
		testutil.TestIdleSleepDuration,
	)
}

func closeClientAndRequireRecordOrder(
	t *testing.T,
	client *aeron.Client,
	fixture *testutil.ConductorFixture,
	orderedTypeIDs ...int,
) {
	t.Helper()

	require.NoError(t, client.Close())
	requireRecordOrder(t, testutil.CommittedRecordTypeIDs(fixture.Ring), orderedTypeIDs...)
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

func setupClientCloseRealPublicationLingerTest(
	t *testing.T,
	logFileName string,
	lingerNs int64,
	nanoClock func() int64,
) (*testutil.ConductorFixture, *aeron.Client, *aeron.Publication, string) {
	t.Helper()

	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.ResourceLingerNs = lingerNs
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
) (*testutil.ConductorFixture, *aeron.Client) {
	t.Helper()

	nanoClock := func() int64 { return testutil.TestStartNanoNs }
	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.ResourceLingerNs = lingerNs
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
	require.Zero(t, client.InterServiceTimeout())
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
	fixture, client := setupClientCloseDrainTest(t, true, false)
	closeClientAndRequireRecordOrder(
		t, client, fixture, conductor.RemovePublication, conductor.ClientClose,
	)
	require.Equal(t, 0, conductor.PublicationCountForTest(fixture.Conductor))
}

func TestClient_Close_drainsPublicationAndSubscriptionBeforeClientClose(t *testing.T) {
	fixture, client := setupClientCloseDrainTest(t, true, true)
	closeClientAndRequireRecordOrder(
		t, client, fixture,
		conductor.RemovePublication, conductor.RemoveSubscription, conductor.ClientClose,
	)
	require.Equal(t, 0, conductor.PublicationCountForTest(fixture.Conductor))
	require.Equal(t, 0, conductor.SubscriptionCountForTest(fixture.Conductor))
}

func TestClient_Close_drainsSubscriptionBeforeClientClose(t *testing.T) {
	fixture, client := setupClientCloseDrainTest(t, false, true)
	closeClientAndRequireRecordOrder(
		t, client, fixture, conductor.RemoveSubscription, conductor.ClientClose,
	)
	require.Equal(t, 0, conductor.SubscriptionCountForTest(fixture.Conductor))
}

func TestClient_Close_realPublication_unmapsLingeringLogBuffers(t *testing.T) {
	const startNs = int64(31_000_000_000)

	fixture, client, _, _ := setupClientCloseRealPublicationLingerTest(
		t, "client-real-linger.log", testResourceLingerNs,
		func() int64 { return startNs },
	)
	closeClientAndRequireRecordOrder(
		t, client, fixture, conductor.RemovePublication, conductor.ClientClose,
	)

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.False(t, registry.HasScheduledLinger())
	require.Equal(t, 0, conductor.PublicationCountForTest(fixture.Conductor))
}

func TestClient_Close_realPublication_closePublicationLingerRetainedThenSwept(t *testing.T) {
	const startNs = int64(32_000_000_000)

	var nanoNs atomic.Int64
	nanoNs.Store(startNs)
	nanoClock := nanoNs.Load

	fixture, client, pub, logPath := setupClientCloseRealPublicationLingerTest(
		t, "client-real-pub-close.log", testResourceLingerNs, nanoClock,
	)

	require.NoError(t, pub.Close())
	require.True(t, pub.IsClosed())
	require.Equal(t, 0, client.PublicationCountForTest())

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.True(t, registry.HasScheduledLinger())
	require.Equal(
		t,
		startNs+testResourceLingerNs,
		conductor.LingerDeadlineNsForTest(registry, logPath),
	)

	assertDrainLingerRetainedThenSwept(
		t, fixture, logPath, &nanoNs, startNs, testResourceLingerNs,
	)
	require.False(t, registry.HasScheduledLinger())

	require.NoError(t, client.Close())
}

func TestClient_Close_unmapsLingeringLogBuffers(t *testing.T) {
	fixture, client := setupClientCloseLingerTest(t, "client-linger.log", testResourceLingerNs)
	closeClientAndRequireRecordOrder(
		t, client, fixture, conductor.RemovePublication, conductor.ClientClose,
	)

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.False(t, registry.HasScheduledLinger())
}

func TestClient_Close_sleepsClientCloseLingerBeforeUnmap(t *testing.T) {
	const (
		startNs           = int64(33_000_000_000)
		clientCloseLinger = 200 * time.Millisecond
	)

	tests := []struct {
		name      string
		lingerNs  int64
		nanoClock func() int64
	}{
		{
			name:      "frozenClock",
			lingerNs:  testResourceLingerNs,
			nanoClock: func() int64 { return startNs },
		},
		{
			name:      "zeroResourceLinger",
			lingerNs:  0,
			nanoClock: func() int64 { return startNs },
		},
		{
			// Each NanoClock read advances 1ns, so every drain DoWork after
			// the release sees a 1ns linger deadline as passed.
			name:     "deadlinePassesDuringDrain",
			lingerNs: 1,
			nanoClock: func() func() int64 {
				var nanoNs atomic.Int64
				nanoNs.Store(startNs)
				return func() int64 { return nanoNs.Add(1) }
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, client, _, _ := setupClientCloseRealPublicationLingerTest(
				t, "client-close-sleep.log", test.lingerNs, test.nanoClock,
			)
			client.SetClientCloseLingerDurationForTest(clientCloseLinger)

			registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
			started := time.Now()
			closeDone := closeClientAsync(client)

			sawLinger := false
			var cleared time.Time
			deadline := time.After(2 * time.Second)
			for {
				if registry.HasScheduledLinger() {
					sawLinger = true
				} else if sawLinger {
					cleared = time.Now()
					break
				}
				select {
				case <-deadline:
					t.Fatal("lingering log buffer was not unmapped")
				default:
					time.Sleep(time.Millisecond)
				}
			}

			require.GreaterOrEqual(t, cleared.Sub(started), clientCloseLinger)
			waitForClientClose(t, closeDone, "client close did not finish after unmap")
			require.Zero(t, conductor.LogBuffersRegistryEntryCountForTest(registry))
		})
	}
}

func TestClient_Close_defaultConfigSkipsLingerWait(t *testing.T) {
	var recorded []error
	client, fixture := aeron.SetupDefaultCloseClientForTest(t, func(err error) {
		recorded = append(recorded, err)
	})

	started := time.Now()
	waitForClientClose(t, closeClientAsync(client), "client close hung with default config")
	require.Less(t, time.Since(started), time.Second)

	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)
	require.Zero(t, conductor.LogBuffersRegistryEntryCountForTest(registry))
	requireRecordOrder(
		t,
		testutil.CommittedRecordTypeIDs(fixture.Ring),
		conductor.RemovePublication,
		conductor.RemoveSubscription,
		conductor.ClientClose,
	)
	require.Empty(t, recorded)
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
