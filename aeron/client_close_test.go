package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestClient_nilGuards(t *testing.T) {
	var client *aeron.Client

	require.Zero(t, client.DoWork())
	require.False(t, client.IsTerminating())
	require.NoError(t, client.Close())
	require.Zero(t, client.ClientId())
}

func TestClient_closeBehavior(t *testing.T) {
	client := &aeron.Client{}

	require.False(t, client.IsTerminating())
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())
	require.False(t, client.IsTerminating())
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
