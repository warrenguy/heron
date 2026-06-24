//go:build rapid

package aeron_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

func rapidDoWorkNTimes(rt *rapid.T, client *aeron.Client, count int) {
	rt.Helper()

	for range count {
		client.DoWork()
	}
}

func rapidPreCloseDoWorkWithQueries(rt *rapid.T, client *aeron.Client, clientID int64, count int) {
	rt.Helper()

	for i := range count {
		if rapid.Bool().Draw(rt, fmt.Sprintf("isTerminating%d", i)) {
			require.False(rt, client.IsTerminating())
		}
		if rapid.Bool().Draw(rt, fmt.Sprintf("clientId%d", i)) {
			require.Equal(rt, clientID, client.ClientId())
		}
		client.DoWork()
	}
}

func TestClient_closeDoWorkGuards_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		clientID := int64(rapid.IntRange(1, 1_000_000).Draw(t, "clientID"))
		client := newRapidTestClient(t, clientID)
		require.Equal(t, clientID, client.ClientId())

		rapidPreCloseDoWorkWithQueries(
			t,
			client,
			clientID,
			rapid.IntRange(0, 32).Draw(t, "doWorkBeforeClose"),
		)
		require.Equal(t, clientID, client.ClientId())

		require.NoError(t, client.Close())

		for range rapid.IntRange(0, 16).Draw(t, "doWorkAfterClose") {
			require.Zero(t, client.DoWork())
		}
		require.False(t, client.IsTerminating())
	})
}

func rapidInterleaveDoWorkAndIsTerminating(rt *rapid.T, client *aeron.Client, count int) {
	rt.Helper()

	for i := range count {
		if rapid.Bool().Draw(rt, fmt.Sprintf("doWork%d", i)) {
			client.DoWork()
		} else {
			_ = client.IsTerminating()
		}
	}
}

func TestClient_doubleClose_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		client := newRapidTestClient(t, 0)

		rapidDoWorkNTimes(t, client, rapid.IntRange(0, 8).Draw(t, "doWorkBeforeClose"))

		require.NoError(t, client.Close())
		rapidInterleaveDoWorkAndIsTerminating(
			t,
			client,
			rapid.IntRange(0, 12).Draw(t, "betweenCloseOps"),
		)
		require.NoError(t, client.Close())
		require.Zero(t, client.DoWork())
		require.False(t, client.IsTerminating())
	})
}
