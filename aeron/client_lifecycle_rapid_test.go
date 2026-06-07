package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

func rapidDoWorkNTimes(_ *rapid.T, client *aeron.Client, count int) {
	for range count {
		client.DoWork()
	}
}

func TestClient_closeDoWorkGuards_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		clientID := int64(rapid.IntRange(1, 1_000_000).Draw(t, "clientID"))
		client := newRapidTestClient(t, clientID)
		require.Equal(t, clientID, client.ClientId())

		rapidDoWorkNTimes(t, client, rapid.IntRange(0, 16).Draw(t, "doWorkBeforeClose"))
		require.Equal(t, clientID, client.ClientId())

		require.NoError(t, client.Close())

		for range rapid.IntRange(0, 16).Draw(t, "doWorkAfterClose") {
			require.Zero(t, client.DoWork())
		}
		require.False(t, client.IsTerminating())
	})
}

func TestClient_doubleClose_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		client := newRapidTestClient(t, 0)

		rapidDoWorkNTimes(t, client, rapid.IntRange(0, 16).Draw(t, "doWorkBeforeClose"))

		require.NoError(t, client.Close())
		require.NoError(t, client.Close())
		require.Zero(t, client.DoWork())
		require.False(t, client.IsTerminating())
	})
}
