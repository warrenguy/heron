//go:build integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const rejoinFalseStreamID = int32(9101)

// TestUDPRejoinFalse_secondPubImagesAfterFirstGone asserts a rejoin=false
// UDP subscription accepts a second publication after the first image is gone.
func TestUDPRejoinFalse_secondPubImagesAfterFirstGone(t *testing.T) {
	t.Parallel()

	t.Run("sameDriver", func(t *testing.T) {
		t.Parallel()
		runRejoinFalseSecondPub(t, false)
	})
	t.Run("crossDriver", func(t *testing.T) {
		t.Parallel()
		runRejoinFalseSecondPub(t, true)
	})
}

func runRejoinFalseSecondPub(t *testing.T, splitDrivers bool) {
	t.Helper()

	clients := startRejoinFalseClients(t, splitDrivers)
	port := FreeUDPPort(t)
	channel := "aeron:udp?endpoint=" + LoopbackUDPEndpoint(port)
	subChannel := aeron.AppendChannelParam(channel, "rejoin=", "false")

	sub, err := clients.sub.AddSubscription(rejoinFalseStreamID, subChannel)
	require.NoError(t, err)
	CleanupClose(t, "close rejoin=false subscription", sub.Close)
	require.Contains(t, sub.Channel(), "rejoin=false")

	pub1, err := clients.pub1.AddPublication(rejoinFalseStreamID, channel)
	require.NoError(t, err)
	CleanupClose(t, "close first publication", pub1.Close)
	WaitUntilImageCount(t, sub, 1)

	require.NoError(t, pub1.Close())
	if splitDrivers {
		PollUntilNoActiveImagesSplit(t, clients.pub1, clients.sub, sub)
	} else {
		PollUntilNoActiveImages(t, clients.sub, sub)
	}

	pub2, err := clients.pub2.AddPublication(rejoinFalseStreamID, channel)
	require.NoError(t, err)
	CleanupClose(t, "close second publication", pub2.Close)
	WaitUntilImageCount(t, sub, 1)
}

type rejoinFalseClients struct {
	sub  *aeron.Client
	pub1 *aeron.Client
	pub2 *aeron.Client
}

func startRejoinFalseClients(t *testing.T, splitDrivers bool) rejoinFalseClients {
	t.Helper()

	if !splitDrivers {
		client := ConnectClient(t, Start(t), aeron.Config{})
		return rejoinFalseClients{
			sub:  client,
			pub1: client,
			pub2: client,
		}
	}

	return rejoinFalseClients{
		sub:  ConnectClient(t, Start(t), aeron.Config{}),
		pub1: ConnectClient(t, Start(t), aeron.Config{}),
		pub2: ConnectClient(t, Start(t), aeron.Config{}),
	}
}
