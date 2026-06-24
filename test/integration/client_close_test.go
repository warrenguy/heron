//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/test/integration/harness"
)

const closeDrainSoakDuration = 500 * time.Millisecond

func TestClientCloseLifecycle(t *testing.T) {
	md := harness.SharedParallel(t)

	client, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)

	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())
	require.NoError(t, client.Close())

	md.AssertProcessAlive(t)
}

func TestClientCloseOffersCLIENT_CLOSEOnLiveRing(t *testing.T) {
	md, attach := harness.SharedAttachedCnc(t)
	toDriver := attach.Rings.ToDriver

	client := harness.ConnectClient(t, md, aeron.Context{})
	clientID := client.ClientId()
	require.GreaterOrEqual(t, clientID, int64(0))

	for range 3 {
		client.DoWork()
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	consumerBefore, producerBefore := harness.QuiescentToDriverSnapshot(t, toDriver, client)

	harness.AssertClientCloseOfferedOnRingAfterClose(
		t,
		toDriver,
		clientID,
		consumerBefore,
		producerBefore,
		client.Close,
	)
	md.AssertProcessAlive(t)
}

func setupIPCPubSubForCloseDrain(
	t *testing.T,
	client *aeron.Client,
	streamID int32,
) (*aeron.Publication, *aeron.Subscription) {
	t.Helper()

	pub, err := client.AddPublication(streamID, harness.IPCChannel)
	require.NoError(t, err)

	sub, err := client.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)

	harness.WaitForIPCLinkConnected(t, client, pub, sub, harness.ConnectedPollTimeout)

	payload := []byte("close-drain")
	require.Positive(t, pub.Offer(payload))
	harness.PollDelivered(t, client, sub, 1, harness.ConnectedPollTimeout)

	return pub, sub
}

// TestClientCloseDrainsRegistrations proves live aeronmd drains active pub/sub
// registrations before CLIENT_CLOSE and accepts ghost subscription reuse.
func TestClientCloseDrainsRegistrations(t *testing.T) {
	md, attach := harness.SharedAttachedCnc(t)
	toDriver := attach.Rings.ToDriver

	client := harness.ConnectClient(t, md, aeron.Context{})

	streamID := harness.PubSubCloseDrainStreamID
	pub, sub := setupIPCPubSubForCloseDrain(t, client, streamID)
	pubRegID := pub.RegistrationID()
	subRegID := sub.RegistrationID()

	consumerBefore, producerBefore := harness.QuiescentToDriverSnapshot(t, toDriver, client)

	closeDrainDiag := harness.AssertClientCloseDrainsRegistrationsOnRing(
		t,
		toDriver,
		client.ClientId(),
		pubRegID,
		subRegID,
		consumerBefore,
		producerBefore,
		client.Close,
		client,
	)
	t.Logf("close-drain ordering proof path: %s", closeDrainDiag.CloseDrainOrderingProof)

	client2, hadClientError := harness.ConnectIPCClient(t, md)

	sub2, err := client2.AddSubscription(streamID, harness.IPCChannel)
	if err != nil {
		harness.DumpCloseDrainGhostReuseFailure(t, toDriver, closeDrainDiag, client2, err)
	}
	require.NoError(t, err)
	harness.CleanupClose(t, "close ghost subscription", sub2.Close)

	harness.PollUntilNoActiveImages(t, client2, sub2, closeDrainSoakDuration)

	harness.RunSoak(t, closeDrainSoakDuration, aeron.DefaultIdleSleepDuration, func() {
		client2.DoWork()
		harness.RequireNoActiveImages(t, sub2)
		md.AssertProcessAlive(t)
	})

	finishPubSubIPCTest(t, md, hadClientError)
}
