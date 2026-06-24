//go:build integration

package integration_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/test/integration/harness"
)

func mpscMatrixMinProducerDelta() int64 {
	return harness.AlignedDriverRecordLength(
		conductor.PublicationMessageBodyLength(harness.IPCChannel),
	) +
		harness.AlignedDriverRecordLength(conductor.GetNextAvailableSessionIDBodyLength) +
		harness.AlignedDriverRecordLength(
			conductor.SubscriptionMessageBodyLength(harness.IPCChannel),
		) +
		harness.AlignedDriverRecordLength(conductor.RemovePublicationBodyLength) +
		harness.AlignedDriverRecordLength(conductor.RemoveMessageBodyLength)
}

func connectMPSCMatrixClient(
	t *testing.T,
	md *harness.MediaDriver,
) (*aeron.Client, *atomic.Bool) {
	t.Helper()

	var hadClientError atomic.Bool
	client := harness.ConnectClient(t, md, aeron.Context{
		InterServiceTimeout: harness.ConnectedPollTimeout,
		ErrorHandler: func(err error) {
			hadClientError.Store(true)
		},
	})
	return client, &hadClientError
}

// TestMPSCCommandMatrix proves six ToDriver command classes from two concurrent
// clients on live aeronmd via public Client API and composite client-side
// witnesses. Wire type-ID proof stays in harness unit tests; ring scan here is
// failure diagnostics only.
func TestMPSCCommandMatrix(t *testing.T) {
	md, attach := harness.SharedAttachedCnc(t)
	toDriver := attach.Rings.ToDriver

	clientA, hadClientErrorA := connectMPSCMatrixClient(t, md)
	clientB, hadClientErrorB := connectMPSCMatrixClient(t, md)

	clientIDA := clientA.ClientId()
	clientIDB := clientB.ClientId()
	require.GreaterOrEqual(t, clientIDA, int64(0))
	require.GreaterOrEqual(t, clientIDB, int64(0))
	require.NotEqual(t, clientIDA, clientIDB)

	_, producerBefore := harness.QuiescentToDriverSnapshot(t, toDriver, clientA, clientB)
	t.Cleanup(func() {
		harness.LogMPSCMatrixRingTypeIDsOnFailure(t, toDriver, producerBefore)
	})

	minProducerDelta := mpscMatrixMinProducerDelta()

	var (
		pub     *aeron.Publication
		sub     *aeron.Subscription
		session int32
	)

	var wg sync.WaitGroup
	wg.Add(2)
	offerErrs := make(chan error, 2)

	go func() {
		defer wg.Done()

		var addErr error
		pub, addErr = clientA.AddPublication(
			harness.MPSCMatrixPubSubStreamID,
			harness.IPCChannel,
		)
		if addErr != nil {
			offerErrs <- addErr
		}
	}()

	go func() {
		defer wg.Done()

		var sessionErr error
		session, sessionErr = clientB.NextAvailableSessionID(
			harness.MPSCMatrixSessionIDStreamID,
		)
		if sessionErr != nil {
			offerErrs <- sessionErr
			return
		}

		var addErr error
		sub, addErr = clientB.AddSubscription(
			harness.MPSCMatrixPubSubStreamID,
			harness.IPCChannel,
		)
		if addErr != nil {
			offerErrs <- addErr
		}
	}()

	wg.Wait()
	for len(offerErrs) > 0 {
		require.NoError(t, <-offerErrs)
	}

	// ADD_PUBLICATION (0x01): ready handle.
	require.NotNil(t, pub)
	pubRegID := pub.RegistrationID()
	require.Positive(t, pubRegID)

	// GET_NEXT_AVAILABLE_SESSION_ID (0x12): round-trip via blocking API.
	t.Logf("next available session id: %d", session)

	// ADD_SUBSCRIPTION (0x04): ready handle with image attach path.
	require.NotNil(t, sub)
	subRegID := sub.RegistrationID()
	require.Positive(t, subRegID)

	harness.CleanupClose(t, "close publication", pub.Close)
	harness.CleanupClose(t, "close subscription", sub.Close)

	// Split-client IPC link and data offer on test goroutine after concurrent Add*;
	// mirrors ConnectIPCPubSubClients. Avoids testify in workers and lost offers
	// when subscriber image is not yet attached.
	harness.PollSplitClientIPCLink(t, clientA, clientB, pub, sub, 0)
	harness.OfferWithRetry(t, clientA, pub, []byte("mpsc-A"), 0)

	fragments := harness.PollDelivered(t, clientB, sub, 1, 0)
	require.Equal(t, []byte("mpsc-A"), fragments[0])

	// REMOVE_PUBLICATION (0x02): via pub.Close.
	require.NoError(t, pub.Close())
	// ON_UNAVAILABLE_IMAGE: subscriber drops image while sub still open.
	harness.PollUntilNoActiveImagesSplit(t, clientA, clientB, sub, 0)

	// REMOVE_SUBSCRIPTION (0x05): via sub.Close.
	require.NoError(t, sub.Close())

	harness.WaitForToDriverRingDrainedWithClients(t, toDriver, 0, clientA, clientB)

	// Remove completion: registration lookup miss after successful close.
	harness.RequirePublicationLookupMiss(t, clientA, pubRegID)
	harness.RequireSubscriptionLookupMiss(t, clientB, subRegID)

	producerAfter := toDriver.ProducerPosition()
	require.GreaterOrEqual(t, producerAfter-producerBefore, minProducerDelta)
	require.Equal(t, producerAfter, toDriver.ConsumerPosition())

	require.False(t, hadClientErrorA.Load())
	require.False(t, hadClientErrorB.Load())
	clientA.DoWork()
	clientB.DoWork()
	harness.AssertConsumerHeartbeatFresh(t, attach)
	md.AssertProcessAlive(t)
}
