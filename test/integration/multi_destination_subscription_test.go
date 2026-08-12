//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMultiDestinationSubscriptionMerge asserts two unicast receive destinations
// with the same exclusive session merge into one image on a control-mode=manual
// subscription. After RemoveDestination of the first feed, offers on that feed
// deliver no fragments. The second feed still delivers.
func TestMultiDestinationSubscriptionMerge(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const (
		streamID         = harness.MultiDestinationSubscriptionStreamID
		sessionID        = int32(4242)
		initialTermID    = int32(1)
		termLength       = 64 * 1024
		mtu              = 1408
		messageCount     = 8
		afterRemoveCount = 4
		payloadSize      = 64
	)

	// Free ports under SharedParallel. Fixed 24610/24611 collide across hosts.
	ports := harness.FreeUDPPorts(t, 2)
	endpointA := harness.LoopbackUDPEndpoint(ports[0])
	endpointB := harness.LoopbackUDPEndpoint(ports[1])

	subChannel := "aeron:udp?control-mode=manual"
	destA := "aeron:udp?endpoint=" + endpointA
	destB := "aeron:udp?endpoint=" + endpointB

	sub, err := client.AddSubscription(streamID, subChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close multi-destination subscription", sub.Close)

	require.NoError(t, sub.AddDestination(destA))

	pubAChannel := exclusiveUDPChannel(
		endpointA,
		sessionID,
		initialTermID,
		initialTermID,
		0,
		termLength,
		mtu,
	)
	pubA, err := client.AddExclusivePublication(streamID, pubAChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close multi-destination publication A", pubA.Close)
	require.Equal(t, sessionID, pubA.SessionID())

	harness.WaitUntilConnected(t, pubA)
	harness.WaitUntilImageCount(t, sub, 1)

	image := harness.RequireFirstImage(t, sub)
	imageCorrelationID := image.CorrelationID()
	require.Equal(t, sessionID, image.SessionID())

	payloadsA := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xD0)
	offerPayloads(t, client, pubA, payloadsA)
	receivedA := harness.PollDelivered(t, client, sub, messageCount)
	require.Equal(t, payloadsA, receivedA)

	position := pubA.Position()
	bitsToShift := logbuffer.PositionBitsToShift(termLength)
	termID := logbuffer.ComputeTermIDFromPosition(position, bitsToShift, initialTermID)
	termOffset := logbuffer.ComputeTermOffsetFromPosition(position, bitsToShift)

	require.NoError(t, sub.AddDestination(destB))

	pubBChannel := exclusiveUDPChannel(
		endpointB,
		sessionID,
		initialTermID,
		termID,
		termOffset,
		termLength,
		mtu,
	)
	pubB, err := client.AddExclusivePublication(streamID, pubBChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close multi-destination publication B", pubB.Close)
	require.Equal(t, sessionID, pubB.SessionID())
	harness.WaitUntilConnected(t, pubB)

	requireMergedImage(t, sub, imageCorrelationID, sessionID)

	payloadsB := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xD1)
	offerPayloads(t, client, pubB, payloadsB)
	receivedB := harness.PollDelivered(t, client, sub, messageCount)
	require.Equal(t, payloadsB, receivedB)

	requireMergedImage(t, sub, imageCorrelationID, sessionID)

	// Keep pubA open. RemoveDestination alone must stop A delivery.
	require.NoError(t, sub.RemoveDestination(destA))

	payloadsFromAAfterRemove := harness.DistinctIPCPayloads(afterRemoveCount, payloadSize, 0xD3)
	offerPayloads(t, client, pubA, payloadsFromAAfterRemove)
	requireNoFragments(t, client, sub, 250*time.Millisecond)

	// Traffic on B continues after the A destination is removed.
	morePayloads := harness.DistinctIPCPayloads(afterRemoveCount, payloadSize, 0xD2)
	offerPayloads(t, client, pubB, morePayloads)
	receivedMore := harness.PollDelivered(t, client, sub, len(morePayloads))
	require.Equal(t, morePayloads, receivedMore)

	requireMergedImage(t, sub, imageCorrelationID, sessionID)

	finishPubSubIPCTest(t, md, hadClientError)
}

// offerPayloads offers each payload with retry and drives the client duty cycle.
func offerPayloads(
	t *testing.T,
	client *aeron.Client,
	pub *aeron.ExclusivePublication,
	payloads [][]byte,
) {
	t.Helper()

	for _, payload := range payloads {
		require.Positive(t, harness.OfferWithRetry(t, pub, payload))
		client.DoWork()
	}
}

// requireMergedImage asserts one image and sticky correlation/session identity.
func requireMergedImage(
	t *testing.T,
	sub *aeron.Subscription,
	wantCorrelationID int64,
	wantSessionID int32,
) {
	t.Helper()

	require.Equal(t, 1, sub.ImageCount())
	image := harness.RequireFirstImage(t, sub)
	require.Equal(t, wantCorrelationID, image.CorrelationID())
	require.Equal(t, wantSessionID, image.SessionID())
}

// requireNoFragments fails when any fragment arrives on sub within bound.
func requireNoFragments(
	t *testing.T,
	client *aeron.Client,
	sub *aeron.Subscription,
	bound time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		client.DoWork()
		fragments := sub.Poll(
			func(_ atomicbuffer.AtomicBuffer, _, _ int, _ *aeron.Header) {},
			10,
		)
		require.Zero(t, fragments, "unexpected fragment after destination remove")
		time.Sleep(5 * time.Millisecond)
	}
}

func exclusiveUDPChannel(
	endpoint string,
	sessionID, initialTermID, termID, termOffset int32,
	termLength, mtu int,
) string {
	const format = "aeron:udp?endpoint=%s|session-id=%d|init-term-id=%d|" +
		"term-id=%d|term-offset=%d|term-length=%d|mtu=%d"
	return fmt.Sprintf(
		format,
		endpoint,
		sessionID,
		initialTermID,
		termID,
		termOffset,
		termLength,
		mtu,
	)
}
