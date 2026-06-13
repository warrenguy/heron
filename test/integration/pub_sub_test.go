//go:build integration

package integration_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

func connectIPCClient(t *testing.T, md *harness.MediaDriver) (*aeron.Client, *atomic.Bool) {
	t.Helper()

	var hadClientError atomic.Bool
	client := harness.ConnectClient(t, md, aeron.Context{
		ErrorHandler: func(err error) {
			hadClientError.Store(true)
			t.Errorf("client error: %v", err)
		},
	})
	return client, &hadClientError
}

func setupIPCPubSub(
	t *testing.T,
	client *aeron.Client,
	streamID int32,
) (*aeron.Publication, *aeron.Subscription) {
	t.Helper()

	pub, err := client.AddPublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close subscription", sub.Close)

	// Poll publication connect before image attach; harness does not use WaitForIPCLinkConnected.
	harness.PollUntilConnected(t, client, pub, harness.ConnectedPollTimeout)
	harness.PollUntilImageCount(t, client, sub, 1, harness.ConnectedPollTimeout)
	return pub, sub
}

func assertPubSubIPCMetadata(
	t *testing.T,
	streamID int32,
	pub *aeron.Publication,
	sub *aeron.Subscription,
	pubPosBefore int64,
	wantAdvance int64,
) {
	t.Helper()

	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)

	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	require.Equal(t, streamID, pub.StreamID())
	require.Equal(t, streamID, sub.StreamID())
	require.Equal(t, streamID, image.StreamID())
	require.Equal(t, pub.SessionID(), image.SessionID())
}

func finishPubSubIPCTest(t *testing.T, md *harness.MediaDriver, hadClientError *atomic.Bool) {
	t.Helper()

	require.False(t, hadClientError.Load())
	md.AssertProcessAlive(t)
}

// TestPubSubIPCRoundtrip proves IPC offer, poll delivery, and position advance on live aeronmd.
func TestPubSubIPCRoundtrip(t *testing.T) {
	md := harness.Start(t)
	client, hadClientError := connectIPCClient(t, md)

	const (
		streamID     = harness.PubSubRoundtripStreamID
		messageCount = 16
		payloadSize  = 64
	)
	payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xA0)

	pub, sub := setupIPCPubSub(t, client, streamID)

	pubPosBefore := pub.Position()
	// Position advance uses aligned DATA frame length, not raw payload sum.
	wantAdvance := harness.AlignedOfferAdvance(payloadSize, messageCount)

	for _, payload := range payloads {
		require.Positive(t, pub.Offer(payload))
		client.DoWork()
	}

	received := harness.PollDelivered(t, client, sub, messageCount, 0)

	require.Equal(t, payloads, received)

	assertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestPubSubIPCFragmentedRoundtrip proves fragmented Offer, PollReassembled delivery,
// and aligned position advance on live aeronmd.
func TestPubSubIPCFragmentedRoundtrip(t *testing.T) {
	md := harness.Start(t)
	client, hadClientError := connectIPCClient(t, md)

	const (
		streamID     = harness.PubSubFragmentedRoundtripStreamID
		messageCount = 1
	)

	pub, sub := setupIPCPubSub(t, client, streamID)

	maxPayload := pub.MaxPayloadLength()
	payload := harness.FragmentedRoundtripPayload(maxPayload, harness.FragmentedRoundtripExtraBytes)

	pubPosBefore := pub.Position()
	wantAdvance := int64(logbuffer.ComputeFragmentedFrameLength(len(payload), maxPayload))

	result := harness.OfferWithRetry(t, client, pub, payload, 0)
	require.Equal(t, pubPosBefore+wantAdvance, result)
	client.DoWork()

	received, fragments := harness.PollReassembled(t, client, sub, messageCount, 0)
	require.Equal(t, [][]byte{payload}, received)
	require.Equal(t, 2, fragments)

	assertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestPublicationTermRollover proves sustained IPC offers cross a term boundary
// without permanent stall and deliver all payloads in order on live aeronmd.
func TestPublicationTermRollover(t *testing.T) {
	md := harness.StartWithConfig(t, harness.Config{Env: harness.RolloverDriverEnv()})
	client, hadClientError := connectIPCClient(t, md)

	const (
		streamID    = harness.PubSubTermRolloverStreamID
		payloadSize = 256
	)

	termDataLength, err := harness.TermDataLengthFromCmdEnv(md)
	require.NoError(t, err)
	require.Equal(t, logbuffer.TermMinLength, termDataLength)
	messagesToRollover := harness.ComputeRolloverMessageCount(termDataLength, payloadSize)

	payloads := harness.DistinctIPCPayloads(messagesToRollover, payloadSize, 0xB0)

	pub, sub := setupIPCPubSub(t, client, streamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(payloadSize, messagesToRollover)
	shift := logbuffer.PositionBitsToShift(termDataLength)

	sawBackpressureOrAdmin := false
	prevPosition := pubPosBefore
	received := make([][]byte, 0, messagesToRollover)
	for _, payload := range payloads {
		result, sawTransient := harness.OfferWithRetryWitness(
			t,
			client,
			pub,
			payload,
			harness.ConnectedPollTimeout,
		)
		sawBackpressureOrAdmin = sawBackpressureOrAdmin || sawTransient
		require.Greater(t, result, prevPosition)
		prevPosition = result
		client.DoWork()
		// Drain the subscriber during the burst so sustained BackPressured cannot stall
		// offers on 64 KiB terms with a large message count.
		received = harness.CollectSubscriptionFragments(sub, messagesToRollover, received)
	}

	require.GreaterOrEqual(
		t,
		(pub.Position()>>shift)-(pubPosBefore>>shift),
		int64(1),
		"publication position did not cross a term boundary (termDataLength=%d)",
		termDataLength,
	)
	require.True(
		t,
		sawBackpressureOrAdmin,
		"expected transient BackPressured or AdminAction during rollover burst",
	)

	if remaining := messagesToRollover - len(received); remaining > 0 {
		tail := harness.PollDelivered(t, client, sub, remaining, 0)
		received = append(received, tail...)
	}
	require.Equal(t, payloads, received)

	actualAdvance := pub.Position() - pubPosBefore
	require.GreaterOrEqual(
		t,
		actualAdvance,
		wantAdvance,
		"publication position should advance by at least %d bytes (wantAdvance)",
		wantAdvance,
	)
	assertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, actualAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}
