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

func finishPubSubIPCTest(t *testing.T, md *harness.MediaDriver, clientErrors ...*atomic.Bool) {
	t.Helper()

	for _, hadClientError := range clientErrors {
		require.False(t, hadClientError.Load())
	}
	md.AssertProcessAlive(t)
}

// TestPubSubIPCRoundtrip proves IPC offer, poll delivery, and position advance on live aeronmd.
func TestPubSubIPCRoundtrip(t *testing.T) {
	md := harness.Shared(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const (
		streamID     = harness.PubSubRoundtripStreamID
		messageCount = 16
		payloadSize  = 64
	)
	payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xA0)

	pub, sub := harness.ConnectIPCPubSubClients(t, client, client, streamID)

	pubPosBefore := pub.Position()
	// Position advance uses aligned DATA frame length, not raw payload sum.
	wantAdvance := harness.AlignedOfferAdvance(payloadSize, messageCount)

	for _, payload := range payloads {
		require.Positive(t, pub.Offer(payload))
		client.DoWork()
	}

	received := harness.PollDelivered(t, client, sub, messageCount, 0)

	require.Equal(t, payloads, received)

	harness.AssertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestPubSubIPCTwoClientsRoundtrip proves split-client IPC offer, poll delivery,
// and position advance on live aeronmd.
func TestPubSubIPCTwoClientsRoundtrip(t *testing.T) {
	md := harness.Shared(t)

	splitClientCtx := aeron.Context{
		InterServiceTimeout: harness.ConnectedPollTimeout,
	}
	pubClient, hadPubClientError := harness.ConnectIPCClientContext(t, md, splitClientCtx)
	subClient, hadSubClientError := harness.ConnectIPCClientContext(t, md, splitClientCtx)

	const (
		streamID     = harness.PubSubTwoClientsRoundtripStreamID
		messageCount = 16
		payloadSize  = 64
	)
	payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xC0)

	pub, sub := harness.ConnectIPCPubSubClients(t, pubClient, subClient, streamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(payloadSize, messageCount)

	for _, payload := range payloads {
		require.Positive(t, pub.Offer(payload))
		pubClient.DoWork()
		subClient.DoWork()
	}

	received := harness.PollDeliveredSplit(t, pubClient, subClient, sub, messageCount, 0)
	require.Equal(t, payloads, received)

	harness.AssertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadPubClientError, hadSubClientError)
}

// TestPubSubIPCFragmentedRoundtrip proves fragmented Offer, PollReassembled delivery,
// and aligned position advance on live aeronmd.
func TestPubSubIPCFragmentedRoundtrip(t *testing.T) {
	md := harness.Shared(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const (
		streamID     = harness.PubSubFragmentedRoundtripStreamID
		messageCount = 1
	)

	pub, sub := harness.ConnectIPCPubSubClients(t, client, client, streamID)

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

	harness.AssertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestPublicationTermRollover proves sustained IPC offers cross a term boundary
// without permanent stall and deliver all payloads in order on live aeronmd.
func TestPublicationTermRollover(t *testing.T) {
	md := harness.StartWithConfig(t, harness.Config{Env: harness.RolloverDriverEnv()})
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const (
		streamID    = harness.PubSubTermRolloverStreamID
		payloadSize = 256
	)

	termDataLength, err := harness.TermDataLengthFromCmdEnv(md)
	require.NoError(t, err)
	require.Equal(t, logbuffer.TermMinLength, termDataLength)
	messagesToRollover := harness.ComputeRolloverMessageCount(termDataLength, payloadSize)

	payloads := harness.DistinctIPCPayloads(messagesToRollover, payloadSize, 0xB0)

	pub, sub := harness.ConnectIPCPubSubClients(t, client, client, streamID)

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
	harness.AssertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, actualAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}
