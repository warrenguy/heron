//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestAeronHelpers_unfragmentedRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	ctx := t.Context()
	payload := []byte("aeron-helpers-smoke")

	pub, sub := harness.ConnectIPCPubSubClients(t, client, client, harness.AeronHelpersSmokeStreamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(len(payload), 1)

	position, err := client.OfferWithRetry(ctx, pub, payload)
	require.NoError(t, err)
	require.Positive(t, position)

	got, err := client.PollOneMessage(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

func TestAeronHelpers_fragmentedRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	ctx := t.Context()

	pub, sub := harness.ConnectIPCPubSubClients(
		t,
		client,
		client,
		harness.AeronHelpersFragmentedSmokeStreamID,
	)

	maxPayload := pub.MaxPayloadLength()
	payload := harness.FragmentedRoundtripPayload(maxPayload, harness.FragmentedRoundtripExtraBytes)
	buf := make([]byte, len(payload))

	pubPosBefore := pub.Position()
	wantAdvance := int64(logbuffer.ComputeFragmentedFrameLength(len(payload), maxPayload))

	position, err := client.OfferWithRetry(ctx, pub, payload)
	require.NoError(t, err)
	require.Positive(t, position)

	n, err := client.PollReassembled(ctx, sub, buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf[:n])

	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}
