//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestAeronHelpers_unfragmentedRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	ctx := context.Background()
	timeout := aeron.DefaultConnectedPollTimeout
	payload := []byte("aeron-helpers-smoke")

	pub, sub := harness.ConnectIPCPubSubClients(t, client, client, harness.AeronHelpersSmokeStreamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(len(payload), 1)

	position, err := aeron.OfferWithRetry(client, pub, payload, timeout)
	require.NoError(t, err)
	require.Positive(t, position)

	got, err := aeron.PollOneMessage(ctx, client, sub, timeout)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

func TestAeronHelpers_fragmentedRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	ctx := context.Background()
	timeout := aeron.DefaultConnectedPollTimeout

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

	position, err := aeron.OfferWithRetry(client, pub, payload, timeout)
	require.NoError(t, err)
	require.Positive(t, position)

	n, err := aeron.PollReassembled(ctx, client, sub, timeout, buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf[:n])

	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}
