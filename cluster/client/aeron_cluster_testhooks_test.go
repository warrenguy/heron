//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

var aeronClusterPollZeroAllocSink int64

func setupAeronClusterWithPendingEgressFragment(
	t *testing.T,
	payload []byte,
) (*AeronCluster, *aeron.Image, *FragmentAssembler) {
	t.Helper()

	sub, image, termBuffer := setupEgressPollFixtures(t)
	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		payload,
	)
	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire)[:1])

	ac := newAeronClusterWithEgressImage(t, sub, image)
	assembler := ac.egressAdapter.assembler
	require.Equal(t, 1, ac.PollEgress())
	require.Contains(t, assembler.builders, hookFrameSessionID)
	return ac, image, assembler
}

// TestNewAeronCluster_rejectsClosedEgressImage lives here because it needs
// ImageInternalCloseForHooks to retire the connect-time egress image.
func TestNewAeronCluster_rejectsClosedEgressImage(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	// export_test hook is ImageInternalCloseForTest; cluster tests use ImageInternalCloseForHooks.
	aeron.ImageInternalCloseForHooks(image)

	result := newConnectResultWithEgressImage(t, sub, image)
	_, err := NewAeronCluster(result)
	require.EqualError(t, err, "cluster client: egress image is closed")
}

func TestAeronCluster_PollEgress_dispatchesMessage(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)

	appPayload := []byte("session-poll-payload")
	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionMessageHeaderWire(
			t,
			egressTestClusterSessionID,
			egressTestMessageTimestamp,
			appPayload,
		),
	)

	ac := newAeronClusterWithEgressImage(t, sub, image)
	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)

	require.Equal(t, 1, ac.PollEgress())
	requireDispatchedSessionMessage(
		t,
		rec,
		appPayload,
		logbuffer.DataHeaderLength+SessionHeaderLength(),
	)
}

func TestAeronCluster_PollEgress_imageUnavailableClearsAssembler(t *testing.T) {
	ac, image, assembler := setupAeronClusterWithPendingEgressFragment(
		t,
		[]byte("fragmented-teardown-payload"),
	)

	var teardownOps []string
	SetEgressAdapterOpRecorderForTest(func(op string) {
		teardownOps = append(teardownOps, op)
	})
	t.Cleanup(func() { SetEgressAdapterOpRecorderForTest(nil) })

	aeron.ImageInternalCloseForHooks(image)

	require.Equal(t, 0, ac.PollEgress())
	require.Equal(t, []string{"FreeSessionBuffer", "Clear"}, teardownOps)
	require.Nil(t, ac.egressImage)
	require.NotContains(t, assembler.builders, hookFrameSessionID)

	require.Equal(t, 0, ac.PollEgress())
	require.NotContains(t, assembler.builders, image.SessionID())
}

func TestAeronCluster_PollEgress_stableAfterTeardown(t *testing.T) {
	ac, image, assembler := setupAeronClusterWithPendingEgressFragment(
		t,
		[]byte("idempotent-teardown"),
	)

	aeron.ImageInternalCloseForHooks(image)
	require.Equal(t, 0, ac.PollEgress())
	require.Nil(t, ac.egressImage)
	require.NotContains(t, assembler.builders, hookFrameSessionID)

	require.Equal(t, 0, ac.PollEgress())
	require.Nil(t, ac.egressImage)
	require.NotContains(t, assembler.builders, hookFrameSessionID)
}

func TestAeronCluster_Close_afterPollTeardown_onlyClearsAdapter(t *testing.T) {
	ac, image, _ := setupAeronClusterWithPendingEgressFragment(
		t,
		[]byte("close-after-teardown"),
	)

	var teardownOps []string
	SetEgressAdapterOpRecorderForTest(func(op string) {
		teardownOps = append(teardownOps, op)
	})
	t.Cleanup(func() { SetEgressAdapterOpRecorderForTest(nil) })

	aeron.ImageInternalCloseForHooks(image)

	require.Equal(t, 0, ac.PollEgress())
	require.Equal(t, []string{"FreeSessionBuffer", "Clear"}, teardownOps)
	teardownOps = nil

	require.NoError(t, ac.Close())
	require.Equal(t, []string{"Clear"}, teardownOps)
}

func TestAeronCluster_PollEgress_continuesPollingAfterTeardown(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)

	ac := newAeronClusterWithEgressImage(t, sub, image)
	aeron.ImageInternalCloseForHooks(image)
	require.Equal(t, 0, ac.PollEgress())
	require.Nil(t, ac.egressImage)

	image2 := aeron.NewTestImageForSubscriptionHooksWithCorrelation(
		t,
		sub,
		0,
		9103,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image2))
	termBuffer2 := aeron.ImageTermBufferForHooks(image2)
	appPayload := []byte("post-teardown-payload")
	writeUnfragmentedWireToTerm(
		t,
		termBuffer2,
		encodeSessionMessageHeaderWire(
			t,
			egressTestClusterSessionID,
			egressTestMessageTimestamp,
			appPayload,
		),
	)

	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)
	require.Equal(t, 1, ac.PollEgress())
	requireDispatchedSessionMessage(
		t,
		rec,
		appPayload,
		logbuffer.DataHeaderLength+SessionHeaderLength(),
	)
}

func TestAeronCluster_PollEgress_ZeroAllocs(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)

	appPayload := []byte("zero-alloc-egress")
	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionMessageHeaderWire(
			t,
			egressTestClusterSessionID,
			egressTestMessageTimestamp,
			appPayload,
		),
	)

	result := newConnectResultWithEgressImage(t, sub, image)
	ac, err := NewAeronCluster(result)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	testutil.AssertZeroAllocs(t, "AeronCluster_PollEgress", 1000, func() {
		aeron.ImageSetSubscriberPositionForHooks(image, 0)
		aeronClusterPollZeroAllocSink = int64(ac.PollEgress())
	})
}

func TestAeronCluster_PollEgress_wrongSessionIgnored(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionEventWire(t, egressTestOtherSessionID),
	)

	ac := newAeronClusterWithEgressImage(t, sub, image)
	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)

	require.Equal(t, 1, ac.PollEgress())
	requireZeroRecordingEgressCalls(t, rec)
}
