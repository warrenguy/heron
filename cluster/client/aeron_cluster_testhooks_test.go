//go:build testhooks

package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	agronaatomic "github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/internal/testutil"
)

var aeronClusterPollZeroAllocSink int64

const replacementEgressImageCorrelation int64 = 9103

type egressAdapterOpLog struct {
	Ops []string
}

func setupEgressAdapterOpRecorder(t *testing.T) *egressAdapterOpLog {
	t.Helper()

	recorder := &egressAdapterOpLog{}
	SetEgressAdapterOpRecorderForTest(func(op string) {
		recorder.Ops = append(recorder.Ops, op)
	})
	t.Cleanup(func() { SetEgressAdapterOpRecorderForTest(nil) })
	return recorder
}

func writeDefaultSessionMessageToTerm(
	t *testing.T,
	termBuffer agronaatomic.AtomicBuffer,
	appPayload []byte,
) {
	t.Helper()

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
}

func requireAeronClusterDispatchedSessionMessage(
	t *testing.T,
	ac *AeronCluster,
	appPayload []byte,
) {
	t.Helper()

	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)
	requireDispatchedSessionMessage(
		t,
		rec,
		appPayload,
		logbuffer.DataHeaderLength+SessionHeaderLength(),
	)
}

func addReplacementEgressImage(t *testing.T, sub *aeron.Subscription) *aeron.Image {
	t.Helper()

	image := aeron.NewTestImageForSubscriptionHooksWithCorrelation(
		t,
		sub,
		0,
		replacementEgressImageCorrelation,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return image
}

func closeEgressImageAndRequirePollWork(t *testing.T, ac *AeronCluster, image *aeron.Image) {
	t.Helper()

	aeron.ImageInternalCloseForHooks(image)
	require.Positive(t, ac.PollEgress())
}

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
	writeFirstSplitWireFragmentToTerm(t, termBuffer, wire)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
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
	writeDefaultSessionMessageToTerm(t, termBuffer, appPayload)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)

	require.Equal(t, 1, ac.PollEgress())
	requireAeronClusterDispatchedSessionMessage(t, ac, appPayload)
}

func TestAeronCluster_PollEgress_imageUnavailableClearsAssembler(t *testing.T) {
	ac, image, assembler := setupAeronClusterWithPendingEgressFragment(
		t,
		[]byte("fragmented-teardown-payload"),
	)

	teardownRecorder := setupEgressAdapterOpRecorder(t)

	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Equal(t, []string{"FreeSessionBuffer", "Clear"}, teardownRecorder.Ops)
	require.Nil(t, ac.egressImage)
	require.NotContains(t, assembler.builders, hookFrameSessionID)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.IngressPublication())

	require.Equal(t, 0, ac.PollEgress())
	require.NotContains(t, assembler.builders, image.SessionID())
}

func TestAeronCluster_PollEgress_replacementClosesDuringPoll_entersAwaitNewLeader(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	replacement := addReplacementEgressImage(t, sub)
	replacementTerm := aeron.ImageTermBufferForHooks(replacement)
	writeDefaultSessionMessageToTerm(t, replacementTerm, []byte("close-replacement-during-poll"))

	var closedReplacementDuringPoll bool
	ac := newAeronClusterWithEgressImage(t, sub, image, func(result *ConnectResult) {
		rec := newRecordingEgressHandlers()
		recordMessage := rec.MessageFunc
		rec.MessageFunc = func(
			clusterSessionID int64,
			timestamp int64,
			buffer agronaatomic.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) {
			if !closedReplacementDuringPoll {
				closedReplacementDuringPoll = true
				aeron.ImageInternalCloseForHooks(replacement)
			}
			recordMessage(clusterSessionID, timestamp, buffer, offset, length, header)
		}
		result.Context.EgressHandler = rec
	})
	aeron.ImageInternalCloseForHooks(image)

	require.Positive(t, ac.PollEgress())
	require.True(t, closedReplacementDuringPoll)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
}

func TestAeronCluster_PollEgress_closeDuringPoll_entersAwaitNewLeader(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	writeDefaultSessionMessageToTerm(t, termBuffer, []byte("close-during-poll"))

	var closedDuringPoll bool
	ac := newAeronClusterWithEgressImage(t, sub, image, func(result *ConnectResult) {
		rec := newRecordingEgressHandlers()
		recordMessage := rec.MessageFunc
		rec.MessageFunc = func(
			clusterSessionID int64,
			timestamp int64,
			buffer agronaatomic.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) {
			if !closedDuringPoll {
				closedDuringPoll = true
				aeron.ImageInternalCloseForHooks(image)
			}
			recordMessage(clusterSessionID, timestamp, buffer, offset, length, header)
		}
		result.Context.EgressHandler = rec
	})

	require.NotNil(t, ac.IngressPublication())
	require.Positive(t, ac.PollEgress())
	require.True(t, closedDuringPoll)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
}

func TestAeronCluster_PollEgress_closedEgressImage_entersAwaitNewLeader(t *testing.T) {
	const startNs int64 = 9_000_000_000
	timeout := 8 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	sub, image, _ := setupEgressPollFixtures(t)
	result := newConnectResultWithEgressImage(t, sub, image)
	mutateConnectContext(result, func(ctx *ClusterContext) {
		ctx.NewLeaderTimeout = timeout
	})
	withAtomicNanoClock(result, &nowNs)
	ac := newAeronClusterFromConnectResult(t, result)

	require.NotNil(t, ac.IngressPublication())
	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
}

func TestAeronCluster_PollEgress_stableAfterTeardown(t *testing.T) {
	ac, image, assembler := setupAeronClusterWithPendingEgressFragment(
		t,
		[]byte("idempotent-teardown"),
	)

	closeEgressImageAndRequirePollWork(t, ac, image)
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

	teardownRecorder := setupEgressAdapterOpRecorder(t)

	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Equal(t, []string{"FreeSessionBuffer", "Clear"}, teardownRecorder.Ops)
	teardownRecorder.Ops = nil

	require.NoError(t, ac.Close())
	require.Equal(t, []string{"Clear"}, teardownRecorder.Ops)
}

func TestAeronCluster_PollEgress_samePollReattach_staysConnected(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	addReplacementEgressImage(t, sub)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	ingressBefore := ac.IngressPublication()

	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.NotNil(t, ac.egressImage)
	require.False(t, ac.egressImage.IsClosed())
	require.Same(t, ingressBefore, ac.IngressPublication())
}

func TestAeronCluster_PollEgress_closedTracked_replacementFragmentDelivered(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	replacement := addReplacementEgressImage(t, sub)
	replacementTerm := aeron.ImageTermBufferForHooks(replacement)

	appPayload := []byte("replacement-fragment-payload")
	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		appPayload,
	)
	writeFirstSplitWireFragmentToTerm(t, replacementTerm, wire)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	aeron.ImageInternalCloseForHooks(image)

	require.Positive(t, ac.PollEgress())
	require.NotNil(t, ac.egressImage)
	require.False(t, ac.egressImage.IsClosed())

	appendRemainingSplitWireFragmentsToTerm(t, replacementTerm, wire)

	require.Positive(t, ac.PollEgress())

	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)
	requireDispatchedSessionMessage(t, rec, appPayload, SessionHeaderLength())
}

func TestAeronCluster_PollEgress_continuesPollingAfterTeardown(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Nil(t, ac.egressImage)

	image2 := addReplacementEgressImage(t, sub)
	termBuffer2 := aeron.ImageTermBufferForHooks(image2)
	appPayload := []byte("post-teardown-payload")
	writeDefaultSessionMessageToTerm(t, termBuffer2, appPayload)

	require.Equal(t, 1, ac.PollEgress())
	requireAeronClusterDispatchedSessionMessage(t, ac, appPayload)
}

func TestAeronCluster_PollEgress_ZeroAllocs(t *testing.T) {
	// CONNECTED path: pollLeaderRecovery and pollStateChanges return 0 without alloc.
	sub, image, termBuffer := setupEgressPollFixtures(t)

	appPayload := []byte("zero-alloc-egress")
	writeDefaultSessionMessageToTerm(t, termBuffer, appPayload)

	ac := newAeronClusterFromConnectResult(t, newConnectResultWithEgressImage(t, sub, image))

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

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)

	require.Equal(t, 1, ac.PollEgress())
	requireZeroRecordingEgressCalls(t, rec)
}
