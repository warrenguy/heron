//go:build testhooks

package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
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
	termBuffer atomicbuffer.AtomicBuffer,
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

	rec := ac.cfg.EgressHandler.(*recordingEgressHandlers)
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

func writePendingEgressFragmentToTerm(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	appPayload []byte,
) []byte {
	t.Helper()

	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		appPayload,
	)
	writeFirstSplitWireFragmentToTerm(t, termBuffer, wire)
	return wire
}

func requireFragmentAssemblerHasBuilder(
	t *testing.T,
	assembler *aeron.FragmentAssembler,
	sessionID int32,
	want bool,
) {
	t.Helper()

	require.Equal(t, want, aeron.FragmentAssemblerHasBuilderForTest(assembler, sessionID))
}

func requireAwaitNewLeaderWithoutIngress(t *testing.T, ac *AeronCluster) {
	t.Helper()

	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
}

func mutateConnectResultClosingImageOnFirstMessage(
	imageToClose *aeron.Image,
	closed *bool,
) func(*ConnectResult) {
	return func(result *ConnectResult) {
		rec := newRecordingEgressHandlers()
		recordMessage := rec.MessageFunc
		rec.MessageFunc = func(
			clusterSessionID int64,
			timestamp int64,
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) {
			if !*closed {
				*closed = true
				aeron.ImageInternalCloseForHooks(imageToClose)
			}
			recordMessage(clusterSessionID, timestamp, buffer, offset, length, header)
		}
		result.Config.EgressHandler = rec
	}
}

func setupAeronClusterWithPendingEgressFragment(
	t *testing.T,
	payload []byte,
) (*AeronCluster, *aeron.Image, *aeron.FragmentAssembler) {
	t.Helper()

	sub, image, termBuffer := setupEgressPollFixtures(t)
	writePendingEgressFragmentToTerm(t, termBuffer, payload)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	assembler := ac.egressAdapter.assembler
	require.Equal(t, 1, ac.PollEgress())
	requireFragmentAssemblerHasBuilder(t, assembler, hookFrameSessionID, true)
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
	requireFragmentAssemblerHasBuilder(t, assembler, hookFrameSessionID, false)
	requireAwaitNewLeaderWithoutIngress(t, ac)

	require.Equal(t, 0, ac.PollEgress())
	requireFragmentAssemblerHasBuilder(t, assembler, image.SessionID(), false)
}

func TestAeronCluster_PollEgress_replacementClosesDuringPoll_entersAwaitNewLeader(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	replacement := addReplacementEgressImage(t, sub)
	replacementTerm := aeron.ImageTermBufferForHooks(replacement)
	writeDefaultSessionMessageToTerm(t, replacementTerm, []byte("close-replacement-during-poll"))

	var closedReplacementDuringPoll bool
	ac := newAeronClusterWithEgressImage(
		t,
		sub,
		image,
		mutateConnectResultClosingImageOnFirstMessage(replacement, &closedReplacementDuringPoll),
	)
	aeron.ImageInternalCloseForHooks(image)

	require.Positive(t, ac.PollEgress())
	require.True(t, closedReplacementDuringPoll)
	requireAwaitNewLeaderWithoutIngress(t, ac)
}

func TestAeronCluster_PollEgress_closeDuringPoll_entersAwaitNewLeader(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	writeDefaultSessionMessageToTerm(t, termBuffer, []byte("close-during-poll"))

	var closedDuringPoll bool
	ac := newAeronClusterWithEgressImage(
		t,
		sub,
		image,
		mutateConnectResultClosingImageOnFirstMessage(image, &closedDuringPoll),
	)

	require.NotNil(t, ac.IngressPublication())
	require.Positive(t, ac.PollEgress())
	require.True(t, closedDuringPoll)
	requireAwaitNewLeaderWithoutIngress(t, ac)
}

func TestAeronCluster_PollEgress_closedEgressImage_entersAwaitNewLeader(t *testing.T) {
	const startNs int64 = 9_000_000_000
	timeout := 8 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	sub, image, _ := setupEgressPollFixtures(t)
	result := newConnectResultWithEgressImage(t, sub, image)
	mutateConnectConfig(result, func(cfg *Config) {
		cfg.NewLeaderTimeout = timeout
	})
	withAtomicNanoClock(result, &nowNs)
	ac := newAeronClusterFromConnectResult(t, result)

	require.NotNil(t, ac.IngressPublication())
	closeEgressImageAndRequirePollWork(t, ac, image)
	requireAwaitNewLeaderWithoutIngress(t, ac)
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
}

func TestAeronCluster_PollEgress_stableAfterTeardown(t *testing.T) {
	ac, image, assembler := setupAeronClusterWithPendingEgressFragment(
		t,
		[]byte("idempotent-teardown"),
	)

	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Nil(t, ac.egressImage)
	requireFragmentAssemblerHasBuilder(t, assembler, hookFrameSessionID, false)

	require.Equal(t, 0, ac.PollEgress())
	require.Nil(t, ac.egressImage)
	requireFragmentAssemblerHasBuilder(t, assembler, hookFrameSessionID, false)
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
	wire := writePendingEgressFragmentToTerm(t, replacementTerm, appPayload)

	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	aeron.ImageInternalCloseForHooks(image)

	require.Positive(t, ac.PollEgress())
	require.NotNil(t, ac.egressImage)
	require.False(t, ac.egressImage.IsClosed())

	appendRemainingSplitWireFragmentsToTerm(t, replacementTerm, wire)

	require.Positive(t, ac.PollEgress())

	rec := ac.cfg.EgressHandler.(*recordingEgressHandlers)
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
	rec := ac.cfg.EgressHandler.(*recordingEgressHandlers)

	require.Equal(t, 1, ac.PollEgress())
	requireZeroRecordingEgressCalls(t, rec)
}
