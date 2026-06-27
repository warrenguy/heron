//go:build testhooks

package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs/gensbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	agronaatomic "github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/codecs"
)

const (
	// pollEgressLeaderRecoverySpinBudget caps PollEgress recovery spins in hooks tests.
	// Idle spins may block up to the harness pollRing timeout when using RespondPendingAdds;
	// wall time is capped by pollEgressLeaderRecoveryTimeout.
	pollEgressLeaderRecoverySpinBudget = 300
	pollEgressLeaderRecoveryTimeout    = 10 * time.Second
)

func newMultiMemberWireRecoveryCluster(
	t *testing.T,
	sub *aeron.Subscription,
	image *aeron.Image,
	mutate func(*ConnectResult),
) (*AeronCluster, *aeron.AsyncConnectTestHarness, IngressPublication, *recordingEgressHandlers) {
	t.Helper()

	harness := aeron.NewAsyncConnectTestHarness(t)
	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	result := newMultiMemberConnectResult(t, connectFixtureLeaderMemberID, leaderPub)
	result.Aeron = harness.Client
	result.EgressSubscription = sub
	result.EgressImage = image
	ac := newAeronClusterFromConnectFixtureResult(t, result, mutate)
	rec, ok := ac.ctx.EgressHandler.(*recordingEgressHandlers)
	require.True(t, ok, "wire recovery cluster requires recording egress handler")
	return ac, harness, leaderPub, rec
}

func writeNewLeaderEventToEgressTerm(
	t *testing.T,
	termBuffer agronaatomic.AtomicBuffer,
	leaderMemberID int32,
	endpoints []byte,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeNewLeaderWireWithEndpoints(
			t,
			egressTestClusterSessionID,
			leaderMemberID,
			endpoints,
		),
	)
}

func writeSessionEventRedirectToEgressTerm(
	t *testing.T,
	termBuffer agronaatomic.AtomicBuffer,
	correlationID int64,
	endpoints string,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionEventRedirectWire(t, correlationID, endpoints),
	)
}

func pollEgressUntilLeaderConnectedPending(
	t *testing.T,
	ac *AeronCluster,
	harness *aeron.AsyncConnectTestHarness,
	blockingPendingAdds bool,
) {
	t.Helper()

	deadline := time.Now().Add(pollEgressLeaderRecoveryTimeout)
	var spins int
	for spins = 0; spins < pollEgressLeaderRecoverySpinBudget && time.Now().Before(deadline); spins++ {
		if ac.SessionState() == SessionStateConnected && ac.IngressPublication() != nil {
			return
		}
		ac.PollEgress()
		if harness != nil {
			for range 4 {
				harness.DoWork()
			}
			if blockingPendingAdds {
				harness.RespondPendingAdds()
			} else {
				harness.TryRespondPendingAdds()
			}
		}
	}
	t.Fatalf(
		"timed out after %d spins (state=%s ingress=%v)",
		spins,
		ac.SessionState(),
		ac.IngressPublication(),
	)
}

func pollEgressLeaderRecoveryOnce(
	t *testing.T,
	ac *AeronCluster,
	harness *aeron.AsyncConnectTestHarness,
) {
	t.Helper()

	require.Positive(t, ac.PollEgress())
	if harness != nil {
		harness.DoWork()
	}
}

func applyWireNewLeaderAndPoll(
	t *testing.T,
	ac *AeronCluster,
	termBuffer agronaatomic.AtomicBuffer,
	endpoints []byte,
) {
	t.Helper()

	writeNewLeaderEventToEgressTerm(t, termBuffer, connectFixtureLeaderMemberID, endpoints)
	require.Positive(t, ac.PollEgress())
}

func applyWireRedirectAndPoll(
	t *testing.T,
	ac *AeronCluster,
	termBuffer agronaatomic.AtomicBuffer,
	correlationID int64,
	endpoints string,
) {
	t.Helper()

	writeSessionEventRedirectToEgressTerm(t, termBuffer, correlationID, endpoints)
	require.Positive(t, ac.PollEgress())
}

func completeWireLeaderRecovery(
	t *testing.T,
	ac *AeronCluster,
	harness *aeron.AsyncConnectTestHarness,
	leaderMemberID int32,
	leaderPub ...IngressPublication,
) IngressPublication {
	t.Helper()

	var newLeaderPub IngressPublication
	if len(leaderPub) > 0 {
		newLeaderPub = leaderPub[0]
	} else {
		newLeaderPub = aeron.NewTestExclusivePublicationForHooks(t)
	}
	setLeaderPollAddPublication(t, ac, leaderMemberID, newLeaderPub)
	pollEgressUntilLeaderConnectedPending(t, ac, harness, false)
	return newLeaderPub
}

func channelOnlyMemberIngressPublication(t *testing.T, ac *AeronCluster) IngressPublication {
	t.Helper()

	member0, ok := ac.MemberIngressMap().Lookup(channelOnlyMemberID)
	require.True(t, ok)
	return member0.Publication
}

func requireWireLeaderRecoveryConnected(
	t *testing.T,
	ac *AeronCluster,
	newLeaderPub IngressPublication,
	oldLeaderPub, member0Pub IngressPublication,
) {
	t.Helper()

	require.True(t, oldLeaderPub.IsClosed())
	require.True(t, member0Pub.IsClosed())
	leader, ok := ac.MemberIngressMap().Lookup(connectFixtureLeaderMemberID)
	require.True(t, ok)
	require.Equal(t, "host-c:20113", leader.Endpoint)
	require.Same(t, newLeaderPub, ac.IngressPublication())
}

func newWireRecoveryClusterWithInvalidIngress(
	t *testing.T,
	sub *aeron.Subscription,
	image *aeron.Image,
	startNs int64,
	timeout time.Duration,
) (*AeronCluster, IngressPublication, *recordingEgressHandlers) {
	t.Helper()

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	ac, _, oldLeaderPub, rec := newMultiMemberWireRecoveryCluster(
		t,
		sub,
		image,
		func(result *ConnectResult) {
			mutateConnectContext(result, func(ctx *ClusterContext) {
				ctx.IngressChannel = invalidIngressClusterContext().IngressChannel
				ctx.NewLeaderTimeout = timeout
			})
			withAtomicNanoClock(result, &nowNs)
		},
	)
	return ac, oldLeaderPub, rec
}

// setLeaderPollAddPublication injects a leader publication via poll-add hook; use for
// deterministic pub wiring. Harness-only recovery (DoWork + RespondPendingAdds) is
// covered by TestAeronCluster_NewLeader_entersConnectedViaHarnessPendingAdd.
func setLeaderPollAddPublication(
	t *testing.T,
	ac *AeronCluster,
	leaderMemberID int32,
	pub IngressPublication,
) {
	t.Helper()

	leader, ok := ac.MemberIngressMap().Lookup(leaderMemberID)
	require.True(t, ok)
	leader.SetAsyncPollAddHookForTest(t, func(
		ing *MemberIngress,
		_ ClusterContext,
		_ *aeron.Client,
	) (bool, error) {
		ing.RegistrationID = conductor.NullCorrelationID
		ing.Publication = pub
		return true, nil
	})
}

func requireExclusivePubFirstOfferIsSessionKeepAlive(
	t *testing.T,
	pub IngressPublication,
	wantClusterSessionID int64,
	wantLeadershipTermID int64,
) {
	t.Helper()

	ep, ok := pub.(*aeron.ExclusivePublication)
	require.True(t, ok)
	termBuf := ep.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)
	wire := termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLength]

	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, gensbe.SessionKeepAliveSbeTemplateID, decoded.TemplateID)

	fields, ok := decoded.Fields.(codecs.SessionKeepAliveFields)
	require.True(t, ok)
	require.Equal(t, wantClusterSessionID, fields.ClusterSessionID)
	require.Equal(t, wantLeadershipTermID, fields.LeadershipTermID)
}

func TestAeronCluster_NewLeader_rebuildsIngressMap(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, oldLeaderPub, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
	member0Pub := channelOnlyMemberIngressPublication(t, ac)

	applyWireNewLeaderAndPoll(t, ac, termBuffer, multiMemberEndpointsAC)
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
	require.Equal(t, int64(200), ac.LeadershipTermID())
	require.Equal(t, 1, rec.newLeaderCalls)
	require.Equal(t, int64(200), rec.gotNewLeaderLeadershipTermID)
	require.Equal(t, connectFixtureLeaderMemberID, rec.gotNewLeaderLeaderMemberID)
	require.Equal(t, multiMemberEndpointsAC, rec.gotNewLeaderIngressEndpoints)

	newLeaderPub := completeWireLeaderRecovery(t, ac, harness, connectFixtureLeaderMemberID)
	requireWireLeaderRecoveryConnected(t, ac, newLeaderPub, oldLeaderPub, member0Pub)
}

func TestAeronCluster_NewLeader_sendsKeepAliveBeforeConnected(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, _, _ := newMultiMemberWireRecoveryCluster(t, sub, image, nil)

	applyWireNewLeaderAndPoll(t, ac, termBuffer, multiMemberEndpointsAC)
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())

	newLeaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	setLeaderPollAddPublication(t, ac, connectFixtureLeaderMemberID, newLeaderPub)
	pollEgressLeaderRecoveryOnce(t, ac, harness)
	require.Equal(t, SessionStateConnected, ac.SessionState())

	requireExclusivePubFirstOfferIsSessionKeepAlive(
		t,
		newLeaderPub,
		egressTestClusterSessionID,
		200,
	)
}

func TestAeronCluster_NewLeader_entersConnectedAfterKeepAlive(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, _, _ := newMultiMemberWireRecoveryCluster(t, sub, image, nil)

	applyWireNewLeaderAndPoll(t, ac, termBuffer, multiMemberEndpointsAC)
	completeWireLeaderRecovery(t, ac, harness, connectFixtureLeaderMemberID)

	require.Equal(t, SessionStateConnected, ac.SessionState())
	pos := ac.Offer([]byte("post-recovery"))
	require.Positive(t, pos)
}

func TestAeronCluster_NewLeader_entersConnectedViaHarnessPendingAdd(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, oldLeaderPub, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
	member0Pub := channelOnlyMemberIngressPublication(t, ac)

	applyWireNewLeaderAndPoll(t, ac, termBuffer, multiMemberEndpointsAC)
	require.Equal(t, 1, rec.newLeaderCalls)

	pollEgressUntilLeaderConnectedPending(t, ac, harness, true)

	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.NotNil(t, ac.IngressPublication())
	require.NotSame(t, oldLeaderPub, ac.IngressPublication())
	require.True(t, oldLeaderPub.IsClosed())
	require.True(t, member0Pub.IsClosed())
}

func TestAeronCluster_NewLeader_reattachesEgressImage(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	ac, harness, _, _ := newMultiMemberWireRecoveryCluster(t, sub, image, nil)

	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.egressImage)

	replacement := addReplacementEgressImage(t, sub)
	replacementTerm := aeron.ImageTermBufferForHooks(replacement)
	writeNewLeaderEventToEgressTerm(
		t,
		replacementTerm,
		connectFixtureLeaderMemberID,
		multiMemberEndpointsAC,
	)

	completeWireLeaderRecovery(t, ac, harness, connectFixtureLeaderMemberID)

	require.NotNil(t, ac.egressImage)
	require.False(t, ac.egressImage.IsClosed())
	require.NotSame(t, image, ac.egressImage)
}

func TestAeronCluster_SessionEvent_redirect_rebuildsMap(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, oldLeaderPub, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
	termBefore := ac.LeadershipTermID()
	member0Pub := channelOnlyMemberIngressPublication(t, ac)

	applyWireRedirectAndPoll(t, ac, termBuffer, 77, string(multiMemberEndpointsAC))
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, termBefore, ac.LeadershipTermID())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, EventCodeRedirect, rec.gotSessionEventCode)
	require.Equal(t, int64(77), rec.gotSessionEventCorrelationID)

	newLeaderPub := completeWireLeaderRecovery(t, ac, harness, connectFixtureLeaderMemberID)
	requireWireLeaderRecoveryConnected(t, ac, newLeaderPub, oldLeaderPub, member0Pub)
}

func TestAeronCluster_awaitNewLeader_timeout(t *testing.T) {
	const startNs int64 = 12_000_000_000
	timeout := 5 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	sub, image, _ := setupEgressPollFixtures(t)
	ac := newAeronClusterWithEgressImage(t, sub, image, func(result *ConnectResult) {
		mutateConnectContext(result, func(ctx *ClusterContext) {
			ctx.NewLeaderTimeout = timeout
		})
		withAtomicNanoClock(result, &nowNs)
	})

	closeEgressImageAndRequirePollWork(t, ac, image)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.False(t, ac.IsClosed())

	nowNs.Store(startNs + timeout.Nanoseconds() - 1)
	require.Equal(t, 0, ac.PollEgress())
	require.False(t, ac.IsClosed())

	nowNs.Store(startNs + timeout.Nanoseconds())
	require.Equal(t, 1, ac.PollEgress())
	require.True(t, ac.IsClosed())
}

func TestAeronCluster_PollEgress_redirectDuringSession(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, _, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
	require.Equal(t, SessionStateConnected, ac.SessionState())
	termBefore := ac.LeadershipTermID()

	applyWireRedirectAndPoll(t, ac, termBuffer, 88, string(multiMemberEndpointsAC))
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, EventCodeRedirect, rec.gotSessionEventCode)
	require.Equal(t, int64(88), rec.gotSessionEventCorrelationID)
	require.Equal(t, termBefore, ac.LeadershipTermID())

	completeWireLeaderRecovery(t, ac, harness, connectFixtureLeaderMemberID)

	require.Equal(t, SessionStateConnected, ac.SessionState())
	pos := ac.Offer([]byte("redirect-recovery"))
	require.Positive(t, pos)
}

func TestAeronCluster_PollEgress_ingressPlanError_noMutation(t *testing.T) {
	type wireEgressEvent struct {
		name  string
		apply func(
			t *testing.T,
			ac *AeronCluster,
			termBuffer agronaatomic.AtomicBuffer,
			endpoints []byte,
		)
		handlerCalls func(rec *recordingEgressHandlers) int
	}

	events := []wireEgressEvent{
		{
			name: "newLeader",
			apply: func(
				t *testing.T,
				ac *AeronCluster,
				termBuffer agronaatomic.AtomicBuffer,
				endpoints []byte,
			) {
				applyWireNewLeaderAndPoll(t, ac, termBuffer, endpoints)
			},
			handlerCalls: func(rec *recordingEgressHandlers) int { return rec.newLeaderCalls },
		},
		{
			name: "redirect",
			apply: func(
				t *testing.T,
				ac *AeronCluster,
				termBuffer agronaatomic.AtomicBuffer,
				endpoints []byte,
			) {
				applyWireRedirectAndPoll(t, ac, termBuffer, 77, string(endpoints))
			},
			handlerCalls: func(rec *recordingEgressHandlers) int { return rec.sessionEventCalls },
		},
	}

	for _, event := range events {
		for _, tc := range ingressPlanErrorCases {
			t.Run(event.name+"/"+tc.name, func(t *testing.T) {
				sub, image, termBuffer := setupEgressPollFixtures(t)
				ac, _, _, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
				before := snapshotSessionRecovery(ac)

				event.apply(t, ac, termBuffer, tc.endpoints)

				require.Zero(t, event.handlerCalls(rec))
				requireSessionRecoveryUnchanged(t, before, ac)
			})
		}
	}
}

func TestAeronCluster_PollEgress_applyError_entersAwaitNewLeader(t *testing.T) {
	const timeout = 6 * time.Second

	tests := []struct {
		name    string
		startNs int64
		apply   func(t *testing.T, ac *AeronCluster, termBuffer agronaatomic.AtomicBuffer)
		verify  func(t *testing.T, ac *AeronCluster, termBefore int64)
		calls   func(rec *recordingEgressHandlers) int
	}{
		{
			name:    "newLeader",
			startNs: 8_000_000_000,
			apply: func(t *testing.T, ac *AeronCluster, termBuffer agronaatomic.AtomicBuffer) {
				applyWireNewLeaderAndPoll(t, ac, termBuffer, multiMemberEndpointsAC)
			},
			verify: func(t *testing.T, ac *AeronCluster, _ int64) {
				require.Equal(t, int64(200), ac.LeadershipTermID())
			},
			calls: func(rec *recordingEgressHandlers) int { return rec.newLeaderCalls },
		},
		{
			name:    "redirect",
			startNs: 9_000_000_000,
			apply: func(t *testing.T, ac *AeronCluster, termBuffer agronaatomic.AtomicBuffer) {
				applyWireRedirectAndPoll(t, ac, termBuffer, 77, string(multiMemberEndpointsAC))
			},
			verify: func(t *testing.T, ac *AeronCluster, termBefore int64) {
				require.Equal(t, termBefore, ac.LeadershipTermID())
			},
			calls: func(rec *recordingEgressHandlers) int { return rec.sessionEventCalls },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sub, image, termBuffer := setupEgressPollFixtures(t)
			ac, oldLeaderPub, rec := newWireRecoveryClusterWithInvalidIngress(
				t,
				sub,
				image,
				tc.startNs,
				timeout,
			)
			termBefore := ac.LeadershipTermID()

			tc.apply(t, ac, termBuffer)

			require.Zero(t, tc.calls(rec))
			require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
			require.Equal(t, tc.startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
			require.Equal(t, connectFixtureLeaderMemberID, ac.LeaderMemberID())
			tc.verify(t, ac, termBefore)
			require.Nil(t, ac.IngressPublication())
			require.True(t, oldLeaderPub.IsClosed())

			leader, ok := ac.MemberIngressMap().Lookup(connectFixtureLeaderMemberID)
			require.True(t, ok)
			require.Equal(t, "host-c:20113", leader.Endpoint)
		})
	}
}

func TestAeronCluster_PollEgress_sessionEventClosed_pendingClose(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, _, _, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionEventClosedWire(t, egressTestClusterSessionID),
	)
	require.Positive(t, ac.PollEgress())

	require.Equal(t, SessionStatePendingClose, ac.SessionState())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, EventCodeClosed, rec.gotSessionEventCode)
}

func TestAeronCluster_PollEgress_sessionEventClosedThenNewLeader_sessionCloses(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, _, _, rec := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
	termBefore := ac.LeadershipTermID()

	writeTwoSequentialUnfragmentedWiresToTerm(
		t,
		termBuffer,
		encodeSessionEventClosedWire(t, egressTestClusterSessionID),
		encodeNewLeaderWireWithEndpoints(
			t,
			egressTestClusterSessionID,
			connectFixtureLeaderMemberID,
			multiMemberEndpointsAC,
		),
	)
	require.Positive(t, ac.PollEgress())

	require.True(t, ac.IsClosed())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, EventCodeClosed, rec.gotSessionEventCode)
	require.Equal(t, 1, rec.newLeaderCalls)
	require.Equal(t, termBefore, ac.LeadershipTermID())
	require.NotEqual(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	leader, ok := ac.MemberIngressMap().Lookup(connectFixtureLeaderMemberID)
	require.True(t, ok)
	require.Equal(t, "host-b:20111", leader.Endpoint)
}

func TestAeronCluster_leaderSwap_resizesIngressScratch_pollEgress(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	ac, harness, _, _ := newMultiMemberWireRecoveryCluster(t, sub, image, nil)
	scratchBefore := cap(ac.ingressScratch)

	applyWireNewLeaderAndPoll(t, ac, termBuffer, multiMemberEndpointsAC)

	basePub := aeron.NewTestExclusivePublicationForHooks(t)
	largerMaxPayload := basePub.MaxPayloadLength() * 2
	newLeaderPub := fixedMaxPayloadIngressPublication{
		IngressPublication: basePub,
		maxPayload:         largerMaxPayload,
	}
	completeWireLeaderRecovery(t, ac, harness, connectFixtureLeaderMemberID, newLeaderPub)

	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Greater(t, cap(ac.ingressScratch), scratchBefore)
	require.Equal(t, SessionHeaderLength()+largerMaxPayload, cap(ac.ingressScratch))
}

func TestAeronCluster_PollEgress_awaitConnectionEgressClose_entersAwaitNewLeader(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	enterChannelOnlyAwaitNewLeaderConnection(t, ac)

	aeron.ImageInternalCloseForHooks(image)

	require.Positive(t, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
}

func TestImageFromHeader_nonImageContextReturnsNil(t *testing.T) {
	require.Nil(t, imageFromHeader(nil))
	require.Nil(t, imageFromHeader(&aeron.Header{}))
}

func TestAeronCluster_handleSessionEgressFragment_nilHeaderImage_skipsRebind(t *testing.T) {
	sub, image, _ := setupEgressPollFixtures(t)
	ac := newAeronClusterWithEgressImage(t, sub, image, nil)
	rec := ac.ctx.EgressHandler.(*recordingEgressHandlers)

	wire := encodeNewLeaderWireWithEndpoints(
		t,
		egressTestClusterSessionID,
		connectFixtureLeaderMemberID,
		nil,
	)
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Equal(t, 1, rec.newLeaderCalls)
	require.Same(t, image, ac.egressImage)
}

func TestAeronCluster_handleSessionEgressFragment_newLeaderPlanError_dropsHandler(t *testing.T) {
	ac, rec := newAeronClusterFromConnectFixtureRecording(t, nil)
	before := snapshotSessionRecovery(ac)

	wire := encodeNewLeaderWireWithEndpoints(
		t,
		egressTestClusterSessionID,
		connectFixtureLeaderMemberID,
		[]byte("x=host:1"),
	)
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Zero(t, rec.newLeaderCalls)
	requireSessionRecoveryUnchanged(t, before, ac)
}

func TestAeronCluster_handleSessionEgressFragment_redirectPlanError_dropsHandler(t *testing.T) {
	ac, rec := newAeronClusterFromConnectFixtureRecording(t, nil)
	before := snapshotSessionRecovery(ac)

	wire := encodeSessionEventRedirectWire(t, 77, "x=host:1")
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Zero(t, rec.sessionEventCalls)
	requireSessionRecoveryUnchanged(t, before, ac)
}

func TestAeronCluster_handleSessionEgressFragment_applyError_dropsHandler(t *testing.T) {
	ac, rec := newMultiMemberAeronClusterInvalidIngress(t)

	wire := encodeNewLeaderWireWithEndpoints(
		t,
		egressTestClusterSessionID,
		connectFixtureLeaderMemberID,
		multiMemberEndpointsAC,
	)
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Zero(t, rec.newLeaderCalls)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
}

func TestAeronCluster_handleSessionEgressFragment_redirectApplyError_dropsHandler(t *testing.T) {
	ac, rec := newMultiMemberAeronClusterInvalidIngress(t)

	wire := encodeSessionEventRedirectWire(t, 77, string(multiMemberEndpointsAC))
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Zero(t, rec.sessionEventCalls)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
}

func TestAeronCluster_handleSessionEgressFragment_newLeaderSuccess_invokesHandler(t *testing.T) {
	ac, rec := newAeronClusterFromConnectFixtureRecording(t, nil)

	wire := encodeNewLeaderWireWithEndpoints(
		t,
		egressTestClusterSessionID,
		connectFixtureLeaderMemberID,
		nil,
	)
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Equal(t, 1, rec.newLeaderCalls)
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, egressTestClusterSessionID, rec.gotNewLeaderClusterSessionID)
}

func TestAeronCluster_SessionEvent_closed_pendingClose(t *testing.T) {
	ac, rec := newAeronClusterFromConnectFixtureRecording(t, nil)

	wire := encodeSessionEventClosedWire(t, egressTestClusterSessionID)
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Equal(t, SessionStatePendingClose, ac.SessionState())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, EventCodeClosed, rec.gotSessionEventCode)
}

func TestAeronCluster_handleSessionEgressFragment_redirect_success_mutatesSession(t *testing.T) {
	ac, rec := newAeronClusterFromConnectFixtureRecording(t, nil)
	ingressBefore := ac.IngressPublication()
	termBefore := ac.LeadershipTermID()

	wire := encodeSessionEventRedirectWire(t, 77, "")
	invokeHandleSessionEgressFragment(t, ac, rec, egressTestClusterSessionID, wire)

	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, termBefore, ac.LeadershipTermID())
	require.Nil(t, ac.IngressPublication())
	require.False(t, ingressBefore.IsClosed())
	require.Same(t, ingressBefore, ac.MemberIngressMap()[channelOnlyMemberID].Publication)
}
