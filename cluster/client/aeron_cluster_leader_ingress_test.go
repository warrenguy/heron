//go:build testhooks

package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const (
	trackIngressAwaitNewLeaderStartNs int64 = 2_000_000_000
	trackIngressAwaitNewLeaderTimeout       = 7 * time.Second
)

func TestAeronCluster_trackIngress_notConnected_entersAwaitNewLeader(t *testing.T) {
	ac := newTrackIngressDeadlineOracleCluster(t, func(result *ConnectResult) {
		withFixedOfferResultIngress(t, result, aeron.NotConnected)
	})

	require.Equal(t, SessionStateConnected, ac.SessionState())

	ingress := ac.IngressPublication()

	got := ac.Offer([]byte("payload"))
	require.Equal(t, aeron.NotConnected, got)
	requireIngressEntersAwaitNewLeaderWithDeadline(t, ac, ingress)
}

func TestAeronCluster_trackIngress_tryClaim_notConnected_entersAwaitNewLeader(t *testing.T) {
	ac := newTrackIngressDeadlineOracleCluster(t, func(result *ConnectResult) {
		withFixedOfferResultIngress(t, result, aeron.NotConnected)
	})

	require.Equal(t, SessionStateConnected, ac.SessionState())
	ingress := ac.IngressPublication()

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Equal(t, int64(aeron.NotConnected), position)
	require.Zero(t, claim)
	requireIngressEntersAwaitNewLeaderWithDeadline(t, ac, ingress)
}

func TestAeronCluster_trackIngress_offerClosed_entersAwaitNewLeader(t *testing.T) {
	ac := newTrackIngressDeadlineOracleCluster(t, func(result *ConnectResult) {
		withFixedOfferResultIngress(t, result, aeron.Closed)
	})

	require.Equal(t, SessionStateConnected, ac.SessionState())
	ingress := ac.IngressPublication()
	require.True(t, ingress.IsConnected())
	require.False(t, ingress.IsClosed())

	got := ac.Offer([]byte("payload"))
	require.Equal(t, aeron.Closed, got)
	requireIngressEntersAwaitNewLeaderWithDeadline(t, ac, ingress)
}

func TestAeronCluster_trackIngress_tryClaim_closed_entersAwaitNewLeader(t *testing.T) {
	ac := newTrackIngressDeadlineOracleCluster(t, func(result *ConnectResult) {
		withFixedOfferResultIngress(t, result, aeron.Closed)
	})

	require.Equal(t, SessionStateConnected, ac.SessionState())
	ingress := ac.IngressPublication()
	require.True(t, ingress.IsConnected())
	require.False(t, ingress.IsClosed())

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Equal(t, int64(aeron.Closed), position)
	require.Zero(t, claim)
	requireIngressEntersAwaitNewLeaderWithDeadline(t, ac, ingress)
}

func TestAeronCluster_trackIngress_maxPositionExceeded_pendingClose(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		withFixedOfferResultIngress(t, result, aeron.MaxPositionExceeded)
	})

	require.Equal(t, SessionStateConnected, ac.SessionState())

	got := ac.Offer([]byte("payload"))
	require.Equal(t, int64(aeron.MaxPositionExceeded), got)
	require.Equal(t, SessionStatePendingClose, ac.SessionState())

	require.Equal(t, 1, ac.PollEgress())
	require.True(t, ac.IsClosed())
}

func TestAeronCluster_trackIngress_scratchOverflow_staysConnected(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	ac.ingressScratch = make([]byte, 1)

	got := ac.Offer([]byte("payload"))
	require.Equal(t, int64(aeron.MaxPositionExceeded), got)
	require.Equal(t, SessionStateConnected, ac.SessionState())
}

func TestAeronCluster_leaderIngressPublication_disconnected_entersAwaitNewLeader(t *testing.T) {
	ac := newTrackIngressDeadlineOracleCluster(t, func(result *ConnectResult) {
		disconnected := aeron.NewTestExclusivePublicationForHooksWithConnected(t, false)
		installConnectResultIngress(result, disconnected)
	})

	require.Equal(t, SessionStateConnected, ac.SessionState())
	disconnected := ac.IngressPublication()
	require.False(t, disconnected.IsConnected())

	got := ac.Offer([]byte("payload"))
	require.Equal(t, aeron.NotConnected, got)
	requireIngressEntersAwaitNewLeaderWithDeadline(t, ac, disconnected)
}

func TestAeronCluster_TryClaim_negativeLengthBeforeSessionGuard(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	ac.sessionState = SessionStatePendingClose

	claim, position, err := ac.TryClaim(-1)
	require.EqualError(t, err, "cluster client: negative payload length -1")
	require.Equal(t, int64(0), position)
	require.Zero(t, claim)
	require.Equal(t, SessionStatePendingClose, ac.SessionState())
}

func TestAeronCluster_CommitClaim_leaderMismatchAborts(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	claimPub := ac.IngressPublication()
	ac.ingressPublication = aeron.NewTestExclusivePublicationForHooks(t)

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.NotConnected), committed)

	ac.ingressPublication = claimPub
	_, position2, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position2)
}

func TestAeronCluster_ingressGuards_blockBySessionState(t *testing.T) {
	tests := []struct {
		name      string
		state     SessionState
		wantOffer int64
	}{
		{
			name:      "await_new_leader",
			state:     SessionStateAwaitNewLeader,
			wantOffer: aeron.NotConnected,
		},
		{
			name:      "await_new_leader_connection",
			state:     SessionStateAwaitNewLeaderConnection,
			wantOffer: aeron.NotConnected,
		},
		{
			name:      "pending_close",
			state:     SessionStatePendingClose,
			wantOffer: aeron.Closed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac := newAeronClusterFromConnectFixture(t, nil)
			ingressBefore := ac.IngressPublication()

			ac.sessionState = tc.state

			gotOffer := ac.Offer([]byte("payload"))
			require.Equal(t, tc.wantOffer, gotOffer)

			claim, gotTryClaim, err := ac.TryClaim(4)
			require.NoError(t, err)
			require.Equal(t, tc.wantOffer, gotTryClaim)
			require.Zero(t, claim)

			gotKeepAlive := ac.SendKeepAlive()
			require.Equal(t, tc.wantOffer, gotKeepAlive)

			gotAdmin := ac.SendSnapshotRequest(testAdminCorrelationID)
			require.Equal(t, tc.wantOffer, gotAdmin)

			require.Equal(t, tc.state, ac.SessionState())
			require.Same(t, ingressBefore, ac.IngressPublication())
		})
	}
}

func TestAeronCluster_CommitClaim_pendingClose_returnsClosed(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	ac.enterPendingClose()

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.Closed), committed)
}

func TestAeronCluster_CommitClaim_closedIngress_entersAwaitNewLeader(t *testing.T) {
	ac := newTrackIngressDeadlineOracleCluster(t, nil)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	pub := claim.publication
	require.NoError(t, pub.Close())

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.Closed), committed)
	requireIngressEntersAwaitNewLeaderWithDeadline(t, ac, pub)
}

func TestAeronCluster_CommitClaim_staleClosedClaimAfterRecovery_staysConnected(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	result := newMultiMemberConnectResult(t, connectFixtureLeaderMemberID, leaderPub)
	result.Aeron = harness.Client
	ac := newAeronClusterFromConnectResult(t, result)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)
	stalePub := claim.publication

	err = ac.handleNewLeader(
		ac.ClusterSessionID(),
		200,
		connectFixtureLeaderMemberID,
		multiMemberEndpointsAC,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, ac.IngressPublication())
	require.True(t, stalePub.IsClosed())

	newLeaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	leader, ok := ac.MemberIngressMap().Lookup(connectFixtureLeaderMemberID)
	require.True(t, ok)
	leader.SetAsyncPollAddHookForTest(t, func(
		ing *MemberIngress,
		_ ClusterContext,
		_ *aeron.Client,
	) (bool, error) {
		ing.RegistrationID = conductor.NullCorrelationID
		ing.Publication = newLeaderPub
		return true, nil
	})
	require.Positive(t, ac.PollEgress())
	harness.DoWork()
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Same(t, newLeaderPub, ac.IngressPublication())

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.Closed), committed)
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Same(t, newLeaderPub, ac.IngressPublication())
	require.False(t, newLeaderPub.IsClosed())
}

func TestAeronCluster_CommitClaim_zeroClaim_noStateMutation(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	pub := ac.IngressPublication()
	require.NotNil(t, pub)

	committed, err := ac.CommitClaim(IngressClaim{}, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.NotConnected), committed)
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Same(t, pub, ac.IngressPublication())
}

func TestAeronCluster_CommitClaim_closedSessionStateStable(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	require.NoError(t, ac.Close())

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.Closed), committed)
	require.Equal(t, SessionStateConnected, ac.SessionState())
}

func TestAeronCluster_AbortClaim_afterHandleNewLeader_reuse_releasesFrame(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	pub := ac.IngressPublication()

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	termBuf := pub.TermBuffer(claim.TermIndex)
	require.NotZero(t, logbuffer.FrameLengthVolatile(termBuf, claim.FrameOffset))

	err = ac.handleNewLeader(
		ac.ClusterSessionID(),
		200,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, ac.IngressPublication())

	reused, ok := ac.MemberIngressMap().Lookup(channelOnlyMemberID)
	require.True(t, ok)
	require.Same(t, pub, reused.Publication)
	require.False(t, pub.IsClosed())

	require.NoError(t, ac.AbortClaim(claim))
	require.True(t, logbuffer.IsPaddingFrame(termBuf, claim.FrameOffset))

	_, _, reclaimPos, reclaimErr := pub.TryClaim(SessionHeaderLength() + 4)
	require.NoError(t, reclaimErr)
	require.Positive(t, reclaimPos)
}

func TestAeronCluster_CommitClaim_afterRecoveryReusedPub_notConnected(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)
	require.Equal(t, connectFixtureLeadershipTermID, claim.leadershipTermID)

	err = ac.handleNewLeader(
		ac.ClusterSessionID(),
		200,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, ac.IngressPublication())

	require.Positive(t, ac.PollEgress())
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Equal(t, int64(200), ac.LeadershipTermID())

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.NotConnected), committed)
}

func TestAeronCluster_CommitClaim_afterHandleNewLeader_reuse_notConnected(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	err = ac.handleNewLeader(
		ac.ClusterSessionID(),
		200,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, ac.IngressPublication())

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.NotConnected), committed)
}

func TestAeronCluster_CommitClaim_afterNewLeader_differentEndpoint_notConnected(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	result := newMultiMemberConnectResult(t, connectFixtureLeaderMemberID, leaderPub)
	result.Aeron = harness.Client
	ac := newAeronClusterFromConnectResult(t, result)

	claim, position, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position)

	err = ac.handleNewLeader(
		ac.ClusterSessionID(),
		200,
		connectFixtureLeaderMemberID,
		multiMemberEndpointsAC,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, ac.IngressPublication())
	require.True(t, leaderPub.IsClosed())

	leader, ok := ac.MemberIngressMap().Lookup(connectFixtureLeaderMemberID)
	require.True(t, ok)
	regBefore := leader.RegistrationID
	pubBefore := leader.Publication
	pubErrBefore := leader.PublicationErr
	endpointBefore := leader.Endpoint

	committed, err := ac.CommitClaim(claim, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(aeron.Closed), committed)

	require.Equal(t, regBefore, leader.RegistrationID)
	require.Equal(t, pubBefore, leader.Publication)
	require.Equal(t, pubErrBefore, leader.PublicationErr)
	require.Equal(t, endpointBefore, leader.Endpoint)

	newLeaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	leader.SetAsyncPollAddHookForTest(t, func(
		ing *MemberIngress,
		_ ClusterContext,
		_ *aeron.Client,
	) (bool, error) {
		ing.RegistrationID = conductor.NullCorrelationID
		ing.Publication = newLeaderPub
		return true, nil
	})
	require.Positive(t, ac.PollEgress())
	harness.DoWork()
	require.Same(t, newLeaderPub, ac.IngressPublication())

	claim2, position2, err := ac.TryClaim(4)
	require.NoError(t, err)
	require.Positive(t, position2)
	require.NoError(t, ac.AbortClaim(claim2))
}

func newTrackIngressDeadlineOracleCluster(
	t *testing.T,
	mutate func(*ConnectResult),
) *AeronCluster {
	t.Helper()

	var nowNs atomic.Int64
	nowNs.Store(trackIngressAwaitNewLeaderStartNs)

	return newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		withConnectNanoClock(result, &nowNs, func(clusterCtx *ClusterContext) {
			clusterCtx.NewLeaderTimeout = trackIngressAwaitNewLeaderTimeout
		})
		if mutate != nil {
			mutate(result)
		}
	})
}

func requireIngressEntersAwaitNewLeaderWithDeadline(
	t *testing.T,
	ac *AeronCluster,
	ingress IngressPublication,
) {
	t.Helper()

	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Equal(
		t,
		trackIngressAwaitNewLeaderStartNs+trackIngressAwaitNewLeaderTimeout.Nanoseconds(),
		ac.stateDeadlineNs,
	)
	require.Nil(t, ac.IngressPublication())

	leader, ok := ac.MemberIngressMap().Lookup(channelOnlyMemberID)
	require.True(t, ok)
	require.Nil(t, leader.Publication)
	require.True(t, ingress.IsClosed())
}
