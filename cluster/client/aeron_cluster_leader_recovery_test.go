//go:build testhooks

package client

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestSnapshotSessionRecovery_shallowAliasMissesInPlaceMutation(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	shallow := ac.MemberIngressMap()
	ac.MemberIngressMap()[channelOnlyMemberID].Endpoint = "mutated-in-place"
	require.Equal(t, "mutated-in-place", shallow[channelOnlyMemberID].Endpoint)

	ac2 := newAeronClusterFromConnectFixture(t, nil)
	deep := snapshotSessionRecovery(ac2)
	ac2.MemberIngressMap()[channelOnlyMemberID].Endpoint = "mutated-in-place"
	require.Empty(t, deep.memberMap[channelOnlyMemberID].Endpoint)
}

func TestAeronCluster_memberMap_storedAtConstruct(t *testing.T) {
	var memberMap MemberIngressMap
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		memberMap = result.MemberIngressMap
	})

	require.Equal(t, memberMap, ac.MemberIngressMap())
	// Maps are reference types; mutation through one alias is visible via the accessor.
	memberMap[channelOnlyMemberID].Endpoint = "alias-check"
	require.Equal(t, "alias-check", ac.MemberIngressMap()[channelOnlyMemberID].Endpoint)
}

func TestAeronCluster_handleNewLeader_planError_noMutation(t *testing.T) {
	testIngressPlanErrorNoMutation(t, func(ac *AeronCluster, endpoints []byte) error {
		return ac.handleNewLeader(
			ac.ClusterSessionID(),
			999,
			connectFixtureLeaderMemberID,
			endpoints,
			nil,
		)
	})
}

func TestAeronCluster_handleNewLeader_sessionIDMismatch_noMutation(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	before := snapshotSessionRecovery(ac)

	err := ac.handleNewLeader(
		ac.ClusterSessionID()+1,
		999,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.ErrorContains(t, err, "cluster session id mismatch")
	requireSessionRecoveryUnchanged(t, before, ac)
}

func TestAeronCluster_handleRedirect_planError_noMutation(t *testing.T) {
	testIngressPlanErrorNoMutation(t, func(ac *AeronCluster, endpoints []byte) error {
		return ac.handleRedirect(connectFixtureLeaderMemberID, endpoints, nil)
	})
}

func TestAeronCluster_handleRedirect_preservesLeadershipTermID(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	termBefore := ac.LeadershipTermID()

	err := ac.handleRedirect(connectFixtureLeaderMemberID, nil, nil)
	require.NoError(t, err)
	require.Equal(t, termBefore, ac.LeadershipTermID())
}

func TestAeronCluster_channelOnly_emptyEndpoints_recovery(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	require.Equal(t, SessionStateConnected, ac.SessionState())

	leader := enterChannelOnlyAwaitNewLeaderConnection(t, ac)
	leaderPub := leader.Publication
	require.NotNil(t, leaderPub)
	require.True(t, leaderPub.IsConnected())

	require.Positive(t, ac.PollEgress())
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Same(t, leaderPub, ac.IngressPublication())

	pos := ac.Offer([]byte("post-recovery"))
	require.Positive(t, pos)
}

// requireRecoveryCrossSpinUntilConnected models the invoker duty cycle for retriable
// keep-alive sequences.
//
// zeroWorkSpins is len(offerResults)-1: each retriable code yields PollEgress work 0,
// then one final DoWork+PollEgress spin succeeds.
func requireRecoveryCrossSpinUntilConnected(
	t *testing.T,
	ac *AeronCluster,
	seqPub *sequentialOfferResultIngressPublication,
	zeroWorkSpins int,
) {
	t.Helper()

	for range zeroWorkSpins {
		ac.Aeron().DoWork()
		requirePollLeaderRecoveryZeroWork(t, ac)
	}
	ac.Aeron().DoWork()
	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.NotNil(t, ac.IngressPublication())
	require.Same(t, seqPub, ac.IngressPublication())
	require.Equal(t, zeroWorkSpins+1, seqPub.calls)
}

// TestAeronCluster_pollLeaderRecovery_crossSpinSucceeds models the invoker duty cycle.
//
// DoWork before each PollEgress spin while keep-alive returns retriable codes.
func TestAeronCluster_pollLeaderRecovery_crossSpinSucceeds(t *testing.T) {
	tests := []struct {
		name         string
		offerResults []int64
	}{
		{
			name:         "backPressured",
			offerResults: []int64{aeron.BackPressured, aeron.BackPressured, 100},
		},
		{
			name:         "adminAction",
			offerResults: []int64{aeron.AdminAction, 100},
		},
		{
			name:         "backPressuredThenAdminAction",
			offerResults: []int64{aeron.BackPressured, aeron.AdminAction, 100},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac, seqPub := channelOnlyAwaitWithSequentialOffers(t, tc.offerResults...)

			requireRecoveryCrossSpinUntilConnected(t, ac, seqPub, len(tc.offerResults)-1)
		})
	}
}

func TestAeronCluster_pollLeaderRecovery_zeroWork(t *testing.T) {
	tests := []struct {
		name        string
		offerResult int64
	}{
		{name: "backPressured", offerResult: aeron.BackPressured},
		{name: "adminAction", offerResult: aeron.AdminAction},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
			setLeaderFixedOfferResult(t, leader, tc.offerResult)
			requirePollLeaderRecoveryZeroWork(t, ac)
		})
	}
}

func TestAeronCluster_pollLeaderRecovery_publicationErr_entersAwaitNewLeader(t *testing.T) {
	const startNs int64 = 11_000_000_000
	timeout := 9 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, func(result *ConnectResult) {
		withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
			ctx.NewLeaderTimeout = timeout
		})
	})
	leader.Publication = nil
	leader.RegistrationID = conductor.NullCorrelationID
	leader.PublicationErr = errors.New("ingress publication rejected")

	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
}

func TestAeronCluster_pollLeaderRecovery_expiredTimeoutClosesOnPublicationErr(t *testing.T) {
	const startNs int64 = 12_000_000_000
	timeout := 7 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, func(result *ConnectResult) {
		withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
			ctx.MessageTimeout = timeout
		})
	})
	leader.Publication = nil
	leader.RegistrationID = conductor.NullCorrelationID
	leader.PublicationErr = errors.New("ingress publication rejected")

	nowNs.Store(ac.stateDeadlineNs)

	require.Equal(t, 1, ac.PollEgress())
	require.True(t, ac.IsClosed())
	require.NotEqual(t, SessionStateAwaitNewLeader, ac.SessionState())
}

func TestAeronCluster_pollLeaderRecovery_pendingRegistrationResolves(t *testing.T) {
	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	newPub := aeron.NewTestExclusivePublicationForHooks(t)
	leader.Publication = nil
	leader.RegistrationID = 42
	leader.SetAsyncPollAddHookForTest(t, func(
		ing *MemberIngress,
		_ ClusterContext,
		_ *aeron.Client,
	) (bool, error) {
		ing.RegistrationID = conductor.NullCorrelationID
		ing.Publication = newPub
		return true, nil
	})

	require.Positive(t, ac.PollEgress())
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Same(t, newPub, ac.IngressPublication())
}

func TestAeronCluster_pollLeaderRecovery_notYetConnectedLeader_staysAwaitConnection(t *testing.T) {
	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	deadlineBefore := ac.stateDeadlineNs
	notConnectedPub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, false)
	leader.Publication = notConnectedPub
	leader.RegistrationID = conductor.NullCorrelationID

	require.Equal(t, 0, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, deadlineBefore, ac.stateDeadlineNs)
	require.Same(t, notConnectedPub, leader.Publication)
	require.False(t, notConnectedPub.IsClosed())
	require.Nil(t, ac.IngressPublication())
}

func TestAeronCluster_pollLeaderRecovery_closedLeader_entersAwaitNewLeader(t *testing.T) {
	const startNs int64 = 2_000_000_000
	timeout := 7 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, func(result *ConnectResult) {
		withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
			ctx.NewLeaderTimeout = timeout
		})
	})
	closedPub := aeron.NewTestExclusivePublicationForHooks(t)
	require.NoError(t, closedPub.Close())
	leader.Publication = closedPub
	leader.RegistrationID = conductor.NullCorrelationID

	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
	require.Nil(t, ac.IngressPublication())
	require.Nil(t, leader.Publication)
	require.Equal(t, conductor.NullCorrelationID, leader.RegistrationID)
	require.True(t, closedPub.IsClosed())
}

func TestAeronCluster_pollLeaderRecovery_keepAliveClosed_staysAwaitConnection(t *testing.T) {
	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	deadlineBefore := ac.stateDeadlineNs
	setLeaderFixedOfferResult(t, leader, aeron.Closed)
	leaderPub := leader.Publication
	require.False(t, leaderPub.IsClosed())

	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, deadlineBefore, ac.stateDeadlineNs)
	require.Equal(t, leaderPub, leader.Publication)
	require.False(t, leaderPub.IsClosed())
	require.Nil(t, ac.IngressPublication())
}

func TestAeronCluster_pollLeaderRecovery_keepAliveNotConnected_staysAwaitConnection(t *testing.T) {
	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	deadlineBefore := ac.stateDeadlineNs
	setLeaderFixedOfferResult(t, leader, aeron.NotConnected)
	leaderPub := leader.Publication
	require.False(t, leaderPub.IsClosed())

	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, deadlineBefore, ac.stateDeadlineNs)
	require.Equal(t, leaderPub, leader.Publication)
	require.False(t, leaderPub.IsClosed())
	require.Nil(t, ac.IngressPublication())
}

func TestAeronCluster_pollLeaderRecovery_connectedButClosedPub_entersAwaitNewLeader(t *testing.T) {
	const startNs int64 = 4_000_000_000
	timeout := 7 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, func(result *ConnectResult) {
		withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
			ctx.NewLeaderTimeout = timeout
		})
	})
	basePub := aeron.NewTestExclusivePublicationForHooks(t)
	closedPub := connectedButClosedIngressPublication{IngressPublication: basePub}
	leader.Publication = closedPub
	leader.RegistrationID = conductor.NullCorrelationID

	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
	require.Nil(t, ac.IngressPublication())
	require.Nil(t, leader.Publication)
	require.True(t, closedPub.IsClosed())
}

func TestAeronCluster_pollLeaderRecovery_keepAliveMaxPosition_pendingClose(t *testing.T) {
	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	setLeaderFixedOfferResult(t, leader, aeron.MaxPositionExceeded)

	require.Equal(t, 2, ac.PollEgress())
	require.True(t, ac.IsClosed())
	require.Nil(t, ac.IngressPublication())
}

func TestAeronCluster_leaderSwap_resizesIngressScratch(t *testing.T) {
	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	scratchBefore := cap(ac.ingressScratch)

	basePub := aeron.NewTestExclusivePublicationForHooks(t)
	largerMaxPayload := basePub.MaxPayloadLength() * 2
	leader.Publication = fixedMaxPayloadIngressPublication{
		IngressPublication: basePub,
		maxPayload:         largerMaxPayload,
	}

	require.Positive(t, ac.PollEgress())
	require.Equal(t, SessionStateConnected, ac.SessionState())
	require.Greater(t, cap(ac.ingressScratch), scratchBefore)
	require.Equal(t, SessionHeaderLength()+largerMaxPayload, cap(ac.ingressScratch))
}

func TestAeronCluster_handleNewLeader_success_entersAwaitConnection(t *testing.T) {
	const newTermID int64 = 999
	const startNs int64 = 5_000_000_000
	timeout := 4 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
			ctx.MessageTimeout = timeout
		})
	})
	ingressBefore := ac.IngressPublication()

	err := ac.handleNewLeader(
		ac.ClusterSessionID(),
		newTermID,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
	require.Nil(t, ac.IngressPublication())
	require.Equal(t, newTermID, ac.LeadershipTermID())
	require.Equal(t, connectFixtureLeaderMemberID, ac.LeaderMemberID())

	leader, ok := ac.MemberIngressMap().Lookup(channelOnlyMemberID)
	require.True(t, ok)
	require.Same(t, ingressBefore, leader.Publication)
	require.False(t, leader.Publication.IsClosed())
}

func TestAeronCluster_handleNewLeader_reuse_sameEndpoint_pubNotClosed(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	ingressBefore := ac.IngressPublication()

	err := ac.handleNewLeader(
		ac.ClusterSessionID(),
		200,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.False(t, ingressBefore.IsClosed())
	require.Same(t, ingressBefore, ac.MemberIngressMap()[channelOnlyMemberID].Publication)
}

func TestAeronCluster_handleNewLeader_applyError_entersAwaitNewLeader(t *testing.T) {
	const startNs int64 = 8_000_000_000
	timeout := 6 * time.Second

	var nowNs atomic.Int64
	nowNs.Store(startNs)

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	result := newMultiMemberConnectResult(t, connectFixtureLeaderMemberID, leaderPub)
	withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
		ctx.IngressChannel = invalidIngressClusterContext().IngressChannel
		ctx.NewLeaderTimeout = timeout
	})

	ac := newAeronClusterFromConnectResult(t, result)

	err := ac.handleNewLeader(
		ac.ClusterSessionID(),
		300,
		connectFixtureLeaderMemberID,
		multiMemberEndpointsAC,
		nil,
	)
	require.Error(t, err)
	require.Equal(t, SessionStateAwaitNewLeader, ac.SessionState())
	require.Equal(t, startNs+timeout.Nanoseconds(), ac.stateDeadlineNs)
	require.Equal(t, connectFixtureLeaderMemberID, ac.LeaderMemberID())
	require.Equal(t, int64(300), ac.LeadershipTermID())
	require.Nil(t, ac.IngressPublication())
	require.True(t, leaderPub.IsClosed())

	leader, ok := ac.MemberIngressMap().Lookup(connectFixtureLeaderMemberID)
	require.True(t, ok)
	require.Equal(t, "host-c:20113", leader.Endpoint)
}

func TestAeronCluster_Close_closesAllMemberIngressPublications(t *testing.T) {
	member0Pub := aeron.NewTestExclusivePublicationForHooks(t)
	member1Pub := aeron.NewTestExclusivePublicationForHooks(t)
	result := newMultiMemberConnectResult(t, connectFixtureLeaderMemberID, member1Pub)
	result.MemberIngressMap[channelOnlyMemberID].Publication = member0Pub

	ac := newAeronClusterFromConnectResult(t, result)
	require.NoError(t, ac.Close())

	require.True(t, member0Pub.IsClosed())
	require.True(t, member1Pub.IsClosed())
}

func TestAeronCluster_pollStateChanges_timeoutCloses(t *testing.T) {
	tests := []struct {
		name             string
		state            SessionState
		startNs          int64
		timeout          time.Duration
		configureTimeout func(*ClusterContext, time.Duration)
	}{
		{
			name:    "await_new_leader",
			state:   SessionStateAwaitNewLeader,
			startNs: 1_000_000_000,
			timeout: 5 * time.Second,
			configureTimeout: func(ctx *ClusterContext, timeout time.Duration) {
				ctx.NewLeaderTimeout = timeout
			},
		},
		{
			name:    "await_connection",
			state:   SessionStateAwaitNewLeaderConnection,
			startNs: 3_000_000_000,
			timeout: 3 * time.Second,
			configureTimeout: func(ctx *ClusterContext, timeout time.Duration) {
				ctx.MessageTimeout = timeout
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var nowNs atomic.Int64
			nowNs.Store(tc.startNs)

			ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
				withConnectNanoClock(result, &nowNs, func(ctx *ClusterContext) {
					tc.configureTimeout(ctx, tc.timeout)
				})
			})

			ac.sessionState = tc.state
			ac.stateDeadlineNs = tc.startNs + tc.timeout.Nanoseconds()
			nowNs.Store(ac.stateDeadlineNs)
			if tc.state == SessionStateAwaitNewLeaderConnection {
				// Recovery must not complete before the MessageTimeout fires.
				ac.ingressPublication = nil
				leader, ok := ac.MemberIngressMap().Lookup(channelOnlyMemberID)
				require.True(t, ok)
				setLeaderFixedOfferResult(t, leader, aeron.BackPressured)
			}

			require.Equal(t, 1, ac.PollEgress())
			require.True(t, ac.IsClosed())
		})
	}
}
