//go:build testhooks

package client

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

func channelOnlyIngressMap(pub IngressPublication) MemberIngressMap {
	return MemberIngressMap{
		channelOnlyMemberID: {
			MemberID:    channelOnlyMemberID,
			Publication: pub,
		},
	}
}

func singleMemberIngressMap(
	memberID int32,
	endpoint string,
	pub IngressPublication,
) MemberIngressMap {
	return MemberIngressMap{
		memberID: {MemberID: memberID, Endpoint: endpoint, Publication: pub},
	}
}

func invalidIngressClusterContext() ClusterContext {
	clusterCtx := validConnectFixtureClusterContext()
	clusterCtx.IngressChannel = "not-a-valid-channel"
	return clusterCtx
}

func TestLeaderIngressMemberID_channelOnlyUsesSyntheticZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		m        MemberIngressMap
		leaderID int32
		want     int32
	}{
		{
			name:     "channel_only",
			m:        MemberIngressMap{channelOnlyMemberID: {MemberID: channelOnlyMemberID}},
			leaderID: connectFixtureLeaderMemberID,
			want:     channelOnlyMemberID,
		},
		{
			name:     "explicit_member",
			m:        MemberIngressMap{2: {MemberID: 2}},
			leaderID: 2,
			want:     2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, leaderIngressMemberID(tc.m, tc.leaderID))
		})
	}
}

func TestPlanMemberIngressMapRebuild(t *testing.T) {
	t.Parallel()

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	oldMap := channelOnlyIngressMap(leaderPub)

	tests := []struct {
		name        string
		leaderID    int32
		endpoints   []byte
		wantErr     string
		wantReuse   bool
		wantIngress int32
	}{
		{
			name:      "parse_error",
			leaderID:  connectFixtureLeaderMemberID,
			endpoints: []byte("x=host:1"),
			wantErr:   "invalid ingress member id",
		},
		{
			name:      "leader_missing_in_new_map",
			leaderID:  connectFixtureLeaderMemberID,
			endpoints: []byte("0=host-a:20110"),
			wantErr:   "member id 1 not in ingress map",
		},
		{
			name:        "channel_only_empty_endpoints_reuse",
			leaderID:    connectFixtureLeaderMemberID,
			endpoints:   nil,
			wantReuse:   true,
			wantIngress: channelOnlyMemberID,
		},
		{
			name:        "non_reuse_endpoint_change",
			leaderID:    channelOnlyMemberID,
			endpoints:   []byte("0=newhost:9999"),
			wantReuse:   false,
			wantIngress: channelOnlyMemberID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planMemberIngressMapRebuild(oldMap, tc.leaderID, tc.endpoints)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantIngress, plan.ingressMemberID)
			require.Equal(t, tc.wantReuse, plan.reusedPub != nil)
			if tc.wantReuse {
				require.Same(t, leaderPub, plan.reusedPub)
			}
		})
	}
}

func TestPlanMemberIngressMapRebuild_closedPublicationNotReused(t *testing.T) {
	t.Parallel()

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	require.NoError(t, leaderPub.Close())
	oldMap := channelOnlyIngressMap(leaderPub)

	plan, err := planMemberIngressMapRebuild(oldMap, connectFixtureLeaderMemberID, nil)
	require.NoError(t, err)
	require.Nil(t, plan.reusedPub)
}

func TestApplyMemberIngressMapRebuild_reusePreservesPublication(t *testing.T) {
	t.Parallel()

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	oldMap := channelOnlyIngressMap(leaderPub)
	plan, err := planMemberIngressMapRebuild(oldMap, connectFixtureLeaderMemberID, nil)
	require.NoError(t, err)
	require.NotNil(t, plan.reusedPub)

	newMap, gotPub, err := applyMemberIngressMapRebuild(
		&aeron.Client{},
		validConnectFixtureClusterContext(),
		oldMap,
		plan,
	)
	require.NoError(t, err)
	require.Same(t, leaderPub, gotPub)
	require.False(t, gotPub.IsClosed())

	leader, ok := newMap.Lookup(channelOnlyMemberID)
	require.True(t, ok)
	require.Same(t, leaderPub, leader.Publication)
}

func TestMemberIngressMapRebuild_invalidChannelReturnsPartialMap(t *testing.T) {
	t.Parallel()

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	oldMap := singleMemberIngressMap(
		connectFixtureLeaderMemberID,
		"host-b:20111",
		leaderPub,
	)
	endpoints := []byte("0=host-a:20110,1=host-c:20113")
	clusterCtx := invalidIngressClusterContext()

	plan, err := planMemberIngressMapRebuild(oldMap, connectFixtureLeaderMemberID, endpoints)
	require.NoError(t, err)
	require.Nil(t, plan.reusedPub)

	newMap, gotPub, err := applyMemberIngressMapRebuild(&aeron.Client{}, clusterCtx, oldMap, plan)
	require.Error(t, err)
	require.Nil(t, gotPub)
	require.NotNil(t, newMap)
	require.True(t, leaderPub.IsClosed())
}

func TestPlanMemberIngressMapRebuild_planErrorNoMutation(t *testing.T) {
	t.Parallel()

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	oldMap := channelOnlyIngressMap(leaderPub)

	_, err := planMemberIngressMapRebuild(
		oldMap,
		connectFixtureLeaderMemberID,
		[]byte("x=host:1"),
	)
	require.Error(t, err)
	require.False(t, leaderPub.IsClosed())
	require.Same(t, leaderPub, oldMap[channelOnlyMemberID].Publication)
}

func TestApplyMemberIngressMapRebuild_closesEvictedMemberPublications(t *testing.T) {
	t.Parallel()

	member0Pub := aeron.NewTestExclusivePublicationForHooks(t)
	member1Pub := aeron.NewTestExclusivePublicationForHooks(t)
	oldMap := MemberIngressMap{
		channelOnlyMemberID: {
			MemberID:    channelOnlyMemberID,
			Endpoint:    "host-a:20110",
			Publication: member0Pub,
		},
		connectFixtureLeaderMemberID: {
			MemberID:    connectFixtureLeaderMemberID,
			Endpoint:    "host-b:20111",
			Publication: member1Pub,
		},
	}
	plan, err := planMemberIngressMapRebuild(
		oldMap,
		connectFixtureLeaderMemberID,
		[]byte("1=host-c:20113"),
	)
	require.NoError(t, err)
	require.Nil(t, plan.reusedPub)

	_, _, err = applyMemberIngressMapRebuild(
		&aeron.Client{},
		validConnectFixtureClusterContext(),
		oldMap,
		plan,
	)
	require.Error(t, err)
	require.True(t, member0Pub.IsClosed())
	require.True(t, member1Pub.IsClosed())
}

func TestFailIfIngressUnrecoverable(t *testing.T) {
	t.Parallel()

	terminalErr := errors.New("ingress publication rejected")

	tests := []struct {
		name           string
		m              MemberIngressMap
		wantErrSubstrs []string
	}{
		{
			name: "nil_map",
			m:    nil,
		},
		{
			name: "publication_present",
			m: MemberIngressMap{
				channelOnlyMemberID: {
					Publication: aeron.NewTestExclusivePublicationForHooks(t),
				},
			},
		},
		{
			name: "pending_registration",
			m: MemberIngressMap{
				channelOnlyMemberID: {RegistrationID: 42},
			},
		},
		{
			name: "terminal_publication_err",
			m: MemberIngressMap{
				channelOnlyMemberID: {
					RegistrationID: conductor.NullCorrelationID,
					PublicationErr: terminalErr,
				},
			},
			wantErrSubstrs: []string{"ingress publication rejected"},
		},
		{
			name: "recoverable_member_blocks_terminal_err",
			m: MemberIngressMap{
				channelOnlyMemberID: {
					RegistrationID: conductor.NullCorrelationID,
					PublicationErr: terminalErr,
				},
				1: {RegistrationID: 99},
			},
		},
		{
			name: "all_members_terminal_err",
			m: MemberIngressMap{
				0: {
					RegistrationID: conductor.NullCorrelationID,
					PublicationErr: errors.New("member 0 rejected"),
				},
				1: {
					RegistrationID: conductor.NullCorrelationID,
					PublicationErr: errors.New("member 1 rejected"),
				},
			},
			// Map iteration order is undefined; either member's terminal err may surface.
			wantErrSubstrs: []string{"member 0 rejected", "member 1 rejected"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.failIfIngressUnrecoverable()
			if len(tc.wantErrSubstrs) == 0 {
				require.NoError(t, err)
				return
			}
			requireErrorContainsAny(t, err, tc.wantErrSubstrs)
		})
	}
}

func TestPollPendingMemberIngressRegistration(t *testing.T) {
	t.Parallel()

	clusterCtx := validConnectFixtureClusterContext()
	client := &aeron.Client{}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "skips_idle_entries",
			run: func(t *testing.T) {
				pollPendingMemberIngressRegistration(
					MemberIngressMap{
						channelOnlyMemberID: {
							Publication: aeron.NewTestExclusivePublicationForHooks(t),
						},
						1: {RegistrationID: conductor.NullCorrelationID},
					},
					clusterCtx,
					client,
				)
			},
		},
		{
			name: "polls_pending_registration",
			run: func(t *testing.T) {
				ingress := &MemberIngress{MemberID: 0, RegistrationID: 99}
				var polled bool
				ingress.SetAsyncPollAddHookForTest(t, func(
					*MemberIngress,
					ClusterContext,
					*aeron.Client,
				) (bool, error) {
					polled = true
					return false, nil
				})
				pollPendingMemberIngressRegistration(
					MemberIngressMap{channelOnlyMemberID: ingress},
					clusterCtx,
					client,
				)
				require.True(t, polled)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func requireErrorContainsAny(t *testing.T, err error, substrs []string) {
	t.Helper()

	require.Error(t, err)
	msg := err.Error()
	for _, substr := range substrs {
		if strings.Contains(msg, substr) {
			return
		}
	}
	t.Fatalf("error %q did not contain any of %v", err, substrs)
}
