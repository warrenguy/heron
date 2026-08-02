//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

type higherTermProbe struct {
	count int
	nowNs int64
	term  int64
}

func probeHigherTerms(t *testing.T) *higherTermProbe {
	t.Helper()
	p := &higherTermProbe{}
	SetUnexpectedHigherTermRecorderForTest(t, func(nowNs, term int64) {
		p.count++
		p.nowNs = nowNs
		p.term = term
	})
	return p
}

func TestSteadyLeaderCommit_higherTerm_invokesHook(t *testing.T) {
	tests := []struct {
		name         string
		wire         string
		role         Role
		term         int64
		leader       int32
		seedNotify   int64
		higherTerm   int64
		higherLeader int32
		higherPos    int64
		nowNs        int64
		checkLower   bool
		wireFactory  bool
	}{
		{
			name: "CommitPosition_follower", wire: "commit", role: RoleFollower,
			term: 2, leader: 1, seedNotify: 0,
			higherTerm: 3, higherLeader: 1, higherPos: 999, nowNs: 700,
			checkLower: true,
		},
		{
			name: "CommitPosition_leader", wire: "commit", role: RoleLeader,
			term: 2, leader: nullMemberID, seedNotify: 11,
			higherTerm: 4, higherLeader: 1, higherPos: 50, nowNs: 900,
		},
		{
			name: "NewLeadershipTerm_follower", wire: "nlt", role: RoleFollower,
			term: 1, leader: 1, seedNotify: 0,
			higherTerm: 5, higherLeader: 9, higherPos: 500, nowNs: 3000,
		},
		{
			name: "NewLeadershipTerm_leader", wire: "nlt", role: RoleLeader,
			term: 1, leader: nullMemberID, seedNotify: 7,
			higherTerm: 8, higherLeader: 3, higherPos: 400, nowNs: 3500,
		},
		{
			name: "CommitPosition_follower_withFactory", wire: "commit", role: RoleFollower,
			term: 2, leader: 1, seedNotify: 0,
			higherTerm: 3, higherLeader: 1, higherPos: 999, nowNs: 700,
			wireFactory: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			higher := probeHigherTerms(t)
			q := &QueueConsensusIngress{}
			a := NewAgent(AgentConfig{
				Role:             tc.role,
				Ingress:          q,
				LeadershipTermID: tc.term,
				LeaderMemberID:   tc.leader,
			})
			a.notifiedCommitPosition = tc.seedNotify
			if tc.wireFactory {
				a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
					return &orderRunner{n: 1}
				})
			}

			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.higherTerm, tc.higherLeader, tc.higherPos)
			require.Equal(t, 1, a.pollConsensusIngress(tc.nowNs))
			require.Equal(t, 1, higher.count)
			require.Equal(t, tc.nowNs, higher.nowNs)
			require.Equal(t, tc.higherTerm, higher.term)
			require.Equal(t, tc.seedNotify, a.NotifiedCommitPosition())
			if tc.wireFactory {
				require.NotNil(t, a.Election())
				require.Equal(t, ElectionTriggerHigherTerm, a.LastElectionTrigger())
			} else {
				require.Nil(t, a.Election())
			}

			if !tc.checkLower {
				return
			}
			// Lower term must not invoke the higher-term hook.
			enqueueSteadyLeaderCommit(t, q, tc.wire, 1, 1, 1000)
			require.Equal(t, 1, a.pollConsensusIngress(tc.nowNs+100))
			require.Equal(t, 1, higher.count)
		})
	}
}

func TestSteadyAppendPosition_higherTerm_noHook(t *testing.T) {
	higher := probeHigherTerms(t)

	pt := &recordingPositionTable{}
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		Positions:        pt,
		Ingress:          q,
		LeadershipTermID: 2,
	})
	a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
		return &orderRunner{n: 1}
	})

	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 5,
		LogPosition:      200,
		FollowerMemberID: 3,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(100))
	require.Zero(t, higher.count)
	require.Empty(t, pt.updates)
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Nil(t, a.Election())
	require.Equal(t, int64(0), a.ElectionCount())
}
