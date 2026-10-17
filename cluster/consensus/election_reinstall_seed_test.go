package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// roleSensitiveAppendSource returns distinct ready positions by role.
//
// Mirrors LogAppendPosition: leader path vs follower path.
type roleSensitiveAppendSource struct {
	agent       *Agent
	leaderPos   int64
	followerPos int64
}

func (s *roleSensitiveAppendSource) AppendPosition() (int64, bool) {
	if s.agent.Role() == RoleLeader {
		return s.leaderPos, true
	}
	return s.followerPos, true
}

// notReadyAppendSource always reports not-ready.
type notReadyAppendSource struct{}

func (notReadyAppendSource) AppendPosition() (int64, bool) { return 0, false }

func storedInt64(v int64) *atomic.Int64 {
	c := new(atomic.Int64)
	c.Store(v)
	return c
}

// TestElectionReinstall_seedMapping asserts ElectionSeed fields from agent state.
func TestElectionReinstall_seedMapping(t *testing.T) {
	const timeoutNs = DefaultLeaderHeartbeatTimeoutNs

	settledSeeds := []struct {
		name string
		cfg  AgentConfig
		want ElectionSeed
	}{
		{
			name: "term_commit_and_append_carried",
			cfg: AgentConfig{
				Role:             RoleFollower,
				Commit:           storedInt64(55),
				LeadershipTermID: 3,
				AppendSource:     &AtomicAppendPosition{V: storedInt64(777)},
			},
			want: ElectionSeed{
				LeadershipTermID:       3,
				CommitPosition:         55,
				AppendPosition:         777,
				AppendPositionReady:    true,
				GracefulClosedLeaderID: nullMemberID,
			},
		},
		{
			name: "nil_append_source",
			cfg: AgentConfig{
				Role:             RoleFollower,
				LeadershipTermID: 1,
			},
			want: ElectionSeed{
				LeadershipTermID:       1,
				GracefulClosedLeaderID: nullMemberID,
			},
		},
		{
			name: "commit_seeded_from_nonzero_counter",
			cfg: AgentConfig{
				Role:   RoleFollower,
				Commit: storedInt64(12345),
			},
			want: ElectionSeed{
				CommitPosition:         12345,
				GracefulClosedLeaderID: nullMemberID,
			},
		},
		{
			name: "term_zero_is_not_sentinel",
			cfg:  AgentConfig{Role: RoleFollower},
			want: ElectionSeed{GracefulClosedLeaderID: nullMemberID},
		},
	}
	for _, tc := range settledSeeds {
		t.Run(tc.name, func(t *testing.T) {
			fac := &recordingElectionFactory{}
			cfg := tc.cfg
			cfg.LeaderHeartbeatTimeoutNs = timeoutNs
			a := NewAgent(cfg)
			a.timeOfLastLogUpdateNs = 0
			a.SetElectionFactory(fac.factory())

			require.Positive(t, a.DoWork(timeoutNs))
			require.Len(t, fac.seeds, 1)
			require.Equal(t, tc.want, fac.seeds[0])
		})
	}

	t.Run("stepping_down_leader_carries_leader_append", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		const leaderAppend int64 = 9001
		const followerAppend int64 = 0
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			Positions:                &scriptedActiveQuorumTable{active: false},
			LeadershipTermID:         2,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		src := &roleSensitiveAppendSource{
			agent:       a,
			leaderPos:   leaderAppend,
			followerPos: followerAppend,
		}
		a.SetAppendSource(src)
		a.SetElectionFactory(fac.factory())

		// activeQuorumTick while still leader installs and flips role.
		require.Equal(t, 1, a.activeQuorumTick(0))
		require.Equal(t, RoleFollower, a.Role())
		require.Len(t, fac.seeds, 1)
		require.Equal(t, leaderAppend, fac.seeds[0].AppendPosition)
		require.True(t, fac.seeds[0].AppendPositionReady)
	})

	t.Run("not_ready_source_does_not_latch_rank_failure", func(t *testing.T) {
		// Seed must not call resolveAppendPosition. Any resolve on a not-ready
		// source opens the not-ready run and stamps appendNotReadySinceNs. Seed
		// reads keep the stamp at nullValue.
		fac := &recordingElectionFactory{}
		const shortTimeout int64 = 1
		a := NewAgent(AgentConfig{
			Role:                            RoleFollower,
			AppendSource:                    notReadyAppendSource{},
			AppendPositionNotReadyTimeoutNs: shortTimeout,
			LeaderHeartbeatTimeoutNs:        timeoutNs,
		})
		a.SetLogRecordingControl(&fakeLogRecordingControl{active: true})
		a.SetLogRecordingID(7)
		a.timeOfLastLogUpdateNs = 0
		a.SetElectionFactory(fac.factory())

		// DoWork installs the election and takes the seed read.
		require.Positive(t, a.DoWork(timeoutNs))
		require.Len(t, fac.seeds, 1)
		require.Equal(t, int64(0), fac.seeds[0].AppendPosition)
		require.False(t, fac.seeds[0].AppendPositionReady)
		require.NoError(t, a.AppendRankFailure())

		// A miswired resolve would have stamped the not-ready run open.
		require.Equal(t, int64(nullValue), a.appendNotReadySinceNs)

		// Second seed via enterElection after complete still leaves
		// appendNotReadySinceNs at nullValue and AppendRankFailure unset.
		a.ElectionComplete(timeoutNs)
		require.Equal(t, 1, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, timeoutNs))
		require.NoError(t, a.AppendRankFailure())
		require.Equal(t, int64(nullValue), a.appendNotReadySinceNs)
		require.Len(t, fac.seeds, 2)
		require.False(t, fac.seeds[1].AppendPositionReady)
	})
}
