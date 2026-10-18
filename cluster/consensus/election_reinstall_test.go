package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

// scriptedActiveQuorumTable scripts HasActiveQuorum for trigger matrix rows.
type scriptedActiveQuorumTable struct {
	stubPositionTable

	active    bool
	lastNowNs int64
	lastTONs  int64
	calls     int
}

func (p *scriptedActiveQuorumTable) HasActiveQuorum(nowNs, timeoutNs int64) bool {
	p.calls++
	p.lastNowNs = nowNs
	p.lastTONs = timeoutNs
	return p.active
}

// recordingElectionFactory records seeds and returns a fresh call-counting
// runner.
type recordingElectionFactory struct {
	seeds     []ElectionSeed
	runners   []*orderRunner
	returnNil bool
}

func (f *recordingElectionFactory) factory() ElectionFactory {
	return func(seed ElectionSeed) ElectionRunner {
		f.seeds = append(f.seeds, seed)
		if f.returnNil {
			return nil
		}
		r := &orderRunner{n: 1}
		f.runners = append(f.runners, r)
		return r
	}
}

// TestElectionReinstall_triggerMatrix covers every settled re-install branch.
func TestElectionReinstall_triggerMatrix(t *testing.T) {
	const timeoutNs = DefaultLeaderHeartbeatTimeoutNs

	t.Run("leader_heartbeat_timeout", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.timeOfLastLogUpdateNs = 0
		a.SetElectionFactory(fac.factory())

		// Install (1) plus same-cycle runner.DoWork (1).
		require.Equal(t, 2, a.DoWork(timeoutNs))
		require.NotNil(t, a.Election())
		require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Equal(t, RoleFollower, a.Role())
		require.Len(t, fac.seeds, 1)
		// Same-cycle tick of the installed runner.
		require.Equal(t, 1, fac.runners[0].calls)
	})

	t.Run("leader_heartbeat_not_yet_due", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.timeOfLastLogUpdateNs = 0
		a.SetElectionFactory(fac.factory())

		require.Equal(t, 0, a.DoWork(timeoutNs-1))
		require.Nil(t, a.Election())
		require.Equal(t, int64(0), a.ElectionCount())
		require.Empty(t, fac.seeds)
	})

	t.Run("inactive_follower_quorum", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		const nowNs int64 = 42
		pt := &scriptedActiveQuorumTable{active: false}
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			Positions:                pt,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())

		require.Equal(t, 2, a.DoWork(nowNs))
		require.NotNil(t, a.Election())
		require.Equal(t, ElectionTriggerInactiveFollowerQuorum, a.LastElectionTrigger())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Equal(t, RoleFollower, a.Role())
		require.Len(t, fac.seeds, 1)
		require.Equal(t, 1, fac.runners[0].calls)
		// Agent passes duty-cycle clock and its timeout into HasActiveQuorum.
		require.Equal(t, 1, pt.calls)
		require.Equal(t, nowNs, pt.lastNowNs)
		require.Equal(t, timeoutNs, pt.lastTONs)
	})

	t.Run("active_follower_quorum", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		pt := &scriptedActiveQuorumTable{active: true}
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			Positions:                pt,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())

		require.Equal(t, 0, a.DoWork(0))
		require.Nil(t, a.Election())
		require.Equal(t, RoleLeader, a.Role())
		require.Empty(t, fac.seeds)
		require.Equal(t, 1, pt.calls)
		require.Equal(t, int64(0), pt.lastNowNs)
		require.Equal(t, timeoutNs, pt.lastTONs)
	})

	t.Run("leader_with_nil_positions", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())

		require.NotPanics(t, func() {
			require.Equal(t, 0, a.DoWork(0))
		})
		require.Nil(t, a.Election())
		require.Empty(t, fac.seeds)
	})

	higherTermNotifies := []struct {
		name   string
		nowNs  int64
		notify func(a *Agent, nowNs int64)
	}{
		{
			name:  "higher_term_commit_position",
			nowNs: 700,
			notify: func(a *Agent, nowNs int64) {
				a.handleSteadyCommitPosition(codecs.CommitPositionFields{
					LeadershipTermID: 9,
					LogPosition:      999,
					LeaderMemberID:   2,
				}, nowNs)
			},
		},
		{
			name:  "higher_term_new_leadership_term",
			nowNs: 800,
			notify: func(a *Agent, nowNs int64) {
				a.handleSteadyNewLeadershipTerm(codecs.NewLeadershipTermFields{
					LeadershipTermID: 9,
					LeaderMemberID:   2,
					CommitPosition:   999,
				}, nowNs)
			},
		},
	}
	for _, tc := range higherTermNotifies {
		t.Run(tc.name, func(t *testing.T) {
			fac := &recordingElectionFactory{}
			a := NewAgent(AgentConfig{
				Role:                     RoleFollower,
				LeadershipTermID:         4,
				LeaderMemberID:           1,
				LeaderHeartbeatTimeoutNs: timeoutNs,
			})
			a.notifiedCommitPosition = 10
			a.SetElectionFactory(fac.factory())

			tc.notify(a, tc.nowNs)
			require.NotNil(t, a.Election())
			require.Equal(t, ElectionTriggerHigherTerm, a.LastElectionTrigger())
			require.Equal(t, int64(10), a.NotifiedCommitPosition())
			require.Equal(t, tc.nowNs, a.TimeOfLastLeaderUpdateNs())
			require.Len(t, fac.seeds, 1)
			require.Equal(t, int64(4), fac.seeds[0].LeadershipTermID)
		})
	}

	t.Run("higher_term_through_DoWork", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		q := &QueueConsensusIngress{}
		const nowNs int64 = 1_000_000_000_000
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			Ingress:                  q,
			LeadershipTermID:         2,
			LeaderMemberID:           1,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		// Fresh settled clock so the heartbeat branch is not due.
		a.ElectionComplete(nowNs)
		require.Nil(t, a.Election())
		a.SetElectionFactory(fac.factory())

		enqueueSteadyLeaderCommit(t, q, "commit", 5, 1, 50)
		// Ingress poll (1) plus same-cycle runner.DoWork (1).
		require.Equal(t, 2, a.DoWork(nowNs+1))
		require.NotNil(t, a.Election())
		require.Equal(t, ElectionTriggerHigherTerm, a.LastElectionTrigger())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Len(t, fac.seeds, 1)
		require.Equal(t, int64(2), fac.seeds[0].LeadershipTermID)
		require.Equal(t, 1, fac.runners[0].calls)
	})

	t.Run("same_term_commit_position", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeadershipTermID:         2,
			LeaderMemberID:           1,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())

		a.handleSteadyCommitPosition(codecs.CommitPositionFields{
			LeadershipTermID: 2,
			LogPosition:      40,
			LeaderMemberID:   1,
		}, 500)
		require.Nil(t, a.Election())
		require.Equal(t, int64(40), a.NotifiedCommitPosition())
		require.Empty(t, fac.seeds)
	})

	t.Run("lower_term_commit_position", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeadershipTermID:         5,
			LeaderMemberID:           1,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.notifiedCommitPosition = 20
		a.SetElectionFactory(fac.factory())

		a.handleSteadyCommitPosition(codecs.CommitPositionFields{
			LeadershipTermID: 3,
			LogPosition:      100,
			LeaderMemberID:   1,
		}, 600)
		require.Nil(t, a.Election())
		require.Equal(t, int64(20), a.NotifiedCommitPosition())
		require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
		require.Empty(t, fac.seeds)
	})

	t.Run("reentry_while_election_live", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		live := &orderRunner{n: 1}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElection(live)
		require.Equal(t, int64(1), a.ElectionCount())
		require.Equal(t, ElectionTriggerBootstrap, a.LastElectionTrigger())
		a.timeOfLastLogUpdateNs = 0
		a.SetElectionFactory(fac.factory())

		// Outer slow-tick guard: live election, no factory call.
		require.Equal(t, 1, a.DoWork(timeoutNs))
		require.Equal(t, ElectionRunner(live), a.Election())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Empty(t, fac.seeds)

		// enterElection re-check: refuse while live, same runner, no seed.
		require.Zero(t, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, timeoutNs))
		require.Equal(t, ElectionRunner(live), a.Election())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Equal(t, ElectionTriggerBootstrap, a.LastElectionTrigger())
		require.Empty(t, fac.seeds)
	})

	t.Run("nil_factory", func(t *testing.T) {
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.timeOfLastLogUpdateNs = 0

		require.Equal(t, 0, a.DoWork(timeoutNs))
		require.Nil(t, a.Election())
		require.Equal(t, int64(0), a.ElectionCount())
	})

	t.Run("factory_returns_nil", func(t *testing.T) {
		fac := &recordingElectionFactory{returnNil: true}
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			Positions:                &scriptedActiveQuorumTable{active: false},
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())

		require.Equal(t, 0, a.DoWork(0))
		require.Nil(t, a.Election())
		require.Equal(t, int64(0), a.ElectionCount())
		require.Equal(t, RoleLeader, a.Role())
		require.Len(t, fac.seeds, 1)
	})

	t.Run("clears_pending_join_log", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			Positions:                &scriptedActiveQuorumTable{active: false},
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())
		a.pendingJoinLog = &codecs.JoinLogFields{LogPosition: 99}
		require.True(t, a.HasPendingJoinLog())

		require.Equal(t, 1, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, 0))
		require.False(t, a.HasPendingJoinLog())
		require.Equal(t, RoleFollower, a.Role())
		require.NotNil(t, a.Election())
		require.Len(t, fac.seeds, 1)
	})

	t.Run("refuse_nil_factory_keeps_pending_join_log", func(t *testing.T) {
		fac := &recordingElectionFactory{returnNil: true}
		a := NewAgent(AgentConfig{
			Role:                     RoleLeader,
			Positions:                &scriptedActiveQuorumTable{active: false},
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())
		a.pendingJoinLog = &codecs.JoinLogFields{LogPosition: 99}
		require.True(t, a.HasPendingJoinLog())

		require.Zero(t, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, 0))
		require.True(t, a.HasPendingJoinLog())
		require.Equal(t, RoleLeader, a.Role())
		require.Nil(t, a.Election())
	})

	t.Run("refuse_live_election_keeps_pending_join_log", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(fac.factory())
		require.Equal(t, 1, a.enterElection(ElectionTriggerBootstrap, false, 0))
		require.NotNil(t, a.Election())
		a.pendingJoinLog = &codecs.JoinLogFields{LogPosition: 99}

		require.Zero(t, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, 0))
		require.True(t, a.HasPendingJoinLog())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Len(t, fac.seeds, 1)
	})

	t.Run("slow_tick_not_due", func(t *testing.T) {
		// Small timeout so a later tick past the slow-tick interval is overdue.
		const shortTimeoutNs int64 = 100
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: shortTimeoutNs,
		})
		a.timeOfLastLogUpdateNs = 0
		a.SetElectionFactory(fac.factory())

		// Unproductive first tick: not yet overdue. Advances deadline by interval.
		require.Equal(t, 0, a.DoWork(50))
		require.Empty(t, fac.seeds)
		// Second call inside the interval: no re-eval (even though still short).
		require.Equal(t, 0, a.DoWork(51))
		require.Empty(t, fac.seeds)
		// Third past the interval: now overdue relative to stamp 0, installs.
		require.Equal(t, 2, a.DoWork(50+slowTickIntervalNs))
		require.Len(t, fac.seeds, 1)
		require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
	})

	t.Run("election_complete_stamps_follower_clock", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		const nowNs int64 = 5_000_000_000_000
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElection(&orderRunner{n: 1})
		a.ElectionComplete(nowNs)
		a.SetElectionFactory(fac.factory())

		require.Equal(t, 0, a.DoWork(nowNs+1))
		require.Nil(t, a.Election())
		require.Empty(t, fac.seeds)

		// Past timeout and past the unproductive slow-tick deadline.
		// ElectionComplete set timeOfLastLogUpdateNs = nowNs. First DoWork
		// at nowNs+1 advanced deadline by slowTickIntervalNs from nowNs+1.
		// Install at the first slow tick past stamp + timeout.
		installAt := nowNs + timeoutNs + slowTickIntervalNs
		require.Equal(t, 2, a.DoWork(installAt))
		require.NotNil(t, a.Election())
		require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
	})

	t.Run("election_complete_stamps_leader_clock", func(t *testing.T) {
		const nowNs int64 = 9_000_000_000_000
		const intervalNs int64 = DefaultLeaderHeartbeatIntervalNs
		a := NewAgent(AgentConfig{
			Role:                      RoleLeader,
			Commit:                    new(atomic.Int64),
			Egress:                    &recordingEgress{},
			LeaderHeartbeatIntervalNs: intervalNs,
		})
		// Recent stamp blocks UpdateLeaderPosition until ElectionComplete
		// reopens the interval.
		a.timeOfLastLogUpdateNs = nowNs
		require.Equal(t, 0, a.UpdateLeaderPosition(nowNs, 0))

		a.SetElection(&orderRunner{n: 1})
		a.ElectionComplete(nowNs)
		// Stamp reopens: nowNs - interval makes interval due at nowNs.
		require.Equal(t, 1, a.UpdateLeaderPosition(nowNs, 0))
	})

	t.Run("last_trigger_survives_election_complete", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.timeOfLastLogUpdateNs = 0
		a.SetElectionFactory(fac.factory())
		require.Positive(t, a.DoWork(timeoutNs))
		require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
		count := a.ElectionCount()

		a.ElectionComplete(timeoutNs)
		require.Nil(t, a.Election())
		require.Equal(t, count, a.ElectionCount())
		require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
	})
}

// TestElectionTrigger_String covers known and unknown trigger names.
func TestElectionTrigger_String(t *testing.T) {
	tests := []struct {
		trigger ElectionTrigger
		want    string
	}{
		{ElectionTriggerNone, "NONE"},
		{ElectionTriggerBootstrap, "BOOTSTRAP"},
		{ElectionTriggerLeaderHeartbeatTimeout, "LEADER_HEARTBEAT_TIMEOUT"},
		{ElectionTriggerInactiveFollowerQuorum, "INACTIVE_FOLLOWER_QUORUM"},
		{ElectionTriggerHigherTerm, "HIGHER_TERM"},
		{ElectionTriggerLogRecordingStopped, "LOG_RECORDING_STOPPED"},
		{ElectionTriggerLogImageClosed, "LOG_IMAGE_CLOSED"},
		{ElectionTriggerVoteRequest, "VOTE_REQUEST"},
		{ElectionTrigger(99), "Trigger(99)"},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, tc.trigger.String())
	}
}
