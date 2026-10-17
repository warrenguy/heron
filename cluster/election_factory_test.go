package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// mapTermLookup is a RecordingLogLookup for factory mapping tests.
type mapTermLookup map[int64]*recordinglog.Entry

func (m mapTermLookup) FindTermEntry(logLeadershipTermID int64) *recordinglog.Entry {
	if m == nil {
		return nil
	}
	return m[logLeadershipTermID]
}

// factoryFixture holds one base config, deps, and plan for mapping rows.
type factoryFixture struct {
	base election.ElectionConfig
	deps election.ElectionDeps
	plan recordinglog.RecoveryPlan
}

func newFactoryFixture(t *testing.T) factoryFixture {
	t.Helper()

	member := &election.Member{ID: 7}
	store, err := election.OpenNodeStateFile(t.TempDir(), true, recordinglog.FileSyncNone)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	base := election.ElectionConfig{
		IsNodeStartup:             true,
		GracefulClosedLeaderID:    3,
		LeadershipTermID:          99,
		TermBaseLogPosition:       111,
		LogPosition:               222,
		AppendPosition:            333,
		AppointedLeaderID:         5,
		MemberID:                  7,
		AppVersion:                42,
		ElectionTimeoutNs:         1_111_000_000,
		ElectionStatusIntervalNs:  222_000_000,
		LeaderHeartbeatTimeoutNs:  3_333_000_000,
		LeaderHeartbeatIntervalNs: 444_000_000,
		StartupCanvassTimeoutNs:   5_555_000_000,
	}
	deps := election.ElectionDeps{
		Handler:            electiontest.NoopAgentHandler{},
		Host:               electiontest.NoopElectionHost{},
		Publisher:          &election.SbePublisher{},
		NanoClock:          func() int64 { return 1_000_000_000 },
		EpochClock:         func() int64 { return 1_000 },
		NodeState:          store,
		Members:            []*election.Member{member},
		ThisMember:         member,
		RecordingLogLookup: mapTermLookup{},
	}
	plan := recordinglog.RecoveryPlan{
		LastTermBaseLogPosition: 9001,
		AppendedLogPosition:     8002,
		CommittedLogPosition:    7003,
	}
	return factoryFixture{
		base: base,
		deps: deps,
		plan: plan,
	}
}

func requireCarriedForward(t *testing.T, base, got election.ElectionConfig) {
	t.Helper()
	require.Equal(t, base.ElectionTimeoutNs, got.ElectionTimeoutNs)
	require.Equal(t, base.ElectionStatusIntervalNs, got.ElectionStatusIntervalNs)
	require.Equal(t, base.LeaderHeartbeatTimeoutNs, got.LeaderHeartbeatTimeoutNs)
	require.Equal(t, base.LeaderHeartbeatIntervalNs, got.LeaderHeartbeatIntervalNs)
	require.Equal(t, base.StartupCanvassTimeoutNs, got.StartupCanvassTimeoutNs)
	require.Equal(t, base.AppointedLeaderID, got.AppointedLeaderID)
	require.Equal(t, base.MemberID, got.MemberID)
	require.Equal(t, base.AppVersion, got.AppVersion)
	require.False(t, got.IsNodeStartup)
}

func TestSettledElectionConfig_mapping(t *testing.T) {
	t.Parallel()

	fx := newFactoryFixture(t)
	termEntry := &recordinglog.Entry{
		LeadershipTermID:    10,
		TermBaseLogPosition: 4040,
	}
	lookupWithTerm := mapTermLookup{10: termEntry}

	readySeed := consensus.ElectionSeed{
		LeadershipTermID:       10,
		CommitPosition:         500,
		AppendPosition:         600,
		AppendPositionReady:    true,
		GracefulClosedLeaderID: election.NullMemberID,
	}
	notReadySeed := consensus.ElectionSeed{
		LeadershipTermID:       10,
		CommitPosition:         500,
		AppendPosition:         600,
		AppendPositionReady:    false,
		GracefulClosedLeaderID: election.NullMemberID,
	}
	gracefulSeed := readySeed
	gracefulSeed.GracefulClosedLeaderID = 1
	nullAppendPlan := recordinglog.RecoveryPlan{
		LastTermBaseLogPosition: fx.plan.LastTermBaseLogPosition,
		AppendedLogPosition:     recordinglog.NullPosition,
		CommittedLogPosition:    7003,
	}

	tests := []struct {
		name string
		seed consensus.ElectionSeed
		look election.RecordingLogLookup
		plan recordinglog.RecoveryPlan
		// check asserts the row-specific field mapping.
		check func(t *testing.T, got election.ElectionConfig)
	}{
		{
			name: "term base from the index",
			seed: readySeed,
			look: lookupWithTerm,
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, termEntry.TermBaseLogPosition, got.TermBaseLogPosition)
			},
		},
		{
			name: "term base falls back",
			seed: readySeed,
			look: mapTermLookup{},
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, fx.plan.LastTermBaseLogPosition, got.TermBaseLogPosition)
			},
		},
		{
			name: "nil lookup",
			seed: readySeed,
			look: nil,
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, fx.plan.LastTermBaseLogPosition, got.TermBaseLogPosition)
			},
		},
		{
			name: "commit into LogPosition",
			seed: readySeed,
			look: lookupWithTerm,
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, readySeed.CommitPosition, got.LogPosition)
			},
		},
		{
			name: "append ready",
			seed: readySeed,
			look: lookupWithTerm,
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, readySeed.AppendPosition, got.AppendPosition)
			},
		},
		{
			name: "append not ready",
			seed: notReadySeed,
			look: lookupWithTerm,
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, fx.plan.AppendedLogPosition, got.AppendPosition)
			},
		},
		{
			name: "graceful closed leader from the seed",
			seed: gracefulSeed,
			look: lookupWithTerm,
			plan: fx.plan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, int32(1), got.GracefulClosedLeaderID)
			},
		},
		{
			name: "append not ready and plan appended is the null sentinel",
			seed: notReadySeed,
			look: lookupWithTerm,
			plan: nullAppendPlan,
			check: func(t *testing.T, got election.ElectionConfig) {
				require.Equal(t, nullAppendPlan.CommittedLogPosition, got.AppendPosition)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := settledElectionConfig(fx.base, tt.seed, tt.look, tt.plan)
			require.Equal(t, tt.seed.LeadershipTermID, got.LeadershipTermID)
			require.Equal(t, tt.seed.GracefulClosedLeaderID, got.GracefulClosedLeaderID)
			requireCarriedForward(t, fx.base, got)
			tt.check(t, got)
		})
	}
}

func TestNewElectionFactory_usableAndNoPanic(t *testing.T) {
	t.Parallel()

	fx := newFactoryFixture(t)
	factory := newElectionFactory(fx.base, fx.deps, fx.plan)

	t.Run("a returned election is usable", func(t *testing.T) {
		t.Parallel()

		runner := factory(consensus.ElectionSeed{
			LeadershipTermID:       10,
			CommitPosition:         500,
			AppendPosition:         600,
			AppendPositionReady:    true,
			GracefulClosedLeaderID: election.NullMemberID,
		})
		require.NotNil(t, runner)
		e, ok := runner.(*election.Election)
		require.True(t, ok)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, int64(10), e.LeadershipTermID())
		require.Equal(t, int64(500), e.LogPosition())
		require.Equal(t, int64(600), e.AppendPosition())
	})

	t.Run("no panic on a seed the agent could produce", func(t *testing.T) {
		t.Parallel()

		require.NotPanics(t, func() {
			runner := factory(consensus.ElectionSeed{
				LeadershipTermID:       0,
				CommitPosition:         0,
				AppendPosition:         0,
				AppendPositionReady:    false,
				GracefulClosedLeaderID: election.NullMemberID,
			})
			require.NotNil(t, runner)
			e, ok := runner.(*election.Election)
			require.True(t, ok)
			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, int64(0), e.LeadershipTermID())
			require.Equal(t, int64(0), e.LogPosition())
			require.Equal(t, fx.plan.AppendedLogPosition, e.AppendPosition())
		})
	})
}

// TestElectionFactory_openWireStyle covers the open wire, Close clear, and
// non-default timeout reaching the agent.
//
// Mirrors OpenConsensusModule assembly and Close unbind on a bare agent. Does
// not call OpenConsensusModule or ConsensusModule.Close: those need a live
// media driver and Archive control client. Unit package has no light fixture
// for that path. Live open/re-elect stays under archive_live coverage.
func TestElectionFactory_openWireStyle(t *testing.T) {
	t.Parallel()

	const timeoutNs int64 = 3_000_000_000

	newWiredAgent := func(t *testing.T) *consensus.Agent {
		t.Helper()
		fx := newFactoryFixture(t)
		fx.base.LeaderHeartbeatTimeoutNs = timeoutNs
		factory := newElectionFactory(fx.base, fx.deps, fx.plan)
		agent := consensus.NewAgent(consensus.AgentConfig{
			Commit:                   new(atomic.Int64),
			ThisMemberID:             fx.base.MemberID,
			LeaderMemberID:           election.NullMemberID,
			LeaderHeartbeatTimeoutNs: timeoutNs,
			AppVersion:               fx.base.AppVersion,
		})
		// Open-time install, then factory wire (Open order).
		agent.SetElection(election.NewElection(fx.base, fx.deps))
		agent.SetElectionFactory(factory)
		return agent
	}

	t.Run("open wires factory installs second election", func(t *testing.T) {
		t.Parallel()

		agent := newWiredAgent(t)
		require.Equal(t, int64(1), agent.ElectionCount())

		// Force settled quiet clock and clear the open-time runner.
		agent.ElectionComplete(0)
		require.Nil(t, agent.Election())

		work := agent.DoWork(timeoutNs)
		require.Positive(t, work)
		require.NotNil(t, agent.Election())
		require.Equal(t, int64(2), agent.ElectionCount())
		require.Equal(
			t,
			consensus.ElectionTriggerLeaderHeartbeatTimeout,
			agent.LastElectionTrigger(),
		)
	})

	t.Run("close clears factory and blocks re-install", func(t *testing.T) {
		t.Parallel()

		agent := newWiredAgent(t)
		agent.ElectionComplete(0)

		// Close unbind: factory first, then runner.
		agent.SetElectionFactory(nil)
		agent.SetElection(nil)

		// Past the timeout window: no factory, no install.
		require.Equal(t, 0, agent.DoWork(timeoutNs*2))
		require.Nil(t, agent.Election())
		require.Equal(t, int64(1), agent.ElectionCount())
	})

	t.Run("configured timeout reaches agent", func(t *testing.T) {
		t.Parallel()

		// Fresh agent: one nanosecond short of the configured timeout holds.
		short := newWiredAgent(t)
		short.ElectionComplete(0)
		require.Equal(t, 0, short.DoWork(timeoutNs-1))
		require.Nil(t, short.Election())
		require.Equal(t, int64(1), short.ElectionCount())

		// Fresh agent: at the configured (non-default) timeout the quiet tick
		// installs. Separate agent so slow-tick pacing from the short tick
		// does not push the deadline past timeoutNs.
		due := newWiredAgent(t)
		due.ElectionComplete(0)
		require.Positive(t, due.DoWork(timeoutNs))
		require.NotNil(t, due.Election())
		require.Equal(t, int64(2), due.ElectionCount())
	})
}
