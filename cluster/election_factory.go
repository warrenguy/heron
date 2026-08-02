package cluster

import (
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// newElectionFactory returns the settled-state election factory the agent
// calls when a trigger fires on a member that holds no election.
//
// Behaviour:
//   - copies base
//   - forces non-startup and null graceful-closed leader
//   - seeds leadership term id and commit position from the seed
//   - resolves term base from deps.RecordingLogLookup with the recovery plan
//     as fallback
//   - sets append position from the seed when ready, else from the recovery
//     plan via [recordinglog.RecoveryPlan.AppendedOrCommittedPosition]
//   - carries timeout fields, appointed leader id, member id, and app version
//     from base
//   - must not panic on a seed the agent can produce
//
// Concurrency: invoker-only. The agent calls it from its duty cycle.
//
// Lifetime: the factory retains base, deps, and plan. The factory must not
// outlive the recovery that owns the recording log. Close clears the factory
// from the agent before closing recovery.
//
// Ownership: the agent owns each returned runner until
// [consensus.Agent.ElectionComplete] or [consensus.Agent.SetElection] with a
// nil runner clears it. The factory does not own the runner.
//
// OpenConsensusModule wraps this factory with [trackModuleElection] so each
// non-nil install also updates [ConsensusModule] latest-election fields.
func newElectionFactory(
	base election.ElectionConfig,
	deps election.ElectionDeps,
	plan recordinglog.RecoveryPlan,
) consensus.ElectionFactory {
	return func(seed consensus.ElectionSeed) consensus.ElectionRunner {
		return election.NewElection(
			settledElectionConfig(base, seed, deps.RecordingLogLookup, plan),
			deps,
		)
	}
}

// trackModuleElection wraps factory so each non-nil *election.Election install
// becomes the module's latest election handle.
//
// Behaviour:
//   - calls factory
//   - on a non-nil *election.Election, assigns m.election
//   - leaves m.election unchanged when factory returns nil or a runner that is
//     not *election.Election (test doubles)
//   - does not clear m.election on [consensus.Agent.ElectionComplete]
//
// OpenConsensusModule wires this around [newElectionFactory]. Open-time install
// still assigns m.election before [consensus.Agent.SetElection].
//
// Concurrency: invoker-only. Same duty cycle as the inner factory.
func trackModuleElection(
	m *ConsensusModule,
	factory consensus.ElectionFactory,
) consensus.ElectionFactory {
	return func(seed consensus.ElectionSeed) consensus.ElectionRunner {
		runner := factory(seed)
		if e, ok := runner.(*election.Election); ok && e != nil {
			m.election = e
		}
		return runner
	}
}

// settledElectionConfig maps base and seed into the config a re-entered
// election starts with.
func settledElectionConfig(
	base election.ElectionConfig,
	seed consensus.ElectionSeed,
	lookup election.RecordingLogLookup,
	plan recordinglog.RecoveryPlan,
) election.ElectionConfig {
	cfg := base
	cfg.IsNodeStartup = false
	cfg.GracefulClosedLeaderID = election.NullMemberID
	cfg.LeadershipTermID = seed.LeadershipTermID
	cfg.TermBaseLogPosition = termBaseFor(lookup, plan, seed.LeadershipTermID)
	cfg.LogPosition = seed.CommitPosition
	if seed.AppendPositionReady {
		cfg.AppendPosition = seed.AppendPosition
	} else {
		cfg.AppendPosition = plan.AppendedOrCommittedPosition()
	}
	return cfg
}

// termBaseFor resolves the term base for leadershipTermID from the recording
// log term index, falling back to the recovery plan.
//
// Returns the plan's last term base when lookup is nil or holds no entry for
// leadershipTermID.
func termBaseFor(
	lookup election.RecordingLogLookup,
	plan recordinglog.RecoveryPlan,
	leadershipTermID int64,
) int64 {
	if lookup != nil {
		if entry := lookup.FindTermEntry(leadershipTermID); entry != nil {
			return entry.TermBaseLogPosition
		}
	}
	return plan.LastTermBaseLogPosition
}
