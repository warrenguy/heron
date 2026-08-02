//go:build testhooks

package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

// TestTrackModuleElection_reentryUpdatesLatest covers the dual-handle fix:
// settled re-install replaces m.election so module accessors track the live
// machine, not the open-time Closed handle.
//
// Mirrors OpenConsensusModule open order with trackModuleElection. Does not
// open a live media driver. Needs testhooks for election.SetStateForTest.
func TestTrackModuleElection_reentryUpdatesLatest(t *testing.T) {
	t.Parallel()

	const timeoutNs int64 = 3_000_000_000

	fx := newFactoryFixture(t)
	fx.base.LeaderHeartbeatTimeoutNs = timeoutNs

	openElection := election.NewElection(fx.base, fx.deps)
	// On completion the election marks the machine Closed before the agent
	// clears its runner. Force Closed so ElectionComplete matches that
	// post-settle surface without driving the full state machine.
	election.SetStateForTest(openElection, election.ElectionStateClosed)

	agent := consensus.NewAgent(consensus.AgentConfig{
		Commit:                   new(atomic.Int64),
		ThisMemberID:             fx.base.MemberID,
		LeaderMemberID:           election.NullMemberID,
		LeaderHeartbeatTimeoutNs: timeoutNs,
		AppVersion:               fx.base.AppVersion,
	})
	module := &ConsensusModule{
		agent:    agent,
		election: openElection,
	}
	agent.SetElection(openElection)
	agent.SetElectionFactory(trackModuleElection(
		module,
		newElectionFactory(fx.base, fx.deps, fx.plan),
	))

	require.True(t, module.ElectionComplete())
	require.Equal(t, election.ElectionStateClosed, module.ElectionState())

	// Settle agent runner; module keeps the Closed open-time handle until
	// re-install.
	agent.ElectionComplete(0)
	require.Nil(t, agent.Election())
	require.True(t, module.ElectionComplete())

	// Quiet tick re-installs. Same-cycle DoWork may advance the new machine
	// past Init. Module must track that live runner, not the open-time Closed
	// handle.
	require.Positive(t, agent.DoWork(timeoutNs))
	require.NotNil(t, agent.Election())
	require.Equal(t, int64(2), agent.ElectionCount())
	require.False(t, module.ElectionComplete())
	require.NotEqual(t, election.ElectionStateClosed, module.ElectionState())
	require.Same(t, agent.Election(), module.election)
	require.NotSame(t, openElection, module.election)

	// Second settle: latest machine Closed; complete still true after agent
	// clears its runner.
	reinstalled := module.election
	require.NotNil(t, reinstalled)
	election.SetStateForTest(reinstalled, election.ElectionStateClosed)
	agent.ElectionComplete(timeoutNs)
	require.Nil(t, agent.Election())
	require.True(t, module.ElectionComplete())
	require.Equal(t, election.ElectionStateClosed, module.ElectionState())
	require.Same(t, reinstalled, module.election)
}
