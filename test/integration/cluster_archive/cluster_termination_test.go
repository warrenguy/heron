//go:build integration && archive_live

package cluster_archive_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/service"
	"github.com/warrenguy/luas/test/integration/harness"
)

func amendTerminationMember(cfg cluster.Config) cluster.Config {
	cfg = amendServiceMember(cfg)
	cfg.TerminationTimeoutNs = int64(10 * time.Minute)
	return cfg
}

// terminatingRecorder records the log position of each Terminate call.
type terminatingRecorder struct {
	*sessionRecorder
	positions []int64
}

func (r *terminatingRecorder) Terminate(c service.Cluster) error {
	r.positions = append(r.positions, c.LogPosition())
	return nil
}

// TestClusterTermination_shutdownStopsEveryMemberAtOnePosition toggles
// SHUTDOWN on the leader of a three-member cluster. Every module and service
// stops at the snapshot position.
func TestClusterTermination_shutdownStopsEveryMemberAtOnePosition(t *testing.T) {
	t.Parallel()

	terminators := map[*sessionRecorder]*terminatingRecorder{}
	sc := startWrappedServiceCluster(
		t,
		3,
		harness.ClusterTerminationPortBase,
		harness.ClusterTerminationLogEndpoint,
		amendTerminationMember,
		func(rec *sessionRecorder) service.ClusteredService {
			terminator := &terminatingRecorder{sessionRecorder: rec}
			terminators[rec] = terminator
			return terminator
		},
	)
	set := sc.set
	requireServicesJoined(t, sc)
	leader := requireSingleLeader(t, set, "")

	toggles := findMemberToggles(t, sc)
	leaderToggle := toggles[int(leader.ThisMemberID())].control
	requireServiceAwaitTick(t, sc, "leader toggle neutral", durableWorkBound, func() bool {
		return leaderToggle.Load() == int64(consensus.ControlToggleNeutral)
	})

	require.True(t, consensus.ControlToggleShutdown.Toggle(leaderToggle), sc.dump())
	requireServiceAwaitTick(t, sc, "snapshot at one position", durableWorkBound, func() bool {
		requireNoServiceFailures(t, sc, " during shutdown snapshot")
		result, ok := leader.Agent().LastSnapshotTake()
		if !ok {
			return false
		}
		return snapshotTakenOnEveryMember(t, set, result.LogPosition, sc.dump)
	})
	result, ok := leader.Agent().LastSnapshotTake()
	require.True(t, ok, sc.dump())
	position := result.LogPosition
	require.Positive(t, position, sc.dump())

	requireServiceAwaitTick(t, sc, "modules closed", durableWorkBound, func() bool {
		requireNoServiceFailures(t, sc, " during shutdown")
		return everyMemberInState(set, consensus.ModuleStateClosed)
	})

	for i, module := range set.Modules() {
		msg := fmt.Sprintf("member %d:\n%s", i, sc.dump())
		agent := module.Agent()
		require.Equal(t, position, agent.TerminationPosition(), msg)
		require.Equal(t, position, module.CommitPosition(), msg)

		termErr := agent.TerminationError()
		var planned *consensus.TerminationError
		require.ErrorAs(t, termErr, &planned, msg)
		require.True(t, planned.Expected, msg)
		require.ErrorIs(t, termErr, consensus.ErrTerminated, msg)
		require.ErrorIs(t, termErr, consensus.ErrClusterTerminated, msg)

		mem := sc.members[i]
		require.True(t, mem.agent.IsTerminated(), msg)
		require.NoError(t, mem.agent.ServiceFailure(), msg)

		terminator := terminators[mem.rec]
		require.NotNil(t, terminator, msg)
		require.Equal(t, []int64{position}, terminator.positions, msg)
	}

	require.Equal(t, int64(consensus.ControlToggleShutdown), leaderToggle.Load(), sc.dump())
}
