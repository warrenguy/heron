//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/test/integration/harness"
)

// isolateNewLeader commits an append in the new leader's term while the
// ex-leader at exIndex is down. It then stops the other survivor, which costs
// the leader its quorum. It waits until the leader enters an election.
func isolateNewLeader(
	t *testing.T,
	set *harness.ClusterMemberSet,
	exIndex int,
	newLeader *cluster.ConsensusModule,
) {
	t.Helper()

	missedPos := mustAppend(t, set, newLeader, "missed-term-append")
	down := []int{exIndex}
	requireAwaitTick(t, set, "survivors commit in the new term", durableWorkBound, func() bool {
		return survivorsReachCommit(
			set, down, newLeader.ThisMemberID(), newLeader.CommitPosition(), missedPos,
		)
	})

	survivorIndex := firstNonLeaderIndex(set, newLeader.ThisMemberID())
	require.NoError(t, set.StopMember(survivorIndex))
	require.Nil(t, set.Module(survivorIndex))
	requireAwaitTick(t, set, "leader enters an election", durableReElectBound, func() bool {
		return !newLeader.ElectionComplete()
	})
}

// awaitReplicatedRejoin ticks until the ex-leader at exIndex and the leader
// at leaderIndex complete an election in the same term. The ex-leader must
// pass through FOLLOWER_LOG_REPLICATION without a commit timeout in
// moduleErrors. A non-nil perTick runs at the start of each rejoin tick.
// Returns the ex-leader's log recording stop position on entry to
// FOLLOWER_LOG_REPLICATION.
func awaitReplicatedRejoin(
	t *testing.T,
	set *harness.ClusterMemberSet,
	moduleErrors *harness.ModuleErrors,
	exIndex int,
	leaderIndex int,
	perTick func(),
) int64 {
	t.Helper()

	watch := harness.NewReplicationWatch(t, set.Module(exIndex), moduleErrors, set.State)
	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)
	requireAwaitTick(t, set, "ex-leader rejoins through replication", durableElectBound, func() bool {
		if perTick != nil {
			perTick()
		}
		watch.Observe()
		ex := set.Module(exIndex)
		leader := set.Module(leaderIndex)
		if ex == nil || leader == nil || !leader.IsLeader() {
			return false
		}
		return ex.ElectionComplete() &&
			leader.ElectionComplete() &&
			ex.LeadershipTermID() == leader.LeadershipTermID()
	})
	return watch.RequireReplicated()
}

// requireReplicatedCommit appends on the leader. Both live members must commit
// the append. The ex-leader must record it under recordingID.
func requireReplicatedCommit(
	t *testing.T,
	set *harness.ClusterMemberSet,
	exIndex int,
	leaderIndex int,
	recordingID int64,
) {
	t.Helper()

	leader := set.Module(leaderIndex)
	appendPos := mustAppend(t, set, leader, "post-replication-commit")
	requireAwaitTick(t, set, "ex-leader commits after replication", durableWorkBound, func() bool {
		ex := set.Module(exIndex)
		return ex != nil &&
			leader.CommitPosition() >= appendPos &&
			ex.CommitPosition() >= appendPos &&
			ex.NotifiedCommitPosition() >= appendPos &&
			localRecordingAtLeast(ex, appendPos)
	})

	ex := set.Module(exIndex)
	exRecordingID := ex.Agent().LocalLogRecordingID()
	require.Equal(
		t,
		appendPos,
		harness.RecordingCounterPosition(ex, exRecordingID),
		"ex-leader recording counter:\n%s",
		set.State(),
	)
	require.Equal(
		t,
		recordingID,
		exRecordingID,
		"ex-leader recording id after replication:\n%s",
		set.State(),
	)
}

// TestDurableCluster_replicatesAnExLeaderBehindTheTermBase asserts that a
// stopped leader rejoins through log replication. Its recording ends at the new
// term base. One member of three stays down. The leader runs an election when
// the ex-leader returns.
//
// Asserts:
//   - the ex-leader passes through FOLLOWER_LOG_REPLICATION
//   - the ex-leader does not report a commit timeout
//   - both live members complete one election in the same term
//   - a later append commits and records on the ex-leader
//   - the ex-leader keeps its recording id
//   - the ex-leader's entry for the missed term starts at the floor
func TestDurableCluster_replicatesAnExLeaderBehindTheTermBase(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set, moduleErrors := openDurableClusterWithErrors(
		t,
		harness.DurableReplicatePortBase,
		harness.DurableReplicateLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	exIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()
	recordingID := leader.Agent().LocalLogRecordingID()
	require.NotEqual(t, int64(-1), recordingID, "leader LocalLogRecordingID:\n%s", set.State())

	floorPos := mustAppend(t, set, leader, "pre-stop-floor")
	requireAwaitTick(t, set, "pre-stop commit floor", durableWorkBound, func() bool {
		return membersReachCommit(set, floorPos)
	})

	// Phase: the leader stops at the floor. The survivors elect a new leader.
	require.NoError(t, set.StopMember(exIndex))
	require.Nil(t, set.Module(exIndex))
	newLeader := awaitReElectionAfterStop(t, set, exIndex, preTerm)
	newLeaderIndex := int(newLeader.ThisMemberID())
	missedTerm := newLeader.LeadershipTermID()
	require.Equal(
		t,
		floorPos,
		harness.TermBaseLogPosition(newLeader, missedTerm),
		"new term base:\n%s",
		set.State(),
	)

	// Phase: the leader commits in its term. The second survivor stops, so the
	// leader loses its quorum.
	isolateNewLeader(t, set, exIndex, newLeader)

	// Phase: the ex-leader returns with a recording that ends at the new term
	// base.
	require.NoError(t, set.RestartMember(exIndex))
	requireWarmRestartOpen(t, set, exIndex, "returning ex-leader")
	awaitReplicatedRejoin(t, set, moduleErrors, exIndex, newLeaderIndex, nil)

	requireReplicatedCommit(t, set, exIndex, newLeaderIndex, recordingID)
	require.Equal(
		t,
		floorPos,
		harness.TermBaseLogPosition(set.Module(exIndex), missedTerm),
		"ex-leader entry for the missed term:\n%s",
		set.State(),
	)
}

// TestDurableCluster_replicatesATruncatedExLeader asserts that a divergent
// ex-leader truncates to the new term base. The ex-leader then rejoins through
// log replication. One member of three stays down. The leader runs an election
// when the ex-leader returns.
//
// Asserts:
//   - the ex-leader recording stops at the new term base when it enters
//     FOLLOWER_LOG_REPLICATION
//   - the ex-leader does not report a commit timeout
//   - both live members complete one election in the same term
//   - a later append commits and records on the ex-leader
//   - the ex-leader keeps its recording id
//   - the ex-leader's entry for the missed term starts at the new term base
func TestDurableCluster_replicatesATruncatedExLeader(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set, moduleErrors := openDurableClusterWithErrors(
		t,
		harness.DurableReplicateTruncatePortBase,
		harness.DurableReplicateTruncateLogEndpoint,
	)

	recordingID := requireSingleLeader(t, set, "").Agent().LocalLogRecordingID()
	require.NotEqual(t, int64(-1), recordingID, "leader LocalLogRecordingID:\n%s", set.State())

	diverged := divergeAndReElect(t, set, 1)
	exIndex := diverged.index
	newLeader := requireSingleLeader(t, set, "after re-elect")
	newLeaderIndex := int(newLeader.ThisMemberID())

	// Phase: the leader commits in its term. The second survivor stops, so the
	// leader loses its quorum.
	isolateNewLeader(t, set, exIndex, newLeader)

	// Phase: the ex-leader returns with a tail above the new term base.
	restartDivergedLeader(t, set, diverged)
	entryStopPosition := awaitReplicatedRejoin(
		t,
		set,
		moduleErrors,
		exIndex,
		newLeaderIndex,
		nil,
	)
	require.Equal(
		t,
		diverged.newTermBase,
		entryStopPosition,
		"ex-leader stop position on entry to FOLLOWER_LOG_REPLICATION:\n%s",
		set.State(),
	)

	requireReplicatedCommit(t, set, exIndex, newLeaderIndex, recordingID)
	require.Equal(
		t,
		diverged.newTermBase,
		harness.TermBaseLogPosition(set.Module(exIndex), diverged.newTerm),
		"ex-leader entry for the missed term:\n%s",
		set.State(),
	)
}
