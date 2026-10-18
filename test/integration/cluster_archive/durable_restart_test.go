//go:build integration && archive_live

package cluster_archive_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

// Await bounds shared by the durable scenarios. Quiet localhost completes most
// waits in milliseconds; the bounds absorb loaded CI hosts.
const (
	durableImageBound   = 30 * time.Second
	durableElectBound   = 60 * time.Second
	durableWorkBound    = 60 * time.Second
	durableReElectBound = 45 * time.Second
)

// openDurableCluster starts a three-member durable set on the given port block.
// Waits for session-keyed log images, then ticks until every member reports
// ElectionComplete.
func openDurableCluster(
	t *testing.T,
	portBase int,
	logEndpoint string,
) *harness.ClusterMemberSet {
	t.Helper()
	return openDurableClusterN(t, 3, portBase, logEndpoint)
}

// openDurableClusterN starts a durable set of memberCount members on the given
// port block. Waits for session-keyed log images, then ticks until every member
// reports ElectionComplete.
func openDurableClusterN(
	t *testing.T,
	memberCount int,
	portBase int,
	logEndpoint string,
) *harness.ClusterMemberSet {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          memberCount,
		PortBase:             portBase,
		LogMulticastEndpoint: logEndpoint,
	})
	require.Len(t, set.Modules(), memberCount)

	requireAwait(t, set, "log image fan-in", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", durableElectBound, set.AwaitElection)

	return set
}

// nonLeaderIndices returns live module slots whose ThisMemberID is not
// leaderMemberID, in ascending index order.
func nonLeaderIndices(set *harness.ClusterMemberSet, leaderMemberID int32) []int {
	var out []int
	for i, m := range set.Modules() {
		if m != nil && m.ThisMemberID() != leaderMemberID {
			out = append(out, i)
		}
	}
	return out
}

// firstNonLeaderIndex returns the first live module slot whose ThisMemberID is
// not leaderMemberID. Returns -1 when no such slot exists.
func firstNonLeaderIndex(set *harness.ClusterMemberSet, leaderMemberID int32) int {
	idxs := nonLeaderIndices(set, leaderMemberID)
	if len(idxs) == 0 {
		return -1
	}
	return idxs[0]
}

// survivorsAppendAtLeast reports whether every live member except those in
// exclude has an AppendSource ready at or above target.
func survivorsAppendAtLeast(
	set *harness.ClusterMemberSet,
	exclude []int,
	target int64,
) bool {
	for i, m := range set.Modules() {
		if m == nil || slices.Contains(exclude, i) {
			continue
		}
		src := m.Agent().AppendSource()
		if src == nil {
			return false
		}
		pos, ready := src.AppendPosition()
		if !ready || pos < target {
			return false
		}
	}
	return true
}

// membersReachCommit reports whether every live member has CommitPosition at
// least target, and every non-leader has NotifiedCommitPosition at least
// target.
//
// NotifiedCommitPosition advances only while a member is a follower. A
// leader's value stays at whatever it held when leadership was won.
func membersReachCommit(set *harness.ClusterMemberSet, target int64) bool {
	for _, m := range set.Modules() {
		if m == nil {
			return false
		}
		if m.CommitPosition() < target {
			return false
		}
		if m.IsLeader() {
			continue
		}
		if m.NotifiedCommitPosition() < target {
			return false
		}
	}
	return true
}

// survivorsReachCommit reports leader commit and follower notified positions
// among live members, skipping slots in exclude (stopped slots).
//
// leaderCommit must be sampled each tick from the current leader module.
func survivorsReachCommit(
	set *harness.ClusterMemberSet,
	exclude []int,
	leaderMemberID int32,
	leaderCommit int64,
	target int64,
) bool {
	if leaderCommit < target {
		return false
	}
	for i, m := range set.Modules() {
		if m == nil || slices.Contains(exclude, i) {
			continue
		}
		if m.ThisMemberID() == leaderMemberID {
			continue
		}
		if m.NotifiedCommitPosition() < target {
			return false
		}
	}
	return true
}

// survivorsRecordingAtLeast reports whether every live member except exclude
// has a type-100 counter for recIDs[i] at least target.
func survivorsRecordingAtLeast(
	set *harness.ClusterMemberSet,
	exclude []int,
	recIDs []int64,
	target int64,
) bool {
	for i, m := range set.Modules() {
		if m == nil || slices.Contains(exclude, i) {
			continue
		}
		if i >= len(recIDs) {
			return false
		}
		if !recordingAtLeast(m, recIDs[i], target) {
			return false
		}
	}
	return true
}

// requireWarmRestartOpen asserts RestartMember refilled the slot without cold
// start and with a non-empty recording log.
func requireWarmRestartOpen(
	t *testing.T,
	set *harness.ClusterMemberSet,
	index int,
	label string,
) {
	t.Helper()

	m := set.Module(index)
	require.NotNil(t, m, "RestartMember left slot empty:\n%s", set.State())
	require.False(t, m.IsColdStart(), "%s cold start:\n%s", label, set.State())
	require.NotEmpty(
		t,
		m.RecordingLog().Entries(),
		"%s recording log empty at open:\n%s",
		label,
		set.State(),
	)
}

// awaitPostRestartReady waits for log images and ElectionComplete on the
// returning member after RestartMember.
func awaitPostRestartReady(
	t *testing.T,
	set *harness.ClusterMemberSet,
	electionComplete func() bool,
) {
	t.Helper()

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)
	requireAwaitTick(t, set, "post-restart election", durableElectBound, electionComplete)
}

// TestDurableCluster_restartsAFollowerAndRejoins asserts a stopped follower
// rejoins a live durable cluster and commits a post-restart append.
//
// Asserts:
//   - commit advances while the follower slot is empty
//   - every member reaches the post-restart append position
//   - recording id continuity and active recording after rejoin
func TestDurableCluster_restartsAFollowerAndRejoins(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableRestartPortBase,
		harness.DurableRestartLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()

	// Drive a non-zero commit floor on every member before the stop.
	appendPos := mustAppend(t, set, leader, "pre-restart-commit")

	requireAwaitTick(t, set, "pre-restart commit floor", durableWorkBound, func() bool {
		return membersReachCommit(set, appendPos)
	})

	followerIndex := firstNonLeaderIndex(set, leaderMemberID)
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", set.State())
	follower := set.Module(followerIndex)
	require.NotNil(t, follower)
	require.False(t, follower.IsLeader(), "chosen member must not be leader:\n%s", set.State())
	down := []int{followerIndex}

	// Capture the pre-stop recording id.
	preRecordingID := follower.RecordingLog().TermRecordingID()
	require.NotEqual(t, int64(-1), preRecordingID, "follower TermRecordingID:\n%s", set.State())
	require.Equal(
		t,
		preRecordingID,
		follower.Agent().LocalLogRecordingID(),
		"follower LocalLogRecordingID vs TermRecordingID:\n%s",
		set.State(),
	)

	// Await the follower type-100 counter at the leader publication position
	// before StopMember. Recovery seeds the append position from that high-water.
	leaderPub := leader.LogPublisher().Position()
	require.Positive(t, leaderPub, "leader publication position:\n%s", set.State())
	requireAwaitTick(
		t, set, "follower recording current", durableWorkBound,
		func() bool {
			return localRecordingAtLeast(set.Module(followerIndex), leaderPub)
		},
	)

	preStopLeaderCommit := leader.CommitPosition()

	// Stop the follower. Survivors keep quorum.
	require.NoError(t, set.StopMember(followerIndex))
	require.Nil(t, set.Module(followerIndex))

	// Commit advances while the slot is empty.
	midAppendPos := mustAppend(t, set, leader, "while-down-commit")

	requireAwaitTick(t, set, "commit advances while follower down", durableWorkBound, func() bool {
		return survivorsReachCommit(
			set, down, leaderMemberID, leader.CommitPosition(), midAppendPos,
		)
	})
	require.Greater(
		t,
		leader.CommitPosition(),
		preStopLeaderCommit,
		"leader commit must advance while follower down:\n%s",
		set.State(),
	)

	// Let survivors' recordings reach the mid-append high-water before restart.
	// The returner's own recording stopped at the pre-stop high-water. Mid-append
	// bytes exist only on survivor Archives. After open the returner must rejoin
	// the live log for post-restart bytes.
	requireAwaitTick(t, set, "survivor recording catch-up", durableWorkBound, func() bool {
		return survivorsAppendAtLeast(set, down, midAppendPos)
	})

	// Restart against the same cluster directory and Archive.
	require.NoError(t, set.RestartMember(followerIndex))
	requireWarmRestartOpen(t, set, followerIndex, "returning member")
	returning := set.Module(followerIndex)

	archiveClient := returning.ArchiveClient()
	require.NotNil(t, archiveClient, "returning ArchiveClient:\n%s", set.State())
	require.Positive(
		t,
		archiveClient.ControlSessionID(),
		"returning archive control session:\n%s",
		set.State(),
	)

	// Returning member must re-image peer log publications and finish election.
	awaitPostRestartReady(t, set, returning.ElectionComplete)

	// Recording continuity after rejoin.
	require.Equal(
		t,
		preRecordingID,
		returning.Agent().LocalLogRecordingID(),
		"LocalLogRecordingID after rejoin:\n%s",
		set.State(),
	)
	require.Equal(
		t,
		preRecordingID,
		returning.RecordingLog().TermRecordingID(),
		"TermRecordingID after rejoin:\n%s",
		set.State(),
	)
	require.True(
		t,
		returning.Agent().IsRecordingActive(),
		"IsRecordingActive after rejoin:\n%s",
		set.State(),
	)

	// Append after restart. Every member reaches the new position.
	// membersReachCommit requires NotifiedCommitPosition on followers, so a
	// returner that only holds a pre-restart CommitPosition does not pass.
	leader = requireSingleLeader(t, set, "after rejoin")

	postAppendPos := mustAppend(t, set, leader, "post-restart-commit")

	requireAwaitTick(t, set, "post-restart commit advance", durableWorkBound, func() bool {
		return membersReachCommit(set, postAppendPos)
	})
}

// TestDurableCluster_restartsAFollowerWhoseRecordingRuns asserts a crashed follower
// stops its running recording at restart and rejoins.
func TestDurableCluster_restartsAFollowerWhoseRecordingRuns(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableRestartRunningPortBase,
		harness.DurableRestartRunningLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()

	appendPos := mustAppend(t, set, leader, "pre-crash-commit")
	requireAwaitTick(t, set, "pre-crash commit floor", durableWorkBound, func() bool {
		return membersReachCommit(set, appendPos)
	})

	followerIndex := firstNonLeaderIndex(set, leaderMemberID)
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", set.State())
	follower := set.Module(followerIndex)
	require.NotNil(t, follower)
	down := []int{followerIndex}

	preRecordingID := follower.RecordingLog().TermRecordingID()
	require.NotEqual(t, int64(-1), preRecordingID, "follower TermRecordingID:\n%s", set.State())

	leaderPub := leader.LogPublisher().Position()
	require.Positive(t, leaderPub, "leader publication position:\n%s", set.State())
	requireAwaitTick(
		t, set, "follower recording current", durableWorkBound,
		func() bool {
			return localRecordingAtLeast(set.Module(followerIndex), leaderPub)
		},
	)

	// Crash the follower. The recording stop fails on the closed archive
	// client. The Archive keeps recording the log.
	crashErr := set.CrashMember(followerIndex)
	var terminal *archive.ControlTerminalError
	require.ErrorAs(t, crashErr, &terminal, "CrashMember error:\n%s", set.State())
	require.Nil(t, set.Module(followerIndex))

	midAppendPos := mustAppend(t, set, leader, "while-crashed-commit")
	requireAwaitTick(t, set, "commit advances while follower crashed", durableWorkBound, func() bool {
		return survivorsReachCommit(
			set, down, leaderMemberID, leader.CommitPosition(), midAppendPos,
		)
	})

	require.NoError(t, set.RestartMember(followerIndex))
	requireWarmRestartOpen(t, set, followerIndex, "returning member")
	returning := set.Module(followerIndex)

	// Open stops the running recording before the first tick.
	stopPosition := returning.LogRecordingStopPosition()
	require.GreaterOrEqual(
		t,
		stopPosition,
		leaderPub,
		"LogRecordingStopPosition after restart:\n%s",
		set.State(),
	)

	awaitPostRestartReady(t, set, returning.ElectionComplete)

	require.Equal(
		t,
		preRecordingID,
		returning.Agent().LocalLogRecordingID(),
		"LocalLogRecordingID after rejoin:\n%s",
		set.State(),
	)
	require.Equal(
		t,
		preRecordingID,
		returning.RecordingLog().TermRecordingID(),
		"TermRecordingID after rejoin:\n%s",
		set.State(),
	)
	require.True(
		t,
		returning.Agent().IsRecordingActive(),
		"IsRecordingActive after rejoin:\n%s",
		set.State(),
	)

	leader = requireSingleLeader(t, set, "after rejoin")
	postAppendPos := mustAppend(t, set, leader, "post-restart-commit")
	requireAwaitTick(t, set, "post-restart commit advance", durableWorkBound, func() bool {
		return membersReachCommit(set, postAppendPos)
	})

	// Stop the leader. The returner's next election polls the recording stop
	// in prepare, then completes in a higher term.
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()
	preElectionCount := returning.Agent().ElectionCount()
	require.NoError(t, set.StopMember(leaderIndex))
	requireAwaitTick(t, set, "survivors complete a higher term", durableReElectBound, func() bool {
		for i, m := range set.Modules() {
			if i == leaderIndex {
				continue
			}
			if m == nil || !m.ElectionComplete() || m.LeadershipTermID() <= preTerm {
				return false
			}
		}
		return true
	})
	require.Greater(
		t,
		returning.Agent().ElectionCount(),
		preElectionCount,
		"returner ElectionCount after leader stop:\n%s",
		set.State(),
	)
}

// TestDurableCluster_restartsTheOutgoingLeader asserts a stopped leader's
// survivors elect a new term, record under that term, and the returning
// member rejoins as a follower and commits under the new term.
func TestDurableCluster_restartsTheOutgoingLeader(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableRestartLeaderPortBase,
		harness.DurableRestartLeaderLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()
	preRecordingID := leader.RecordingLog().TermRecordingID()
	require.NotEqual(t, int64(-1), preRecordingID, "leader TermRecordingID:\n%s", set.State())

	for i, m := range set.Modules() {
		if m == nil {
			continue
		}
		require.Equal(
			t,
			int64(1),
			m.Agent().ElectionCount(),
			"member %d ElectionCount before stop:\n%s",
			i,
			set.State(),
		)
	}

	// Drive a non-zero commit floor so the cluster is active before the fault.
	floorPos := mustAppend(t, set, leader, "pre-leader-stop-commit")

	requireAwaitTick(t, set, "pre-stop commit floor", durableWorkBound, func() bool {
		return membersReachCommit(set, floorPos)
	})

	down := []int{leaderIndex}
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))

	// Survivors install a second election.
	awaitReElectionAfterStop(t, set, leaderIndex, preTerm)

	requireLogStopTriggerSeen(t, set, leaderIndex)

	// New leader records under the new term. Read id, active, and term entry
	// in one predicate so the term is not sampled from a later state.
	requireAwaitTick(t, set, "new leader records under new term", durableWorkBound, func() bool {
		return leaderRecordsNewTerm(currentLeader(set), leaderIndex, preTerm)
	})
	newLeader := set.Leaders()[0]
	newTerm := newLeader.LeadershipTermID()
	newLeaderID := newLeader.ThisMemberID()

	// Append under the new term. Both survivors commit.
	midAppendPos := mustAppend(t, set, newLeader, "post-leader-stop-commit")

	requireAwaitTick(t, set, "post-re-elect commit advance", durableWorkBound, func() bool {
		return survivorsReachCommit(
			set, down, newLeaderID, newLeader.CommitPosition(), midAppendPos,
		)
	})

	// Restart the stopped leader after survivors settled.
	require.NoError(t, set.RestartMember(leaderIndex))
	requireWarmRestartOpen(t, set, leaderIndex, "returning ex-leader")
	returning := set.Module(leaderIndex)

	awaitPostRestartReady(t, set, returning.ElectionComplete)

	// Returning member rejoins as a follower under the settled term.
	newLeader = requireSingleLeader(t, set, "after return")
	require.NotEqual(
		t,
		int32(leaderIndex),
		newLeader.ThisMemberID(),
		"returning member must not be leader:\n%s",
		set.State(),
	)

	postAppendPos := mustAppend(t, set, newLeader, "post-restart-commit")

	// Returner is follower with notified commit; full set reaches the append.
	requireAwaitTick(
		t, set, "returning follower commits post-restart", durableWorkBound,
		func() bool {
			m := set.Module(leaderIndex)
			if m == nil || m.Role() != consensus.RoleFollower {
				return false
			}
			return membersReachCommit(set, postAppendPos)
		},
	)
	require.Equal(
		t,
		consensus.RoleFollower,
		returning.Role(),
		"returning member Role after rejoin:\n%s",
		set.State(),
	)

	// Recording continuity on the returning member.
	require.Equal(
		t,
		preRecordingID,
		returning.Agent().LocalLogRecordingID(),
		"LocalLogRecordingID after rejoin:\n%s",
		set.State(),
	)

	// Term under which survivors recorded must still be the active term.
	require.Equal(
		t,
		newTerm,
		newLeader.LeadershipTermID(),
		"new leader term drifted after return:\n%s",
		set.State(),
	)
}
