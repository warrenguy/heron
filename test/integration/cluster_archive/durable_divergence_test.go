//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

// openDurableDivergenceCluster starts three members on the durable-divergence
// port block.
func openDurableDivergenceCluster(t *testing.T) *harness.ClusterMemberSet {
	t.Helper()
	return openDurableCluster(
		t,
		harness.DurableDivergencePortBase,
		harness.DurableDivergenceLogEndpoint,
	)
}

// termBaseLogPosition returns the term-base log position for leadershipTermID
// from m's recording.log, or recordinglog.NullPosition when no entry matches.
func termBaseLogPosition(m *cluster.ConsensusModule, leadershipTermID int64) int64 {
	if m == nil || m.RecordingLog() == nil {
		return recordinglog.NullPosition
	}
	e := m.RecordingLog().FindTermEntry(leadershipTermID)
	if e == nil {
		return recordinglog.NullPosition
	}
	return e.TermBaseLogPosition
}

// lastTermLogPosition returns LogPosition of the highest valid TERM entry, or
// recordinglog.NullPosition when the log holds none.
//
// Entries are comparator-sorted by leadershipTermID ascending. Reverse scan
// hits the highest term first.
func lastTermLogPosition(m *cluster.ConsensusModule) int64 {
	if m == nil || m.RecordingLog() == nil {
		return recordinglog.NullPosition
	}
	entries := m.RecordingLog().Entries()
	for _, e := range slices.Backward(entries) {
		if e.Valid && e.Type == recordinglog.EntryTypeTerm {
			return e.LogPosition
		}
	}
	return recordinglog.NullPosition
}

// followersRecordingAtMost reports every live non-skipIndex member's type-100
// counter is at most floor.
func followersRecordingAtMost(
	set *harness.ClusterMemberSet,
	skipIndex int,
	floor int64,
) bool {
	for i, m := range set.Modules() {
		if m == nil || i == skipIndex {
			continue
		}
		if harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID()) > floor {
			return false
		}
	}
	return true
}

// tickMemberStreams runs transport and archive polls over m only. Omits
// Agent.DoWork, so no consensus duty runs.
//
// Use to advance one member's recording while peers stay frozen. A settled
// leader that runs Agent.DoWork against frozen peers loses its active quorum
// after LeaderHeartbeatTimeoutNs and re-enters election. The recording
// high-water is an archive fact and needs no duty cycle to advance.
func tickMemberStreams(m *cluster.ConsensusModule) {
	m.Client().DoWork()
	m.ArchiveClient().Poll()
}

// TestDurableCluster_truncatesADivergentMemberAndCommits asserts a member that
// recorded past the survivors' new term base truncates on rejoin.
//
// Asserts:
//   - truncate lowers stop position and last term LogPosition to the new term base
//   - returning member installs the new leader's term and finishes election
//   - post-truncate append reaches every member
//
// Does not assert snapshot refuse. No fixture plants a snapshot entry.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestDurableCluster_truncatesADivergentMemberAndCommits(t *testing.T) {
	harness.RequireArchiveRuntime(t)

	set := openDurableDivergenceCluster(t)

	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()
	require.NotEqual(
		t,
		int64(-1),
		leader.Agent().LocalLogRecordingID(),
		"leader LocalLogRecordingID:\n%s",
		set.State(),
	)

	// Drive a shared commit floor before the divergent high-water.
	floorPos := mustAppend(t, set, leader, "pre-divergence-floor")

	requireAwaitTick(t, set, "pre-divergence commit floor", durableWorkBound, func() bool {
		return membersReachCommit(set, floorPos)
	})

	// Stop follower log recordings so further leader appends raise only the
	// leader's catalog high-water. Survivors re-elect after the leader stops.
	// The new term base sits at the shared floor, below the stopped leader's
	// recorded high-water.
	followerFloor := floorPos
	for i, m := range set.Modules() {
		if m == nil || i == leaderIndex {
			continue
		}
		followerFloor = max(
			followerFloor,
			harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID()),
		)
		// PrepareForNewLeadership stops the recording and returns the catalog
		// stop. Leaves settled election state alone. Survivors re-enter via
		// heartbeat timeout after the leader stops.
		stopPos, prepErr := m.Agent().PrepareForNewLeadership(
			m.CommitPosition(),
			m.NanoClock()(),
		)
		require.NoError(t, prepErr, "follower %d PrepareForNewLeadership:\n%s", i, set.State())
		require.False(
			t,
			m.Agent().IsRecordingActive(),
			"follower %d still recording after prepare:\n%s",
			i,
			set.State(),
		)
		followerFloor = max(followerFloor, stopPos)
	}

	// Set state is built only on the failing branch: the loop runs per tick and
	// an eager dump per iteration would dominate the wait.
	deadline := time.Now().Add(durableWorkBound)
	var divergentHighWater int64
	for time.Now().Before(deadline) {
		leader = set.Module(leaderIndex)
		require.NotNil(t, leader)
		if !leader.IsLeader() {
			require.FailNow(t, "leader lost role while building gap", "%s", set.State())
		}

		mustAppend(t, set, leader, "divergent-tail")
		tickMemberStreams(leader)

		// Follower catalogs must stay at the floor. Remote recording is
		// stopped. Counters must not advance with the leader.
		leaderRec := harness.RecordingCounterPosition(leader, leader.Agent().LocalLogRecordingID())
		if leaderRec > followerFloor &&
			followersRecordingAtMost(set, leaderIndex, followerFloor) {
			divergentHighWater = leaderRec
			break
		}
	}
	require.Greater(
		t,
		divergentHighWater,
		followerFloor,
		"leader recording high-water=%d followerFloor=%d:\n%s",
		divergentHighWater,
		followerFloor,
		set.State(),
	)
	t.Logf(
		"divergent high-water=%d followerFloor=%d preTerm=%d",
		divergentHighWater,
		followerFloor,
		preTerm,
	)

	// Stop the divergent leader. Survivors re-elect under a lower term base.
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))

	requireAwaitTick(
		t, set, "survivors re-elect after leader stop", durableReElectBound,
		func() bool {
			ls := set.Leaders()
			if len(ls) != 1 {
				return false
			}
			nl := ls[0]
			return nl.ThisMemberID() != int32(leaderIndex) &&
				nl.LeadershipTermID() > preTerm &&
				nl.ElectionComplete()
		},
	)

	newLeader := requireSingleLeader(t, set, "after re-elect")
	newTerm := newLeader.LeadershipTermID()
	require.Greater(t, newTerm, preTerm, "new term after re-elect:\n%s", set.State())
	require.NotEqual(
		t,
		int32(leaderIndex),
		newLeader.ThisMemberID(),
		"new leader must be a survivor:\n%s",
		set.State(),
	)

	newTermBase := termBaseLogPosition(newLeader, newTerm)
	require.NotEqual(
		t,
		recordinglog.NullPosition,
		newTermBase,
		"new leader term base missing for term %d:\n%s",
		newTerm,
		set.State(),
	)
	require.Less(
		t,
		newTermBase,
		divergentHighWater,
		"new term base=%d must be below divergent high-water=%d:\n%s",
		newTermBase,
		divergentHighWater,
		set.State(),
	)

	// Restart the divergent member. Plan high-water must still sit above the
	// new term base before truncate closes the gap.
	require.NoError(t, set.RestartMember(leaderIndex))
	returning := set.Module(leaderIndex)
	require.NotNil(t, returning, "RestartMember left slot empty:\n%s", set.State())
	require.False(t, returning.IsColdStart(), "returning cold start:\n%s", set.State())

	recordingID := returning.RecordingLog().TermRecordingID()
	require.NotEqual(
		t,
		int64(-1),
		recordingID,
		"returning TermRecordingID:\n%s",
		set.State(),
	)
	desc, err := returning.ArchiveClient().ListRecording(
		context.Background(),
		recordingID,
	)
	require.NoError(t, err, "ListRecording before truncate:\n%s", set.State())
	planAppended := desc.StopPosition
	require.Greater(
		t,
		planAppended,
		newTermBase,
		"plan/recorded append position must be above new term base before truncate; "+
			"planAppended=%d newTermBase=%d recordingID=%d:\n%s",
		planAppended,
		newTermBase,
		recordingID,
		set.State(),
	)
	t.Logf(
		"pre-truncate divergence planAppended=%d newTermBase=%d recordingID=%d",
		planAppended,
		newTermBase,
		recordingID,
	)

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)

	// Wait until truncate lands three independent ways: agent stop position,
	// archive catalog stop, and last term LogPosition.
	requireAwaitTick(t, set, "truncate to new term base", durableWorkBound, func() bool {
		m := set.Module(leaderIndex)
		if m == nil {
			return false
		}
		if m.LogRecordingStopPosition() != newTermBase {
			return false
		}
		id := m.Agent().LocalLogRecordingID()
		if id < 0 {
			return false
		}
		d, listErr := m.ArchiveClient().ListRecording(
			context.Background(),
			id,
		)
		if listErr != nil || d.StopPosition != newTermBase {
			return false
		}
		return lastTermLogPosition(m) == newTermBase
	})

	// Index survives a reopen at the truncated term position.
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	require.NoError(t, set.RestartMember(leaderIndex))
	returning = set.Module(leaderIndex)
	require.NotNil(t, returning, "second RestartMember left slot empty:\n%s", set.State())
	require.Equal(
		t,
		newTermBase,
		lastTermLogPosition(returning),
		"reopened RecordingLog must report truncated term position %d:\n%s",
		newTermBase,
		set.State(),
	)

	// Member installs the new leader's term and finishes election.
	requireAwaitTick(t, set, "divergent member installs new term", durableWorkBound, func() bool {
		m := set.Module(leaderIndex)
		if m == nil {
			return false
		}
		ls := set.Leaders()
		if len(ls) != 1 {
			return false
		}
		return m.ElectionComplete() &&
			m.LeadershipTermID() == ls[0].LeadershipTermID()
	})

	returning = set.Module(leaderIndex)
	require.NotNil(t, returning)
	newLeader = requireSingleLeader(t, set, "after install")

	// Post-truncate append reaches every member.
	postAppendPos := mustAppend(t, set, newLeader, "post-truncate-commit")

	requireAwaitTick(t, set, "all members reach post-truncate commit", durableWorkBound, func() bool {
		return membersReachCommit(set, postAppendPos)
	})
}
