//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/harness"
)

// openDurableCatchupCluster starts three members on the durable-catchup port
// block.
func openDurableCatchupCluster(t *testing.T) *harness.ClusterMemberSet {
	t.Helper()
	return openDurableCluster(
		t,
		harness.DurableCatchupPortBase,
		harness.DurableCatchupLogEndpoint,
	)
}

// TestDurableCluster_catchesUpARestartedFollower asserts a stopped follower
// rejoins below the leader. The follower catches up through Archive-backed log
// replay. After catchup the set commits a post-catchup append on every member.
//
// Log replication has no helper. The scenario drives the follower replay path.
// The test fails if the member is ever observed on the replication path.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestDurableCluster_catchesUpARestartedFollower(t *testing.T) {
	harness.RequireArchiveRuntime(t)

	set := openDurableCatchupCluster(t)

	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()

	followerIndex := firstNonLeaderIndex(set, leaderMemberID)
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", set.State())
	follower := set.Module(followerIndex)
	require.NotNil(t, follower)
	require.False(t, follower.IsLeader(), "chosen member must not be leader:\n%s", set.State())

	// Append on the leader. Recording must capture the bytes before the stop.
	mustAppend(t, set, leader, "pre-stop-catchup")

	// Wait until the follower type-100 counter reaches the leader publication
	// position. Recovery reads that high-water as the append position.
	require.Positive(
		t,
		leader.LogPublisher().Position(),
		"leader publication position:\n%s",
		set.State(),
	)

	// Stop while the follower log adapter is still below the recorded high-water.
	// Both conditions are one predicate. The adapter bound is
	// min(notifiedCommit, recordingCounter), so the tick that lifts the recording
	// past the publication can also lift the adapter to that high-water. Waiting
	// on the recording first and asserting the lag second observes a state that
	// no longer holds.
	requireAwaitTick(
		t, set, "follower recording current with adapter still behind", durableWorkBound,
		func() bool {
			m := set.Module(followerIndex)
			if m == nil {
				return false
			}
			leaderPub := leader.LogPublisher().Position()
			pos := harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID())
			return pos >= leaderPub && m.LogAdapter().Position() < leaderPub
		},
	)

	require.NoError(t, set.StopMember(followerIndex))
	require.Nil(t, set.Module(followerIndex))

	// Append while the follower is down so the returner commit stays strictly
	// below the leader after restart.
	midAppendPos := mustAppend(t, set, leader, "while-down-catchup")

	requireAwaitTick(t, set, "survivors commit while follower down", durableWorkBound, func() bool {
		if leader.CommitPosition() < midAppendPos {
			return false
		}
		for i, m := range set.Modules() {
			if m == nil || i == followerIndex {
				continue
			}
			if m.ThisMemberID() == leaderMemberID {
				continue
			}
			if m.NotifiedCommitPosition() < midAppendPos {
				return false
			}
		}
		return true
	})

	// Survivors' recordings reach the mid-append high-water before restart.
	requireAwaitTick(t, set, "survivor recording catch-up", durableWorkBound, func() bool {
		return survivorsAppendAtLeast(set, followerIndex, midAppendPos)
	})

	leaderPublicationPosition := leader.LogPublisher().Position()
	require.Positive(
		t,
		leaderPublicationPosition,
		"leader publication position after gap append:\n%s",
		set.State(),
	)

	require.NoError(t, set.RestartMember(followerIndex))
	returning := set.Module(followerIndex)
	require.NotNil(t, returning, "RestartMember left slot empty:\n%s", set.State())

	// Assert the commit gap before catchup closes it.
	require.False(
		t,
		returning.IsColdStart(),
		"returning member cold start:\n%s",
		set.State(),
	)
	require.Less(
		t,
		returning.CommitPosition(),
		leader.CommitPosition(),
		"returning commit=%d leader commit=%d; gap must exist before catchup:\n%s",
		returning.CommitPosition(),
		leader.CommitPosition(),
		set.State(),
	)

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)

	// Catchup: adapter and notified commit reach the captured publication
	// position. The test fails if the member enters the nil-factory
	// log-replication path.
	requireAwaitTick(
		t, set, "archive catchup to leader publication", durableWorkBound,
		func() bool {
			m := set.Module(followerIndex)
			if m == nil {
				return false
			}
			state := m.ElectionState()
			if state == election.ElectionStateFollowerLogReplication {
				require.FailNow(
					t,
					"returning member entered nil-factory log-replication path",
					"ElectionState=%v:\n%s",
					state,
					set.State(),
				)
			}
			adapter := m.LogAdapter()
			if adapter == nil {
				return false
			}
			return adapter.Position() == leaderPublicationPosition &&
				m.NotifiedCommitPosition() == leaderPublicationPosition
		},
	)

	returning = set.Module(followerIndex)
	require.NotNil(t, returning)
	require.Empty(
		t,
		returning.LogReplayReason(),
		"LogReplayReason after catchup:\n%s",
		set.State(),
	)

	// Caught-up member is a live follower. Append once more and every member
	// reaches the new position.
	leader = requireSingleLeader(t, set, "after catchup")

	postAppendPos := mustAppend(t, set, leader, "post-catchup-commit")

	requireAwaitTick(t, set, "all members reach post-catchup commit", durableWorkBound, func() bool {
		return membersReachCommit(set, postAppendPos)
	})
}
