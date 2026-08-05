//go:build integration && archive_live

package cluster_archive_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestDurableCluster_catchesUpARestartedFollower asserts a stopped follower
// rejoins below the leader. The follower catches up through Archive-backed log
// replay. After catchup the set commits a post-catchup append on every member.
//
// Log replication has no helper. The scenario drives the follower replay path.
// The test fails if the member is ever observed on the replication path.
func TestDurableCluster_catchesUpARestartedFollower(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableCatchupPortBase,
		harness.DurableCatchupLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()

	followerIndex := firstNonLeaderIndex(set, leaderMemberID)
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", set.State())
	follower := set.Module(followerIndex)
	require.NotNil(t, follower)
	require.False(t, follower.IsLeader(), "chosen member must not be leader:\n%s", set.State())

	// Append on the leader. Recording must capture the bytes before the stop.
	// Mid-append while the follower is down builds the catchup gap. Do not wait
	// for adapter lag on this message.
	mustAppend(t, set, leader, "pre-stop-catchup")

	leaderPub := leader.LogPublisher().Position()
	require.Positive(t, leaderPub, "leader publication position:\n%s", set.State())

	// Recovery reads the type-100 high-water as the append position. Wait until
	// the follower recording reaches the leader publication. Adapter and
	// recording often advance on the same tick, so do not also require adapter
	// lag here.
	requireAwaitTick(
		t, set, "follower recording current", durableWorkBound,
		func() bool {
			return localRecordingAtLeast(set.Module(followerIndex), leaderPub)
		},
	)

	require.NoError(t, set.StopMember(followerIndex))
	require.Nil(t, set.Module(followerIndex))

	// Append while the follower is down so the returner commit stays strictly
	// below the leader after restart. This mid-append is the catchup gap.
	midAppendPos := mustAppend(t, set, leader, "while-down-catchup")

	requireAwaitTick(t, set, "survivors commit while follower down", durableWorkBound, func() bool {
		return survivorsReachCommit(
			set, []int{followerIndex}, leaderMemberID, leader.CommitPosition(), midAppendPos,
		)
	})

	// Survivors' recordings reach the mid-append high-water before restart.
	requireAwaitTick(t, set, "survivor recording catch-up", durableWorkBound, func() bool {
		return survivorsAppendAtLeast(set, []int{followerIndex}, midAppendPos)
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

	// Await adapter and notified commit at the captured publication position.
	// The returner must stay ElectionComplete with an active recording. Progress
	// alone can pass during LEADER_HEARTBEAT_TIMEOUT re-entry under short harness
	// heartbeats. Fail if the member enters the nil-factory log-replication path.
	requireAwaitTick(
		t, set, "archive catchup to leader publication", durableWorkBound,
		func() bool {
			return returnerCaughtUpTo(
				t, set, set.Module(followerIndex), followerIndex, leaderPublicationPosition,
			)
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

// TestDurableCluster_catchesUpConcurrentFollowers asserts two stopped
// followers rejoin a five-member durable cluster and catch up concurrently.
//
// Five members are required: with two followers down a three-member leader
// cannot commit the advance, and commit arithmetic allows at most one member
// below the committed high-water in a three-member set.
//
// Asserts:
//   - leader tracks two concurrent catchup replays (per-member map)
//   - both returners reach the leader publication position
//   - StopCatchup completes for each (map returns to idle; catchup destinations
//     clear)
//   - every member commits a post-catchup append
func TestDurableCluster_catchesUpConcurrentFollowers(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableClusterN(
		t,
		5,
		harness.DurableConcurrentCatchupPortBase,
		harness.DurableConcurrentCatchupLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()

	followers := nonLeaderIndices(set, leaderMemberID)
	require.GreaterOrEqual(
		t,
		len(followers),
		2,
		"need two follower slots:\n%s",
		set.State(),
	)
	stopped := followers[:2]

	// Append on the leader. Recordings must capture the bytes before the stop.
	mustAppend(t, set, leader, "pre-stop-concurrent-catchup")

	leaderPub := leader.LogPublisher().Position()
	require.Positive(t, leaderPub, "leader publication position:\n%s", set.State())

	// Recovery reads the type-100 high-water as the append position. Wait until
	// each stopped follower recording reaches the leader publication.
	requireAwaitTick(
		t, set, "follower recordings current", durableWorkBound,
		func() bool {
			for _, idx := range stopped {
				if !localRecordingAtLeast(set.Module(idx), leaderPub) {
					return false
				}
			}
			return true
		},
	)

	for _, idx := range stopped {
		require.NoError(t, set.StopMember(idx))
		require.Nil(t, set.Module(idx))
	}

	// Mid-appends while both followers are down. A large static gap keeps the
	// first returner's archive replay live long enough for the second to open
	// its map entry. Small payloads stay under the term frame limit.
	var midAppendPos int64
	for i := range 256 {
		midAppendPos = mustAppend(
			t, set, leader, "while-down-concurrent-catchup-"+strconv.Itoa(i),
		)
	}

	requireAwaitTick(t, set, "survivors commit while followers down", durableWorkBound, func() bool {
		return survivorsReachCommit(
			set, stopped, leaderMemberID, leader.CommitPosition(), midAppendPos,
		)
	})

	requireAwaitTick(t, set, "survivor recording catch-up", durableWorkBound, func() bool {
		return survivorsAppendAtLeast(set, stopped, midAppendPos)
	})

	require.Positive(
		t,
		leader.LogPublisher().Position(),
		"leader publication position after gap appends:\n%s",
		set.State(),
	)

	for _, idx := range stopped {
		require.NoError(t, set.RestartMember(idx))
		returning := set.Module(idx)
		require.NotNil(t, returning, "RestartMember left slot %d empty:\n%s", idx, set.State())
		require.False(
			t,
			returning.IsColdStart(),
			"returning member %d cold start:\n%s",
			idx,
			set.State(),
		)
		require.Less(
			t,
			returning.CommitPosition(),
			leader.CommitPosition(),
			"returning %d commit=%d leader commit=%d; gap must exist before catchup:\n%s",
			idx,
			returning.CommitPosition(),
			leader.CommitPosition(),
			set.State(),
		)
	}

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)

	// Latch peak CatchupReplayCount each tick. Extend every tick while the peak
	// is below two so follow-mode StopCatchup cannot retire the first entry
	// before the second opens. Fail on the nil-factory log-replication path.
	maxConcurrentReplays := 0
	requireAwaitTick(
		t, set, "two concurrent catchup replays live", durableWorkBound,
		func() bool {
			for _, idx := range stopped {
				m := set.Module(idx)
				if m == nil {
					return false
				}
				failIfFollowerLogReplication(t, set, m, idx)
			}

			ls := set.Leaders()
			if len(ls) != 1 {
				return false
			}
			curLeader := ls[0]
			n := curLeader.Agent().CatchupReplayCount()
			if n > maxConcurrentReplays {
				maxConcurrentReplays = n
			}
			if maxConcurrentReplays >= 2 {
				return true
			}
			// Keep the publication ahead of every returner adapter so neither
			// catchup can reach StopCatchup while we wait for the second entry.
			mustAppend(t, set, curLeader, "extend-catchup-window")
			return false
		},
	)

	require.GreaterOrEqual(
		t,
		maxConcurrentReplays,
		2,
		"latched peak concurrent catchup replays:\n%s",
		set.State(),
	)

	// Capture the publication high-water after the concurrent window is proven.
	// Both returners must reach at least this position once the leader stops
	// extending.
	leader = requireSingleLeader(t, set, "during concurrent catchup")
	leaderPublicationPosition := leader.LogPublisher().Position()

	requireAwaitTick(
		t, set, "both returners reach leader publication", durableWorkBound,
		func() bool {
			if ls := set.Leaders(); len(ls) == 1 {
				n := ls[0].Agent().CatchupReplayCount()
				if n > maxConcurrentReplays {
					maxConcurrentReplays = n
				}
			}

			for _, idx := range stopped {
				if !returnerCaughtUpTo(t, set, set.Module(idx), idx, leaderPublicationPosition) {
					return false
				}
			}
			return true
		},
	)

	// StopCatchup deletes map entries on the leader before followers clear the
	// catchup destination. Await map idle and destination postconditions in one
	// predicate so a one-tick race cannot fail a hard assert.
	requireAwaitTick(
		t, set, "StopCatchup idle and destinations settled", durableWorkBound,
		func() bool {
			ls := set.Leaders()
			if len(ls) != 1 || ls[0].Agent().CatchupReplayCount() != 0 {
				return false
			}
			for _, idx := range stopped {
				m := set.Module(idx)
				if m == nil {
					return false
				}
				if m.LogReplayReason() != "" {
					return false
				}
				if m.Agent().CatchupLogDestination() != "" {
					return false
				}
				if m.Agent().LiveLogDestination() == "" {
					return false
				}
			}
			return true
		},
	)

	leader = requireSingleLeader(t, set, "after concurrent catchup")
	require.Equal(
		t,
		0,
		leader.Agent().CatchupReplayCount(),
		"leader catchup map idle after StopCatchup:\n%s",
		set.State(),
	)
	require.GreaterOrEqual(
		t,
		maxConcurrentReplays,
		2,
		"latched peak concurrent catchup replays after settle:\n%s",
		set.State(),
	)

	postAppendPos := mustAppend(t, set, leader, "post-concurrent-catchup-commit")

	requireAwaitTick(t, set, "all members reach post-catchup commit", durableWorkBound, func() bool {
		return membersReachCommit(set, postAppendPos)
	})
}

// localRecordingAtLeast reports whether m's local log recording counter is at
// least target. Nil m is false.
func localRecordingAtLeast(m *cluster.ConsensusModule, target int64) bool {
	if m == nil {
		return false
	}
	pos := harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID())
	return pos >= target
}

// failIfFollowerLogReplication fails the test when m is on the nil-factory
// follower log-replication path.
func failIfFollowerLogReplication(
	t *testing.T,
	set *harness.ClusterMemberSet,
	m *cluster.ConsensusModule,
	memberIndex int,
) {
	t.Helper()

	state := m.ElectionState()
	if state != election.ElectionStateFollowerLogReplication {
		return
	}
	require.FailNow(
		t,
		"returning member entered nil-factory log-replication path",
		"member=%d ElectionState=%v:\n%s",
		memberIndex,
		state,
		set.State(),
	)
}

// returnerCaughtUpTo reports whether m is ElectionComplete with an active
// recording and has adapter plus notified commit at least target. Nil m is
// false.
//
// Fails the test if m is on the nil-factory follower log-replication path.
func returnerCaughtUpTo(
	t *testing.T,
	set *harness.ClusterMemberSet,
	m *cluster.ConsensusModule,
	memberIndex int,
	target int64,
) bool {
	t.Helper()

	if m == nil {
		return false
	}
	failIfFollowerLogReplication(t, set, m, memberIndex)
	if !m.ElectionComplete() || !m.Agent().IsRecordingActive() {
		return false
	}
	adapter := m.LogAdapter()
	if adapter == nil {
		return false
	}
	return adapter.Position() >= target &&
		m.NotifiedCommitPosition() >= target
}
