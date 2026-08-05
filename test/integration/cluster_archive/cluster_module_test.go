//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterModule_electsOneLeaderOverLiveDrivers asserts one leader across
// three connected media drivers on a shared multicast cluster log.
func TestClusterModule_electsOneLeaderOverLiveDrivers(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterModuleElectPortBase,
		harness.ClusterModuleElectLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()
	require.Equal(t, int32(-1), leader.LeaderMemberID())
	require.Nil(t, leader.LogAdapter().Image())

	term := leader.LeadershipTermID()
	require.GreaterOrEqual(t, term, int64(0))
	for _, m := range set.Modules() {
		require.Equal(t, term, m.LeadershipTermID())
		if m.ThisMemberID() == leaderMemberID {
			continue
		}
		require.Equal(t, leader.ThisMemberID(), m.LeaderMemberID())
		require.NotNil(t, m.LogAdapter().Image())
	}

	// Durable log recording after elect: each member records into its own
	// Archive. TERM names that member's local id. Catalog descriptors are
	// session-scoped to the leader log publication.
	requireMembersRecordLeaderLog(t, set, leader)

	// Cold elect: contiguous TERM entries [0, term]. Target term stays open.
	for _, m := range set.Modules() {
		recordingID := m.Agent().LocalLogRecordingID()
		entries := m.RecordingLog().Entries()
		require.Len(t, entries, int(term+1), "member %d", m.ThisMemberID())
		for k, e := range entries {
			require.Equal(t, recordinglog.EntryTypeTerm, e.Type, "member %d entry %d", m.ThisMemberID(), k)
			require.True(t, e.Valid, "member %d entry %d", m.ThisMemberID(), k)
			require.Equal(t, recordingID, e.RecordingID, "member %d entry %d", m.ThisMemberID(), k)
			require.Equal(t, int64(k), e.LeadershipTermID, "member %d entry %d", m.ThisMemberID(), k)
			if int64(k) < term {
				require.NotEqual(
					t,
					recordinglog.NullPosition,
					e.LogPosition,
					"member %d entry %d",
					m.ThisMemberID(),
					k,
				)
			} else {
				require.Equal(
					t,
					recordinglog.NullPosition,
					e.LogPosition,
					"member %d entry %d",
					m.ThisMemberID(),
					k,
				)
			}
		}
	}

	// Capture archive clients and log handles before Close.
	type logHandles struct {
		pub *aeron.ExclusivePublication
		sub *aeron.Subscription
	}
	handles := make([]logHandles, 0, len(set.Modules()))
	archiveClients := make([]*archive.Client, 0, len(set.Modules()))
	for _, m := range set.Modules() {
		archiveClient := m.ArchiveClient()
		require.NotNil(t, archiveClient)
		require.False(t, archiveClient.IsClosed())
		require.Equal(
			t,
			archive.DefaultLocalControlChannel,
			archiveClient.Config().EffectiveControlRequestChannel(),
		)
		require.False(t, archiveClient.Config().OwnsAeronClient())
		archiveClients = append(archiveClients, archiveClient)
		handles = append(handles, logHandles{
			pub: m.LogStreams().Publication(),
			sub: m.LogStreams().Subscription(),
		})
	}

	require.NoError(t, set.Close())

	for i, m := range set.Modules() {
		require.True(t, handles[i].pub.IsClosed())
		require.True(t, handles[i].sub.IsClosed())
		require.Nil(t, m.Agent().AppendSource())
		require.Equal(t, int32(-1), m.Agent().AddLogPublication(0))
		require.True(t, archiveClients[i].IsClosed())
		require.Nil(t, m.ArchiveClient())
	}
}

// TestClusterModule_advancesCommitAcrossFollowers asserts follower notified
// commit advances past a second log induction after election.
func TestClusterModule_advancesCommitAcrossFollowers(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterModuleCommitPortBase,
		harness.ClusterModuleCommitLogEndpoint,
	)

	leaders := set.Leaders()
	require.Len(t, leaders, 1, "expected exactly one leader:\n%s", set.State())
	leader := leaders[0]
	leaderMemberID := leader.ThisMemberID()

	appendPos := leader.LogPublisher().AppendMessage(
		leader.LeadershipTermID(),
		1,
		time.Now().UnixNano(),
		[]byte("commit-advance"),
	)
	require.Positive(t, appendPos, "AppendMessage failed code=%d:\n%s", appendPos, set.State())

	// Quiet localhost: commit advance often ~1ms after election.
	const commitBound = 60 * time.Second
	requireAwait(t, set, "commit advance", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			if leader.CommitPosition() < appendPos {
				return false
			}
			for _, m := range set.Modules() {
				if m.ThisMemberID() == leaderMemberID {
					continue
				}
				if m.NotifiedCommitPosition() < appendPos {
					return false
				}
			}
			return true
		})
	})
}

// TestClusterModule_rejoinsCommitAfterMemberRestart asserts commit participation
// after a follower stop and restart against the same cluster directory and Archive.
//
// Asserts:
//   - non-empty recovery plan
//   - term install
//   - commit advance with the cluster
func TestClusterModule_rejoinsCommitAfterMemberRestart(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterModuleRejoinPortBase,
		harness.ClusterModuleRejoinLogEndpoint,
	)

	leaders := set.Leaders()
	require.Len(t, leaders, 1, "expected exactly one leader:\n%s", set.State())
	leader := leaders[0]
	leaderMemberID := leader.ThisMemberID()

	// Drive a non-zero commit floor on every member before the stop.
	appendPos := leader.LogPublisher().AppendMessage(
		leader.LeadershipTermID(),
		1,
		time.Now().UnixNano(),
		[]byte("pre-restart-commit"),
	)
	require.Positive(t, appendPos, "AppendMessage failed code=%d:\n%s", appendPos, set.State())

	const commitBound = 60 * time.Second
	requireAwait(t, set, "pre-restart commit floor", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			for _, m := range set.Modules() {
				if m == nil || m.CommitPosition() == 0 {
					return false
				}
			}
			return true
		})
	})

	// Choose a non-leader follower by slot index.
	followerIndex := -1
	for i, m := range set.Modules() {
		if m != nil && m.ThisMemberID() != leaderMemberID {
			followerIndex = i
			break
		}
	}
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", set.State())
	follower := set.Module(followerIndex)
	require.NotNil(t, follower)

	// Preconditions:
	//   - follower
	//   - non-empty recording log
	//   - connected recording id
	require.False(t, follower.IsLeader(), "chosen member must not be leader:\n%s", set.State())
	preEntries := follower.RecordingLog().Entries()
	require.NotEmpty(t, preEntries, "follower recording log empty:\n%s", set.State())
	preRecordingID := follower.Agent().LocalLogRecordingID()
	require.NotEqual(t, int64(-1), preRecordingID, "follower LocalLogRecordingID:\n%s", set.State())
	require.Equal(
		t,
		preRecordingID,
		preEntries[len(preEntries)-1].RecordingID,
		"last TERM recording id must match LocalLogRecordingID:\n%s",
		set.State(),
	)

	// Retain pre-stop state for post-restart assertions.
	preEntryCounts := make([]int, len(set.Modules()))
	preSurvivorLeaderIDs := make([]int32, len(set.Modules()))
	for i, m := range set.Modules() {
		if m == nil {
			continue
		}
		preEntryCounts[i] = len(m.RecordingLog().Entries())
		if i != followerIndex {
			preSurvivorLeaderIDs[i] = m.LeaderMemberID()
		}
	}

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

	// Stop and restart:
	//  1. graceful stop
	//  2. tick survivors
	//  3. restart against the same cluster dir and Archive
	require.NoError(t, set.StopMember(followerIndex))
	require.Nil(t, set.Module(followerIndex))
	for range 10 {
		set.Tick()
	}
	require.NoError(t, set.RestartMember(followerIndex))
	returning := set.Module(followerIndex)
	require.NotNil(t, returning, "RestartMember left slot empty:\n%s", set.State())

	// Non-empty recovery plan branch.
	require.False(
		t,
		returning.IsColdStart(),
		"returning member cold start:\n%s",
		set.State(),
	)

	// Previous boot's index was read, not recreated empty.
	openEntries := returning.RecordingLog().Entries()
	require.NotEmpty(
		t,
		openEntries,
		"returning member recording log empty at open:\n%s",
		set.State(),
	)

	// Plan-seeded LocalLogRecordingID at open equals pre-stop id.
	require.Equal(
		t,
		preRecordingID,
		returning.Agent().LocalLogRecordingID(),
		"LocalLogRecordingID at open:\n%s",
		set.State(),
	)

	// Commit seed at open is at least the plan committed floor. Floor is the last
	// TERM entry's LogPosition when committed. NullPosition leaves the plan
	// floor at 0.
	planFloor := int64(0)
	if lp := openEntries[len(openEntries)-1].LogPosition; lp != recordinglog.NullPosition {
		planFloor = lp
	}
	openCommit := returning.CommitPosition()
	require.GreaterOrEqual(
		t,
		openCommit,
		planFloor,
		"CommitPosition at open below plan floor %d:\n%s",
		planFloor,
		set.State(),
	)

	// Returning member must re-image peer log publications and finish election.
	const imageBound = 30 * time.Second
	const electBound = 60 * time.Second
	requireAwait(t, set, "post-restart log images", imageBound, set.AwaitLogImages)

	// Election completes within the election bound.
	requireAwait(t, set, "post-restart election", electBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, returning.ElectionComplete)
	})

	// Leader after rejoin: still the settled leader.
	leaders = set.Leaders()
	require.Len(t, leaders, 1, "expected exactly one leader after rejoin:\n%s", set.State())
	leader = leaders[0]
	leaderMemberID = leader.ThisMemberID()

	// Installed term matches the leader.
	require.Equal(
		t,
		leader.LeadershipTermID(),
		returning.LeadershipTermID(),
		"LeadershipTermID after rejoin:\n%s",
		set.State(),
	)

	// Returning follower knows the leader.
	require.Equal(
		t,
		leaderMemberID,
		returning.LeaderMemberID(),
		"LeaderMemberID after rejoin:\n%s",
		set.State(),
	)

	// Drive commit past the open floor. Separates recovered position from
	// connected participation:
	//   - returner must replicate and commit the post-restart entry
	//   - leader must commit past its pre-restart position
	postAppendPos := leader.LogPublisher().AppendMessage(
		leader.LeadershipTermID(),
		1,
		time.Now().UnixNano(),
		[]byte("post-restart-commit"),
	)
	require.Positive(
		t,
		postAppendPos,
		"post-restart AppendMessage failed code=%d:\n%s",
		postAppendPos,
		set.State(),
	)

	// Await every live member at the post-restart commit. Followers must also
	// advance NotifiedCommitPosition so a recovered CommitPosition alone does not
	// pass. The returner must stay ElectionComplete with an active recording.
	// Commit alone can pass during LEADER_HEARTBEAT_TIMEOUT re-entry under short
	// harness heartbeats.
	requireAwait(t, set, "post-restart commit advance", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			ret := set.Module(followerIndex)
			if ret == nil {
				return false
			}
			if !ret.ElectionComplete() || !ret.Agent().IsRecordingActive() {
				return false
			}
			return membersReachCommit(set, postAppendPos)
		})
	})

	// No member lost durable term state.
	for i, m := range set.Modules() {
		if m == nil {
			continue
		}
		require.GreaterOrEqual(
			t,
			len(m.RecordingLog().Entries()),
			preEntryCounts[i],
			"member %d recording log shrank:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}

	// Survivors stayed settled with the same leader view.
	for i, m := range set.Modules() {
		if m == nil || i == followerIndex {
			continue
		}
		require.True(
			t,
			m.ElectionComplete(),
			"survivor %d election incomplete:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
		require.Equal(
			t,
			preSurvivorLeaderIDs[i],
			m.LeaderMemberID(),
			"survivor %d LeaderMemberID changed:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}

	// After join, LocalLogRecordingID still equals pre-stop id (extend path, not
	// a fresh catalog id).
	require.Equal(
		t,
		preRecordingID,
		returning.Agent().LocalLogRecordingID(),
		"LocalLogRecordingID after join (extend arm):\n%s",
		set.State(),
	)

	// Extend path is active, not a stored idle id.
	require.True(
		t,
		returning.Agent().IsRecordingActive(),
		"IsRecordingActive after join:\n%s",
		set.State(),
	)
}

// TestClusterModule_reElectsAfterTheLeaderStops asserts a settled cluster
// installs a new leadership term when its leader stops publishing. Asserts the
// surviving members commit under the new term.
func TestClusterModule_reElectsAfterTheLeaderStops(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterModuleReElectPortBase,
		harness.ClusterModuleReElectLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()

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

	// Drive a non-zero commit floor so the cluster is live before the fault.
	mustAppend(t, set, leader, "pre-leader-stop-commit")

	requireAwaitTick(t, set, "pre-stop commit floor", durableWorkBound, func() bool {
		for _, m := range set.Modules() {
			if m == nil || m.CommitPosition() == 0 {
				return false
			}
		}
		return true
	})

	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))

	newLeader := awaitReElectionAfterStop(t, set, leaderIndex, preTerm)
	newLeaderID := newLeader.ThisMemberID()

	requireHeartbeatTimeoutTriggerSeen(t, set, leaderIndex)

	// Append under the new term; both survivors commit.
	postAppendPos := mustAppend(t, set, newLeader, "post-leader-stop-commit")

	requireAwaitTick(t, set, "post-re-elect commit advance", durableWorkBound, func() bool {
		if newLeader.CommitPosition() < postAppendPos {
			return false
		}
		for i, m := range set.Modules() {
			if m == nil || i == leaderIndex {
				continue
			}
			if m.ThisMemberID() == newLeaderID {
				continue
			}
			if m.NotifiedCommitPosition() < postAppendPos {
				return false
			}
		}
		return true
	})
}

// TestClusterModule_leaderReElectsWhenItsFollowersStop asserts a leader that
// loses its active follower quorum re-enters an election. Asserts that member
// stops being the leader.
func TestClusterModule_leaderReElectsWhenItsFollowersStop(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterModuleFollowerStopPortBase,
		harness.ClusterModuleFollowerStopLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())

	stopFollowers(t, set, leaderIndex)

	requireAwaitTick(t, set, "leader re-elect after followers stop", durableReElectBound,
		func() bool {
			m := set.Module(leaderIndex)
			if m == nil {
				return false
			}
			return m.Agent().ElectionCount() == 2
		})

	survivor := requireQuorumLossFollower(t, set, leaderIndex)

	// Lone member of three cannot complete an election. Pump stays bounded.
	const pumpBound = 2 * time.Second
	require.False(
		t,
		set.TickUntil(pumpBound, func() bool {
			return set.Module(leaderIndex).Agent().Election() == nil
		}),
		"lone member completed election within %s:\n%s",
		pumpBound,
		set.State(),
	)
	require.NotNil(
		t,
		survivor.Agent().Election(),
		"Election cleared on lone member:\n%s",
		set.State(),
	)
}

// openThreeNodeCluster starts three members on the given port block and log
// endpoint. Waits for session-keyed log images. Ticks until every member
// reports ElectionComplete. Caller must pass a unique PortBase per Test*.
func openThreeNodeCluster(
	t *testing.T,
	portBase int,
	logEndpoint string,
) *harness.ClusterMemberSet {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          3,
		PortBase:             portBase,
		LogMulticastEndpoint: logEndpoint,
	})
	require.Len(t, set.Modules(), 3)

	// Quiet localhost: image fan-in often ~4ms for three members.
	const imageBound = 30 * time.Second
	requireAwait(t, set, "log image fan-in", imageBound, set.AwaitLogImages)

	sessions := set.LogSessionIDs()
	for _, m := range set.Modules() {
		require.Equal(t, 3, m.LogStreams().Subscription().ImageCount())
		require.Nil(t, m.LogAdapter().Image())
		sub := m.LogStreams().Subscription()
		for _, session := range sessions {
			img, ok := harness.ImageBySessionID(sub, session)
			require.True(t, ok, "missing session %d on member %d", session, m.ThisMemberID())
			require.Equal(t, int64(0), img.JoinPosition())
		}
	}

	// Quiet localhost: election often ~41ms for three members.
	const electBound = 60 * time.Second
	requireAwait(t, set, "election", electBound, set.AwaitElection)

	return set
}

// requireAwait runs await with bound and requires success. Dumps set state on
// failure. Logs elapsed on success for calibration.
func requireAwait(
	t *testing.T,
	set *harness.ClusterMemberSet,
	label string,
	bound time.Duration,
	await func(time.Duration) bool,
) {
	t.Helper()

	start := time.Now()
	done := await(bound)
	require.True(
		t,
		done,
		"%s did not complete after %s (bound %s):\n%s",
		label,
		time.Since(start),
		bound,
		set.State(),
	)
	t.Logf("%s elapsed=%s\n%s", label, time.Since(start), set.State())
}

// requireAwaitTick runs requireAwait over set.TickUntil with pred.
func requireAwaitTick(
	t *testing.T,
	set *harness.ClusterMemberSet,
	label string,
	bound time.Duration,
	pred func() bool,
) {
	t.Helper()

	requireAwait(t, set, label, bound, func(bound time.Duration) bool {
		return set.TickUntil(bound, pred)
	})
}

// requireSingleLeader returns the one leader. Fails with the set state when the
// set does not report exactly one leader. context labels the scenario
// (for example "after re-elect"); empty is fine.
func requireSingleLeader(
	t *testing.T,
	set *harness.ClusterMemberSet,
	context string,
) *cluster.ConsensusModule {
	t.Helper()

	label := "expected exactly one leader"
	if context != "" {
		label += " " + context
	}
	leaders := set.Leaders()
	require.Len(t, leaders, 1, "%s:\n%s", label, set.State())
	return leaders[0]
}

// mustAppend appends payload on the leader log publication and requires a
// positive position. The set state is built only on failure so mustAppend is
// safe inside polling loops.
func mustAppend(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	payload string,
) int64 {
	t.Helper()

	pos := leader.LogPublisher().AppendMessage(
		leader.LeadershipTermID(),
		1,
		time.Now().UnixNano(),
		[]byte(payload),
	)
	if pos <= 0 {
		require.FailNow(
			t,
			"AppendMessage failed",
			"payload=%q code=%d:\n%s",
			payload,
			pos,
			set.State(),
		)
	}
	return pos
}

// awaitReElectionAfterStop waits for a survivor ElectionCount of 2 and one new
// leader above preTerm after the member at stoppedIndex stopped. Returns the
// new leader.
func awaitReElectionAfterStop(
	t *testing.T,
	set *harness.ClusterMemberSet,
	stoppedIndex int,
	preTerm int64,
) *cluster.ConsensusModule {
	t.Helper()

	requireAwaitTick(t, set, "re-elect after leader stop", durableReElectBound, func() bool {
		countTwo := false
		for i, m := range set.Modules() {
			if m == nil || i == stoppedIndex {
				continue
			}
			if m.Agent().ElectionCount() == 2 {
				countTwo = true
				break
			}
		}
		if !countTwo {
			return false
		}
		ls := set.Leaders()
		return len(ls) == 1 && ls[0].LeadershipTermID() > preTerm
	})

	newLeader := requireSingleLeader(t, set, "after re-elect")
	require.NotEqual(
		t,
		int32(stoppedIndex),
		newLeader.ThisMemberID(),
		"new leader must be a survivor:\n%s",
		set.State(),
	)
	require.Greater(
		t,
		newLeader.LeadershipTermID(),
		preTerm,
		"LeadershipTermID after re-elect:\n%s",
		set.State(),
	)
	return newLeader
}

// requireHeartbeatTimeoutTriggerSeen requires at least one member other than
// stoppedIndex re-entered election on leader heartbeat timeout.
func requireHeartbeatTimeoutTriggerSeen(
	t *testing.T,
	set *harness.ClusterMemberSet,
	stoppedIndex int,
) {
	t.Helper()

	for i, m := range set.Modules() {
		if m == nil || i == stoppedIndex {
			continue
		}
		if m.Agent().LastElectionTrigger() == consensus.ElectionTriggerLeaderHeartbeatTimeout {
			return
		}
	}
	require.FailNow(
		t,
		"no survivor reported ElectionTriggerLeaderHeartbeatTimeout",
		"%s",
		set.State(),
	)
}

// stopFollowers stops every live member except leaderIndex. Leader alone is
// below the quorum threshold for three members (need 2).
func stopFollowers(t *testing.T, set *harness.ClusterMemberSet, leaderIndex int) {
	t.Helper()

	for i, m := range set.Modules() {
		if m == nil || i == leaderIndex {
			continue
		}
		require.NoError(t, set.StopMember(i))
		require.Nil(t, set.Module(i))
	}
}

// requireQuorumLossFollower requires the member at leaderIndex latched the
// inactive-follower-quorum trigger and dropped to the follower role. Returns
// that member.
func requireQuorumLossFollower(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leaderIndex int,
) *cluster.ConsensusModule {
	t.Helper()

	survivor := set.Module(leaderIndex)
	require.NotNil(t, survivor, "leader slot empty after followers stop:\n%s", set.State())
	require.Equal(
		t,
		consensus.ElectionTriggerInactiveFollowerQuorum,
		survivor.Agent().LastElectionTrigger(),
		"LastElectionTrigger after lost quorum:\n%s",
		set.State(),
	)
	require.Equal(
		t,
		consensus.RoleFollower,
		survivor.Agent().Role(),
		"Role after lost quorum:\n%s",
		set.State(),
	)
	return survivor
}

// requireMembersRecordLeaderLog requires every member holds an active local
// recording whose last recording-log entry and catalog descriptor are
// session-scoped to the leader log publication.
//
// Recording ids are not compared across members: each Archive catalog
// allocates ids independently, so the first recording is often id 0 on every
// node.
func requireMembersRecordLeaderLog(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
) {
	t.Helper()

	leaderLogSessionID := leader.LogPublisher().SessionID()
	require.NotEqual(t, int32(-1), leaderLogSessionID)
	leaderLogStreamID := leader.Topology().LogStreamID()
	for _, m := range set.Modules() {
		recordingID := m.Agent().LocalLogRecordingID()
		require.NotEqual(t, int64(-1), recordingID, "member %d:\n%s", m.ThisMemberID(), set.State())
		require.True(t, m.Agent().IsRecordingActive(), "member %d:\n%s", m.ThisMemberID(), set.State())
		entries := m.RecordingLog().Entries()
		require.NotEmpty(t, entries, "member %d recording log empty:\n%s", m.ThisMemberID(), set.State())
		require.Equal(
			t,
			recordingID,
			entries[len(entries)-1].RecordingID,
			"member %d last entry RecordingID:\n%s",
			m.ThisMemberID(),
			set.State(),
		)

		archiveClient := m.ArchiveClient()
		require.NotNil(t, archiveClient, "member %d", m.ThisMemberID())
		desc, err := archiveClient.ListRecording(context.Background(), recordingID)
		require.NoError(t, err, "member %d ListRecording:\n%s", m.ThisMemberID(), set.State())
		require.Equal(t, leaderLogStreamID, desc.StreamID, "member %d", m.ThisMemberID())
		require.Equal(
			t,
			leaderLogSessionID,
			desc.SessionID,
			"member %d recording session must match leader log publication:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}
}
