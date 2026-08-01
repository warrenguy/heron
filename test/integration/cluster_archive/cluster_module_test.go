//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterModule_electsOneLeaderOverLiveDrivers asserts one leader across
// three connected media drivers on a shared multicast cluster log.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestClusterModule_electsOneLeaderOverLiveDrivers(t *testing.T) {
	set := openThreeNodeCluster(t)

	leaders := set.Leaders()
	require.Len(t, leaders, 1, "expected exactly one leader:\n%s", set.State())
	leader := leaders[0]
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
	//
	// Recording ids are not compared across members: each Archive catalog
	// allocates ids independently, so the first recording is often id 0 on
	// every node.
	leaderLogSessionID := leader.LogPublisher().SessionID()
	require.NotEqual(t, int32(-1), leaderLogSessionID)
	leaderLogStreamID := leader.Topology().LogStreamID()
	for _, m := range set.Modules() {
		recordingID := m.Agent().LocalLogRecordingID()
		require.NotEqual(t, int64(-1), recordingID, "member %d", m.ThisMemberID())
		require.True(t, m.Agent().IsRecordingActive(), "member %d", m.ThisMemberID())
		entries := m.RecordingLog().Entries()
		require.NotEmpty(t, entries, "member %d", m.ThisMemberID())
		require.Equal(
			t,
			recordingID,
			entries[len(entries)-1].RecordingID,
			"member %d",
			m.ThisMemberID(),
		)
		archiveClient := m.ArchiveClient()
		require.NotNil(t, archiveClient, "member %d", m.ThisMemberID())
		desc, err := archiveClient.ListRecording(context.Background(), recordingID)
		require.NoError(t, err, "member %d ListRecording", m.ThisMemberID())
		require.Equal(t, leaderLogStreamID, desc.StreamID, "member %d", m.ThisMemberID())
		require.Equal(
			t,
			leaderLogSessionID,
			desc.SessionID,
			"member %d recording session must match leader log publication",
			m.ThisMemberID(),
		)
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
//
// No t.Parallel: the set owns fixed UDP ports.
func TestClusterModule_advancesCommitAcrossFollowers(t *testing.T) {
	set := openThreeNodeCluster(t)

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
//
// No t.Parallel: the set owns fixed UDP ports.
func TestClusterModule_rejoinsCommitAfterMemberRestart(t *testing.T) {
	set := openThreeNodeCluster(t)

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

	// Leader and returner both commit the post-restart entry.
	requireAwait(t, set, "post-restart commit advance", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			return returning.CommitPosition() >= postAppendPos &&
				leader.CommitPosition() >= postAppendPos
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

// openThreeNodeCluster starts three members on the module multicast block.
// Waits for session-keyed log images. Ticks until every member reports
// ElectionComplete.
func openThreeNodeCluster(t *testing.T) *harness.ClusterMemberSet {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          3,
		PortBase:             harness.ClusterModulePortBase,
		LogMulticastEndpoint: harness.ClusterModuleLogEndpoint,
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
