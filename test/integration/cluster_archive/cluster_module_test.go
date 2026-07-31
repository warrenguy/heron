//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
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
