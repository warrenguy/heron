//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterModule_replaysOwnRecordingAfterRestart asserts a single member
// replays its local recording after stop and restart to the pre-stop
// publication position.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestClusterModule_replaysOwnRecordingAfterRestart(t *testing.T) {
	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          1,
		PortBase:             harness.ClusterReplayPortBase,
		LogMulticastEndpoint: harness.ClusterReplayLogEndpoint,
	})
	require.Len(t, set.Modules(), 1)

	const electBound = 60 * time.Second
	requireAwait(t, set, "election", electBound, set.AwaitElection)

	leader := set.Modules()[0]
	require.True(t, leader.IsLeader(), "expected leader:\n%s", set.State())
	require.True(t, leader.IsColdStart(), "expected cold start:\n%s", set.State())

	term := leader.LeadershipTermID()
	var appendPos int64
	for i := range 4 {
		pos := leader.LogPublisher().AppendMessage(
			term,
			1,
			time.Now().UnixNano(),
			[]byte("replay-induction"),
		)
		require.Positive(t, pos, "AppendMessage %d failed code=%d:\n%s", i, pos, set.State())
		appendPos = pos
	}

	const commitBound = 60 * time.Second
	requireAwait(t, set, "induction commit", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			return leader.CommitPosition() >= appendPos
		})
	})

	published := leader.LogStreams().Publication().Position()
	recordingID := leader.Agent().LocalLogRecordingID()
	term1 := leader.LeadershipTermID()
	require.Positive(t, published, "published position:\n%s", set.State())
	require.NotEqual(t, int64(-1), recordingID, "LocalLogRecordingID:\n%s", set.State())

	// Recorder catch-up: append rank (RecordingPos) must reach published before
	// Close so the post-restart append bound is byte-exact with published.
	appendSrc := leader.Agent().AppendSource()
	require.NotNil(t, appendSrc, "AppendSource:\n%s", set.State())
	requireAwait(t, set, "recorder catch-up", commitBound, func(bound time.Duration) bool {
		return set.PumpTransportUntil(bound, func() bool {
			pos, ready := appendSrc.AppendPosition()
			return ready && pos >= published
		})
	})

	// No tick between stop and restart: single member leaves no survivor work.
	require.NoError(t, set.StopMember(0))
	require.NoError(t, set.RestartMember(0))
	restarted := set.Modules()[0]
	require.NotNil(t, restarted, "RestartMember left slot empty:\n%s", set.State())
	require.False(t, restarted.IsColdStart(), "restarted cold start:\n%s", set.State())
	require.Equal(t, int64(0), restarted.CommitPosition(), "commit at open:\n%s", set.State())

	// Open-time seed places exclusive log publication at recovered append stop.
	require.Equal(t, published, restarted.LogStreams().Publication().Position(),
		"log publication position at open:\n%s", set.State())

	requireAwait(t, set, "post-restart replay commit", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			return restarted.CommitPosition() >= published
		})
	})

	// Byte-exact: ProposeMaxCommit is monotonic and the bound is published.
	require.Equal(t, published, restarted.CommitPosition(), "byte-exact commit:\n%s", set.State())
	require.Empty(t, restarted.LogReplayReason(), "replay reason:\n%s", set.State())

	// LEADER_INIT commits term1 via EnsureCoherent after replay. Commit wait
	// can return during LEADER_REPLAY, so tick until that write lands.
	// Subject is local replay, not rejoin. Assert nothing about ElectionComplete.
	requireAwait(t, set, "term1 LogPosition commit", commitBound, func(bound time.Duration) bool {
		return set.TickUntil(bound, func() bool {
			for _, e := range restarted.RecordingLog().Entries() {
				if e.Type == recordinglog.EntryTypeTerm && e.LeadershipTermID == term1 {
					return e.LogPosition == published
				}
			}
			return false
		})
	})

	foundTerm1 := false
	for _, e := range restarted.RecordingLog().Entries() {
		if e.Type != recordinglog.EntryTypeTerm || e.LeadershipTermID != term1 {
			continue
		}
		require.Equal(t, published, e.LogPosition, "term1 LogPosition:\n%s", set.State())
		require.Equal(t, recordingID, e.RecordingID, "term1 RecordingID:\n%s", set.State())
		foundTerm1 = true
		break
	}
	require.True(t, foundTerm1, "no TERM entry for term1=%d:\n%s", term1, set.State())
}
