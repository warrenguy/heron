//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/archive"
	archivecodecs "luas.fast/archive/codecs"
	"luas.fast/archive/codecs/archivewire"
	"luas.fast/cluster"
	"luas.fast/cluster/consensus"
	"luas.fast/cluster/recordinglog"
	"luas.fast/test/integration/harness"
)

// TestClusterModule_replaysOwnRecordingAfterRestart asserts a single member
// replays its local recording after stop and restart to the pre-stop
// publication position.
func TestClusterModule_replaysOwnRecordingAfterRestart(t *testing.T) {
	t.Parallel()

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

// TestLogAdapter_replayHeldAtStopClosesAtEndOfStream asserts an archive replay
// image drained to its stop closes at end of stream.
func TestLogAdapter_replayHeldAtStopClosesAtEndOfStream(t *testing.T) {
	// No t.Parallel: the Archive binds the control port that
	// TestArchiveClient_startAutoStopReleasesSubscription also binds.
	const (
		recordStreamID int32 = 4201
		replayStreamID int32 = 4202
	)

	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})
	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)
	archiveClient := connectArchiveClient(t, aeronClient)

	await := func(label string, ok func() bool) {
		t.Helper()
		harness.PollClientUntilTransportClean(
			t,
			aeronClient,
			hadClientError,
			durableWorkBound,
			"client transport error during "+label,
			ok,
			func() string { return label + " not done within " + durableWorkBound.String() },
		)
	}

	// Phase: record four messages. Closing the publication stops the recording.
	pub, err := aeronClient.AddPublication(recordStreamID, archiveClientRecordingChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close record publication", pub.Close)
	recordingID := awaitStartRecording(
		t,
		aeronClient,
		hadClientError,
		archiveClient,
		recordStreamID,
		true,
		false,
	)

	for range 4 {
		require.Positive(t, harness.OfferWithRetry(t, pub, []byte("replay-to-stop")))
	}

	stopped := false
	archiveClient.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
		func(event archivecodecs.RecordingSignalEventFields) {
			if event.Signal == archivewire.RecordingSignal_STOP &&
				event.RecordingID == recordingID {
				stopped = true
			}
		},
	))
	t.Cleanup(func() { archiveClient.SetRecordingSignalHandler(nil) })
	require.NoError(t, pub.Close())
	await("recording stop", func() bool {
		_ = archiveClient.Poll()
		return stopped
	})

	descriptor := awaitListRecording(t, aeronClient, hadClientError, archiveClient, recordingID)
	stopPosition := descriptor.StopPosition
	require.Greater(t, stopPosition, descriptor.StartPosition)

	// Phase: replay the whole recording on the default replay channel.
	replaySessionID, err := archiveClient.StartReplay(
		t.Context(),
		recordingID,
		descriptor.StartPosition,
		stopPosition-descriptor.StartPosition,
		replayStreamID,
		[]byte(cluster.DefaultReplayChannel),
	)
	require.NoError(t, err)
	replaySub, err := aeronClient.AddSubscription(replayStreamID, cluster.DefaultReplayChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close replay subscription", replaySub.Close)

	var image *aeron.Image
	await("replay image", func() bool {
		found, ok, findErr := archive.FindReplayImage(replaySub, replaySessionID)
		require.NoError(t, findErr)
		image = found
		return ok
	})

	// Phase: drain to the stop, then stop polling, as a held replay does.
	adapter := consensus.NewLogAdapter(consensus.DefaultLogFragmentLimit)
	adapter.SetImage(image)
	await("replay drain", func() bool {
		adapter.Poll(stopPosition)
		return adapter.Position() >= stopPosition
	})
	await("replay image close", adapter.IsImageClosed)

	// A close short of end of stream starts the hold deadline.
	require.Equal(t, stopPosition, adapter.Position())
	require.True(t, adapter.IsLogEndOfStream())
}
