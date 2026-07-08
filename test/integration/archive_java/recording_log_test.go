//go:build integration && archive_java

package archive_java_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	clientpkg "github.com/warrenguy/heron/archive/client"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/archive_java/archivejava"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestArchiveRecordingLog(t *testing.T) {
	md, client, hadClientError := requireArchiveComposeIPC(t)
	archive := connectArchive(t, md.Dir, client)

	desc := recordArchiveSmoke(t, client, hadClientError, archive)
	recordingID := desc.RecordingID
	expectedPayloads := archivejava.Payloads(archivejava.MessageCount, archivejava.PayloadSize)
	stopPosition := desc.StopPosition

	logPath := filepath.Join(archivejava.ClusterDir, archivejava.RecordingLogFileName)
	err := os.Remove(logPath)
	if err != nil && !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	recLog, err := recordinglog.Open(archivejava.ClusterDir, true)
	require.NoError(t, err)
	harness.CleanupClose(t, "close recording log", recLog.Close)

	ts := time.Now().UnixNano()
	err = recLog.AppendTerm(recordingID, 1, 0, ts)
	require.NoError(t, err)
	err = recLog.CommitLogPosition(1, stopPosition)
	require.NoError(t, err)
	// Force is the recordinglog Sync API; FileSyncFull fsyncs before cross-handle read.
	err = recLog.Force(recordinglog.FileSyncFull)
	require.NoError(t, err)
	// Release write handle so Reload reads bytes from disk, not an open fd cache.
	require.NoError(t, recLog.Close())

	require.FileExists(t, logPath)

	reloaded, err := recordinglog.Reload(logPath)
	require.NoError(t, err)
	require.Len(t, reloaded, 1)
	term := reloaded[0]
	require.Equal(t, recordinglog.EntryTypeTerm, term.Type)
	require.True(t, term.Valid)
	require.Equal(t, recordingID, term.RecordingID)
	require.Equal(t, int64(1), term.LeadershipTermID)
	require.Equal(t, int64(0), term.TermBaseLogPosition)
	require.Equal(t, stopPosition, term.LogPosition)
	require.Equal(t, ts, term.Timestamp)

	reopened, err := recordinglog.Open(archivejava.ClusterDir, false)
	require.NoError(t, err)
	harness.CleanupClose(t, "close reopened recording log", reopened.Close)
	// Cross-handle compare: on-disk bytes match decoded cache; not recovery planning.
	require.Equal(t, reloaded, reopened.Entries())

	// Recovery planning uses the crash-sim reopen handle (Open false), not Reload bytes.
	plan, err := reopened.PlanRecovery(
		clientpkg.NewArchiveLister(context.Background(), archive),
		recordinglog.NewRecoveryPlanConfig(0),
	)
	require.NoError(t, err)
	require.Nil(t, plan.Snapshots)
	require.NotNil(t, plan.Log)
	wantTerm := recordinglog.TermRef{
		RecordingID:         term.RecordingID,
		LeadershipTermID:    term.LeadershipTermID,
		TermBaseLogPosition: term.TermBaseLogPosition,
		LogPosition:         term.LogPosition,
	}
	require.Equal(t, wantTerm, plan.Log.Term)
	require.Equal(t, clientpkg.LogRecordingExtentsFromDescriptor(desc), plan.Log.Extent)
	require.Equal(t, term.LeadershipTermID, plan.LastLeadershipTermID)
	require.Equal(t, term.TermBaseLogPosition, plan.LastTermBaseLogPosition)
	require.Equal(t, stopPosition, plan.AppendedLogPosition)
	require.Equal(t, stopPosition, plan.CommittedLogPosition)

	replaySub, err := client.AddSubscription(archivejava.ReplayStreamID, archivejava.ReplayChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close replay subscription", replaySub.Close)
	// Registration only: no replay image exists until StartReplay; polling
	// IsConnected/ImageCount here deadlocks.

	replayStart := desc.StartPosition
	replayStop := stopPosition

	replaySessionID, err := archive.StartReplay(
		t.Context(),
		recordingID,
		replayStart,
		replayStop-replayStart,
		archivejava.ReplayStreamID,
		[]byte(archivejava.ReplayChannel),
	)
	require.NoError(t, err)
	require.Positive(t, replaySessionID)
	harness.CleanupClose(t, "stop replay", func() error {
		return archive.StopReplay(context.Background(), replaySessionID)
	})

	var replayImage *aeron.Image
	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during replay image poll",
		func() bool {
			image, ok, findErr := clientpkg.FindReplayImage(replaySub, replaySessionID)
			if findErr != nil {
				t.Fatal(findErr)
			}
			if !ok {
				return false
			}
			replayImage = image
			return true
		},
		func() string {
			return "replay image for session " +
				strconv.FormatInt(replaySessionID, 10) +
				" not found within " +
				harness.ConnectedPollTimeout.String()
		},
	)

	received := make([][]byte, 0, archivejava.MessageCount)
	collect := appendReplayPayload(&received)
	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during replay fragment drain",
		func() bool {
			_ = archive.Poll()
			_, done, err := clientpkg.PollReplayFragments(
				replayImage,
				archivejava.MessageCount,
				replayStop,
				len(received),
				collect,
			)
			require.NoError(t, err)
			return done
		},
		func() string {
			return "replay fragments: received " +
				strconv.Itoa(len(received)) +
				" of " +
				strconv.Itoa(archivejava.MessageCount) +
				" within " +
				harness.ConnectedPollTimeout.String()
		},
	)
	require.Equal(t, expectedPayloads, received)
	t.Logf(
		"replay complete: sessionId=%d joinPosition=%d imagePosition=%d fragments=%d",
		replaySessionID,
		replayImage.JoinPosition(),
		replayImage.Position(),
		len(received),
	)

	t.Logf(
		"recordingId=%d startPosition=%d stopPosition=%d leadershipTermId=1 termBaseLogPosition=0",
		recordingID,
		desc.StartPosition,
		stopPosition,
	)

	require.False(t, hadClientError.Load())
	md.AssertAlive(t)
}

func appendReplayPayload(dst *[][]byte) aeron.FragmentHandler {
	return func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		_ *aeron.Header,
	) {
		*dst = append(*dst, slices.Clone(buffer.Bytes()[offset:offset+length]))
	}
}
