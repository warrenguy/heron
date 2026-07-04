//go:build integration && archive_java

package archive_java_test

import (
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	clientpkg "github.com/warrenguy/heron/archive/client"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
	"github.com/warrenguy/heron/test/integration/archive_java/archivejava"
	"github.com/warrenguy/heron/test/integration/harness"
)

func requireArchiveComposeIPC(
	t *testing.T,
) (md *harness.MediaDriver, client *aeron.Client, hadClientError *atomic.Bool) {
	t.Helper()
	if os.Getenv(archivejava.ComposeGateEnvKey) != "1" {
		t.Skip(archivejava.ComposeSkipHint)
	}
	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)
	require.Equal(
		t,
		strconv.Itoa(archivejava.TermMinLength),
		os.Getenv("AERON_IPC_TERM_BUFFER_LENGTH"),
	)
	md = harness.JoinExternal(t, dir)
	client, hadClientError = harness.ConnectIPCClient(t, md)
	return md, client, hadClientError
}

func connectArchive(
	t *testing.T,
	dir string,
	client *aeron.Client,
) *clientpkg.AeronArchive {
	t.Helper()

	archiveCtx := clientpkg.ArchiveContext{
		AeronDir: dir,
		Aeron:    client,
	}.Normalize()
	archive, err := clientpkg.Connect(archiveCtx)
	require.NoError(t, err)
	harness.CleanupClose(t, "close archive", archive.Close)
	return archive
}

func recordingDescriptorStopPending(
	got codecs.RecordingDescriptorFields,
	tracker *clientpkg.StopSignalTracker,
	wantStopPosition int64,
) bool {
	if got.StopPosition <= got.StartPosition {
		return true
	}
	if tracker.Stopped() && got.StopPosition != tracker.StoppedPosition() {
		return true
	}
	return got.StopPosition < wantStopPosition
}

func recordArchiveSmoke(
	t *testing.T,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	archive *clientpkg.AeronArchive,
) codecs.RecordingDescriptorFields {
	t.Helper()

	expectedPayloads := archivejava.Payloads(archivejava.MessageCount, archivejava.PayloadSize)
	wantStopPosition := harness.AlignedOfferAdvance(archivejava.PayloadSize, archivejava.MessageCount)

	var recordingID int64
	gotRecordingID := false
	archive.SetRecordingSignalHandler(clientpkg.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			if event.Signal != gensbe.RecordingSignal_START {
				return
			}
			if event.ControlSessionID != archive.ControlSessionID() {
				return
			}
			recordingID = event.RecordingID
			gotRecordingID = true
		},
	))

	pub, err := client.AddPublication(archivejava.RecordStreamID, archivejava.RecordChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close record publication", pub.Close)

	_, err = archive.StartRecording(
		archivejava.RecordStreamID,
		gensbe.SourceLocation_LOCAL,
		[]byte(archivejava.RecordChannel),
	)
	require.NoError(t, err)

	harness.PollUntilConnected(t, client, pub, harness.ConnectedPollTimeout)

	for _, payload := range expectedPayloads {
		harness.OfferWithRetry(t, client, pub, payload, harness.ConnectedPollTimeout)
	}

	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during recording start signal poll",
		func() bool {
			_ = archive.Poll()
			return gotRecordingID
		},
		func() string {
			return "archive recording START signal not received within " +
				harness.ConnectedPollTimeout.String()
		},
	)

	tracker := clientpkg.NewStopSignalTracker(archive.ControlSessionID(), recordingID)
	err = archive.StopRecordingAwaitSignal(
		archivejava.RecordStreamID,
		[]byte(archivejava.RecordChannel),
		tracker,
	)
	require.NoError(t, err)

	var desc codecs.RecordingDescriptorFields
	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during recording descriptor poll",
		func() bool {
			_ = archive.Poll()
			got, listErr := archive.ListRecording(recordingID)
			require.NoError(t, listErr)
			if recordingDescriptorStopPending(got, tracker, wantStopPosition) {
				return false
			}
			desc = got
			return true
		},
		func() string {
			return "recording stop position not available within " +
				harness.ConnectedPollTimeout.String()
		},
	)
	if tracker.Stopped() {
		require.Equal(t, tracker.StoppedPosition(), desc.StopPosition)
	}
	require.Equal(t, recordingID, desc.RecordingID)
	require.Equal(t, int64(0), desc.StartPosition)
	require.Greater(t, desc.StopPosition, desc.StartPosition)
	require.Equal(t, wantStopPosition, desc.StopPosition)
	require.Equal(t, archivejava.RecordStreamID, desc.StreamID)

	return desc
}

func TestArchiveRecordPath(t *testing.T) {
	md, client, hadClientError := requireArchiveComposeIPC(t)
	archive := connectArchive(t, md.Dir, client)

	desc := recordArchiveSmoke(t, client, hadClientError, archive)

	t.Logf(
		"recordingId=%d startPosition=%d stopPosition=%d streamId=%d",
		desc.RecordingID,
		desc.StartPosition,
		desc.StopPosition,
		desc.StreamID,
	)

	require.False(t, hadClientError.Load())
	md.AssertAlive(t)
}
