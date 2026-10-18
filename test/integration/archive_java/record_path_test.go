//go:build integration && archive_java

package archive_java_test

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/test/integration/archive_java/archivejava"
	"github.com/warrenguy/heron/test/integration/harness"
)

func requireArchiveComposeIPC(
	t *testing.T,
) (md *harness.MediaDriver, aeronClient *aeron.Client, hadClientError *atomic.Bool) {
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
	aeronClient, hadClientError = harness.ConnectIPCClient(t, md)
	return md, aeronClient, hadClientError
}

func connectArchive(
	t *testing.T,
	dir string,
	aeronClient *aeron.Client,
) *archive.Client {
	t.Helper()

	// Docker STOP can lag control under load. Prior shape had ~15s headroom
	// (control await + ConnectedPollTimeout STOP poll); await now owns STOP only.
	cfg := archive.Config{
		AeronDir:               dir,
		Aeron:                  aeronClient,
		ControlResponseTimeout: archive.DefaultControlResponseTimeout + harness.ConnectedPollTimeout,
	}.Normalize()
	archiveClient, err := archive.Connect(context.Background(), cfg)
	require.NoError(t, err)
	harness.CleanupClose(t, "close archive", archiveClient.Close)
	return archiveClient
}

func archiveSmokePollBudget(t *testing.T, reserve time.Duration) time.Duration {
	t.Helper()

	deadline, ok := t.Context().Deadline()
	if !ok {
		return harness.ConnectedPollTimeout
	}
	budget := time.Until(deadline) - reserve
	if budget <= 0 {
		require.FailNowf(
			t,
			"archive smoke poll budget exhausted (reserve=%s)",
			reserve.String(),
		)
	}
	return budget
}

func recordingCatalogStopPending(
	got codecs.RecordingDescriptorFields,
	tracker *archive.StopSignalTracker,
) bool {
	if got.StopPosition <= got.StartPosition {
		return true
	}
	return got.StopPosition != tracker.StoppedPosition()
}

func recordArchiveSmoke(
	t *testing.T,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	archiveClient *archive.Client,
) codecs.RecordingDescriptorFields {
	t.Helper()

	expectedPayloads := archivejava.Payloads(archivejava.MessageCount, archivejava.PayloadSize)
	wantStopPosition := harness.AlignedOfferAdvance(archivejava.PayloadSize, archivejava.MessageCount)

	var recordingID int64
	gotRecordingID := false
	archiveClient.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			if event.Signal != archivewire.RecordingSignal_START {
				return
			}
			if event.ControlSessionID != archiveClient.ControlSessionID() {
				return
			}
			recordingID = event.RecordingID
			gotRecordingID = true
		},
	))

	pub, err := aeronClient.AddPublication(archivejava.RecordStreamID, archivejava.RecordChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close record publication", pub.Close)

	_, err = archiveClient.StartRecording(
		t.Context(),
		archivejava.RecordStreamID,
		archivewire.SourceLocation_LOCAL,
		false,
		[]byte(archivejava.RecordChannel),
	)
	require.NoError(t, err)

	harness.WaitUntilConnected(t, pub)

	for _, payload := range expectedPayloads {
		harness.OfferWithRetry(t, pub, payload)
	}

	// Stop must not run until the publication has advanced past every offered frame.
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during publication position poll",
		func() bool {
			return pub.Position() >= wantStopPosition
		},
		func() string {
			return "publication position did not reach " +
				strconv.FormatInt(wantStopPosition, 10) +
				" within " +
				harness.ConnectedPollTimeout.String()
		},
	)

	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during recording start signal poll",
		func() bool {
			_ = archiveClient.Poll()
			return gotRecordingID
		},
		func() string {
			return "archive recording START signal not received within " +
				harness.ConnectedPollTimeout.String()
		},
	)

	// Send STOP only after the recording position reaches the publication extent.
	reader, err := aeronClient.CountersReader()
	require.NoError(t, err)
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during recording position poll",
		func() bool {
			slot, ok := reader.FindRecordingPositionCounter(
				recordingID,
				archiveClient.ArchiveID(),
			)
			return ok && slot.Get() >= wantStopPosition
		},
		func() string {
			return "recording position did not reach " +
				strconv.FormatInt(wantStopPosition, 10) +
				" within " +
				harness.ConnectedPollTimeout.String()
		},
	)

	// Session sink for steady-state Poll/ListRecording after await. Await demuxes
	// STOP to the per-call tracker itself; return already proves the latch.
	tracker := archive.NewStopSignalTracker(archiveClient.ControlSessionID(), recordingID)
	archiveClient.SetRecordingSignalHandler(tracker)
	err = archiveClient.StopRecordingAwaitSignal(
		t.Context(),
		archivejava.RecordStreamID,
		[]byte(archivejava.RecordChannel),
		tracker,
	)
	require.NoError(t, err)
	require.True(t, tracker.Stopped(), "StopRecordingAwaitSignal returned without STOP latch")
	require.GreaterOrEqual(
		t,
		tracker.StoppedPosition(),
		wantStopPosition,
		"STOP position %d below publication extent %d",
		tracker.StoppedPosition(),
		wantStopPosition,
	)

	// Catalog metadata can trail the STOP signal under load; sync to the signal
	// position using the remaining docker test budget, not ConnectedPollTimeout.
	catalogSyncTimeout := archiveSmokePollBudget(t, 45*time.Second)
	var desc codecs.RecordingDescriptorFields
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		catalogSyncTimeout,
		"client transport error during recording descriptor poll",
		func() bool {
			_ = archiveClient.Poll()
			got, listErr := archiveClient.ListRecording(t.Context(), recordingID)
			require.NoError(t, listErr)
			if recordingCatalogStopPending(got, tracker) {
				return false
			}
			desc = got
			return true
		},
		func() string {
			return "recording catalog stop position did not match STOP signal within " +
				catalogSyncTimeout.String()
		},
	)

	require.Equal(t, tracker.StoppedPosition(), desc.StopPosition)
	require.Equal(t, recordingID, desc.RecordingID)
	require.Equal(t, int64(0), desc.StartPosition)
	require.Greater(t, desc.StopPosition, desc.StartPosition)
	require.Equal(t, wantStopPosition, desc.StopPosition)
	require.Equal(t, archivejava.RecordStreamID, desc.StreamID)

	return desc
}

func TestArchiveRecordPath(t *testing.T) {
	md, aeronClient, hadClientError := requireArchiveComposeIPC(t)
	archiveClient := connectArchive(t, md.Dir, aeronClient)

	desc := recordArchiveSmoke(t, aeronClient, hadClientError, archiveClient)

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
