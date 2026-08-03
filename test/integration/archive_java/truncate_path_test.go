//go:build integration && archive_java

package archive_java_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/test/integration/archive_java/archivejava"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestArchiveTruncatePath exercises TruncateRecording against a live Java Archive.
func TestArchiveTruncatePath(t *testing.T) {
	md, aeronClient, hadClientError := requireArchiveComposeIPC(t)
	archiveClient := connectArchive(t, md.Dir, aeronClient)

	desc := recordArchiveSmoke(t, aeronClient, hadClientError, archiveClient)
	recordingID := desc.RecordingID
	startPosition := desc.StartPosition
	stopPosition := desc.StopPosition

	frameLength := harness.AlignedOfferAdvance(archivejava.PayloadSize, 1)
	require.Greater(t, stopPosition, frameLength)
	require.Equal(t, int64(0), stopPosition%frameLength)

	// Truncate to the current stop: no segments deleted.
	deleted, err := archiveClient.TruncateRecording(t.Context(), recordingID, stopPosition)
	require.NoError(t, err)
	require.Equal(t, int64(0), deleted)

	// Truncate below stop at a data-header boundary.
	alignedBelowStop := stopPosition - frameLength
	_, err = archiveClient.TruncateRecording(t.Context(), recordingID, alignedBelowStop)
	require.NoError(t, err)
	got, err := archiveClient.ListRecording(t.Context(), recordingID)
	require.NoError(t, err)
	require.Equal(t, alignedBelowStop, got.StopPosition)

	// Position above catalog stop is refused (current stop is alignedBelowStop).
	_, err = archiveClient.TruncateRecording(
		t.Context(),
		recordingID,
		alignedBelowStop+int64(logbuffer.FrameAlignment),
	)
	code, ok := archive.ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(archive.ErrorInvalidPosition), code)
	got, err = archiveClient.ListRecording(t.Context(), recordingID)
	require.NoError(t, err)
	require.Equal(t, alignedBelowStop, got.StopPosition)

	// Unaligned position strictly inside (start, current stop) is refused for
	// misalignment, not for being above stop.
	unalignedPosition := startPosition + 1
	require.Less(t, unalignedPosition, alignedBelowStop)
	require.NotEqual(t, int64(0), unalignedPosition&(int64(logbuffer.FrameAlignment)-1))
	_, err = archiveClient.TruncateRecording(t.Context(), recordingID, unalignedPosition)
	code, ok = archive.ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(archive.ErrorInvalidPosition), code)
	got, err = archiveClient.ListRecording(t.Context(), recordingID)
	require.NoError(t, err)
	require.Equal(t, alignedBelowStop, got.StopPosition)

	// Active recording cannot be truncated. Distinct from RecordStreamID and
	// ReplayStreamID. StartRecordingAwaitSignal owns START latch and poll.
	const secondStreamID int32 = archivejava.RecordStreamID + 2
	secondChannel := archivejava.RecordChannel
	secondPub, err := aeronClient.AddPublication(secondStreamID, secondChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close second record publication", secondPub.Close)

	startTracker := archive.NewStartSignalTracker(archiveClient.ControlSessionID())
	archiveClient.SetRecordingSignalHandler(startTracker)
	secondRecordingID, err := archiveClient.StartRecordingAwaitSignal(
		t.Context(),
		secondStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(secondChannel),
		startTracker,
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, secondRecordingID, int64(0))

	_, err = archiveClient.TruncateRecording(t.Context(), secondRecordingID, 0)
	code, ok = archive.ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(archive.ErrorActiveRecording), code)
	require.Contains(
		t,
		strings.ToLower(err.Error()),
		"cannot truncate active recording",
	)

	// Frame-aligned position that is not a data-header boundary: ErrorGeneric,
	// and the catalog stop has already dropped to that position.
	alignedInsideFrame := startPosition + int64(logbuffer.FrameAlignment)
	require.NotEqual(t, int64(0), alignedInsideFrame%frameLength)
	require.Less(t, alignedInsideFrame, alignedBelowStop)
	_, err = archiveClient.TruncateRecording(t.Context(), recordingID, alignedInsideFrame)
	code, ok = archive.ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(archive.ErrorGeneric), code)
	require.Contains(
		t,
		strings.ToLower(err.Error()),
		"position not aligned to a data header",
	)
	got, err = archiveClient.ListRecording(t.Context(), recordingID)
	require.NoError(t, err)
	require.Equal(t, alignedInsideFrame, got.StopPosition)

	// Truncate to start: catalog entry remains with stop == start.
	_, err = archiveClient.TruncateRecording(t.Context(), recordingID, startPosition)
	require.NoError(t, err)
	got, err = archiveClient.ListRecording(t.Context(), recordingID)
	require.NoError(t, err)
	require.Equal(t, startPosition, got.StartPosition)
	require.Equal(t, startPosition, got.StopPosition)
	require.Equal(t, recordingID, got.RecordingID)

	// Unknown recording id.
	const unknownRecordingID int64 = 1 << 40
	_, err = archiveClient.TruncateRecording(t.Context(), unknownRecordingID, 0)
	code, ok = archive.ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(archive.ErrorUnknownRecording), code)

	// Stop the second recording so compose teardown stays clean.
	stopTracker := archive.NewStopSignalTracker(
		archiveClient.ControlSessionID(),
		secondRecordingID,
	)
	archiveClient.SetRecordingSignalHandler(stopTracker)
	err = archiveClient.StopRecordingAwaitSignal(
		t.Context(),
		secondStreamID,
		[]byte(secondChannel),
		stopTracker,
	)
	require.NoError(t, err)

	require.False(t, hadClientError.Load())
	md.AssertAlive(t)
}
