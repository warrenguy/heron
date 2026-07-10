package election_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/nodewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/mmap"
)

func nodeStatePath(dir string) string {
	return filepath.Join(dir, election.NodeStateFileName)
}

func openNodeStateInDir(
	t *testing.T,
	dir string,
	createNew bool,
	syncLevel recordinglog.FileSyncLevel,
) *election.NodeStateFile {
	t.Helper()
	f, err := election.OpenNodeStateFile(dir, createNew, syncLevel)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}

func reopenNodeState(
	t *testing.T,
	dir string,
	syncLevel recordinglog.FileSyncLevel,
) *election.NodeStateFile {
	t.Helper()
	return openNodeStateInDir(t, dir, false, syncLevel)
}

func TestOpenNodeStateFile_missingWithoutCreate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	_, err := election.OpenNodeStateFile(dir, false, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenNodeStateFile_createNewInitialState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncData)
	ct, err := f.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, election.NullValue, ct.CandidateTermID())
	require.Equal(t, election.NullValue, ct.TimestampMs())
	require.Equal(t, election.NullValue, ct.LogPosition())

	info, err := os.Stat(nodeStatePath(dir))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Equal(t, int64(election.MinimumNodeStateFileLength), info.Size())
}

func TestOpenNodeStateFile_updateReopenPersists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const (
		termID      = int64(7)
		logPosition = int64(8192)
		timestampMs = int64(1_700_000_000_123)
	)

	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncData)
	require.NoError(t, f.UpdateCandidateTermID(termID, logPosition, timestampMs))
	require.NoError(t, f.Close())

	reopened := reopenNodeState(t, dir, recordinglog.FileSyncNone)
	ct, err := reopened.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, termID, ct.CandidateTermID())
	require.Equal(t, timestampMs, ct.TimestampMs())
	require.Equal(t, logPosition, ct.LogPosition())
}

func TestNodeStateFile_ProposeMaxCandidateTermID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)

	got, err := f.ProposeMaxCandidateTermID(5, 100, 200)
	require.NoError(t, err)
	require.Equal(t, int64(5), got)

	got, err = f.ProposeMaxCandidateTermID(3, 50, 150)
	require.NoError(t, err)
	require.Equal(t, int64(5), got)

	ct, err := f.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, int64(5), ct.CandidateTermID())
	require.Equal(t, int64(200), ct.TimestampMs())
	require.Equal(t, int64(100), ct.LogPosition())

	got, err = f.ProposeMaxCandidateTermID(5, 999, 888)
	require.NoError(t, err)
	require.Equal(t, int64(5), got)

	ct, err = f.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, int64(5), ct.CandidateTermID())
	require.Equal(t, int64(200), ct.TimestampMs())
	require.Equal(t, int64(100), ct.LogPosition())
}

func TestNodeStateFile_FileSyncLevels(t *testing.T) {
	t.Parallel()

	t.Run("FileSyncNone", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
		require.NoError(t, f.UpdateCandidateTermID(1, 2, 3))
	})

	t.Run("FileSyncData", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncData)
		require.NoError(t, f.UpdateCandidateTermID(9, 10, 11))
		require.NoError(t, f.Close())

		reopened := reopenNodeState(t, dir, recordinglog.FileSyncNone)
		ct, err := reopened.CandidateTerm()
		require.NoError(t, err)
		require.Equal(t, int64(9), ct.CandidateTermID())
	})
}

func TestOpenNodeStateFile_corruptFrameLength(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	require.NoError(t, f.Close())

	mapping, err := mmap.MapExisting(nodeStatePath(dir), election.MinimumNodeStateFileLength)
	require.NoError(t, err)
	var hdr nodewire.MessageHeader
	hdr.Wrap(
		mapping.Data,
		8,
		uint64(nodewire.MessageHeaderSbeSchemaVersion),
		uint64(len(mapping.Data)),
	)
	hdr.SetTemplateId(999).SetFrameLength(-1)
	require.NoError(t, mapping.Close())

	_, err = election.OpenNodeStateFile(dir, false, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
	_, err = os.Stat(nodeStatePath(dir))
	require.NoError(t, err)
}

func TestOpenNodeStateFile_incompatibleMajorVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	require.NoError(t, f.Close())

	mapping, err := mmap.MapExisting(nodeStatePath(dir), election.MinimumNodeStateFileLength)
	require.NoError(t, err)
	atomicbuffer.Wrap(mapping.Data).PutInt32(0, cnc.ComposeSemanticVersion(99, 0, 0))
	require.NoError(t, mapping.Close())

	_, err = election.OpenNodeStateFile(dir, false, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, election.ErrIncompatibleNodeStateVersion)
}

func TestOpenNodeStateFile_missingCandidateTermRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	buf := make([]byte, election.MinimumNodeStateFileLength)
	_, _, err := election.InitialiseOnCreate(buf, election.NodeStateSemanticVersion)
	require.NoError(t, err)
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, 8, uint64(nodewire.MessageHeaderSbeSchemaVersion), uint64(len(buf)))
	hdr.SetTemplateId(nodewire.NodeStateFooterSbeTemplateID)
	require.NoError(t, os.WriteFile(nodeStatePath(dir), buf, 0o600))

	_, err = election.OpenNodeStateFile(dir, false, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
}

func TestNodeStateFile_CloseIdempotentAndPostCloseErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)

	require.NoError(t, f.Close())
	require.NoError(t, f.Close())

	_, err := f.CandidateTerm()
	require.ErrorIs(t, err, election.ErrNodeStateClosed)

	_, err = f.ProposeMaxCandidateTermID(1, 2, 3)
	require.ErrorIs(t, err, election.ErrNodeStateClosed)

	err = f.UpdateCandidateTermID(1, 2, 3)
	require.ErrorIs(t, err, election.ErrNodeStateClosed)
}

func TestInitialiseOnCreate_shortBufferReturnsError(t *testing.T) {
	t.Parallel()
	_, _, err := election.InitialiseOnCreate(make([]byte, 32), election.NodeStateSemanticVersion)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
}

func TestScanForTemplateOffset_findsCandidateTerm(t *testing.T) {
	t.Parallel()
	buf := make([]byte, 64)
	_, _, err := election.InitialiseOnCreate(buf, election.NodeStateSemanticVersion)
	require.NoError(t, err)

	headerPos, ok, err := election.ScanForTemplateOffset(
		buf,
		int(nodewire.NodeStateHeaderSbeBlockLength),
		nodewire.CandidateTermSbeTemplateID,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 8, headerPos)
}

func TestScanForTemplateOffset_zeroFrameLengthStops(t *testing.T) {
	t.Parallel()
	buf := make([]byte, 64)
	_, _, err := election.InitialiseOnCreate(buf, election.NodeStateSemanticVersion)
	require.NoError(t, err)
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, 8, uint64(nodewire.MessageHeaderSbeSchemaVersion), uint64(len(buf)))
	hdr.SetTemplateId(999).SetFrameLength(0)

	_, ok, err := election.ScanForTemplateOffset(
		buf,
		int(nodewire.NodeStateHeaderSbeBlockLength),
		nodewire.CandidateTermSbeTemplateID,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestEncodeFramedCandidateTerm_shortBufferReturnsError(t *testing.T) {
	t.Parallel()
	err := election.EncodeFramedCandidateTerm(make([]byte, 8), 0, 1, 2, 3)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
}

func TestEncodeFramedNodeStateFooter_shortBufferReturnsError(t *testing.T) {
	t.Parallel()
	err := election.EncodeFramedNodeStateFooter(make([]byte, 8), 0)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
}

func TestAlignRecordOffset_roundsUp(t *testing.T) {
	t.Parallel()
	require.Equal(t, 48, election.AlignRecordOffset(41))
	require.Equal(t, 48, election.AlignRecordOffset(48))
}

func TestScanForTemplateOffset_negativeFrameLengthReturnsCorrupt(t *testing.T) {
	t.Parallel()
	buf := make([]byte, 64)
	_, _, err := election.InitialiseOnCreate(buf, election.NodeStateSemanticVersion)
	require.NoError(t, err)
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, 8, uint64(nodewire.MessageHeaderSbeSchemaVersion), uint64(len(buf)))
	hdr.SetTemplateId(999).SetFrameLength(-1)

	_, ok, err := election.ScanForTemplateOffset(
		buf,
		int(nodewire.NodeStateHeaderSbeBlockLength),
		nodewire.CandidateTermSbeTemplateID,
	)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
	require.False(t, ok)
}

func TestScanForTemplateOffset_nonAdvancingScanReturnsCorrupt(t *testing.T) {
	t.Parallel()
	buf := make([]byte, 64)
	_, _, err := election.InitialiseOnCreate(buf, election.NodeStateSemanticVersion)
	require.NoError(t, err)
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, 8, uint64(nodewire.MessageHeaderSbeSchemaVersion), uint64(len(buf)))
	hdr.SetTemplateId(999).SetFrameLength(1)

	_, ok, err := election.ScanForTemplateOffset(
		buf,
		int(nodewire.NodeStateHeaderSbeBlockLength),
		nodewire.CandidateTermSbeTemplateID,
	)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
	require.False(t, ok)
}

func TestOpenNodeStateFile_minorPatchVersionTolerated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	require.NoError(t, f.Close())

	mapping, err := mmap.MapExisting(nodeStatePath(dir), election.MinimumNodeStateFileLength)
	require.NoError(t, err)
	atomicbuffer.Wrap(mapping.Data).PutInt32(0, cnc.ComposeSemanticVersion(0, 3, 1))
	require.NoError(t, mapping.Close())

	reopened := reopenNodeState(t, dir, recordinglog.FileSyncNone)
	ct, err := reopened.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, election.NullValue, ct.CandidateTermID())
}

func TestOpenNodeStateFile_rejectsFileTooLarge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	buf := make([]byte, election.MinimumNodeStateFileLength+1)
	_, _, err := election.InitialiseOnCreate(
		buf[:election.MinimumNodeStateFileLength],
		election.NodeStateSemanticVersion,
	)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(nodeStatePath(dir), buf, 0o600))

	_, err = election.OpenNodeStateFile(dir, false, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
}

func TestOpenNodeStateFile_rejectsTruncatedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(nodeStatePath(dir), make([]byte, 128), 0o600))

	_, err := election.OpenNodeStateFile(dir, false, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
}

func TestOpenNodeStateFile_createNewWhenFileExistsLoadsExisting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	original := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	require.NoError(t, original.UpdateCandidateTermID(3, 4, 5))
	require.NoError(t, original.Close())

	reloaded := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	ct, err := reloaded.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, int64(3), ct.CandidateTermID())
}

func TestOpenNodeStateFile_tightensPermissiveModeOnLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	require.NoError(t, f.Close())
	require.NoError(t, os.Chmod(nodeStatePath(dir), 0o644))

	reopenNodeState(t, dir, recordinglog.FileSyncNone)
	info, err := os.Stat(nodeStatePath(dir))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestOpenNodeStateFile_forceFailureOnReadOnlyExisting(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root may sync read-only files")
	}
	t.Parallel()
	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	require.NoError(t, f.Close())
	require.NoError(t, os.Chmod(nodeStatePath(dir), 0o444))

	_, err := election.OpenNodeStateFile(dir, false, recordinglog.FileSyncData)
	require.Error(t, err)
}
