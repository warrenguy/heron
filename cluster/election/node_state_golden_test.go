package election_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/mmap"
)

type nodeStateCandidateTermExpect struct {
	CandidateTermID int64 `json:"candidateTermId"`
	TimestampMs     int64 `json:"timestampMs"`
	LogPosition     int64 `json:"logPosition"`
}

type nodeStateGoldenFixture struct {
	File          string                       `json:"file"`
	Action        string                       `json:"action"`
	Description   string                       `json:"description"`
	CandidateTerm nodeStateCandidateTermExpect `json:"candidateTerm"`
}

type nodeStateGoldenManifest struct {
	PrefixLength int                      `json:"prefixLength"`
	Fixtures     []nodeStateGoldenFixture `json:"fixtures"`
}

func loadNodeStateGoldenManifest(t *testing.T) nodeStateGoldenManifest {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "node-state", "fixtures.json"))
	require.NoError(t, err)

	var m nodeStateGoldenManifest
	require.NoError(t, json.Unmarshal(raw, &m))
	require.NotEmpty(t, m.Fixtures)
	require.Positive(t, m.PrefixLength)

	validActions := []string{"create", "update"}
	for _, fixture := range m.Fixtures {
		require.NotEmpty(t, fixture.File)
		require.NotEmpty(t, fixture.Action)
		require.Contains(
			t,
			validActions,
			fixture.Action,
			"fixture %q: action must be create or update",
			fixture.File,
		)
	}
	return m
}

func readNodeStateGoldenPrefix(t *testing.T, file string, prefixLen int) []byte {
	t.Helper()

	golden, err := os.ReadFile(filepath.Join("testdata", "node-state", file))
	require.NoError(t, err)
	require.NotEmpty(t, golden)
	require.Len(t, golden, prefixLen, "golden %q must be exactly prefixLength bytes", file)
	return golden
}

func readNodeStatePrefixFromDir(t *testing.T, dir string, prefixLen int) []byte {
	t.Helper()

	mapping, err := mmap.MapExisting(nodeStatePath(dir), prefixLen)
	require.NoError(t, err)
	defer func() { require.NoError(t, mapping.Close()) }()

	prefix := make([]byte, prefixLen)
	copy(prefix, mapping.Data[:prefixLen])
	return prefix
}

func heronNodeStatePrefixForFixture(
	t *testing.T,
	fixture nodeStateGoldenFixture,
	prefixLen int,
) []byte {
	t.Helper()

	dir := t.TempDir()
	f := openNodeStateInDir(t, dir, true, recordinglog.FileSyncNone)
	if fixture.Action == "update" {
		require.NoError(t, f.UpdateCandidateTermID(
			fixture.CandidateTerm.CandidateTermID,
			fixture.CandidateTerm.LogPosition,
			fixture.CandidateTerm.TimestampMs,
		))
	}
	require.NoError(t, f.Close())
	return readNodeStatePrefixFromDir(t, dir, prefixLen)
}

func TestNodeStateFile_matchesJavaGolden(t *testing.T) {
	t.Parallel()

	m := loadNodeStateGoldenManifest(t)

	for _, fixture := range m.Fixtures {
		t.Run(fixture.File, func(t *testing.T) {
			t.Parallel()

			golden := readNodeStateGoldenPrefix(t, fixture.File, m.PrefixLength)
			got := heronNodeStatePrefixForFixture(t, fixture, m.PrefixLength)
			requireNodeStatePrefixEqual(t, got, golden, m.PrefixLength)
		})
	}
}

func TestNodeStateFile_javaGoldenCandidateTermFields(t *testing.T) {
	t.Parallel()

	m := loadNodeStateGoldenManifest(t)

	for _, fixture := range m.Fixtures {
		t.Run(fixture.File, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, fixture.File)

			dir := t.TempDir()
			golden := readNodeStateGoldenPrefix(t, fixture.File, m.PrefixLength)
			require.NoError(t, os.WriteFile(nodeStatePath(dir), padToMinimumNodeStateFile(golden), 0o600))

			f := reopenNodeState(t, dir, recordinglog.FileSyncNone)
			ct, err := f.CandidateTerm()
			require.NoError(t, err)
			require.Equal(t, fixture.CandidateTerm.CandidateTermID, ct.CandidateTermID())
			require.Equal(t, fixture.CandidateTerm.TimestampMs, ct.TimestampMs())
			require.Equal(t, fixture.CandidateTerm.LogPosition, ct.LogPosition())
		})
	}
}

func padToMinimumNodeStateFile(prefix []byte) []byte {
	buf := make([]byte, election.MinimumNodeStateFileLength)
	copy(buf, prefix)
	return buf
}
