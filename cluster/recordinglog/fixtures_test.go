package recordinglog_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

type manifestEntry struct {
	RecordingID         int64  `json:"recordingId"`
	LeadershipTermID    int64  `json:"leadershipTermId"`
	TermBaseLogPosition int64  `json:"termBaseLogPosition"`
	LogPosition         int64  `json:"logPosition"`
	Timestamp           int64  `json:"timestamp"`
	ServiceID           int32  `json:"serviceId"`
	Type                int32  `json:"type"`
	Valid               bool   `json:"valid"`
	ArchiveEndpoint     string `json:"archiveEndpoint"`
	Position            int64  `json:"position"`
	Index               int32  `json:"index"`
}

type manifest struct {
	Fixtures []struct {
		File    string          `json:"file"`
		Entries []manifestEntry `json:"entries"`
	} `json:"fixtures"`
}

// TestReload_goldenFixtures decodes committed recording.log fixtures against fixtures.json.
func TestReload_goldenFixtures(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("testdata", "fixtures.json"))
	require.NoError(t, err)

	var m manifest
	require.NoError(t, json.Unmarshal(raw, &m))
	require.NotEmpty(t, m.Fixtures)

	for _, fixture := range m.Fixtures {
		t.Run(fixture.File, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, fixture.Entries)

			entries, err := recordinglog.Reload(filepath.Join("testdata", fixture.File))
			require.NoError(t, err)

			want := make([]recordinglog.Entry, len(fixture.Entries))
			for i, e := range fixture.Entries {
				want[i] = recordinglog.Entry{
					RecordingID:         e.RecordingID,
					LeadershipTermID:    e.LeadershipTermID,
					TermBaseLogPosition: e.TermBaseLogPosition,
					LogPosition:         e.LogPosition,
					Timestamp:           e.Timestamp,
					ServiceID:           e.ServiceID,
					Type:                e.Type,
					ArchiveEndpoint:     e.ArchiveEndpoint,
					Valid:               e.Valid,
					Position:            e.Position,
					Index:               e.Index,
				}
			}

			require.Equal(t, want, entries)
		})
	}
}
