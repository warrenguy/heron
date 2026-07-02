//go:build rapid

package cnc_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/cnc"
)

type recordingPositionSlot struct {
	typeID      int32
	recordingID int64
	archiveID   int64
}

// TestFindRecordingPositionCounter_matchesOracle_rapid checks the binary key scan against
// a reference model over random counter layouts: it must return the first allocated
// recording-position counter matching the recording id and the archive id filter.
func TestFindRecordingPositionCounter_matchesOracle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		counterCount := rapid.IntRange(1, 8).Draw(rt, "counterCount")
		meta, values := syntheticCountersBuffers(t, counterCount)

		slots := make([]recordingPositionSlot, counterCount)
		for id := range counterCount {
			typeID := rapid.SampledFrom([]int32{
				cnc.ArchiveRecordingPositionCounterTypeID,
				cnc.ClientHeartbeatTypeID,
			}).Draw(rt, "typeID")
			recordingID := rapid.Int64Range(0, 4).Draw(rt, "recordingID")
			archiveID := rapid.Int64Range(0, 3).Draw(rt, "archiveID")
			sessionID := rapid.Int32().Draw(rt, "sessionID")
			sourceIdentityLen := rapid.IntRange(0, 24).Draw(rt, "sourceIdentityLen")
			sourceIdentity := strings.Repeat("x", sourceIdentityLen)

			seedRecordingPositionCounter(
				t, meta, values, int32(id), typeID,
				recordingID, sessionID, sourceIdentity, archiveID, int64(id),
			)
			slots[id] = recordingPositionSlot{
				typeID:      typeID,
				recordingID: recordingID,
				archiveID:   archiveID,
			}
		}

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(rt, err)

		wantRecordingID := rapid.Int64Range(0, 4).Draw(rt, "wantRecordingID")
		wantArchiveID := rapid.SampledFrom([]int64{
			cnc.NullValue, 0, 1, 2, 3,
		}).Draw(rt, "wantArchiveID")

		expectedID, expectedOK := oracleFindRecordingPositionCounter(
			slots,
			wantRecordingID,
			wantArchiveID,
		)

		slot, ok := reader.FindRecordingPositionCounter(wantRecordingID, wantArchiveID)
		require.Equal(rt, expectedOK, ok)
		if expectedOK {
			require.Equal(rt, expectedID, slot.CounterID())
		}
	})
}

func oracleFindRecordingPositionCounter(
	slots []recordingPositionSlot,
	recordingID int64,
	archiveID int64,
) (int32, bool) {
	for id, slot := range slots {
		if slot.typeID != cnc.ArchiveRecordingPositionCounterTypeID {
			continue
		}
		if slot.recordingID != recordingID {
			continue
		}
		if archiveID != cnc.NullValue && slot.archiveID != archiveID {
			continue
		}
		return int32(id), true
	}
	return cnc.NullCounterID, false
}
