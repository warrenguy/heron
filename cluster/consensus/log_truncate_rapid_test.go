//go:build rapid && testhooks

package consensus

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

// TestTruncateLogEntry_positionGuards_rapid asserts local position and active
// guards, and DisconnectAt min latch, for drawn truncate inputs.
func TestTruncateLogEntry_positionGuards_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		position := rapid.Int64Range(-1<<20, 1<<20).Draw(rt, "position")
		biasAligned := rapid.Bool().Draw(rt, "biasAligned")
		if biasAligned {
			position = position &^ (logbuffer.FrameAlignment - 1)
		}
		recordingID := rapid.Int64Range(1, 1<<20).Draw(rt, "recordingID")
		active := rapid.Bool().Draw(rt, "active")
		seedPos := rapid.Int64Range(0, 1<<20).Draw(rt, "seedPos")

		control := &fakeLogTruncateControl{nextDeletedSegmentCount: 1}
		a, adapter, image := truncateAgent(t, seedPos, control, recordingID)
		if active {
			a.SetLogRecordingControl(&fakeLogRecordingControl{active: true})
		}

		beforePos := adapter.Position()

		offerable := position >= 0 && position&(logbuffer.FrameAlignment-1) == 0
		err := a.TruncateLogEntry(0, position)

		if !offerable || active {
			require.Error(rt, err)
			require.Empty(rt, control.calls)
			require.Equal(rt, beforePos, adapter.Position())
			require.Same(rt, image, adapter.Image())
			return
		}

		require.NoError(rt, err)
		require.Equal(rt, []fakeTruncateCall{{
			recordingID: recordingID,
			position:    position,
		}}, control.calls)
		// DisconnectAt latches image position then min with truncate position.
		require.Equal(rt, min(seedPos, position), adapter.Position())
		require.Nil(rt, adapter.Image())
	})
}
