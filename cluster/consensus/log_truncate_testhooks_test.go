//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

// truncateAgent builds an agent with truncate control set, recordingID stored,
// and a cluster log image bound at imagePos.
func truncateAgent(
	tb testing.TB,
	imagePos int64,
	control *fakeLogTruncateControl,
	recordingID int64,
) (*Agent, *LogAdapter, *aeron.Image) {
	tb.Helper()

	adapter, image := boundLogAdapter(tb)
	aeron.ImageSetSubscriberPositionForHooks(image, imagePos)
	a := NewAgent(AgentConfig{LogAdapter: adapter})
	a.SetLogTruncateControl(control)
	a.SetLogRecordingID(recordingID)
	return a, adapter, image
}

// TestAgent_TruncateLogEntry_successWithImage asserts a successful truncate
// calls the control and lowers the adapter through DisconnectAt.
func TestAgent_TruncateLogEntry_successWithImage(t *testing.T) {
	t.Parallel()

	const (
		recordingID int64 = 77
		position    int64 = 128
		imagePos    int64 = 512
	)

	control := &fakeLogTruncateControl{nextDeletedSegmentCount: 3}
	a, adapter, _ := truncateAgent(t, imagePos, control, recordingID)

	require.NoError(t, a.TruncateLogEntry(0, position))
	require.Equal(t, position, adapter.Position())
	require.Nil(t, adapter.Image())
	require.Equal(t, 0, adapter.Poll(1<<30))
	require.Equal(t, []fakeTruncateCall{{
		recordingID: recordingID,
		position:    position,
	}}, control.calls)
}

// TestAgent_TruncateLogEntry_archiveRefuseLeavesAdapterUntouched binds an image
// so a refuse path can assert Position and Image stay put.
func TestAgent_TruncateLogEntry_archiveRefuseLeavesAdapterUntouched(t *testing.T) {
	t.Parallel()

	const (
		recordingID int64 = 77
		position    int64 = 128
		imagePos    int64 = 512
	)

	control := &fakeLogTruncateControl{
		err: &archive.Exception{ErrorCode: archive.ErrorActiveRecording},
	}
	a, adapter, image := truncateAgent(t, imagePos, control, recordingID)

	err := a.TruncateLogEntry(0, position)
	require.Error(t, err)
	code, ok := archive.ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(archive.ErrorActiveRecording), code)
	require.Equal(t, imagePos, adapter.Position())
	require.Same(t, image, adapter.Image())
	require.Len(t, control.calls, 1)
}
