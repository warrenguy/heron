package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

func TestSnapshotRecording_stopWatch(t *testing.T) {
	t.Parallel()

	const (
		controlSessionID int64 = 9
		recordingID      int64 = 42
		stopPosition     int64 = 4096
	)
	stopSignal := func(sessionID, id int64) codecs.RecordingSignalEventFields {
		return codecs.RecordingSignalEventFields{
			ControlSessionID: sessionID,
			RecordingID:      id,
			Position:         stopPosition,
			Signal:           archivewire.RecordingSignal_STOP,
		}
	}
	tests := []struct {
		name        string
		events      []codecs.RecordingSignalEventFields
		clearFirst  bool
		wantStopped bool
	}{
		{
			name:        "matching STOP",
			events:      []codecs.RecordingSignalEventFields{stopSignal(controlSessionID, recordingID)},
			wantStopped: true,
		},
		{
			name:   "foreign recording id",
			events: []codecs.RecordingSignalEventFields{stopSignal(controlSessionID, recordingID+1)},
		},
		{
			name:   "foreign control session",
			events: []codecs.RecordingSignalEventFields{stopSignal(controlSessionID+1, recordingID)},
		},
		{
			name: "START for the recording",
			events: []codecs.RecordingSignalEventFields{{
				ControlSessionID: controlSessionID,
				RecordingID:      recordingID,
				Signal:           archivewire.RecordingSignal_START,
			}},
		},
		{
			name:       "signals after ClearStop",
			events:     []codecs.RecordingSignalEventFields{stopSignal(controlSessionID, recordingID)},
			clearFirst: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &snapshotRecording{controlSessionID: func() int64 { return controlSessionID }}
			_, stopped := r.StopPosition()
			require.False(t, stopped)

			r.WatchStop(recordingID)
			if tt.clearFirst {
				r.ClearStop()
				r.ClearStop()
			}
			for _, event := range tt.events {
				r.handleRecordingSignal(event)
			}
			position, stopped := r.StopPosition()
			require.Equal(t, tt.wantStopped, stopped)
			if tt.wantStopped {
				require.Equal(t, stopPosition, position)
			}
		})
	}
}

func TestSnapshotRecording_watchReplacesEarlierWatch(t *testing.T) {
	t.Parallel()

	r := &snapshotRecording{controlSessionID: func() int64 { return 9 }}
	r.WatchStop(1)
	r.handleRecordingSignal(codecs.RecordingSignalEventFields{
		ControlSessionID: 9,
		RecordingID:      1,
		Position:         64,
		Signal:           archivewire.RecordingSignal_STOP,
	})
	_, stopped := r.StopPosition()
	require.True(t, stopped)

	r.WatchStop(2)
	_, stopped = r.StopPosition()
	require.False(t, stopped)
}
