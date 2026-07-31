package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

func TestStartSignalTracker(t *testing.T) {
	const (
		armedCorrelation = int64(707)
		firstRecordingID = int64(6001)
		secondRecording  = int64(6002)
	)

	type armMode int
	const (
		armBefore armMode = iota
		armNone
		armThenReset
		armZero
	)

	cases := []struct {
		name             string
		trackerSessionID int64
		signalSessionID  int64
		signalCorr       int64
		signalRecording  int64
		signal           archivewire.RecordingSignal
		arm              armMode
		secondEvent      *codecs.RecordingSignalEventFields
		wantStarted      bool
		wantRecordingID  int64
	}{
		{
			name:             "matching_start",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armBefore,
			wantStarted:      true,
			wantRecordingID:  firstRecordingID,
		},
		{
			name:             "unrelated_start_different_correlation",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation + 1,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armBefore,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "unrelated_start_different_control_session",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID + 1,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armBefore,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "tracker_session_0_matches_any_session",
			trackerSessionID: 0,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armBefore,
			wantStarted:      true,
			wantRecordingID:  firstRecordingID,
		},
		{
			name:             "wrong_kind_stop",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_STOP,
			arm:              armBefore,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_extend",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_delete",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_DELETE,
			arm:              armBefore,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_null_value",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_NULL_VALUE,
			arm:              armBefore,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "first_match_wins",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armBefore,
			secondEvent: &codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    armedCorrelation,
				RecordingID:      secondRecording,
				Signal:           archivewire.RecordingSignal_START,
			},
			wantStarted:     true,
			wantRecordingID: firstRecordingID,
		},
		{
			name:             "reset_clears_and_disarms",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armThenReset,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "unarmed_tracker",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armNone,
			wantStarted:      false,
			wantRecordingID:  0,
		},
		{
			name:             "armed_correlation_zero_matches",
			trackerSessionID: controlResponseTestControlSessionID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       0,
			signalRecording:  firstRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armZero,
			wantStarted:      true,
			wantRecordingID:  firstRecordingID,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewStartSignalTracker(tc.trackerSessionID)
			switch tc.arm {
			case armBefore, armThenReset:
				tracker.arm(armedCorrelation)
			case armZero:
				tracker.arm(0)
			}

			event := codecs.RecordingSignalEventFields{
				ControlSessionID: tc.signalSessionID,
				CorrelationID:    tc.signalCorr,
				RecordingID:      tc.signalRecording,
				Signal:           tc.signal,
			}
			tracker.RecordingSignal(event)

			if tc.arm == armThenReset {
				require.True(t, tracker.Started())
				tracker.Reset()
				tracker.RecordingSignal(event)
			}
			if tc.secondEvent != nil {
				tracker.RecordingSignal(*tc.secondEvent)
			}

			require.Equal(t, tc.wantStarted, tracker.Started())
			require.Equal(t, tc.wantRecordingID, tracker.StartedRecordingID())
		})
	}
}

func TestExtendSignalTracker(t *testing.T) {
	const (
		armedCorrelation = int64(808)
		armedRecordingID = int64(7001)
		otherRecording   = int64(7002)
	)

	type armMode int
	const (
		armBefore armMode = iota
		armNone
		armThenReset
		armZero
	)

	cases := []struct {
		name             string
		trackerSessionID int64
		trackerRecording int64
		signalSessionID  int64
		signalCorr       int64
		signalRecording  int64
		signal           archivewire.RecordingSignal
		arm              armMode
		secondEvent      *codecs.RecordingSignalEventFields
		wantExtended     bool
		wantRecordingID  int64
	}{
		{
			name:             "matching_extend",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantExtended:     true,
			wantRecordingID:  armedRecordingID,
		},
		{
			name:             "unrelated_extend_different_correlation",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation + 1,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "unrelated_extend_different_recording",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  otherRecording,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_start",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_START,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_stop",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_STOP,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_replicate",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_REPLICATE,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "unrelated_extend_different_control_session",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID + 1,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "tracker_session_0_matches_any_session",
			trackerSessionID: 0,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantExtended:     true,
			wantRecordingID:  armedRecordingID,
		},
		{
			name:             "repeat_extend_stays_latched",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			secondEvent: &codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    armedCorrelation,
				RecordingID:      armedRecordingID,
				Signal:           archivewire.RecordingSignal_EXTEND,
			},
			wantExtended:    true,
			wantRecordingID: armedRecordingID,
		},
		{
			name:             "sticky_latch_ignores_non_match_after_extend",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			secondEvent: &codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    armedCorrelation,
				RecordingID:      otherRecording,
				Signal:           archivewire.RecordingSignal_EXTEND,
			},
			wantExtended:    true,
			wantRecordingID: armedRecordingID,
		},
		{
			name:             "reset_clears_and_disarms",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armThenReset,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "unarmed_tracker",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       0,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armNone,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "armed_correlation_zero_matches",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       0,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armZero,
			wantExtended:     true,
			wantRecordingID:  armedRecordingID,
		},
		{
			name:             "negative_recording_id_no_wildcard",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: -1,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  otherRecording,
			signal:           archivewire.RecordingSignal_EXTEND,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_delete",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_DELETE,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
		{
			name:             "wrong_kind_null_value",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: armedRecordingID,
			signalSessionID:  controlResponseTestControlSessionID,
			signalCorr:       armedCorrelation,
			signalRecording:  armedRecordingID,
			signal:           archivewire.RecordingSignal_NULL_VALUE,
			arm:              armBefore,
			wantExtended:     false,
			wantRecordingID:  0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewExtendSignalTracker(tc.trackerSessionID, tc.trackerRecording)
			switch tc.arm {
			case armBefore, armThenReset:
				tracker.arm(armedCorrelation)
			case armZero:
				tracker.arm(0)
			}

			event := codecs.RecordingSignalEventFields{
				ControlSessionID: tc.signalSessionID,
				CorrelationID:    tc.signalCorr,
				RecordingID:      tc.signalRecording,
				Signal:           tc.signal,
			}
			tracker.RecordingSignal(event)

			if tc.arm == armThenReset {
				require.True(t, tracker.Extended())
				tracker.Reset()
				tracker.RecordingSignal(event)
			}
			if tc.secondEvent != nil {
				tracker.RecordingSignal(*tc.secondEvent)
			}

			require.Equal(t, tc.wantExtended, tracker.Extended())
			require.Equal(t, tc.wantRecordingID, tracker.ExtendedRecordingID())
		})
	}
}
