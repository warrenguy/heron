//go:build rapid && testhooks

package consensus

import (
	"errors"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// logRecordingModel is the reference model for the recording lifecycle.
//
// The model carries exactly the state the invariants read.
//
// The model transition function is the specification the Agent must match.
type logRecordingModel struct {
	recordingID   int64 // last non-null recording id, -1 before the first start
	active        bool  // a recording is running now
	termBase      int64 // the current term base log position
	lastPrepared  int64 // last value PrepareForNewLeadership returned
	starts        int   // successful starts
	stops         int   // successful stops
	startAttempts int   // steps that reached LogRecordingControl.StartLogRecording
	stopAttempts  int   // steps that reached LogRecordingControl.StopLogRecording
}

func TestRecordingLifecycle_startStopTransition_rapid(t *testing.T) {
	t.Parallel()

	const sessionID int32 = 10

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		a, _, _ := agentWithLogJoin(t, sessionID, 0, AgentConfig{})
		lp, _ := boundLogPublisher(t)
		a.SetLogPublisher(lp)

		control := &fakeLogRecordingControl{}
		a.SetLogRecordingControl(control)

		model := logRecordingModel{recordingID: nullValue}
		n := rapid.IntRange(1, 40).Draw(rt, "n")
		ops := []string{
			"startLeader",
			"startFollower",
			"signalStart",
			"transition",
			"setTermBase",
		}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op")
			wantStartErr := rapid.Bool().Draw(rt, "startErr")
			wantStopErr := rapid.Bool().Draw(rt, "stopErr")
			scriptedStartErr := error(nil)
			scriptedStopErr := error(nil)
			if wantStartErr {
				scriptedStartErr = errors.New("scripted start error")
			}
			if wantStopErr {
				scriptedStopErr = errors.New("scripted stop error")
			}
			control.startErr = scriptedStartErr
			control.stopErr = scriptedStopErr

			drawnID := rapid.Int64Range(1, 1<<20).Draw(rt, "recordingID")
			control.nextRecordingID = drawnID

			switch op {
			case "startLeader":
				drawnLogPosition := rapid.Int64Range(0, 1<<30).Draw(rt, "logPosition")
				model.termBase = drawnLogPosition
				err := a.JoinLogAsLeader(int64(i+1), drawnLogPosition, sessionID, false)
				if model.active {
					require.NoError(rt, err)
				} else {
					model.startAttempts++
					if scriptedStartErr != nil {
						require.ErrorIs(rt, err, scriptedStartErr)
					} else {
						require.NoError(rt, err)
						model.recordingID = drawnID
						model.active = true
						model.starts++
					}
				}

			case "startFollower":
				joined, err := a.JoinLogAsFollower(
					FollowerLogJoinParams{LogSessionID: sessionID},
					int64(i+1),
				)
				if model.active {
					require.NoError(rt, err)
					require.True(rt, joined)
				} else {
					model.startAttempts++
					if scriptedStartErr != nil {
						require.ErrorIs(rt, err, scriptedStartErr)
						require.False(rt, joined)
					} else {
						require.NoError(rt, err)
						require.True(rt, joined)
						model.recordingID = drawnID
						model.active = true
						model.starts++
					}
				}

			case "signalStart":
				a.SetLogRecordingID(drawnID)
				if !model.active {
					model.recordingID = drawnID
				}

			case "transition":
				logPosition := model.termBase + rapid.Int64Range(0, 1<<20).Draw(rt, "logPositionDelta")
				stopPosition := model.termBase + rapid.Int64Range(0, 1<<20).Draw(rt, "stopDelta")
				control.nextStopPosition = stopPosition
				got, err := a.PrepareForNewLeadership(logPosition, int64(i+1))
				// Wired control always receives stop so dirty starts drain.
				model.stopAttempts++
				if !model.active {
					if scriptedStopErr != nil {
						require.ErrorIs(rt, err, scriptedStopErr)
						require.Equal(rt, recordinglog.NullPosition, got)
					} else {
						require.NoError(rt, err)
						model.lastPrepared = logPosition
						require.Equal(rt, model.lastPrepared, got)
						require.GreaterOrEqual(rt, got, model.termBase)
						require.NotEqual(rt, int64(nullValue), got)
					}
				} else if scriptedStopErr != nil {
					require.ErrorIs(rt, err, scriptedStopErr)
					require.Equal(rt, recordinglog.NullPosition, got)
				} else {
					require.NoError(rt, err)
					model.active = false
					model.lastPrepared = stopPosition
					model.stops++
					require.False(rt, a.IsRecordingActive())
					require.Equal(rt, model.recordingID, a.LocalLogRecordingID())
					require.Equal(rt, model.lastPrepared, got)
					require.GreaterOrEqual(rt, got, model.termBase)
					require.NotEqual(rt, int64(nullValue), got)
				}

			case "setTermBase":
				pos := rapid.Int64Range(0, 1<<30).Draw(rt, "termBase")
				a.SetTermBaseLogPosition(pos)
				model.termBase = pos
			}

			require.Equal(rt, model.recordingID, a.LocalLogRecordingID())
			if model.starts > 0 {
				require.NotEqual(rt, int64(nullValue), a.LocalLogRecordingID())
			}
			require.Equal(rt, model.active, a.IsRecordingActive())
			require.Equal(rt, model.stopAttempts, control.stopCalls)
			require.Len(rt, control.startCalls, model.startAttempts)

			for _, call := range control.startCalls {
				switch call.sourceLocation {
				case archivewire.SourceLocation_LOCAL, archivewire.SourceLocation_REMOTE:
				default:
					rt.Fatalf("unexpected source location %v", call.sourceLocation)
				}
			}
		}
	})
}
