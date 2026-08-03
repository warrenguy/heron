//go:build rapid

package cluster

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// branchModel is the reference model for the start-versus-extend branch.
type branchModel struct {
	startSucceeded bool
	lastReturned   int64
	sawWriteTerm   bool
}

func TestLogRecording_startVsExtendBranch_rapid(t *testing.T) {
	t.Parallel()

	const (
		logChannel = "aeron:udp?endpoint=239.255.12.1:9004"
		streamID   = int32(100)
		sessionID  = int32(9)
	)

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		dir := t.TempDir()
		termLog, err := recordinglog.Open(dir, true)
		require.NoError(rt, err)

		fake := &recordingArchiveStub{}
		r := &logRecording{
			ctx:         context.Background(),
			client:      fake,
			terms:       termLog,
			logChannel:  logChannel,
			logStreamID: streamID,
		}

		model := branchModel{}
		n := rapid.IntRange(1, 30).Draw(rt, "n")
		ops := []string{
			"startRecording",
			"writeTerm",
			"stopRecording",
			"reopenLog",
		}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))

			switch op {
			case "startRecording":
				if model.startSucceeded {
					// Idempotent re-start first drains; keep model in step by
					// stopping through the control first.
					require.NoError(rt, r.StopLogRecording())
					model.startSucceeded = false
					require.False(rt, r.IsActive())
				}

				wantStart := termLog.TermRecordingID() == recordinglog.NullRecordingID
				// After any writeTerm, no later step takes start.
				if model.sawWriteTerm {
					require.False(rt, wantStart)
				}

				fake.lastBranch = ""
				fake.startID = rapid.Int64Range(0, 1<<20).Draw(rt, "startID"+strconv.Itoa(i))
				got, startErr := r.StartLogRecording(
					archivewire.SourceLocation_REMOTE,
					sessionID,
				)
				require.NoError(rt, startErr)

				// Branch is a total function of the shared TERM id.
				if wantStart {
					require.Equal(rt, "start", fake.lastBranch)
					require.Equal(rt, fake.startID, got)
				} else {
					require.Equal(rt, "extend", fake.lastBranch)
					require.Equal(rt, termLog.TermRecordingID(), fake.lastExtendID)
					require.True(rt, fake.lastExtendAutoStop)
					// Extend carries the established id forward.
					require.Equal(rt, termLog.TermRecordingID(), got)
				}

				// Control stores the id it returned.
				require.Equal(rt, got, r.recordingID)
				// Successful start or extend leaves control active.
				require.True(rt, r.IsActive())

				model.lastReturned = got
				model.startSucceeded = true

			case "writeTerm":
				if model.lastReturned < 0 || model.lastReturned == recordinglog.NullRecordingID {
					continue
				}
				if !model.startSucceeded {
					continue
				}
				// Pick a free leadership term id.
				leadershipTermID := rapid.Int64Range(0, 40).Draw(rt, "term"+strconv.Itoa(i))
				if termLog.FindTermEntry(leadershipTermID) != nil {
					continue
				}
				err := termLog.AppendTerm(model.lastReturned, leadershipTermID, 0, int64(i+1))
				// Term write after the branch never mismatches.
				require.NoError(rt, err)
				model.sawWriteTerm = true
				require.Equal(rt, model.lastReturned, termLog.TermRecordingID())

			case "stopRecording":
				if !model.startSucceeded {
					continue
				}
				require.NoError(rt, r.StopLogRecording())
				model.startSucceeded = false
				// Inactive after stop.
				require.False(rt, r.IsActive())

			case "reopenLog":
				// Reopen preserves the branch decision.
				before := termLog.TermRecordingID()
				require.NoError(rt, termLog.Close())

				reopened, openErr := recordinglog.Open(dir, false)
				require.NoError(rt, openErr)
				termLog = reopened
				require.Equal(rt, before, termLog.TermRecordingID())

				// Restart control over the reopened log; clear start state.
				r = &logRecording{
					ctx:         context.Background(),
					client:      fake,
					terms:       termLog,
					logChannel:  logChannel,
					logStreamID: streamID,
				}
				model.startSucceeded = false
				model.lastReturned = recordinglog.NullRecordingID
				// Inactive after reopen.
				require.False(rt, r.IsActive())

				// Next startRecording (if drawn) must take the same branch
				// pre-reopen state would have taken.

				wantStart := before == recordinglog.NullRecordingID
				if model.sawWriteTerm {
					require.False(rt, wantStart)
				}
			}
		}

		require.NoError(rt, termLog.Close())
	})
}

// truncateStoreModel is the reference model for the truncate stop position
// store.
type truncateStoreModel struct {
	latchedID int64
	stopPos   int64
}

// TestLogRecording_truncateStopPosition_rapid asserts StopPositionFor reports
// the last successful truncate position across failed truncates and dirty
// drains, with no catalog read once nothing is pending.
func TestLogRecording_truncateStopPosition_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		fake := &recordingArchiveStub{}
		r := &logRecording{
			ctx:                context.Background(),
			client:             fake,
			logChannel:         "aeron:udp?endpoint=239.255.12.1:9004",
			logStreamID:        100,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}

		model := truncateStoreModel{latchedID: recordingIDUnknown}
		recordingID := rapid.Int64Range(0, 1<<20).Draw(rt, "recordingID")

		n := rapid.IntRange(1, 30).Draw(rt, "n")
		ops := []string{"truncateOK", "truncateFail", "armDirtyThenDrain"}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))

			switch op {
			case "truncateOK":
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "pos"+strconv.Itoa(i))
				fake.truncateErr = nil
				_, err := r.TruncateLogRecording(recordingID, pos)
				require.NoError(rt, err)
				model.latchedID = recordingID
				model.stopPos = pos

			case "truncateFail":
				fake.truncateErr = errors.New("archive refused")
				_, err := r.TruncateLogRecording(recordingID, 64)
				require.Error(rt, err)

			case "armDirtyThenDrain":
				// A dirty start keeps the channel. The drain keeps the stored stop.
				// markStopped with an unknown catalog id changes nothing.
				r.startChannel = []byte("aeron:udp?endpoint=239.255.12.1:9004")
				r.recordingID = recordingIDUnknown
				require.NoError(rt, r.StopLogRecording())
			}

			require.Equal(rt, model.latchedID, r.latchedRecordingID)
			require.Equal(rt, model.stopPos, r.stopPosition)
			if model.latchedID >= 0 {
				listBefore := fake.listCalls
				got, err := r.StopPositionFor(model.latchedID)
				require.NoError(rt, err)
				require.Equal(rt, model.stopPos, got)
				require.Equal(rt, listBefore, fake.listCalls)
			}
		}
	})
}
