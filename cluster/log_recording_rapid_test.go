//go:build rapid

package cluster

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
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
			"imageStop",
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
					"",
				)
				require.NoError(rt, startErr)

				// Branch is a total function of the shared TERM id.
				if wantStart {
					require.Equal(rt, "start", fake.lastBranch)
					require.Equal(rt, fake.startID, got)
					require.True(rt, fake.lastStartAutoStop)
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

			case "imageStop":
				if !model.startSucceeded {
					continue
				}
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "stopPos"+strconv.Itoa(i))
				stopCalls := fake.stopCalls
				stopAwaitCalls := fake.stopAwaitCalls
				r.handleRecordingSignal(codecs.RecordingSignalEventFields{
					RecordingID: r.recordingID,
					Position:    pos,
					Signal:      archivewire.RecordingSignal_STOP,
				})
				require.Equal(rt, stopCalls, fake.stopCalls)
				require.Equal(rt, stopAwaitCalls, fake.stopAwaitCalls)
				require.False(rt, r.IsActive())
				got, posErr := r.StopPositionFor(r.latchedRecordingID)
				require.NoError(rt, posErr)
				require.Equal(rt, pos, got)
				model.startSucceeded = false

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

// pollStopModel is the reference model for PollStopLogRecording.
type pollStopModel struct {
	needsStop   bool
	recordingID int64
	latchedID   int64
	stopPos     int64
	pending     bool
	offered     bool
	deadline    int64
	inFlight    int
	nowNs       int64
	proofHeld   bool
}

func (m *pollStopModel) isActive() bool {
	return m.needsStop && m.recordingID >= 0
}

func (m *pollStopModel) clearPending() {
	if m.pending {
		m.inFlight--
	}
	m.pending = false
	m.offered = false
}

func (m *pollStopModel) markStopped(pos int64) {
	if m.recordingID >= 0 {
		m.latchedID = m.recordingID
	}
	m.stopPos = pos
	m.needsStop = false
	m.proofHeld = true
	m.clearPending()
}

func (m *pollStopModel) poll(
	offerOK bool,
	offerErr error,
	responded bool,
	responseErr error,
) (done bool, err error) {
	if !m.needsStop {
		return true, nil
	}
	if !m.pending {
		m.pending = true
		m.offered = false
		m.deadline = m.nowNs + pollStopOfferTimeoutNs
		m.inFlight++
	}
	if !m.offered {
		if offerErr != nil {
			m.clearPending()
			return false, offerErr
		}
		if !offerOK {
			if m.nowNs >= m.deadline {
				m.clearPending()
				return false, errStopOfferTimeout
			}
			return false, nil
		}
		m.offered = true
		m.deadline = m.nowNs + pollStopResponseTimeoutNs
	}
	dirty := m.recordingID < 0
	switch {
	case dirty && isUnknownSubscription(responseErr):
		m.markStopped(m.stopPos)
		return true, nil
	case responseErr != nil:
		m.clearPending()
		return false, responseErr
	case responded && dirty:
		m.markStopped(m.stopPos)
		return true, nil
	}
	if m.nowNs >= m.deadline {
		m.clearPending()
		return false, errStopResponseTimeout
	}
	return false, nil
}

func requirePollStopInvariants(
	rt *rapid.T,
	r *logRecording,
	model *pollStopModel,
	h *stopRequestHarness,
	done bool,
	pollErr error,
) {
	rt.Helper()

	require.Equal(rt, model.needsStop, r.needsStop())
	require.Equal(rt, model.isActive(), r.IsActive())
	require.Equal(rt, model.stopPos, r.stopPosition)
	require.Equal(rt, model.latchedID, r.latchedRecordingID)
	inFlight := 0
	if r.pendingStop != nil {
		inFlight = 1
	}
	require.Equal(rt, inFlight, model.inFlight)
	require.Equal(rt, inFlight, h.inFlight())
	if done {
		require.NoError(rt, pollErr)
		require.False(rt, r.needsStop())
	}
	if pollErr != nil {
		require.False(rt, done)
		require.True(rt, r.needsStop())
		require.NotEmpty(rt, r.startChannel)
		require.Nil(rt, r.pendingStop)
	}
}

// TestLogRecording_pollStop_rapid draws poll, offer, response, signal, clock,
// and sync-stop ops against pollStopModel.
func TestLogRecording_pollStop_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		needsStop := rapid.Bool().Draw(rt, "needsStop")
		dirty := rapid.Bool().Draw(rt, "dirty")
		priorPos := rapid.Int64Range(0, 1<<20).Draw(rt, "priorPos")
		recordingID := pollStopRecordingID
		if dirty {
			recordingID = recordingIDUnknown
		}

		h := &stopRequestHarness{}
		r := newPollStopRecording(recordingID, priorPos, h)
		if !needsStop {
			r.startChannel = nil
		}

		model := pollStopModel{
			needsStop:   needsStop,
			recordingID: recordingID,
			latchedID:   recordingIDUnknown,
			stopPos:     priorPos,
			proofHeld:   !needsStop,
		}

		n := rapid.IntRange(1, 40).Draw(rt, "n")
		ops := []string{
			"poll",
			"offerAccepted",
			"offerBackPressure",
			"deliverOK",
			"deliverUnknownSubscription",
			"deliverError",
			"deliverStopSignal",
			"advanceClock",
			"syncStop",
		}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))
			var (
				done bool
				err  error
			)

			switch op {
			case "offerAccepted":
				h.offerOK = true
				h.offerErr = nil
			case "offerBackPressure":
				h.offerOK = false
				h.offerErr = nil
			case "deliverOK":
				h.responded = true
				h.responseErr = nil
			case "deliverUnknownSubscription":
				h.responded = false
				h.responseErr = &archive.Exception{
					ErrorCode: archive.ErrorUnknownSubscription,
				}
			case "deliverError":
				h.responded = false
				h.responseErr = &archive.Exception{ErrorCode: archive.ErrorGeneric}
			case "deliverStopSignal":
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "stopPos"+strconv.Itoa(i))
				r.handleRecordingSignal(codecs.RecordingSignalEventFields{
					RecordingID: r.recordingID,
					Position:    pos,
					Signal:      archivewire.RecordingSignal_STOP,
				})
				if model.isActive() {
					model.markStopped(pos)
				}
			case "advanceClock":
				delta := rapid.Int64Range(1, 1500).Draw(rt, "delta"+strconv.Itoa(i))
				model.nowNs += delta
			case "syncStop":
				require.NoError(rt, r.StopLogRecording())
				if model.needsStop {
					pos := int64(0)
					if model.recordingID < 0 {
						pos = model.stopPos
					}
					model.markStopped(pos)
				} else {
					model.clearPending()
				}
			case "poll":
				needsStopBefore := r.needsStop()
				builtBefore := len(h.built)
				wantDone, wantErr := model.poll(h.offerOK, h.offerErr, h.responded, h.responseErr)
				done, err = r.PollStopLogRecording(model.nowNs)
				if wantErr != nil {
					require.ErrorIs(rt, err, wantErr)
					require.False(rt, done)
				} else {
					require.NoError(rt, err)
					require.Equal(rt, wantDone, done)
				}
				if !needsStopBefore {
					require.Len(rt, h.built, builtBefore)
				}
				if done && needsStopBefore {
					require.True(rt, model.proofHeld)
				}
			}

			requirePollStopInvariants(rt, r, &model, h, done, err)
		}
	})
}
