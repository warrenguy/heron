//go:build rapid

package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
)

// Small timeouts let drawn clock steps cross every deadline.
const (
	modelOfferNs    int64 = 5
	modelResponseNs int64 = 7
	modelCheckNs    int64 = 3
	modelProgressNs int64 = 11
	modelMaxStepNs  int64 = modelProgressNs + 2
)

const (
	modelOtherID    int64 = testReplicationDestinationID + 1
	modelStartBuild int64 = 100
)

var errModelInject = errors.New("inject")

// replicationModel is the reference model of one log replication. It folds
// the inputs the helper and the fake handles see. It predicts every observable
// result.
type replicationModel struct {
	phase          logReplicationPhase
	closeRequested bool

	offerDeadline    int64
	responseDeadline int64
	nextCheck        int64
	progressDeadline int64

	stopAnchored         bool
	stopOffered          bool
	stopOfferDeadline    int64
	stopResponseDeadline int64
	stopResolved         bool
	stopBuilds           int

	latched        bool
	extended       bool
	stopped        bool
	ended          bool
	deleted        bool
	recordingID    int64
	signalPosition int64
	counter        int64

	offerAccepted bool
	stopStored    bool
	storedID      int64
	storedPos     int64
}

func newReplicationModel(buildNs int64) *replicationModel {
	return &replicationModel{
		phase:          logReplicationOffering,
		offerDeadline:  buildNs + modelOfferNs,
		recordingID:    cnc.NullValue,
		signalPosition: archive.NullPosition,
		counter:        archive.NullPosition,
	}
}

func (m *replicationModel) position() int64 {
	if !m.extended {
		return archive.NullPosition
	}
	return max(m.signalPosition, m.counter)
}

func (m *replicationModel) recording() int64 {
	if !m.extended {
		return cnc.NullValue
	}
	return m.recordingID
}

func (m *replicationModel) closeNow() { m.phase = logReplicationClosed }

func (m *replicationModel) enterStopping() {
	m.phase = logReplicationStopping
	m.stopBuilds++
}

func (m *replicationModel) close() {
	switch m.phase {
	case logReplicationOffering:
		m.closeNow()
	case logReplicationAwaitingResponse:
		m.closeRequested = true
	case logReplicationRunning:
		if m.ended {
			m.closeNow()
			return
		}
		m.enterStopping()
	case logReplicationStopping, logReplicationClosed:
	}
}

// failRunning closes the model after a running failure. It predicts a DoWork
// error.
func (m *replicationModel) failRunning() bool {
	m.close()
	return true
}

// modelResults are the drawn handle results for one DoWork.
type modelResults struct {
	offerAccepted bool
	offerErr      bool
	responded     bool
	responseErr   bool
	counterLive   bool
	counterValue  int64
}

// doWork folds one DoWork call. It returns true when DoWork must return an
// error. A handle result on a deadline tick counts before the deadline does.
func (m *replicationModel) doWork(nowNs int64, in modelResults) bool {
	switch m.phase {
	case logReplicationOffering:
		if in.offerErr {
			m.closeNow()
			return true
		}
		if in.offerAccepted {
			m.offerAccepted = true
			m.phase = logReplicationAwaitingResponse
			m.responseDeadline = nowNs + modelResponseNs
			return false
		}
		if nowNs >= m.offerDeadline {
			m.closeNow()
			return true
		}
	case logReplicationAwaitingResponse:
		if in.responseErr {
			m.closeNow()
			return true
		}
		if !in.responded {
			if nowNs >= m.responseDeadline {
				m.closeNow()
				return true
			}
			return false
		}
		m.latched = true
		if m.closeRequested {
			m.enterStopping()
			return false
		}
		m.phase = logReplicationRunning
		m.nextCheck = nowNs + modelCheckNs
		m.progressDeadline = nowNs + modelProgressNs
		return m.running(nowNs, in)
	case logReplicationRunning:
		return m.running(nowNs, in)
	case logReplicationStopping:
		if !m.stopAnchored {
			m.stopAnchored = true
			m.stopOfferDeadline = nowNs + modelOfferNs
		}
		if !m.stopOffered {
			if in.offerErr {
				m.closeNow()
				return true
			}
			if in.offerAccepted {
				m.stopOffered = true
				m.stopResponseDeadline = nowNs + modelResponseNs
				return false
			}
			if nowNs >= m.stopOfferDeadline {
				m.closeNow()
				return true
			}
			return false
		}
		if in.responseErr {
			m.closeNow()
			return true
		}
		if in.responded {
			m.stopResolved = true
			m.closeNow()
			return false
		}
		if nowNs >= m.stopResponseDeadline {
			m.closeNow()
			return true
		}
	case logReplicationClosed:
	}
	return false
}

// running folds one DoWork call in the running phase. The OK tick runs it too.
func (m *replicationModel) running(nowNs int64, in modelResults) bool {
	if m.deleted {
		return m.failRunning()
	}
	if m.ended && m.stopped {
		if m.position() < testReplicationStopPosition {
			return m.failRunning()
		}
		return false
	}
	if m.ended && !m.extended {
		return m.failRunning()
	}
	if nowNs >= m.nextCheck {
		m.nextCheck = nowNs + modelCheckNs
		if m.extended && in.counterLive && in.counterValue > m.position() {
			m.counter = in.counterValue
			m.progressDeadline = nowNs + modelProgressNs
		}
	}
	if nowNs >= m.progressDeadline {
		return m.failRunning()
	}
	return false
}

// signal folds one signal. The first signal with both STOP and EXTEND seen
// stores the stop position on logRecording.
func (m *replicationModel) signal(
	matching bool,
	kind archivewire.RecordingSignal,
	recordingID int64,
	position int64,
) {
	if m.phase == logReplicationClosed || !m.latched || !matching {
		return
	}
	switch kind {
	case archivewire.RecordingSignal_EXTEND:
		m.extended = true
	case archivewire.RecordingSignal_STOP:
		m.stopped = true
	case archivewire.RecordingSignal_REPLICATE_END:
		m.ended = true
	case archivewire.RecordingSignal_DELETE:
		m.deleted = true
	default:
	}
	m.recordingID = recordingID
	m.signalPosition = position
	if !m.stopStored && m.stopped && m.extended {
		m.stopStored = true
		m.storedID = m.recordingID
		m.storedPos = m.position()
	}
}

// drawSignalKind draws one signal kind. REPLICATE only moves the position. It
// takes both ends of the range and the most slots. EXTEND takes the next most.
// REPLICATE_END, DELETE, MERGE and START take one slot each.
func drawSignalKind(rt *rapid.T) archivewire.RecordingSignal {
	switch draw := rapid.IntRange(0, 21).Draw(rt, "signalKind"); {
	case draw >= 8 && draw < 13:
		return archivewire.RecordingSignal_EXTEND
	case draw >= 13 && draw < 15:
		return archivewire.RecordingSignal_SYNC
	case draw >= 15 && draw < 17:
		return archivewire.RecordingSignal_STOP
	case draw == 17:
		return archivewire.RecordingSignal_REPLICATE_END
	case draw == 18:
		return archivewire.RecordingSignal_DELETE
	case draw == 19:
		return archivewire.RecordingSignal_MERGE
	case draw == 20:
		return archivewire.RecordingSignal_START
	default:
		return archivewire.RecordingSignal_REPLICATE
	}
}

// scriptHandles applies the drawn results to the fake handles.
func scriptHandles(control *fakeReplicationControl, in modelResults) {
	var offerErr, responseErr error
	if in.offerErr {
		offerErr = errModelInject
	}
	if in.responseErr {
		responseErr = errModelInject
	}
	request, stop := control.request, control.stop
	request.offerAccepted, request.offerErr = in.offerAccepted, offerErr
	request.responded, request.responseErr = in.responded, responseErr
	stop.offerAccepted, stop.offerErr = in.offerAccepted, offerErr
	stop.responded, stop.responseErr = in.responded, responseErr
}

// requireModelState checks every observable against the model.
func requireModelState(
	rt *rapid.T,
	fx *replicationFixture,
	m *replicationModel,
	wasDone bool,
) {
	rt.Helper()

	r := fx.r
	require.Equal(rt, m.phase, r.phase)
	require.Equal(rt, m.phase == logReplicationClosed, r.IsClosed())
	done := r.IsDone()
	require.Equal(rt, m.ended && m.stopped, done)
	if wasDone {
		require.True(rt, done, "IsDone turned false")
	}
	require.Equal(rt, m.recording(), r.RecordingID())
	require.Equal(rt, m.position(), r.Position())
	require.Len(rt, fx.control.stopBuilds, m.stopBuilds)
	require.LessOrEqual(rt, len(fx.control.stopBuilds), 1)
	for _, id := range fx.control.stopBuilds {
		require.Equal(rt, testReplicationID, id)
	}

	// logRecording keeps the destination stop until the offer. From the offer
	// to the replication STOP, logRecording does not hold a stop for the
	// destination. After STOP, logRecording holds the STOP position.
	switch {
	case m.stopStored:
		require.Equal(rt, m.storedID, fx.recording.latchedRecordingID)
		require.Equal(rt, m.storedPos, fx.recording.stopPosition)
	case m.offerAccepted:
		require.NotEqual(rt, testReplicationDestinationID, fx.recording.latchedRecordingID)
	default:
		require.Equal(rt, testReplicationDestinationID, fx.recording.latchedRecordingID)
	}
}

// TestLogReplication_model_rapid drives the helper with drawn DoWork calls,
// handle results, signals, counter changes and Close calls. Each step asserts
// the helper state against replicationModel.
func TestLogReplication_model_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		control := newFakeReplicationControl()
		positions := &fakeRecordingPositions{values: map[int64]int64{}}
		recording := &logRecording{
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}
		recording.storeStopPosition(testReplicationDestinationID, testReplicationStartPosition)
		agent := consensus.NewAgent(consensus.AgentConfig{})
		agent.SetLogRecordingID(testReplicationDestinationID)
		deps := logReplicationDeps{
			Control:                     control,
			Positions:                   positions,
			NextCorrelationID:           func() int64 { return testReplicationCorrelationID },
			Agent:                       agent,
			Recording:                   recording,
			LeaderArchiveControlChannel: DefaultLeaderArchiveControlChannel,
			ProgressCheckIntervalNs:     modelCheckNs,
			ProgressTimeoutNs:           modelProgressNs,
			OfferTimeoutNs:              modelOfferNs,
			ResponseTimeoutNs:           modelResponseNs,
		}
		fx := &replicationFixture{
			control:   control,
			positions: positions,
			recording: recording,
			agent:     agent,
			r: newLogReplication(
				deps,
				testReplicationLeaderEndpoint,
				"",
				testReplicationLeaderID,
				testReplicationStopPosition,
				modelStartBuild,
			),
		}
		m := newReplicationModel(modelStartBuild)
		nowNs := modelStartBuild
		nextPosition := testReplicationStartPosition
		done := false

		doWork := func(rt *rapid.T, in modelResults) {
			if value, ok := positions.values[m.recording()]; ok {
				in.counterLive, in.counterValue = true, value
			}
			// A latched OK answers every later PollResponse.
			if m.phase == logReplicationAwaitingResponse && m.latched {
				in.responded = true
			}
			scriptHandles(control, in)
			wantErr := m.doWork(nowNs, in)
			_, err := fx.r.DoWork(nowNs)
			if wantErr {
				require.Error(rt, err)
			} else {
				require.NoError(rt, err)
			}
		}
		signal := func(matching bool, kind archivewire.RecordingSignal, recordingID int64) {
			correlationID := testReplicationID
			if !matching {
				correlationID++
			}
			m.signal(matching, kind, recordingID, nextPosition)
			fx.r.OnSignal(correlationID, recordingID, nextPosition, kind)
		}

		// Prefix: most runs start in the running state after EXTEND. The drawn
		// operations then reach the progress and stop paths.
		if rapid.IntRange(0, 4).Draw(rt, "startOffering") != 4 {
			doWork(rt, modelResults{offerAccepted: true})
			doWork(rt, modelResults{responded: true})
			if rapid.IntRange(0, 4).Draw(rt, "startUnextended") != 4 {
				signal(true, archivewire.RecordingSignal_EXTEND, testReplicationDestinationID)
			}
			requireModelState(rt, fx, m, done)
		}

		// Each rare outcome takes the top of its draw range. doWork moves the
		// clock a little. advanceClock crosses the deadlines.
		rt.Repeat(map[string]func(*rapid.T){
			"advanceClock": func(rt *rapid.T) {
				nowNs += rapid.Int64Range(1, modelMaxStepNs).Draw(rt, "advanceNs")
			},
			"doWork": func(rt *rapid.T) {
				nowNs += rapid.Int64Range(0, 2).Draw(rt, "stepNs")
				doWork(rt, modelResults{
					offerAccepted: rapid.IntRange(0, 3).Draw(rt, "offerRefused") != 3,
					offerErr:      rapid.IntRange(0, 29).Draw(rt, "offerErr") == 29,
					responded:     rapid.IntRange(0, 3).Draw(rt, "responseMissing") != 3,
					responseErr:   rapid.IntRange(0, 29).Draw(rt, "responseErr") == 29,
				})
			},
			"latch": func(rt *rapid.T) {
				// The archive poll latches the OK before the helper polls it.
				// Signals then fold before the OK tick.
				if m.phase != logReplicationAwaitingResponse || m.latched {
					return
				}
				m.latched = true
				control.request.latched = true
			},
			"signal": func(rt *rapid.T) {
				matching := rapid.IntRange(0, 4).Draw(rt, "foreign") != 4
				kind := drawSignalKind(rt)
				recordingID := testReplicationDestinationID
				if rapid.IntRange(0, 9).Draw(rt, "otherRecording") == 9 {
					recordingID = modelOtherID
				}
				nextPosition += rapid.Int64Range(0, 64).Draw(rt, "signalAdvance")
				signal(matching, kind, recordingID)
			},
			"counter": func(rt *rapid.T) {
				if rapid.IntRange(0, 3).Draw(rt, "freeCounter") == 3 {
					delete(positions.values, testReplicationDestinationID)
					return
				}
				value := nextPosition + rapid.Int64Range(-64, 64).Draw(rt, "counterOffset")
				positions.values[testReplicationDestinationID] = value
			},
			"close": func(rt *rapid.T) {
				if rapid.IntRange(0, 7).Draw(rt, "closeNow") != 7 {
					return
				}
				m.close()
				fx.r.Close()
			},
			"": func(rt *rapid.T) {
				requireModelState(rt, fx, m, done)
				done = fx.r.IsDone()
			},
		})

		// Teardown sends one blocking stop only when the replication id is
		// latched, the replication has not ended, and a stop has not resolved.
		wantBlocking := 0
		if m.latched && !m.ended && !m.stopResolved {
			wantBlocking = 1
		}
		require.Equal(rt, wantBlocking == 1, fx.r.hasUnresolvedStop())
		require.NoError(rt, fx.r.stopReplication())
		require.False(rt, fx.r.hasUnresolvedStop())
		require.Len(rt, control.blockingStops, wantBlocking)
		require.True(rt, fx.r.IsClosed())
		require.NoError(rt, fx.r.stopReplication())
		require.Len(rt, control.blockingStops, wantBlocking)
	})
}
