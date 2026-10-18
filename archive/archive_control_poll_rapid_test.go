//go:build rapid && testhooks

package archive

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	latchRapidFirstCorrelationID   int64 = 100
	latchRapidForeignCorrelationID int64 = 1 << 40
	latchRapidMaxDrainPolls              = 256
)

type latchRapidOp int

const (
	latchRapidOpResponse latchRapidOp = iota
	latchRapidOpSignal
	latchRapidOpPoll
	latchRapidOpRead
	latchRapidOpResetAwait
	latchRapidOpArmAwait
	latchRapidOpDisarm
)

type latchRapidFrameKind int

const (
	latchRapidFrameResponse latchRapidFrameKind = iota
	latchRapidFrameBegin
	latchRapidFrameSignal
)

// latchRapidFrame is one committed frame the model has not consumed. A
// response frame is an unfragmented response or the END of a fragmented one.
type latchRapidFrame struct {
	kind   latchRapidFrameKind
	index  int
	offset int
	length int
}

// latchRapidModel mirrors the demux hold, the disarmed clear and the poller
// latch over a frame queue.
type latchRapidModel struct {
	emitted    []codecs.ControlResponseFields
	queue      []latchRapidFrame
	filter     int64
	latched    bool
	latchIndex int

	latchLog      []int
	filterDropLog []int
	readLog       []int
	discardLog    []int
	releaseLog    []int
	signalLog     []int64
}

func (m *latchRapidModel) release(log *[]int) {
	*log = append(*log, m.latchIndex)
	m.releaseLog = append(m.releaseLog, m.latchIndex)
	m.latched = false
}

// poll applies one demux poll. Returns the consumed frames.
func (m *latchRapidModel) poll(fragmentLimit int) []latchRapidFrame {
	if m.latched && m.filter == 0 {
		m.release(&m.discardLog)
	}
	consumed := 0
	for consumed < fragmentLimit && consumed < len(m.queue) {
		frame := m.queue[consumed]
		if frame.kind == latchRapidFrameSignal {
			m.signalLog = append(m.signalLog, int64(frame.index))
		}
		if frame.kind == latchRapidFrameResponse {
			correlationID := m.emitted[frame.index].CorrelationID
			if m.filter != 0 && m.filter != correlationID {
				m.filterDropLog = append(m.filterDropLog, frame.index)
			} else if m.latched {
				break
			} else {
				m.latched = true
				m.latchIndex = frame.index
				m.latchLog = append(m.latchLog, frame.index)
			}
		}
		consumed++
	}
	frames := m.queue[:consumed:consumed]
	m.queue = m.queue[consumed:]
	return frames
}

func requireCaptureBytes(rt *rapid.T, want, got []byte) {
	rt.Helper()

	require.Truef(rt, bytes.Equal(want, got), "capture bytes: want %q, got %q", want, got)
}

func requireLatchRapidCapture(
	rt *rapid.T,
	want codecs.ControlResponseFields,
	capture ControlResponseCapture,
) {
	rt.Helper()

	require.Equal(rt, int32(archivewire.ControlResponseSbeTemplateID), capture.TemplateID)
	require.Equal(rt, want.ControlSessionID, capture.ControlSessionID)
	require.Equal(rt, want.CorrelationID, capture.CorrelationID)
	require.Equal(rt, want.RelevantID, capture.RelevantID)
	require.Equal(rt, want.Code, capture.Code)
	requireCaptureBytes(rt, want.ErrorMessage, capture.ErrorMessage)
	require.Nil(rt, capture.EncodedChallenge)
}

// overwriteConsumedFrames fills the payload of each consumed frame. No poll
// reads a consumed frame again. A capture that still views the term buffer
// reads the fill.
func overwriteConsumedFrames(termBuffer atomicbuffer.AtomicBuffer, frames []latchRapidFrame) {
	for _, frame := range frames {
		termBuffer.SetBytes(frame.offset+logbuffer.DataHeaderLength, frame.length, 'X')
	}
}

// appendLatchRapidFrames queues the frames written from start to the next
// write offset. A fragmented wire spans a BEGIN and an END frame.
func appendLatchRapidFrames(
	m *latchRapidModel,
	termBuffer atomicbuffer.AtomicBuffer,
	start int,
	kind latchRapidFrameKind,
	index int,
) {
	end := nextControlResponseHookTermWriteOffset(termBuffer)
	for offset := start; offset < end; {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuffer, offset))
		next := offset + logbuffer.AlignFrameLength(frameLength)
		frameKind := kind
		if kind == latchRapidFrameResponse && next < end {
			frameKind = latchRapidFrameBegin
		}
		m.queue = append(m.queue, latchRapidFrame{
			kind:   frameKind,
			index:  index,
			offset: offset,
			length: frameLength - logbuffer.DataHeaderLength,
		})
		offset = next
	}
}

// TestPollControlEvents_latchOrder_rapid asserts that the demux latch, hold,
// disarmed clear and latch copy match a frame queue model over random
// operation sequences.
func TestPollControlEvents_latchOrder_rapid(t *testing.T) {
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	termBuffer := aeron.ImageTermBufferForHooks(image)

	rapid.Check(t, func(rt *rapid.T) {
		termBuffer.SetBytes(0, termBuffer.Capacity(), 0)
		aeron.ImageSetSubscriberPositionForHooks(image, 0)

		poller := newControlResponsePollerForTest(sub)
		assembler := NewFragmentAssembler(nil)
		var signals []int64
		targets := ControlPollTargets{
			SignalHandler: RecordingSignalHandlerFunc(
				func(event codecs.RecordingSignalEventFields) {
					signals = append(signals, event.RecordingID)
				},
			),
			ControlSessionID: controlResponseTestControlSessionID,
			ResponsePoller:   poller,
		}
		model := &latchRapidModel{}
		var reads []ControlResponseCapture
		nextCorrelationID := latchRapidFirstCorrelationID
		signalCount := 0

		poll := func(fragmentLimit int) {
			frames := model.poll(fragmentLimit)
			require.Equal(
				rt,
				len(frames),
				PollControlEvents(sub, fragmentLimit, assembler, targets),
			)
			overwriteConsumedFrames(termBuffer, frames)
		}
		read := func() {
			capture, ok := poller.Capture()
			require.Equal(rt, model.latched, ok)
			if ok {
				reads = append(reads, capture)
				model.release(&model.readLog)
			}
			poller.ResetCapture()
		}
		requireStep := func() {
			require.Equal(rt, model.filter, poller.awaitCorrelationID)
			require.Equal(rt, model.latched, poller.IsPollComplete())
			if model.latched {
				capture, ok := poller.Capture()
				require.True(rt, ok)
				requireLatchRapidCapture(rt, model.emitted[model.latchIndex], capture)
			}
			require.Equal(rt, model.signalLog, signals)
			require.Len(rt, reads, len(model.readLog))
			for i, capture := range reads {
				requireLatchRapidCapture(rt, model.emitted[model.readLog[i]], capture)
			}
		}

		stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
		for range stepCount {
			op := latchRapidOp(
				rapid.IntRange(int(latchRapidOpResponse), int(latchRapidOpDisarm)).Draw(rt, "op"),
			)
			switch op {
			case latchRapidOpResponse:
				index := len(model.emitted)
				fields := codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID: rapid.SampledFrom([]int64{
						nextCorrelationID,
						latchRapidForeignCorrelationID,
					}).Draw(rt, "correlationID"),
					RelevantID: int64(index),
					Code: rapid.SampledFrom([]archivewire.ControlResponseCode{
						archivewire.ControlResponseCode_OK,
						archivewire.ControlResponseCode_ERROR,
					}).Draw(rt, "code"),
					Version: 520,
					ErrorMessage: rapid.SliceOfN(rapid.ByteRange('a', 'z'), 0, 48).
						Draw(rt, "errorMessage"),
				}
				model.emitted = append(model.emitted, fields)
				wire := encodeControlResponseWire(t, fields)
				start := nextControlResponseHookTermWriteOffset(termBuffer)
				if rapid.Bool().Draw(rt, "fragmented") {
					appendFragmentedWireToTerm(t, termBuffer, wire)
				} else {
					appendUnfragmentedWireToTerm(t, termBuffer, wire)
				}
				appendLatchRapidFrames(model, termBuffer, start, latchRapidFrameResponse, index)

			case latchRapidOpSignal:
				start := nextControlResponseHookTermWriteOffset(termBuffer)
				appendUnfragmentedWireToTerm(t, termBuffer, encodeRecordingSignalEventWire(
					t,
					codecs.RecordingSignalEventFields{
						ControlSessionID: controlResponseTestControlSessionID,
						CorrelationID:    nextCorrelationID,
						RecordingID:      int64(signalCount),
						SubscriptionID:   2002,
						Position:         3003,
						Signal:           archivewire.RecordingSignal_STOP,
					},
				))
				appendLatchRapidFrames(model, termBuffer, start, latchRapidFrameSignal, signalCount)
				signalCount++

			case latchRapidOpPoll:
				poll(rapid.IntRange(1, DefaultControlResponseFragmentLimit).Draw(rt, "fragmentLimit"))

			case latchRapidOpRead:
				read()

			case latchRapidOpResetAwait:
				nextCorrelationID++
				poller.ResetAwait(nextCorrelationID)
				if model.latched {
					model.release(&model.discardLog)
				}
				model.filter = nextCorrelationID

			case latchRapidOpArmAwait:
				nextCorrelationID++
				poller.ArmAwait(nextCorrelationID)
				model.filter = nextCorrelationID

			case latchRapidOpDisarm:
				poller.ArmAwait(0)
				model.filter = 0
			}
			requireStep()
		}

		poller.ArmAwait(0)
		model.filter = 0
		for range latchRapidMaxDrainPolls {
			if len(model.queue) == 0 && !model.latched {
				break
			}
			poll(DefaultControlResponseFragmentLimit)
			read()
			requireStep()
		}
		require.Empty(rt, model.queue, "drain must consume every frame")
		require.False(rt, model.latched)

		merged := append(append([]int(nil), model.readLog...), model.discardLog...)
		require.ElementsMatch(rt, model.latchLog, merged)
		require.Equal(rt, model.latchLog, model.releaseLog, "each latch releases once, in order")
		require.IsIncreasing(rt, model.latchLog)

		seen := make(map[int]bool, len(model.emitted))
		for _, index := range model.latchLog {
			seen[index] = true
		}
		for _, index := range model.filterDropLog {
			require.False(rt, seen[index], "response %d both latched and dropped", index)
			seen[index] = true
		}
		require.Len(rt, seen, len(model.emitted), "every response latches or drops")
	})
}
