//go:build rapid && testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	asyncSignalRapidForeignCorrelationID int64 = 1 << 40
	asyncSignalRapidMaxHandles                 = 4
	asyncSignalRapidOtherRecordingID     int64 = 7
)

type asyncSignalOp int

const (
	asyncSignalOpBuild asyncSignalOp = iota
	asyncSignalOpOffer
	asyncSignalOpResponse
	asyncSignalOpSignal
	asyncSignalOpClientPoll
	asyncSignalOpPollSignal
	asyncSignalOpClose
)

type asyncSignalHandleKind int

const (
	asyncSignalHandleStart asyncSignalHandleKind = iota
	asyncSignalHandleExtend
)

type asyncSignalExtendShape int

const (
	asyncSignalExtendMatching asyncSignalExtendShape = iota
	asyncSignalExtendOtherRecording
	asyncSignalExtendUniform
)

// asyncSignalHandle is the method set shared by AsyncStartRecording and
// AsyncExtendRecording.
type asyncSignalHandle interface {
	TryOffer() (bool, error)
	PollSignal() (bool, error)
	RecordingID() int64
	Close()
}

// asyncSignalModelHandle mirrors one AsyncStartRecording or
// AsyncExtendRecording.
type asyncSignalModelHandle struct {
	kind                 asyncSignalHandleKind
	handle               asyncSignalHandle
	tracker              RecordingSignalHandler
	trackerLatched       func() bool
	correlationID        int64
	requestedRecordingID int64
	closed               bool
	signalLatched        bool
	recordingID          int64
	proven               bool
}

// asyncSignalModelEvent is one fragment written to the term and not yet
// polled.
type asyncSignalModelEvent struct {
	response codecs.ControlResponseFields
	signal   codecs.RecordingSignalEventFields
	isSignal bool
}

// asyncSignalModel mirrors the session poller latch and filter, the client
// slot, and the session handler log.
type asyncSignalModel struct {
	filter            int64
	latched           bool
	latchedFields     codecs.ControlResponseFields
	handles           []*asyncSignalModelHandle
	pending           []asyncSignalModelEvent
	sessionSignals    []codecs.RecordingSignalEventFields
	hasSessionHandler bool
}

// slot returns the index of the newest build while that handle is open, else -1.
func (m *asyncSignalModel) slot() int {
	last := len(m.handles) - 1
	if last >= 0 && !m.handles[last].closed {
		return last
	}
	return -1
}

func (m *asyncSignalModel) applyResponse(fields codecs.ControlResponseFields) {
	if m.latched {
		return
	}
	if m.filter != 0 && m.filter != fields.CorrelationID {
		return
	}
	m.latched = true
	m.latchedFields = fields
}

func (m *asyncSignalModel) applySignal(event codecs.RecordingSignalEventFields) {
	if m.hasSessionHandler {
		m.sessionSignals = append(m.sessionSignals, event)
	}
	slot := m.slot()
	if slot < 0 {
		return
	}
	handle := m.handles[slot]
	if !handle.isCompletedBy(event) || handle.signalLatched {
		return
	}
	handle.signalLatched = true
	handle.recordingID = event.RecordingID
}

// isCompletedBy reports whether event completes handle. START matches on the
// correlation id. EXTEND also needs the requested recording id.
func (handle *asyncSignalModelHandle) isCompletedBy(event codecs.RecordingSignalEventFields) bool {
	if event.CorrelationID != handle.correlationID {
		return false
	}
	switch handle.kind {
	case asyncSignalHandleExtend:
		return event.Signal == archivewire.RecordingSignal_EXTEND &&
			event.RecordingID == handle.requestedRecordingID
	default:
		return event.Signal == archivewire.RecordingSignal_START
	}
}

func newAsyncSignalModelHandle(
	client *Client,
	kind asyncSignalHandleKind,
	requestedRecordingID int64,
) *asyncSignalModelHandle {
	if kind == asyncSignalHandleExtend {
		handle := client.NewAsyncExtendRecording(
			requestedRecordingID,
			archiveProxyTestRecordingStreamID,
			archivewire.SourceLocation_LOCAL,
			true,
			[]byte(lifecycleRecordingChannel),
		)
		return &asyncSignalModelHandle{
			kind:                 kind,
			handle:               handle,
			tracker:              &handle.tracker,
			trackerLatched:       handle.tracker.Extended,
			correlationID:        handle.request.correlationID,
			requestedRecordingID: requestedRecordingID,
		}
	}
	handle := newAsyncStartRecordingForTest(client)
	return &asyncSignalModelHandle{
		kind:           kind,
		handle:         handle,
		tracker:        &handle.tracker,
		trackerLatched: handle.tracker.Started,
		correlationID:  handle.request.correlationID,
	}
}

func requireOfferedRequest(rt *rapid.T, offer []byte, handle *asyncSignalModelHandle) {
	rt.Helper()

	var correlationID int64
	switch handle.kind {
	case asyncSignalHandleExtend:
		fields := requireProxyWireFields[codecs.ExtendRecordingRequest2Fields](
			rt,
			offer,
			archivewire.ExtendRecordingRequest2SbeTemplateID,
		)
		require.Equal(rt, handle.requestedRecordingID, fields.RecordingID)
		correlationID = fields.CorrelationID
	default:
		fields := requireProxyWireFields[codecs.StartRecordingRequest2Fields](
			rt,
			offer,
			archivewire.StartRecordingRequest2SbeTemplateID,
		)
		correlationID = fields.CorrelationID
	}
	require.Equal(rt, handle.correlationID, correlationID)
}

func (m *asyncSignalModel) latchedFor(correlationID int64) bool {
	return m.latched && m.latchedFields.CorrelationID == correlationID
}

// drawCorrelationID picks a handle correlation id or a foreign id.
func (m *asyncSignalModel) drawCorrelationID(rt *rapid.T) int64 {
	index := rapid.IntRange(0, len(m.handles)).Draw(rt, "correlationIndex")
	if index == len(m.handles) {
		return asyncSignalRapidForeignCorrelationID
	}
	return m.handles[index].correlationID
}

// drawSignal picks a signal event. A draw at a start handle forces START half
// the time. A draw at an extend handle forces a matching EXTEND a third of the
// time. A third more carry EXTEND with the other recording id. Other draws
// pick the signal and recording id uniformly.
func (m *asyncSignalModel) drawSignal(rt *rapid.T) codecs.RecordingSignalEventFields {
	event := codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    asyncSignalRapidForeignCorrelationID,
		SubscriptionID:   asyncStartRecordingTestSubscriptionID,
	}
	index := rapid.IntRange(0, len(m.handles)).Draw(rt, "signalCorrelationIndex")
	if index < len(m.handles) {
		handle := m.handles[index]
		event.CorrelationID = handle.correlationID
		switch handle.kind {
		case asyncSignalHandleExtend:
			shape := rapid.SampledFrom([]asyncSignalExtendShape{
				asyncSignalExtendMatching,
				asyncSignalExtendOtherRecording,
				asyncSignalExtendUniform,
			}).Draw(rt, "extendSignalShape")
			switch shape {
			case asyncSignalExtendMatching:
				event.Signal = archivewire.RecordingSignal_EXTEND
				event.RecordingID = handle.requestedRecordingID
				return event
			case asyncSignalExtendOtherRecording:
				event.Signal = archivewire.RecordingSignal_EXTEND
				event.RecordingID = asyncSignalRapidOtherRecordingIDFor(handle.requestedRecordingID)
				return event
			}
		default:
			if rapid.Bool().Draw(rt, "startSignalMatches") {
				event.Signal = archivewire.RecordingSignal_START
				event.RecordingID = rapid.SampledFrom([]int64{
					asyncSignalRapidOtherRecordingID,
					lifecycleRecordingID,
				}).Draw(rt, "startRecordingID")
				return event
			}
		}
	}
	event.RecordingID = rapid.SampledFrom([]int64{
		0,
		asyncSignalRapidOtherRecordingID,
		lifecycleRecordingID,
	}).Draw(rt, "recordingID")
	event.Signal = rapid.SampledFrom([]archivewire.RecordingSignal{
		archivewire.RecordingSignal_START,
		archivewire.RecordingSignal_EXTEND,
		archivewire.RecordingSignal_STOP,
	}).Draw(rt, "signal")
	return event
}

// asyncSignalRapidOtherRecordingIDFor returns the requestable recording id
// other than recordingID.
func asyncSignalRapidOtherRecordingIDFor(recordingID int64) int64 {
	if recordingID == lifecycleRecordingID {
		return asyncSignalRapidOtherRecordingID
	}
	return lifecycleRecordingID
}

func (m *asyncSignalModel) drawHandle(rt *rapid.T) *asyncSignalModelHandle {
	return m.handles[rapid.IntRange(0, len(m.handles)-1).Draw(rt, "handleIndex")]
}

func requireAsyncSignalModel(
	rt *rapid.T,
	m *asyncSignalModel,
	client *Client,
	received []codecs.RecordingSignalEventFields,
) {
	rt.Helper()

	poller := client.controlPoller
	require.Equal(rt, m.filter, poller.awaitCorrelationID)
	require.Equal(rt, m.latched, poller.IsPollComplete())
	if m.latched {
		resp, ok := poller.Response()
		require.True(rt, ok)
		require.Equal(rt, m.latchedFields.CorrelationID, resp.CorrelationID)
		require.Equal(rt, m.latchedFields.Code, resp.Code)
	}

	if slot := m.slot(); slot < 0 {
		require.Nil(rt, client.pendingSignalTracker)
	} else {
		require.Same(rt, m.handles[slot].tracker, client.pendingSignalTracker)
	}

	for _, handle := range m.handles {
		require.Equal(rt, handle.signalLatched, handle.trackerLatched())
		wantRecordingID := int64(0)
		if handle.proven {
			wantRecordingID = handle.recordingID
		}
		require.Equal(rt, wantRecordingID, handle.handle.RecordingID())
	}

	require.Equal(rt, m.sessionSignals, received)
}

// TestAsyncSignalRequest_model_rapid asserts AsyncStartRecording and
// AsyncExtendRecording match a reference model over random operation
// sequences.
func TestAsyncSignalRequest_model_rapid(t *testing.T) {
	aeronClient := aeron.NewAsyncConnectTestHarness(t).Client
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	termBuffer := aeron.ImageTermBufferForHooks(image)

	rapid.Check(t, func(rt *rapid.T) {
		termBuffer.SetBytes(0, termBuffer.Capacity(), 0)
		aeron.ImageSetSubscriberPositionForHooks(image, 0)
		pub, proxy := newTestProxy(Config{})
		client := &Client{
			controlSessionID:    controlResponseTestControlSessionID,
			proxy:               proxy,
			controlPoller:       newControlResponsePollerForTest(sub),
			descriptorPoller:    NewRecordingDescriptorPoller(),
			controlSubscription: sub,
			controlAssembler:    NewFragmentAssembler(nil),
			aeronClient:         aeronClient,
		}
		model := &asyncSignalModel{}
		var received []codecs.RecordingSignalEventFields
		if rapid.Bool().Draw(rt, "sessionHandler") {
			model.hasSessionHandler = true
			client.signalHandler = RecordingSignalHandlerFunc(
				func(event codecs.RecordingSignalEventFields) {
					received = append(received, event)
				},
			)
		}

		stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
		for range stepCount {
			op := asyncSignalOp(
				rapid.IntRange(int(asyncSignalOpBuild), int(asyncSignalOpClose)).Draw(rt, "op"),
			)
			if len(model.handles) == 0 {
				op = asyncSignalOpBuild
			}

			switch op {
			case asyncSignalOpBuild:
				if len(model.handles) == asyncSignalRapidMaxHandles {
					break
				}
				kind := rapid.SampledFrom([]asyncSignalHandleKind{
					asyncSignalHandleStart,
					asyncSignalHandleExtend,
				}).Draw(rt, "kind")
				var requestedRecordingID int64
				if kind == asyncSignalHandleExtend {
					requestedRecordingID = rapid.SampledFrom([]int64{
						asyncSignalRapidOtherRecordingID,
						lifecycleRecordingID,
					}).Draw(rt, "requestedRecordingID")
				}
				handle := newAsyncSignalModelHandle(client, kind, requestedRecordingID)
				for _, prior := range model.handles {
					require.Less(rt, prior.correlationID, handle.correlationID)
				}
				model.handles = append(model.handles, handle)
				model.latched = false
				model.filter = handle.correlationID

			case asyncSignalOpOffer:
				handle := model.drawHandle(rt)
				status := rapid.SampledFrom([]int64{
					0,
					aeron.BackPressured,
					aeron.NotConnected,
				}).Draw(rt, "offerStatus")
				pub.offerResult = status
				offerCount := len(pub.offered)

				offered, err := handle.handle.TryOffer()
				if handle.closed {
					require.NoError(rt, err)
					require.False(rt, offered, "closed handle must not offer")
					require.Len(rt, pub.offered, offerCount)
					break
				}
				require.Len(rt, pub.offered, offerCount+1)
				requireOfferedRequest(rt, pub.lastOffer(), handle)
				switch status {
				case 0:
					require.NoError(rt, err)
					require.True(rt, offered)
				case aeron.BackPressured:
					require.NoError(rt, err)
					require.False(rt, offered)
				default:
					require.False(rt, offered)
					requireControlTerminal(rt, err, status)
				}

			case asyncSignalOpResponse:
				code := rapid.SampledFrom([]archivewire.ControlResponseCode{
					archivewire.ControlResponseCode_OK,
					archivewire.ControlResponseCode_ERROR,
				}).Draw(rt, "code")
				fields := codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    model.drawCorrelationID(rt),
					RelevantID:       asyncStartRecordingTestSubscriptionID,
					Code:             code,
					Version:          520,
				}
				if code == archivewire.ControlResponseCode_ERROR {
					fields.ErrorMessage = []byte("refused")
				}
				appendUnfragmentedWireToTerm(t, termBuffer, encodeControlResponseWire(t, fields))
				model.pending = append(model.pending, asyncSignalModelEvent{response: fields})

			case asyncSignalOpSignal:
				event := model.drawSignal(rt)
				appendUnfragmentedWireToTerm(t, termBuffer, encodeRecordingSignalEventWire(t, event))
				model.pending = append(model.pending, asyncSignalModelEvent{signal: event, isSignal: true})

			case asyncSignalOpClientPoll:
				// One Client.Poll reads at most DefaultControlResponseFragmentLimit
				// fragments. The rest wait for the next poll.
				batch := min(len(model.pending), DefaultControlResponseFragmentLimit)
				require.Equal(rt, batch, client.Poll())
				for _, event := range model.pending[:batch] {
					if event.isSignal {
						model.applySignal(event.signal)
					} else {
						model.applyResponse(event.response)
					}
				}
				model.pending = model.pending[batch:]

			case asyncSignalOpPollSignal:
				handle := model.drawHandle(rt)
				done, err := handle.handle.PollSignal()
				switch {
				case handle.closed:
					require.NoError(rt, err)
					require.False(rt, done, "closed handle must not complete")
				case handle.signalLatched:
					require.NoError(rt, err)
					require.True(rt, done, "latched signal must complete")
					handle.proven = true
					if model.latchedFor(handle.correlationID) {
						model.latched = false
					}
				case model.latchedFor(handle.correlationID):
					if model.latchedFields.Code == archivewire.ControlResponseCode_ERROR {
						require.True(rt, done, "own error must fail the handle")
						require.Equal(rt, MapControlResponse(model.latchedFields), err)
						break
					}
					require.NoError(rt, err)
					require.False(rt, done, "OK must not complete")
					model.latched = false
				default:
					require.NoError(rt, err)
					require.False(rt, done)
					model.latched = false
				}

			case asyncSignalOpClose:
				handle := model.drawHandle(rt)
				handle.handle.Close()
				if handle.closed {
					break
				}
				handle.closed = true
				if model.latchedFor(handle.correlationID) {
					model.latched = false
				}
				if model.filter == handle.correlationID {
					model.filter = 0
				}
			}

			requireAsyncSignalModel(rt, model, client, received)
		}
	})
}
