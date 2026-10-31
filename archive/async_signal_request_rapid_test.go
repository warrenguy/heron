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

const asyncSignalRapidOtherRecordingID int64 = 7

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
	asyncModelHandle
	kind                 asyncSignalHandleKind
	handle               asyncSignalHandle
	tracker              RecordingSignalHandler
	trackerLatched       func() bool
	requestedRecordingID int64
	signalLatched        bool
	recordingID          int64
	proven               bool
}

func (handle *asyncSignalModelHandle) Close() {
	handle.handle.Close()
}

// asyncSignalModel mirrors the client slot and the session handler log.
type asyncSignalModel struct {
	asyncHandleModel[*asyncSignalModelHandle]
	sessionSignals    []codecs.RecordingSignalEventFields
	hasSessionHandler bool
}

func (m *asyncSignalModel) applySignal(event codecs.RecordingSignalEventFields) {
	if m.hasSessionHandler {
		m.sessionSignals = append(m.sessionSignals, event)
	}
	handle, ok := m.open()
	if !ok || !handle.isCompletedBy(event) || handle.signalLatched {
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
			asyncModelHandle:     asyncModelHandle{correlationID: handle.request.correlationID},
			kind:                 kind,
			handle:               handle,
			tracker:              &handle.tracker,
			trackerLatched:       handle.tracker.Extended,
			requestedRecordingID: requestedRecordingID,
		}
	}
	handle := newAsyncStartRecordingForTest(client)
	return &asyncSignalModelHandle{
		asyncModelHandle: asyncModelHandle{correlationID: handle.request.correlationID},
		kind:             kind,
		handle:           handle,
		tracker:          &handle.tracker,
		trackerLatched:   handle.tracker.Started,
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

// drawSignal picks a signal event. A draw at a start handle forces START half
// the time. A draw at an extend handle forces a matching EXTEND a third of the
// time. A third more carry EXTEND with the other recording id. Other draws
// pick the signal and recording id uniformly.
func (m *asyncSignalModel) drawSignal(rt *rapid.T) codecs.RecordingSignalEventFields {
	event := codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    asyncRapidForeignCorrelationID,
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

func requireAsyncSignalModel(
	rt *rapid.T,
	m *asyncSignalModel,
	client *Client,
	received []codecs.RecordingSignalEventFields,
) {
	rt.Helper()

	requireAsyncControlModel(rt, &m.asyncControlModel, client.controlPoller)

	open, hasOpen := m.open()
	if hasOpen {
		require.Same(rt, open.tracker, client.pendingSignalTracker)
	} else {
		require.Nil(rt, client.pendingSignalTracker)
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
	term := newAsyncRapidTerm(t)

	rapid.Check(t, func(rt *rapid.T) {
		pub, proxy := newTestProxy(Config{})
		client := term.newClient(aeronClient, proxy)
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

		ops := drawAsyncOps(
			rt,
			asyncSignalOpBuild,
			asyncSignalOpResponse,
			asyncSignalOpClientPoll,
			asyncSignalOpClose,
		)
		for _, op := range ops {
			if len(model.handles) == 0 {
				op = asyncSignalOpBuild
			}

			switch op {
			case asyncSignalOpBuild:
				if len(model.handles) == asyncRapidMaxBuilds {
					break
				}
				model.closeOpen()
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
				model.add(rt, newAsyncSignalModelHandle(client, kind, requestedRecordingID))

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
					ControlSessionID: drawAsyncControlSessionID(rt),
					CorrelationID:    model.drawCorrelationID(rt, "correlationIndex"),
					RelevantID:       asyncStartRecordingTestSubscriptionID,
					Code:             code,
					Version:          520,
				}
				if code == archivewire.ControlResponseCode_ERROR {
					fields.ErrorMessage = []byte("refused")
				}
				model.queueResponse(t, term, fields)

			case asyncSignalOpSignal:
				event := model.drawSignal(rt)
				model.queueEvent(t, term, encodeRecordingSignalEventWire(t, event), func() {
					model.applySignal(event)
				})

			case asyncSignalOpClientPoll:
				require.Equal(rt, model.poll(), client.Poll())

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
					model.clearMatching(handle.correlationID)
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
				model.closeHandle(model.drawHandle(rt))
			}

			requireAsyncSignalModel(rt, model, client, received)
		}
	})
}
