//go:build rapid && testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	asyncReplicateRapidForeignCorrelationID int64 = 1 << 40
	asyncReplicateRapidReplicationIDBase    int64 = 1 << 42
	asyncReplicateRapidReplicationIDCount         = 3
	asyncReplicateRapidMaxHandles                 = 4
)

type asyncReplicateOp int

const (
	asyncReplicateOpBuild asyncReplicateOp = iota
	asyncReplicateOpOffer
	asyncReplicateOpResponse
	asyncReplicateOpOKBurst
	asyncReplicateOpSignal
	asyncReplicateOpClientPoll
	asyncReplicateOpPollResponse
	asyncReplicateOpClose
)

var (
	asyncReplicateRapidRecordingIDs  = []int64{cnc.NullValue, 5, 6}
	asyncReplicateRapidPositions     = []int64{NullPosition, 0, 4096, 8192}
	asyncReplicateRapidOfferStatuses = []int64{0, aeron.BackPressured, aeron.NotConnected}
	asyncReplicateRapidResponseCodes = []archivewire.ControlResponseCode{
		archivewire.ControlResponseCode_OK,
		archivewire.ControlResponseCode_ERROR,
	}
)

var asyncReplicateRapidSignals = []archivewire.RecordingSignal{
	archivewire.RecordingSignal_START,
	archivewire.RecordingSignal_STOP,
	archivewire.RecordingSignal_EXTEND,
	archivewire.RecordingSignal_REPLICATE,
	archivewire.RecordingSignal_MERGE,
	archivewire.RecordingSignal_SYNC,
	archivewire.RecordingSignal_DELETE,
	archivewire.RecordingSignal_REPLICATE_END,
}

// asyncReplicateModelHandle mirrors one AsyncReplicate.
type asyncReplicateModelHandle struct {
	handle        *AsyncReplicate
	correlationID int64
	closed        bool
	responded     bool
	replicationID int64
	recordingID   int64
	position      int64
	seen          replicateFlags
}

// asyncReplicateModelEvent is one fragment written to the term and not yet
// polled.
type asyncReplicateModelEvent struct {
	response codecs.ControlResponseFields
	signal   codecs.RecordingSignalEventFields
	isSignal bool
}

// asyncReplicateModel mirrors the session poller latch and filter, the
// unpolled term, and every handle's fold.
type asyncReplicateModel struct {
	filter        int64
	latched       bool
	latchedFields codecs.ControlResponseFields
	handles       []*asyncReplicateModelHandle
	pending       []asyncReplicateModelEvent
}

func (m *asyncReplicateModel) latchedFor(correlationID int64) bool {
	return m.latched && m.latchedFields.CorrelationID == correlationID
}

// resolve mirrors resolveReplicationID: an open handle with an unknown id
// reads the id from its own latched OK.
func (m *asyncReplicateModel) resolve(handle *asyncReplicateModelHandle) {
	if handle.closed || handle.replicationID != cnc.NullValue {
		return
	}
	ownOK := m.latchedFor(handle.correlationID) &&
		m.latchedFields.Code == archivewire.ControlResponseCode_OK
	if ownOK {
		handle.replicationID = m.latchedFields.RelevantID
	}
}

// applyResponse mirrors one response delivery. The filter drops a foreign
// response. Returns false when an earlier response still holds the latch.
func (m *asyncReplicateModel) applyResponse(fields codecs.ControlResponseFields) bool {
	if m.filter != 0 && m.filter != fields.CorrelationID {
		return true
	}
	if m.latched {
		return false
	}
	m.latched = true
	m.latchedFields = fields
	return true
}

// applySignal mirrors Client.Poll dispatch to the session handler, which
// fans out to every handle. A foreign session never reaches the handler.
func (m *asyncReplicateModel) applySignal(event codecs.RecordingSignalEventFields) {
	if event.ControlSessionID != controlResponseTestControlSessionID {
		return
	}
	for _, handle := range m.handles {
		m.resolve(handle)
		if handle.closed || handle.replicationID == cnc.NullValue ||
			event.CorrelationID != handle.replicationID {
			continue
		}
		switch event.Signal {
		case archivewire.RecordingSignal_EXTEND:
			handle.seen.extended = true
		case archivewire.RecordingSignal_SYNC:
			handle.seen.synced = true
		case archivewire.RecordingSignal_MERGE:
			handle.seen.merged = true
		case archivewire.RecordingSignal_STOP:
			handle.seen.stopped = true
		case archivewire.RecordingSignal_REPLICATE_END:
			handle.seen.replicationEnded = true
		case archivewire.RecordingSignal_DELETE:
			handle.seen.recordingDeleted = true
		}
		if event.RecordingID != cnc.NullValue {
			handle.recordingID = event.RecordingID
		}
		if event.Position != NullPosition {
			handle.position = event.Position
		}
	}
}

// poll mirrors one Client.Poll. A disarmed latch clears first. The poll reads
// at most DefaultControlResponseFragmentLimit fragments. A held response stops
// the read. Returns the consumed fragment count.
func (m *asyncReplicateModel) poll() int {
	if m.latched && m.filter == 0 {
		m.latched = false
	}
	consumed := 0
	for consumed < min(len(m.pending), DefaultControlResponseFragmentLimit) {
		event := m.pending[consumed]
		if event.isSignal {
			m.applySignal(event.signal)
		} else if !m.applyResponse(event.response) {
			break
		}
		consumed++
	}
	m.pending = m.pending[consumed:]
	return consumed
}

func (m *asyncReplicateModel) drawHandle(rt *rapid.T) *asyncReplicateModelHandle {
	return m.handles[rapid.IntRange(0, len(m.handles)-1).Draw(rt, "handleIndex")]
}

// drawCorrelationID picks a handle correlation id or a foreign id.
func (m *asyncReplicateModel) drawCorrelationID(rt *rapid.T) int64 {
	index := rapid.IntRange(0, len(m.handles)).Draw(rt, "correlationIndex")
	if index == len(m.handles) {
		return asyncReplicateRapidForeignCorrelationID
	}
	return m.handles[index].correlationID
}

func drawAsyncReplicateReplicationID(rt *rapid.T, label string) int64 {
	index := rapid.IntRange(0, asyncReplicateRapidReplicationIDCount-1).Draw(rt, label)
	return asyncReplicateRapidReplicationIDBase + int64(index)
}

// drawAsyncReplicateSignal picks a signal of any kind. Its correlation id is
// a replication id or a foreign id. Its session is the client's or a foreign
// one.
func drawAsyncReplicateSignal(rt *rapid.T) codecs.RecordingSignalEventFields {
	event := codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    asyncReplicateRapidForeignCorrelationID,
		SubscriptionID:   2002,
		Signal:           rapid.SampledFrom(asyncReplicateRapidSignals).Draw(rt, "signal"),
		RecordingID:      rapid.SampledFrom(asyncReplicateRapidRecordingIDs).Draw(rt, "recordingID"),
		Position:         rapid.SampledFrom(asyncReplicateRapidPositions).Draw(rt, "position"),
	}
	if rapid.IntRange(0, 3).Draw(rt, "signalCorrelationKind") > 0 {
		event.CorrelationID = drawAsyncReplicateReplicationID(rt, "signalReplicationIndex")
	}
	if rapid.IntRange(0, 4).Draw(rt, "signalSessionKind") == 0 {
		event.ControlSessionID++
	}
	return event
}

func requireAsyncReplicateModel(rt *rapid.T, m *asyncReplicateModel, client *Client) {
	rt.Helper()

	poller := client.controlPoller
	require.Equal(rt, m.filter, poller.awaitCorrelationID)
	require.Equal(rt, m.latched, poller.IsPollComplete())
	if m.latched {
		resp, ok := poller.Response()
		require.True(rt, ok)
		require.Equal(rt, m.latchedFields.CorrelationID, resp.CorrelationID)
		require.Equal(rt, m.latchedFields.Code, resp.Code)
		require.Equal(rt, m.latchedFields.RelevantID, resp.RelevantID)
	}
	for _, handle := range m.handles {
		m.resolve(handle)
		require.Equal(rt, handle.replicationID, handle.handle.ReplicationID())
		require.Equal(rt, handle.recordingID, handle.handle.RecordingID())
		require.Equal(rt, handle.position, handle.handle.Position())
		require.Equal(rt, handle.seen, replicateFlagsOf(handle.handle))
	}
}

// TestAsyncReplicate_model_rapid asserts AsyncReplicate matches a reference
// model over random builds, offers, responses, signals, polls, and closes.
// Each handle is the session handler through a fanout.
func TestAsyncReplicate_model_rapid(t *testing.T) {
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
		model := &asyncReplicateModel{}
		pendResponse := func(fields codecs.ControlResponseFields) {
			appendUnfragmentedWireToTerm(t, termBuffer, encodeControlResponseWire(t, fields))
			model.pending = append(model.pending, asyncReplicateModelEvent{response: fields})
		}
		pendSignal := func(event codecs.RecordingSignalEventFields) {
			appendUnfragmentedWireToTerm(t, termBuffer, encodeRecordingSignalEventWire(t, event))
			model.pending = append(model.pending, asyncReplicateModelEvent{
				signal:   event,
				isSignal: true,
			})
		}
		client.signalHandler = RecordingSignalHandlerFunc(
			func(event codecs.RecordingSignalEventFields) {
				for _, handle := range model.handles {
					handle.handle.RecordingSignal(event)
				}
			},
		)

		stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
		for range stepCount {
			op := asyncReplicateOp(rapid.IntRange(
				int(asyncReplicateOpBuild),
				int(asyncReplicateOpClose),
			).Draw(rt, "op"))
			if len(model.handles) == 0 {
				op = asyncReplicateOpBuild
			}

			switch op {
			case asyncReplicateOpBuild:
				if len(model.handles) == asyncReplicateRapidMaxHandles {
					break
				}
				handle := client.NewAsyncReplicate(
					asyncReplicateTestSourceRecordingID,
					asyncReplicateTestSourceControlStreamID,
					[]byte(asyncReplicateTestSourceControlChannel),
					NewReplicationParams(),
				)
				for _, prior := range model.handles {
					require.Less(rt, prior.correlationID, handle.correlationID)
				}
				model.handles = append(model.handles, &asyncReplicateModelHandle{
					handle:        handle,
					correlationID: handle.correlationID,
					replicationID: cnc.NullValue,
					recordingID:   cnc.NullValue,
					position:      NullPosition,
				})
				model.latched = false
				model.filter = handle.correlationID

			case asyncReplicateOpOffer:
				handle := model.drawHandle(rt)
				status := rapid.SampledFrom(asyncReplicateRapidOfferStatuses).Draw(rt, "offerStatus")
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
				fields := requireProxyWireFields[codecs.ReplicateRequest2Fields](
					rt,
					pub.lastOffer(),
					archivewire.ReplicateRequest2SbeTemplateID,
				)
				require.Equal(rt, handle.correlationID, fields.CorrelationID)
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

			case asyncReplicateOpResponse:
				code := rapid.SampledFrom(asyncReplicateRapidResponseCodes).Draw(rt, "code")
				fields := codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    model.drawCorrelationID(rt),
					RelevantID:       ErrorGeneric,
					Code:             code,
					Version:          520,
				}
				switch {
				case code == archivewire.ControlResponseCode_OK:
					fields.RelevantID = drawAsyncReplicateReplicationID(rt, "okReplicationIndex")
				case rapid.Bool().Draw(rt, "errorCarriesReplicationID"):
					// A signal on this id must not fold into the refused handle.
					fields.RelevantID = drawAsyncReplicateReplicationID(rt, "errorReplicationIndex")
					fields.ErrorMessage = []byte("refused")
				default:
					fields.ErrorMessage = []byte("refused")
				}
				pendResponse(fields)

			case asyncReplicateOpOKBurst:
				// An OK for a handle, then signals on its replication id, so one
				// poll can carry both.
				handle := model.drawHandle(rt)
				fields := codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    handle.correlationID,
					RelevantID:       drawAsyncReplicateReplicationID(rt, "burstReplicationIndex"),
					Code:             archivewire.ControlResponseCode_OK,
					Version:          520,
				}
				pendResponse(fields)
				for range rapid.IntRange(1, 3).Draw(rt, "burstSignalCount") {
					event := drawAsyncReplicateSignal(rt)
					event.CorrelationID = fields.RelevantID
					pendSignal(event)
				}

			case asyncReplicateOpSignal:
				pendSignal(drawAsyncReplicateSignal(rt))

			case asyncReplicateOpClientPoll:
				require.Equal(rt, model.poll(), client.Poll())

			case asyncReplicateOpPollResponse:
				handle := model.drawHandle(rt)
				done, err := handle.handle.PollResponse()
				switch {
				case handle.closed:
					require.NoError(rt, err)
					require.False(rt, done, "closed handle must not complete")
				case handle.responded:
					require.NoError(rt, err)
					require.True(rt, done, "completed handle stays complete")
				case model.latchedFor(handle.correlationID):
					require.True(rt, done, "matching response must complete")
					if model.latchedFields.Code == archivewire.ControlResponseCode_ERROR {
						require.Equal(rt, MapControlResponse(model.latchedFields), err)
						break
					}
					require.NoError(rt, err)
					handle.responded = true
					handle.replicationID = model.latchedFields.RelevantID
					model.latched = false
					if model.filter == handle.correlationID {
						model.filter = 0
					}
				default:
					require.NoError(rt, err)
					require.False(rt, done, "only a matching response completes")
					model.latched = false
				}

			case asyncReplicateOpClose:
				handle := model.drawHandle(rt)
				handle.handle.Close()
				if handle.closed {
					break
				}
				model.resolve(handle)
				handle.closed = true
				if model.latchedFor(handle.correlationID) {
					model.latched = false
				}
				if model.filter == handle.correlationID {
					model.filter = 0
				}
			}

			requireAsyncReplicateModel(rt, model, client)
		}
	})
}
