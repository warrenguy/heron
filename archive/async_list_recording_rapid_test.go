//go:build rapid && testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	asyncListRapidForeignCorrelationID int64 = 1 << 40
	asyncListRapidMaxHandles                 = 4
)

type asyncListOp int

const (
	asyncListOpBuild asyncListOp = iota
	asyncListOpOffer
	asyncListOpResponse
	asyncListOpDescriptor
	asyncListOpPoll
	asyncListOpClose
)

// asyncListModelHandle mirrors one AsyncListRecording.
type asyncListModelHandle struct {
	handle        *AsyncListRecording
	correlationID int64
	recordingID   int64
	closed        bool
}

// asyncListModel mirrors the capture and filter of both session pollers.
type asyncListModel struct {
	controlFilter      int64
	controlLatched     bool
	controlFields      codecs.ControlResponseFields
	descriptorFilter   int64
	descriptorComplete bool
	descriptorFields   codecs.RecordingDescriptorFields
	handles            []*asyncListModelHandle
}

func (m *asyncListModel) applyResponse(fields codecs.ControlResponseFields) {
	if m.controlLatched {
		return
	}
	if m.controlFilter != 0 && m.controlFilter != fields.CorrelationID {
		return
	}
	m.controlLatched = true
	m.controlFields = fields
}

func (m *asyncListModel) applyDescriptor(fields codecs.RecordingDescriptorFields) {
	if m.descriptorComplete || m.descriptorFilter != fields.CorrelationID {
		return
	}
	m.descriptorComplete = true
	m.descriptorFields = fields
}

func (m *asyncListModel) controlLatchedFor(correlationID int64) bool {
	return m.controlLatched && m.controlFields.CorrelationID == correlationID
}

// drawCorrelationID picks a handle correlation id or a foreign id.
func (m *asyncListModel) drawCorrelationID(rt *rapid.T, label string) int64 {
	index := rapid.IntRange(0, len(m.handles)).Draw(rt, label)
	if index == len(m.handles) {
		return asyncListRapidForeignCorrelationID
	}
	return m.handles[index].correlationID
}

func (m *asyncListModel) drawHandle(rt *rapid.T) *asyncListModelHandle {
	return m.handles[rapid.IntRange(0, len(m.handles)-1).Draw(rt, "handleIndex")]
}

func requireAsyncListModel(rt *rapid.T, m *asyncListModel, client *Client) {
	rt.Helper()

	controlPoller := client.controlPoller
	require.Equal(rt, m.controlFilter, controlPoller.awaitCorrelationID)
	require.Equal(rt, m.controlLatched, controlPoller.IsPollComplete())
	if m.controlLatched {
		resp, ok := controlPoller.Response()
		require.True(rt, ok)
		require.Equal(rt, m.controlFields.CorrelationID, resp.CorrelationID)
		require.Equal(rt, m.controlFields.Code, resp.Code)
	}

	descriptorPoller := client.descriptorPoller
	require.Equal(rt, m.descriptorFilter, descriptorPoller.awaitCorrelationID)
	require.Equal(rt, m.descriptorComplete, descriptorPoller.IsPollComplete())
	require.Equal(rt, m.descriptorFields, descriptorPoller.Descriptor())
	_, hasResponse := descriptorPoller.ControlResponse()
	require.False(rt, hasResponse, "session routing must keep responses off the descriptor poller")
}

// TestAsyncListRecording_model_rapid asserts AsyncListRecording matches a
// reference model over random responses, descriptors, polls, and closes.
func TestAsyncListRecording_model_rapid(t *testing.T) {
	aeronClient := aeron.NewAsyncConnectTestHarness(t).Client

	rapid.Check(t, func(rt *rapid.T) {
		pub, proxy := newTestProxy(Config{})
		client := &Client{
			controlSessionID: controlResponseTestControlSessionID,
			proxy:            proxy,
			controlPoller:    NewControlResponsePoller(nil, 1),
			descriptorPoller: NewRecordingDescriptorPoller(),
			aeronClient:      aeronClient,
		}
		targets := client.sessionPollTargets(nil)
		model := &asyncListModel{}

		stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
		for range stepCount {
			op := asyncListOp(
				rapid.IntRange(int(asyncListOpBuild), int(asyncListOpClose)).Draw(rt, "op"),
			)
			if len(model.handles) == 0 {
				op = asyncListOpBuild
			}

			switch op {
			case asyncListOpBuild:
				if len(model.handles) == asyncListRapidMaxHandles {
					break
				}
				recordingID := rapid.Int64Range(0, 3).Draw(rt, "recordingID")
				handle := client.NewAsyncListRecording(recordingID)
				for _, prior := range model.handles {
					require.Less(rt, prior.correlationID, handle.correlationID)
				}
				model.handles = append(model.handles, &asyncListModelHandle{
					handle:        handle,
					correlationID: handle.correlationID,
					recordingID:   recordingID,
				})
				model.controlFilter = handle.correlationID
				model.controlLatched = false
				model.descriptorFilter = handle.correlationID
				model.descriptorComplete = false
				model.descriptorFields = codecs.RecordingDescriptorFields{}

			case asyncListOpOffer:
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
				fields := requireProxyWireFields[codecs.ListRecordingRequestFields](
					rt,
					pub.lastOffer(),
					archivewire.ListRecordingRequestSbeTemplateID,
				)
				require.Equal(rt, handle.correlationID, fields.CorrelationID)
				require.Equal(rt, handle.recordingID, fields.RecordingID)
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

			case asyncListOpResponse:
				code := rapid.SampledFrom([]archivewire.ControlResponseCode{
					archivewire.ControlResponseCode_OK,
					archivewire.ControlResponseCode_ERROR,
					archivewire.ControlResponseCode_RECORDING_UNKNOWN,
				}).Draw(rt, "code")
				fields := codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    model.drawCorrelationID(rt, "responseCorrelationIndex"),
					Code:             code,
					Version:          520,
				}
				if code == archivewire.ControlResponseCode_ERROR {
					fields.ErrorMessage = []byte("refused")
				}
				invokeHandleControlFragment(t, targets, encodeControlResponseWire(t, fields))
				model.applyResponse(fields)

			case asyncListOpDescriptor:
				fields := codecs.RecordingDescriptorFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    model.drawCorrelationID(rt, "descriptorCorrelationIndex"),
					RecordingID:      rapid.Int64Range(0, 3).Draw(rt, "descriptorRecordingID"),
					StartPosition:    0,
					StopPosition:     rapid.Int64Range(-1, 1<<20).Draw(rt, "stopPosition"),
					StrippedChannel:  []byte("aeron:ipc"),
					OriginalChannel:  []byte("aeron:ipc?alias=log"),
					SourceIdentity:   []byte("source-identity"),
				}
				invokeHandleControlFragment(t, targets, encodeRecordingDescriptorWire(t, fields))
				model.applyDescriptor(fields)

			case asyncListOpPoll:
				handle := model.drawHandle(rt)
				done, descriptor, err := handle.handle.PollDescriptor()
				switch {
				case handle.closed:
					require.NoError(rt, err)
					require.False(rt, done, "closed handle must not complete")
					require.Zero(rt, descriptor)
				case model.descriptorComplete && model.descriptorFilter == handle.correlationID:
					require.NoError(rt, err, "descriptor must win over a response")
					require.True(rt, done, "matching descriptor must complete")
					require.Equal(rt, model.descriptorFields, descriptor)
					if model.controlLatchedFor(handle.correlationID) {
						model.controlLatched = false
					}
				case model.controlLatchedFor(handle.correlationID):
					require.Zero(rt, descriptor)
					if model.controlFields.Code == archivewire.ControlResponseCode_OK {
						require.NoError(rt, err)
						require.False(rt, done, "OK must not complete")
						model.controlLatched = false
						break
					}
					require.True(rt, done, "own non-OK response must fail the handle")
					require.Equal(rt, MapControlResponse(model.controlFields), err)
				default:
					require.NoError(rt, err)
					require.False(rt, done)
					require.Zero(rt, descriptor)
					model.controlLatched = false
				}

			case asyncListOpClose:
				handle := model.drawHandle(rt)
				handle.handle.Close()
				if handle.closed {
					break
				}
				handle.closed = true
				if model.controlLatchedFor(handle.correlationID) {
					model.controlLatched = false
				}
				if model.controlFilter == handle.correlationID {
					model.controlFilter = 0
				}
				if model.descriptorFilter == handle.correlationID {
					model.descriptorFilter = conductor.NullCorrelationID
					model.descriptorComplete = false
					model.descriptorFields = codecs.RecordingDescriptorFields{}
				}
			}

			requireAsyncListModel(rt, model, client)
		}
	})
}
