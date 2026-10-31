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

type asyncListOp int

const (
	asyncListOpBuild asyncListOp = iota
	asyncListOpOffer
	asyncListOpResponse
	asyncListOpDescriptor
	asyncListOpClientPoll
	asyncListOpPoll
	asyncListOpClose
)

// asyncListModelHandle mirrors one AsyncListRecording.
type asyncListModelHandle struct {
	asyncModelHandle
	handle      *AsyncListRecording
	recordingID int64
}

func (handle *asyncListModelHandle) Close() {
	handle.handle.Close()
}

type asyncListModel struct {
	asyncHandleModel[*asyncListModelHandle]
	descriptorFilter   int64
	descriptorComplete bool
	descriptorFields   codecs.RecordingDescriptorFields
}

func (m *asyncListModel) applyDescriptor(fields codecs.RecordingDescriptorFields) {
	if m.descriptorComplete || m.descriptorFilter != fields.CorrelationID {
		return
	}
	m.descriptorComplete = true
	m.descriptorFields = fields
}

// resetDescriptor mirrors RecordingDescriptorPoller.ResetAwait.
func (m *asyncListModel) resetDescriptor(correlationID int64) {
	m.descriptorFilter = correlationID
	m.descriptorComplete = false
	m.descriptorFields = codecs.RecordingDescriptorFields{}
}

// closeOpen shadows the generic closeOpen so the descriptor reset runs.
func (m *asyncListModel) closeOpen() {
	if handle, ok := m.open(); ok {
		m.closeHandle(handle)
	}
}

func (m *asyncListModel) closeHandle(handle *asyncListModelHandle) {
	wasOpen := m.asyncHandleModel.closeHandle(handle)
	if wasOpen && m.descriptorFilter == handle.correlationID {
		m.resetDescriptor(conductor.NullCorrelationID)
	}
}

func requireAsyncListModel(rt *rapid.T, m *asyncListModel, client *Client) {
	rt.Helper()

	requireAsyncControlModel(rt, &m.asyncControlModel, client.controlPoller)

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
	term := newAsyncRapidTerm(t)

	rapid.Check(t, func(rt *rapid.T) {
		pub, proxy := newTestProxy(Config{})
		client := term.newClient(aeronClient, proxy)
		model := &asyncListModel{}

		ops := drawAsyncOps(
			rt,
			asyncListOpBuild,
			asyncListOpResponse,
			asyncListOpClientPoll,
			asyncListOpClose,
		)
		for _, op := range ops {
			if len(model.handles) == 0 {
				op = asyncListOpBuild
			}

			switch op {
			case asyncListOpBuild:
				if len(model.handles) == asyncRapidMaxBuilds {
					break
				}
				model.closeOpen()
				recordingID := rapid.Int64Range(0, 3).Draw(rt, "recordingID")
				handle := client.NewAsyncListRecording(recordingID)
				model.add(rt, &asyncListModelHandle{
					asyncModelHandle: asyncModelHandle{correlationID: handle.correlationID},
					handle:           handle,
					recordingID:      recordingID,
				})
				model.resetDescriptor(handle.correlationID)

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
					ControlSessionID: drawAsyncControlSessionID(rt),
					CorrelationID:    model.drawCorrelationID(rt, "responseCorrelationIndex"),
					Code:             code,
					Version:          520,
				}
				if code == archivewire.ControlResponseCode_ERROR {
					fields.ErrorMessage = []byte("refused")
				}
				model.queueResponse(t, term, fields)

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
				model.queueEvent(t, term, encodeRecordingDescriptorWire(t, fields), func() {
					model.applyDescriptor(fields)
				})

			case asyncListOpClientPoll:
				require.Equal(rt, model.poll(), client.Poll())

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
					model.clearMatching(handle.correlationID)
				case model.latchedFor(handle.correlationID):
					require.Zero(rt, descriptor)
					if model.latchedFields.Code == archivewire.ControlResponseCode_OK {
						require.NoError(rt, err)
						require.False(rt, done, "OK must not complete")
						model.latched = false
						break
					}
					require.True(rt, done, "own non-OK response must fail the handle")
					require.Equal(rt, MapControlResponse(model.latchedFields), err)
				default:
					require.NoError(rt, err)
					require.False(rt, done)
					require.Zero(rt, descriptor)
					model.latched = false
				}

			case asyncListOpClose:
				model.closeHandle(model.drawHandle(rt))
			}

			requireAsyncListModel(rt, model, client)
		}
	})
}
