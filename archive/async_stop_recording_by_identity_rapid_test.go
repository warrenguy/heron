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
	asyncStopByIdentityRapidForeignCorrelationID int64 = 1 << 40
	asyncStopByIdentityRapidMaxHandles                 = 4
)

type asyncStopByIdentityOp int

const (
	asyncStopByIdentityOpBuild asyncStopByIdentityOp = iota
	asyncStopByIdentityOpOffer
	asyncStopByIdentityOpResponse
	asyncStopByIdentityOpPoll
	asyncStopByIdentityOpClose
)

// asyncStopByIdentityModelHandle mirrors one AsyncStopRecordingByIdentity.
type asyncStopByIdentityModelHandle struct {
	handle        *AsyncStopRecordingByIdentity
	correlationID int64
	recordingID   int64
	stopped       bool
	closed        bool
}

// asyncStopByIdentityModel mirrors the capture and filter of the session
// control poller.
type asyncStopByIdentityModel struct {
	controlFilter  int64
	controlLatched bool
	controlFields  codecs.ControlResponseFields
	handles        []*asyncStopByIdentityModelHandle
}

func (m *asyncStopByIdentityModel) applyResponse(fields codecs.ControlResponseFields) {
	if m.controlLatched {
		return
	}
	if m.controlFilter != 0 && m.controlFilter != fields.CorrelationID {
		return
	}
	m.controlLatched = true
	m.controlFields = fields
}

func (m *asyncStopByIdentityModel) controlLatchedFor(correlationID int64) bool {
	return m.controlLatched && m.controlFields.CorrelationID == correlationID
}

// drawCorrelationID picks a handle correlation id or a foreign id.
func (m *asyncStopByIdentityModel) drawCorrelationID(rt *rapid.T, label string) int64 {
	index := rapid.IntRange(0, len(m.handles)).Draw(rt, label)
	if index == len(m.handles) {
		return asyncStopByIdentityRapidForeignCorrelationID
	}
	return m.handles[index].correlationID
}

func (m *asyncStopByIdentityModel) drawHandle(rt *rapid.T) *asyncStopByIdentityModelHandle {
	return m.handles[rapid.IntRange(0, len(m.handles)-1).Draw(rt, "handleIndex")]
}

func requireAsyncStopByIdentityModel(
	rt *rapid.T,
	m *asyncStopByIdentityModel,
	client *Client,
) {
	rt.Helper()

	controlPoller := client.controlPoller
	require.Equal(rt, m.controlFilter, controlPoller.awaitCorrelationID)
	require.Equal(rt, m.controlLatched, controlPoller.IsPollComplete())
	if m.controlLatched {
		resp, ok := controlPoller.Response()
		require.True(rt, ok)
		require.Equal(rt, m.controlFields.CorrelationID, resp.CorrelationID)
		require.Equal(rt, m.controlFields.Code, resp.Code)
		require.Equal(rt, m.controlFields.RelevantID, resp.RelevantID)
	}
	for _, handle := range m.handles {
		require.Equal(rt, handle.stopped, handle.handle.IsStopped())
	}
}

// TestAsyncStopRecordingByIdentity_model_rapid asserts
// AsyncStopRecordingByIdentity matches a reference model over random
// responses, polls, and closes.
func TestAsyncStopRecordingByIdentity_model_rapid(t *testing.T) {
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
		model := &asyncStopByIdentityModel{}

		stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
		for range stepCount {
			op := asyncStopByIdentityOp(rapid.IntRange(
				int(asyncStopByIdentityOpBuild),
				int(asyncStopByIdentityOpClose),
			).Draw(rt, "op"))
			if len(model.handles) == 0 {
				op = asyncStopByIdentityOpBuild
			}

			switch op {
			case asyncStopByIdentityOpBuild:
				if len(model.handles) == asyncStopByIdentityRapidMaxHandles {
					break
				}
				recordingID := rapid.Int64Range(0, 3).Draw(rt, "recordingID")
				handle := client.NewAsyncStopRecordingByIdentity(recordingID)
				for _, prior := range model.handles {
					require.Less(rt, prior.correlationID, handle.correlationID)
				}
				model.handles = append(model.handles, &asyncStopByIdentityModelHandle{
					handle:        handle,
					correlationID: handle.correlationID,
					recordingID:   recordingID,
				})
				model.controlFilter = handle.correlationID
				model.controlLatched = false

			case asyncStopByIdentityOpOffer:
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
				fields := requireProxyWireFields[codecs.StopRecordingByIdentityRequestFields](
					rt,
					pub.lastOffer(),
					archivewire.StopRecordingByIdentityRequestSbeTemplateID,
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

			case asyncStopByIdentityOpResponse:
				code := rapid.SampledFrom([]archivewire.ControlResponseCode{
					archivewire.ControlResponseCode_OK,
					archivewire.ControlResponseCode_ERROR,
				}).Draw(rt, "code")
				fields := codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    model.drawCorrelationID(rt, "responseCorrelationIndex"),
					Code:             code,
					Version:          520,
				}
				if code == archivewire.ControlResponseCode_OK {
					fields.RelevantID = rapid.Int64Range(0, 1).Draw(rt, "okRelevantID")
				} else {
					fields.RelevantID = rapid.SampledFrom([]int64{
						ErrorGeneric,
						ErrorUnknownRecording,
					}).Draw(rt, "errorRelevantID")
					fields.ErrorMessage = []byte("refused")
				}
				invokeHandleControlFragment(t, targets, encodeControlResponseWire(t, fields))
				model.applyResponse(fields)

			case asyncStopByIdentityOpPoll:
				handle := model.drawHandle(rt)
				done, err := handle.handle.PollResponse()
				switch {
				case handle.closed:
					require.NoError(rt, err)
					require.False(rt, done, "closed handle must not complete")
				case model.controlLatchedFor(handle.correlationID):
					require.True(rt, done, "matching response must complete")
					if model.controlFields.Code == archivewire.ControlResponseCode_OK {
						require.NoError(rt, err)
						handle.stopped = handle.stopped || model.controlFields.RelevantID != 0
						model.controlLatched = false
						require.False(
							rt,
							client.controlPoller.IsPollComplete(),
							"consumed OK must clear the capture",
						)
						break
					}
					require.Equal(rt, MapControlResponse(model.controlFields), err)
				default:
					require.NoError(rt, err)
					require.False(rt, done, "only a matching response completes")
					model.controlLatched = false
				}

			case asyncStopByIdentityOpClose:
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
			}

			requireAsyncStopByIdentityModel(rt, model, client)
		}
	})
}
