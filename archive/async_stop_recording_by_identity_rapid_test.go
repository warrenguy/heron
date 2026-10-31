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

type asyncStopByIdentityOp int

const (
	asyncStopByIdentityOpBuild asyncStopByIdentityOp = iota
	asyncStopByIdentityOpOffer
	asyncStopByIdentityOpResponse
	asyncStopByIdentityOpClientPoll
	asyncStopByIdentityOpPoll
	asyncStopByIdentityOpClose
)

// asyncStopByIdentityModelHandle mirrors one AsyncStopRecordingByIdentity.
type asyncStopByIdentityModelHandle struct {
	asyncModelHandle
	handle      *AsyncStopRecordingByIdentity
	recordingID int64
	stopped     bool
}

func (handle *asyncStopByIdentityModelHandle) Close() {
	handle.handle.Close()
}

type asyncStopByIdentityModel = asyncHandleModel[*asyncStopByIdentityModelHandle]

func requireAsyncStopByIdentityModel(
	rt *rapid.T,
	m *asyncStopByIdentityModel,
	client *Client,
) {
	rt.Helper()

	requireAsyncControlModel(rt, &m.asyncControlModel, client.controlPoller)
	for _, handle := range m.handles {
		require.Equal(rt, handle.stopped, handle.handle.IsStopped())
	}
}

// TestAsyncStopRecordingByIdentity_model_rapid asserts
// AsyncStopRecordingByIdentity matches a reference model over random
// responses, polls, and closes.
func TestAsyncStopRecordingByIdentity_model_rapid(t *testing.T) {
	aeronClient := aeron.NewAsyncConnectTestHarness(t).Client
	term := newAsyncRapidTerm(t)

	rapid.Check(t, func(rt *rapid.T) {
		pub, proxy := newTestProxy(Config{})
		client := term.newClient(aeronClient, proxy)
		model := &asyncStopByIdentityModel{}

		ops := drawAsyncOps(
			rt,
			asyncStopByIdentityOpBuild,
			asyncStopByIdentityOpResponse,
			asyncStopByIdentityOpClientPoll,
			asyncStopByIdentityOpClose,
		)
		for _, op := range ops {
			if len(model.handles) == 0 {
				op = asyncStopByIdentityOpBuild
			}

			switch op {
			case asyncStopByIdentityOpBuild:
				if len(model.handles) == asyncRapidMaxBuilds {
					break
				}
				model.closeOpen()
				recordingID := rapid.Int64Range(0, 3).Draw(rt, "recordingID")
				handle := client.NewAsyncStopRecordingByIdentity(recordingID)
				model.add(rt, &asyncStopByIdentityModelHandle{
					asyncModelHandle: asyncModelHandle{correlationID: handle.correlationID},
					handle:           handle,
					recordingID:      recordingID,
				})

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
					ControlSessionID: drawAsyncControlSessionID(rt),
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
				model.queueResponse(t, term, fields)

			case asyncStopByIdentityOpClientPoll:
				require.Equal(rt, model.poll(), client.Poll())

			case asyncStopByIdentityOpPoll:
				handle := model.drawHandle(rt)
				done, err := handle.handle.PollResponse()
				switch {
				case handle.closed:
					require.NoError(rt, err)
					require.False(rt, done, "closed handle must not complete")
				case model.latchedFor(handle.correlationID):
					require.True(rt, done, "matching response must complete")
					if model.latchedFields.Code == archivewire.ControlResponseCode_OK {
						require.NoError(rt, err)
						handle.stopped = handle.stopped || model.latchedFields.RelevantID != 0
						model.latched = false
						require.False(
							rt,
							client.controlPoller.IsPollComplete(),
							"consumed OK must clear the capture",
						)
						break
					}
					require.Equal(rt, MapControlResponse(model.latchedFields), err)
				default:
					require.NoError(rt, err)
					require.False(rt, done, "only a matching response completes")
					model.latched = false
				}

			case asyncStopByIdentityOpClose:
				model.closeHandle(model.drawHandle(rt))
			}

			requireAsyncStopByIdentityModel(rt, model, client)
		}
	})
}
