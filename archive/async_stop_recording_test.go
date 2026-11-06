//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

var asyncStopRecordingKind = asyncHandleKind[*AsyncStopRecording]{
	build: func(client *Client) *AsyncStopRecording {
		return client.NewAsyncStopRecording(
			archiveProxyTestRecordingStreamID,
			[]byte(lifecycleRecordingChannel),
		)
	},
	correlationID: func(handle *AsyncStopRecording) int64 {
		return handle.request.correlationID
	},
	relevantID:     7,
	requireOffered: requireOfferedStopRecording,
}

func newAsyncStopRecordingFixture(t *testing.T) *asyncHandleFixture[*AsyncStopRecording] {
	t.Helper()
	return newAsyncHandleFixture(t, asyncStopRecordingKind)
}

func requireOfferedStopRecording(
	t *testing.T,
	client *Client,
	handle *AsyncStopRecording,
	wire []byte,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.StopRecordingRequestFields](
		t,
		wire,
		archivewire.StopRecordingRequestSbeTemplateID,
	)
	require.Equal(t, client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopRecordingRequestSbeBlockLength,
		len(lifecycleRecordingChannel),
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStopRecording_PollResponse_matchingOKConsumed(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "empty capture is not a response")

	f.latchResponse(t, f.okFor(f.handle))

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching OK must complete")
	require.False(t, f.controlPoller.IsPollComplete(), "matching OK must be consumed")
}
