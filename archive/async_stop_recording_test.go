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
		return handle.correlationID
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
	require.Equal(t, handle.correlationID, fields.CorrelationID)
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

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "consumed capture must not match again")
}

func TestAsyncStopRecording_PollResponse_matchingErrorMapped(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	fields := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		99,
		[]byte("boom"),
	)
	f.latchResponse(t, fields)

	want := MapControlResponse(fields)
	responded, err := f.handle.PollResponse()
	require.True(t, responded, "matching error must complete")
	require.Equal(t, want, err)
	require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")

	responded, err = f.handle.PollResponse()
	require.True(t, responded, "latched error remains visible")
	require.Equal(t, want, err)
	require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")
}

func TestAsyncStopRecording_PollResponse_discardsStaleTerminal(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	stale := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		11,
		[]byte("stale"),
	)
	stale.ControlSessionID++
	f.latchResponse(t, stale)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "stale terminal is not a match")
	require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

	f.latchResponse(t, f.okFor(f.handle))
	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching response after stale discard")
	require.False(t, f.controlPoller.IsPollComplete())
}

func TestAsyncStopRecording_PollResponse_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.latchResponse(t, f.okFor(second))

	responded, err := first.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed handle must not match")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollResponse must leave successor latch")
}

func TestAsyncStopRecording_closedDoesNotOfferOrMatch(t *testing.T) {
	var nilHandle *AsyncStopRecording
	offered, err := nilHandle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "nil TryOffer must not accept")
	responded, err := nilHandle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "nil PollResponse must not match")

	f := newAsyncStopRecordingFixture(t)
	f.handle.Close()

	offered, err = f.handle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "closed TryOffer must not accept")
	require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

	f.latchResponse(t, f.okFor(f.handle))

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed PollResponse must not match")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollResponse must not consume")
}
