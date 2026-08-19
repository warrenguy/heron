//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

var asyncStopRecordingTestChannel = []byte("aeron:ipc")

type asyncStopRecordingFixture struct {
	client *Client
	pub    *stubControlPublication
	poller *ControlResponsePoller
	handle *AsyncStopRecording
}

func newAsyncStopRecordingFixture(t *testing.T) *asyncStopRecordingFixture {
	t.Helper()

	pub, proxy := newTestProxy(Config{})
	poller := NewControlResponsePoller(nil, 1)
	f := &asyncStopRecordingFixture{
		client: &Client{
			controlSessionID: controlResponseTestControlSessionID,
			proxy:            proxy,
			controlPoller:    poller,
			aeronClient:      aeron.NewAsyncConnectTestHarness(t).Client,
		},
		pub:    pub,
		poller: poller,
	}
	f.handle = f.newHandle(t)
	return f
}

func (f *asyncStopRecordingFixture) newHandle(t *testing.T) *AsyncStopRecording {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := f.client.NewAsyncStopRecording(
		archiveProxyTestRecordingStreamID,
		asyncStopRecordingTestChannel,
	)
	require.Equal(t, prior+1, handle.correlationID)
	require.Equal(t, handle.correlationID, f.poller.awaitCorrelationID)
	return handle
}

func (f *asyncStopRecordingFixture) latchControlResponse(
	t *testing.T,
	fields codecs.ControlResponseFields,
) {
	t.Helper()

	invokeHandleControlResponseFragment(
		t,
		f.poller,
		encodeControlResponseWire(t, fields),
		0,
		-1,
	)
}

func (f *asyncStopRecordingFixture) responseFor(
	handle *AsyncStopRecording,
	code archivewire.ControlResponseCode,
	relevantID int64,
	errorMessage []byte,
) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    handle.correlationID,
		RelevantID:       relevantID,
		Code:             code,
		Version:          520,
		ErrorMessage:     errorMessage,
	}
}

func (f *asyncStopRecordingFixture) okFor(handle *AsyncStopRecording) codecs.ControlResponseFields {
	return f.responseFor(handle, archivewire.ControlResponseCode_OK, 7, nil)
}

func (f *asyncStopRecordingFixture) matchingOK() codecs.ControlResponseFields {
	return f.okFor(f.handle)
}

func (f *asyncStopRecordingFixture) latchCompleteResponse(
	t *testing.T,
	fields codecs.ControlResponseFields,
) {
	t.Helper()

	f.latchControlResponse(t, fields)
	require.True(t, f.poller.IsPollComplete())
}

func (f *asyncStopRecordingFixture) requireOfferedStopRecording(t *testing.T) {
	t.Helper()

	wire := f.pub.lastOffer()
	fields := requireProxyWireFields[codecs.StopRecordingRequestFields](
		t,
		wire,
		archivewire.StopRecordingRequestSbeTemplateID,
	)
	require.Equal(t, f.client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, f.handle.correlationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, asyncStopRecordingTestChannel, fields.Channel)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopRecordingRequestSbeBlockLength,
		len(asyncStopRecordingTestChannel),
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStopRecording_TryOffer_retriableThenAccepted(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "back_pressured", status: aeron.BackPressured},
		{name: "admin_action", status: aeron.AdminAction},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopRecordingFixture(t)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.NoError(t, err, "retriable offer must not be terminal")
			require.False(t, offered, "retriable offer must not report accepted")
			require.Len(t, f.pub.offered, 1)
			f.requireOfferedStopRecording(t)

			f.pub.offerResult = 0
			offered, err = f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered, "accepted offer must report true")
			require.Len(t, f.pub.offered, 2)
			f.requireOfferedStopRecording(t)
		})
	}
}

func TestAsyncStopRecording_TryOffer_terminal(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "closed", status: aeron.Closed},
		{name: "max_position_exceeded", status: aeron.MaxPositionExceeded},
		{name: "not_connected", status: aeron.NotConnected},
		{name: "unsupported_message_length", status: aeron.UnsupportedMessageLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopRecordingFixture(t)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.False(t, offered, "terminal offer must not report accepted")
			requireControlTerminal(t, err, tc.status)
			require.Len(t, f.pub.offered, 1)
			f.requireOfferedStopRecording(t)
		})
	}
}

func TestAsyncStopRecording_PollResponse_matchingOKConsumed(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "empty capture is not a response")

	f.latchCompleteResponse(t, f.matchingOK())

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching OK must complete")
	require.False(t, f.poller.IsPollComplete(), "matching OK must be consumed")

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
	f.latchCompleteResponse(t, fields)

	want := MapControlResponse(fields)
	responded, err := f.handle.PollResponse()
	require.True(t, responded, "matching error must complete")
	require.Equal(t, want, err)
	require.True(t, f.poller.IsPollComplete(), "error capture stays latched")

	responded, err = f.handle.PollResponse()
	require.True(t, responded, "latched error remains visible")
	require.Equal(t, want, err)
	require.True(t, f.poller.IsPollComplete(), "error capture stays latched")
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
	f.latchCompleteResponse(t, stale)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "stale terminal is not a match")
	require.False(t, f.poller.IsPollComplete(), "stale terminal must be discarded")

	f.latchCompleteResponse(t, f.matchingOK())
	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching response after stale discard")
	require.False(t, f.poller.IsPollComplete())
}

func TestAsyncStopRecording_Close_disarmsCorrelationFilter(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	other := f.matchingOK()
	other.CorrelationID++
	f.latchControlResponse(t, other)
	require.False(t, f.poller.IsPollComplete(), "armed filter must drop other correlation")

	f.handle.Close()
	require.Equal(t, int64(0), f.poller.awaitCorrelationID)

	f.latchControlResponse(t, other)
	require.True(t, f.poller.IsPollComplete(), "disarmed poller accepts any correlation")
}

func TestAsyncStopRecording_Close_clearsLatchedMatchingTerminal(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	f.latchCompleteResponse(t, f.matchingOK())

	f.handle.Close()
	require.False(t, f.poller.IsPollComplete(), "Close must clear matching terminal")
	require.Equal(t, int64(0), f.poller.awaitCorrelationID)
}

func TestAsyncStopRecording_Close_lateOKLatchesUntilResetAwait(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	f.handle.Close()
	require.Equal(t, int64(0), f.poller.awaitCorrelationID)

	f.latchControlResponse(t, f.matchingOK())
	require.True(t, f.poller.IsPollComplete(), "late OK latches on disarmed poller")

	f.handle.Close()
	require.True(t, f.poller.IsPollComplete(), "second Close must leave residual latch")

	nextCorrelationID := f.handle.correlationID + 1
	f.poller.ResetAwait(nextCorrelationID)
	require.False(t, f.poller.IsPollComplete(), "ResetAwait must clear residual latch")
	require.Equal(t, nextCorrelationID, f.poller.awaitCorrelationID)
}

func TestAsyncStopRecording_Close_keepsSuccessorFilter(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	first := f.handle
	second := f.newHandle(t)
	require.NotEqual(t, first.correlationID, second.correlationID)

	first.Close()
	require.Equal(
		t,
		second.correlationID,
		f.poller.awaitCorrelationID,
		"Close of prior handle must not disarm successor filter",
	)
}

func TestAsyncStopRecording_PollResponse_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncStopRecordingFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.latchCompleteResponse(t, f.okFor(second))

	responded, err := first.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed handle must not match")
	require.True(t, f.poller.IsPollComplete(), "closed PollResponse must leave successor latch")
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

	f.latchCompleteResponse(t, f.matchingOK())

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed PollResponse must not match")
	require.True(t, f.poller.IsPollComplete(), "closed PollResponse must not consume")
}
