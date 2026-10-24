//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

type asyncStopReplicationFixture struct {
	client *Client
	pub    *stubControlPublication
	poller *ControlResponsePoller
	handle *AsyncStopReplication
}

func newAsyncStopReplicationFixture(t *testing.T) *asyncStopReplicationFixture {
	t.Helper()

	pub, proxy := newTestProxy(Config{})
	poller := NewControlResponsePoller(nil, 1)
	f := &asyncStopReplicationFixture{
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

func (f *asyncStopReplicationFixture) newHandle(
	t *testing.T,
) *AsyncStopReplication {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := f.client.NewAsyncStopReplication(asyncReplicateTestReplicationID)
	require.Equal(t, prior+1, handle.correlationID)
	require.Equal(t, handle.correlationID, f.poller.awaitCorrelationID)
	return handle
}

func (f *asyncStopReplicationFixture) latchControlResponse(
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

func (f *asyncStopReplicationFixture) responseFor(
	handle *AsyncStopReplication,
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

func (f *asyncStopReplicationFixture) okFor(
	handle *AsyncStopReplication,
	relevantID int64,
) codecs.ControlResponseFields {
	return f.responseFor(handle, archivewire.ControlResponseCode_OK, relevantID, nil)
}

func (f *asyncStopReplicationFixture) matchingOK() codecs.ControlResponseFields {
	return f.okFor(f.handle, 0)
}

func (f *asyncStopReplicationFixture) latchCompleteResponse(
	t *testing.T,
	fields codecs.ControlResponseFields,
) {
	t.Helper()

	f.latchControlResponse(t, fields)
	require.True(t, f.poller.IsPollComplete())
}

func (f *asyncStopReplicationFixture) requireOfferedStopReplication(t *testing.T) {
	t.Helper()

	wire := f.pub.lastOffer()
	fields := requireProxyWireFields[codecs.StopReplicationRequestFields](
		t,
		wire,
		archivewire.StopReplicationRequestSbeTemplateID,
	)
	require.Equal(t, f.client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, f.handle.correlationID, fields.CorrelationID)
	require.Equal(t, asyncReplicateTestReplicationID, fields.ReplicationID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopReplicationRequestSbeBlockLength,
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStopReplication_TryOffer_retriableThenAccepted(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "back_pressured", status: aeron.BackPressured},
		{name: "admin_action", status: aeron.AdminAction},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopReplicationFixture(t)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.NoError(t, err, "retriable offer must not be terminal")
			require.False(t, offered, "retriable offer must not report accepted")
			require.Len(t, f.pub.offered, 1)
			f.requireOfferedStopReplication(t)

			f.pub.offerResult = 0
			offered, err = f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered, "accepted offer must report true")
			require.Len(t, f.pub.offered, 2)
			f.requireOfferedStopReplication(t)
		})
	}
}

func TestAsyncStopReplication_TryOffer_terminal(t *testing.T) {
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
			f := newAsyncStopReplicationFixture(t)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.False(t, offered, "terminal offer must not report accepted")
			requireControlTerminal(t, err, tc.status)
			require.Len(t, f.pub.offered, 1)
			f.requireOfferedStopReplication(t)
		})
	}
}

func TestAsyncStopReplication_PollResponse_matchingOKConsumed(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "empty capture is not a response")
	require.False(t, f.handle.IsStopped(), "IsStopped must be false before completion")

	f.latchCompleteResponse(t, f.matchingOK())

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching OK must complete")
	require.True(t, f.handle.IsStopped())
	require.False(t, f.poller.IsPollComplete(), "matching OK must be consumed")

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "consumed capture must not match again")
	require.True(t, f.handle.IsStopped())
}

func TestAsyncStopReplication_PollResponse_errorResponses(t *testing.T) {
	cases := []struct {
		name          string
		relevantID    int64
		errorMessage  []byte
		wantErrorCode int32
	}{
		{
			name:          "unknown_recording",
			relevantID:    ErrorUnknownRecording,
			errorMessage:  []byte("unknown recording"),
			wantErrorCode: ErrorUnknownRecording,
		},
		{
			name:          "generic",
			relevantID:    ErrorGeneric,
			errorMessage:  []byte("boom"),
			wantErrorCode: ErrorGeneric,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopReplicationFixture(t)
			fields := f.responseFor(
				f.handle,
				archivewire.ControlResponseCode_ERROR,
				tc.relevantID,
				tc.errorMessage,
			)
			f.latchCompleteResponse(t, fields)

			want := MapControlResponse(fields)
			responded, err := f.handle.PollResponse()
			require.True(t, responded, "matching error must complete")
			require.Equal(t, want, err)
			code, ok := ExceptionCode(err)
			require.True(t, ok)
			require.Equal(t, tc.wantErrorCode, code)
			require.False(t, f.handle.IsStopped(), "error must not report stopped")
			require.True(t, f.poller.IsPollComplete(), "error capture stays latched")

			responded, err = f.handle.PollResponse()
			require.True(t, responded, "latched error remains visible")
			require.Equal(t, want, err)
			require.True(t, f.poller.IsPollComplete(), "error capture stays latched")
		})
	}
}

func TestAsyncStopReplication_PollResponse_unknownReplicationConsumed(t *testing.T) {
	cases := []struct {
		name        string
		priorOK     bool
		wantStopped bool
	}{
		{name: "alone"},
		{name: "after_ok", priorOK: true, wantStopped: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopReplicationFixture(t)
			if tc.priorOK {
				f.latchCompleteResponse(t, f.matchingOK())
				responded, err := f.handle.PollResponse()
				require.NoError(t, err)
				require.True(t, responded, "first OK must complete")
			}

			f.latchCompleteResponse(t, f.responseFor(
				f.handle,
				archivewire.ControlResponseCode_ERROR,
				ErrorUnknownReplication,
				[]byte("unknown replication"),
			))
			responded, err := f.handle.PollResponse()
			require.NoError(t, err, "unknown replication is an outcome")
			require.True(t, responded, "unknown replication must complete")
			require.False(t, f.poller.IsPollComplete(), "unknown replication must be consumed")
			require.Equal(t, tc.wantStopped, f.handle.IsStopped())

			f.handle.Close()
			require.Equal(t, tc.wantStopped, f.handle.IsStopped(), "Close must keep the result")
		})
	}
}

func TestAsyncStopReplication_PollResponse_discardsStaleTerminal(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)
	stale := f.matchingOK()
	stale.ControlSessionID++
	f.latchCompleteResponse(t, stale)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "stale terminal is not a match")
	require.False(t, f.handle.IsStopped(), "stale OK must not report stopped")
	require.False(t, f.poller.IsPollComplete(), "stale terminal must be discarded")

	f.latchCompleteResponse(t, f.matchingOK())
	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching response after stale discard")
	require.True(t, f.handle.IsStopped())
	require.False(t, f.poller.IsPollComplete())
}

func TestAsyncStopReplication_Close_disarmsCorrelationFilter(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)
	other := f.matchingOK()
	other.CorrelationID++
	f.latchControlResponse(t, other)
	require.False(t, f.poller.IsPollComplete(), "armed filter must drop other correlation")

	f.handle.Close()
	require.Equal(t, int64(0), f.poller.awaitCorrelationID)

	f.latchControlResponse(t, other)
	require.True(t, f.poller.IsPollComplete(), "disarmed poller accepts any correlation")
}

func TestAsyncStopReplication_Close_clearsLatchedMatchingTerminal(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)
	f.latchCompleteResponse(t, f.matchingOK())

	f.handle.Close()
	require.False(t, f.poller.IsPollComplete(), "Close must clear matching terminal")
	require.Equal(t, int64(0), f.poller.awaitCorrelationID)
}

func TestAsyncStopReplication_Close_lateOKLatchesUntilResetAwait(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)
	f.handle.Close()
	require.Equal(t, int64(0), f.poller.awaitCorrelationID)

	f.latchControlResponse(t, f.matchingOK())
	require.True(t, f.poller.IsPollComplete(), "late OK latches on disarmed poller")
	require.False(t, f.handle.IsStopped(), "late OK must not report stopped on closed handle")

	f.handle.Close()
	require.True(t, f.poller.IsPollComplete(), "second Close must leave residual latch")
	require.False(t, f.handle.IsStopped(), "second Close must not report stopped")

	nextCorrelationID := f.handle.correlationID + 1
	f.poller.ResetAwait(nextCorrelationID)
	require.False(t, f.poller.IsPollComplete(), "ResetAwait must clear residual latch")
	require.Equal(t, nextCorrelationID, f.poller.awaitCorrelationID)
}

func TestAsyncStopReplication_Close_keepsSuccessorFilter(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)
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

func TestAsyncStopReplication_PollResponse_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.latchCompleteResponse(t, f.okFor(second, 0))

	responded, err := first.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed handle must not match")
	require.False(t, first.IsStopped(), "closed handle must not report stopped")
	require.True(t, f.poller.IsPollComplete(), "closed PollResponse must leave successor latch")
}

func TestAsyncStopReplication_closedDoesNotOfferOrMatch(t *testing.T) {
	var nilHandle *AsyncStopReplication
	offered, err := nilHandle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "nil TryOffer must not accept")
	responded, err := nilHandle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "nil PollResponse must not match")
	require.False(t, nilHandle.IsStopped(), "nil IsStopped must be false")
	nilHandle.Close()

	f := newAsyncStopReplicationFixture(t)
	f.handle.Close()

	offered, err = f.handle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "closed TryOffer must not accept")
	require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

	f.latchCompleteResponse(t, f.matchingOK())

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed PollResponse must not match")
	require.False(t, f.handle.IsStopped(), "closed handle must not report stopped")
	require.True(t, f.poller.IsPollComplete(), "closed PollResponse must not consume")
}
