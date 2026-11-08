//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
)

type asyncHandle interface {
	TryOffer() (bool, error)
	Close()
}

type asyncResponseHandle interface {
	asyncHandle
	PollResponse() (bool, error)
}

type asyncHandleKind[H asyncHandle] struct {
	build           func(client *Client) H
	correlationID   func(handle H) int64
	relevantID      int64
	descriptorArmed bool
	slotTracker     func(handle H) RecordingSignalHandler
	requireOffered  func(t *testing.T, client *Client, handle H, wire []byte)
	isStopped       func(handle H) bool
}

type asyncHandleFixture[H asyncHandle] struct {
	harness          *lifecycleHarness
	client           *Client
	pub              *stubControlPublication
	controlPoller    *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	kind             asyncHandleKind[H]
	handle           H
}

func newAsyncHandleFixture[H asyncHandle](
	t *testing.T,
	kind asyncHandleKind[H],
) *asyncHandleFixture[H] {
	t.Helper()

	harness := newLifecycleHarness(t)
	client := harness.newClient()
	f := &asyncHandleFixture[H]{
		harness:          harness,
		client:           client,
		pub:              harness.pub,
		controlPoller:    client.controlPoller,
		descriptorPoller: client.descriptorPoller,
		kind:             kind,
	}
	f.handle = f.newHandle(t)
	return f
}

func (f *asyncHandleFixture[H]) newHandle(t *testing.T) H {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := f.kind.build(f.client)
	id := f.kind.correlationID(handle)
	require.Equal(t, prior+1, id)
	require.Equal(t, id, f.controlPoller.awaitCorrelationID)
	if f.kind.descriptorArmed {
		require.Equal(t, id, f.descriptorPoller.awaitCorrelationID)
	}
	if f.kind.slotTracker != nil {
		require.Same(t, f.kind.slotTracker(handle), f.client.pendingSignalTracker)
	}
	return handle
}

func (f *asyncHandleFixture[H]) deliver(t *testing.T, wires ...[]byte) {
	t.Helper()

	for _, wire := range wires {
		appendUnfragmentedWireToTerm(t, f.harness.termBuffer, wire)
	}
	require.Equal(t, len(wires), f.client.Poll())
}

func (f *asyncHandleFixture[H]) responseFor(
	handle H,
	code archivewire.ControlResponseCode,
	relevantID int64,
	errorMessage []byte,
) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    f.kind.correlationID(handle),
		RelevantID:       relevantID,
		Code:             code,
		Version:          520,
		ErrorMessage:     errorMessage,
	}
}

func (f *asyncHandleFixture[H]) okFor(handle H) codecs.ControlResponseFields {
	return f.okWithRelevantID(handle, f.kind.relevantID)
}

func (f *asyncHandleFixture[H]) okWithRelevantID(
	handle H,
	relevantID int64,
) codecs.ControlResponseFields {
	return f.responseFor(handle, archivewire.ControlResponseCode_OK, relevantID, nil)
}

func (f *asyncHandleFixture[H]) okWire(t *testing.T, handle H) []byte {
	t.Helper()

	return encodeControlResponseWire(t, f.okFor(handle))
}

func (f *asyncHandleFixture[H]) errorFor(
	handle H,
	errorMessage string,
) codecs.ControlResponseFields {
	return f.responseFor(
		handle,
		archivewire.ControlResponseCode_ERROR,
		f.kind.relevantID,
		[]byte(errorMessage),
	)
}

func (f *asyncHandleFixture[H]) errorWire(t *testing.T, handle H, errorMessage string) []byte {
	t.Helper()

	return encodeControlResponseWire(t, f.errorFor(handle, errorMessage))
}

func (f *asyncHandleFixture[H]) deliverResponse(t *testing.T, fields codecs.ControlResponseFields) {
	t.Helper()

	f.deliver(t, encodeControlResponseWire(t, fields))
}

func (f *asyncHandleFixture[H]) latchResponse(t *testing.T, fields codecs.ControlResponseFields) {
	t.Helper()

	f.deliverResponse(t, fields)
	require.True(t, f.controlPoller.IsPollComplete())
}

func (f *asyncHandleFixture[H]) requireOffered(t *testing.T) {
	t.Helper()

	f.kind.requireOffered(t, f.client, f.handle, f.pub.lastOffer())
}

func (f *asyncHandleFixture[H]) requireStopped(t *testing.T, handle H, want bool, message string) {
	t.Helper()

	if f.kind.isStopped != nil {
		require.Equal(t, want, f.kind.isStopped(handle), message)
	}
}

func TestAsyncHandle_TryOffer(t *testing.T) {
	t.Run("stop_recording", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncStopRecordingKind)
	})
	t.Run("list_recording", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncListRecordingKind)
	})
	t.Run("stop_recording_by_identity", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncStopRecordingByIdentityKind)
	})
	t.Run("start_recording", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncStartRecordingKind)
	})
	t.Run("extend_recording", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncExtendRecordingKind)
	})
	t.Run("stop_replication", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncStopReplicationKind)
	})
	t.Run("replicate", func(t *testing.T) {
		testAsyncHandleTryOffer(t, asyncReplicateKind)
	})
}

func TestAsyncHandle_Close(t *testing.T) {
	t.Run("stop_recording", func(t *testing.T) {
		testAsyncHandleClose(t, asyncStopRecordingKind)
	})
	t.Run("list_recording", func(t *testing.T) {
		testAsyncHandleClose(t, asyncListRecordingKind)
	})
	t.Run("stop_recording_by_identity", func(t *testing.T) {
		testAsyncHandleClose(t, asyncStopRecordingByIdentityKind)
	})
	t.Run("start_recording", func(t *testing.T) {
		testAsyncHandleClose(t, asyncStartRecordingKind)
	})
	t.Run("extend_recording", func(t *testing.T) {
		testAsyncHandleClose(t, asyncExtendRecordingKind)
	})
	t.Run("stop_replication", func(t *testing.T) {
		testAsyncHandleClose(t, asyncStopReplicationKind)
	})
	t.Run("replicate", func(t *testing.T) {
		testAsyncHandleClose(t, asyncReplicateKind)
	})
}

func TestAsyncHandle_PollResponse_responded(t *testing.T) {
	t.Run("stop_recording", func(t *testing.T) {
		testAsyncHandleResponded(t, asyncStopRecordingKind)
	})
	t.Run("stop_recording_by_identity", func(t *testing.T) {
		testAsyncHandleResponded(t, asyncStopRecordingByIdentityKind)
	})
	t.Run("stop_replication", func(t *testing.T) {
		testAsyncHandleResponded(t, asyncStopReplicationKind)
	})
	t.Run("replicate", func(t *testing.T) {
		testAsyncHandleResponded(t, asyncReplicateKind)
	})
}

func TestAsyncHandle_PollResponse(t *testing.T) {
	t.Run("stop_recording", func(t *testing.T) {
		testAsyncHandlePollResponse(t, asyncStopRecordingKind)
	})
	t.Run("stop_recording_by_identity", func(t *testing.T) {
		testAsyncHandlePollResponse(t, asyncStopRecordingByIdentityKind)
	})
	t.Run("stop_replication", func(t *testing.T) {
		testAsyncHandlePollResponse(t, asyncStopReplicationKind)
	})
}

// testAsyncHandlePollResponse asserts the error, stale, closed, and nil rules
// that the stop handles share.
func testAsyncHandlePollResponse[H asyncResponseHandle](t *testing.T, kind asyncHandleKind[H]) {
	t.Helper()

	errorResponses := []struct {
		name      string
		errorCode int32
	}{
		{name: "unknown_recording", errorCode: ErrorUnknownRecording},
		{name: "generic", errorCode: ErrorGeneric},
	}
	for _, tc := range errorResponses {
		t.Run("error_response/"+tc.name, func(t *testing.T) {
			f := newAsyncHandleFixture(t, kind)
			fields := f.responseFor(
				f.handle,
				archivewire.ControlResponseCode_ERROR,
				int64(tc.errorCode),
				[]byte("boom"),
			)
			f.latchResponse(t, fields)

			want := MapControlResponse(fields)
			responded, err := f.handle.PollResponse()
			require.True(t, responded, "matching error must complete")
			require.Equal(t, want, err)
			code, ok := ExceptionCode(err)
			require.True(t, ok)
			require.Equal(t, tc.errorCode, code)
			f.requireStopped(t, f.handle, false, "error must not report stopped")
			require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")

			responded, err = f.handle.PollResponse()
			require.True(t, responded, "latched error remains visible")
			require.Equal(t, want, err)
			require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")
		})
	}

	staleResponses := []struct {
		name         string
		code         archivewire.ControlResponseCode
		errorMessage []byte
	}{
		{name: "ok", code: archivewire.ControlResponseCode_OK},
		{name: "error", code: archivewire.ControlResponseCode_ERROR, errorMessage: []byte("stale")},
	}
	for _, tc := range staleResponses {
		t.Run("discards_stale_terminal/"+tc.name, func(t *testing.T) {
			f := newAsyncHandleFixture(t, kind)
			stale := f.responseFor(f.handle, tc.code, kind.relevantID, tc.errorMessage)
			stale.ControlSessionID++
			f.latchResponse(t, stale)

			responded, err := f.handle.PollResponse()
			require.NoError(t, err)
			require.False(t, responded, "stale terminal is not a match")
			f.requireStopped(t, f.handle, false, "stale terminal must not report stopped")
			require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

			f.latchResponse(t, f.okFor(f.handle))
			responded, err = f.handle.PollResponse()
			require.NoError(t, err)
			require.True(t, responded, "matching response after stale discard")
			f.requireStopped(t, f.handle, true, "matching OK must report stopped")
			require.False(t, f.controlPoller.IsPollComplete())
		})
	}

	t.Run("closed_does_not_touch_successor", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		first := f.handle
		first.Close()

		second := f.newHandle(t)
		f.latchResponse(t, f.okFor(second))

		responded, err := first.PollResponse()
		require.NoError(t, err)
		require.False(t, responded, "closed handle must not match")
		f.requireStopped(t, first, false, "closed handle must not report stopped")
		require.True(
			t,
			f.controlPoller.IsPollComplete(),
			"closed PollResponse must leave successor latch",
		)
	})

	t.Run("closed_does_not_offer_or_match", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		f.handle.Close()

		offered, err := f.handle.TryOffer()
		require.NoError(t, err)
		require.False(t, offered, "closed TryOffer must not accept")
		require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

		f.latchResponse(t, f.okFor(f.handle))

		responded, err := f.handle.PollResponse()
		require.NoError(t, err)
		require.False(t, responded, "closed PollResponse must not match")
		f.requireStopped(t, f.handle, false, "closed handle must not report stopped")
		require.True(t, f.controlPoller.IsPollComplete(), "closed PollResponse must not consume")
	})

	t.Run("nil_handle", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		var nilHandle H

		offered, err := nilHandle.TryOffer()
		require.NoError(t, err)
		require.False(t, offered, "nil TryOffer must not accept")
		responded, err := nilHandle.PollResponse()
		require.NoError(t, err)
		require.False(t, responded, "nil PollResponse must not match")
		f.requireStopped(t, nilHandle, false, "nil IsStopped must be false")
		nilHandle.Close()
	})
}

func testAsyncHandleResponded[H asyncResponseHandle](t *testing.T, kind asyncHandleKind[H]) {
	t.Helper()

	t.Run("try_offer_does_not_offer", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		offered, err := f.handle.TryOffer()
		require.NoError(t, err)
		require.True(t, offered)

		f.latchResponse(t, f.okFor(f.handle))
		responded, err := f.handle.PollResponse()
		require.NoError(t, err)
		require.True(t, responded, "matching OK must complete")

		offered, err = f.handle.TryOffer()
		require.NoError(t, err)
		require.False(t, offered, "responded TryOffer must not accept")
		require.Len(t, f.pub.offered, 1, "responded TryOffer must not offer")
	})

	t.Run("poll_response_keeps_successor_latch", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		first := f.handle
		f.latchResponse(t, f.okFor(first))
		responded, err := first.PollResponse()
		require.NoError(t, err)
		require.True(t, responded, "matching OK must complete")

		// Phase: a successor OK latches while the responded handle stays open.
		second := f.newHandle(t)
		f.latchResponse(t, f.okFor(second))

		responded, err = first.PollResponse()
		require.NoError(t, err)
		require.True(t, responded, "responded handle stays complete")
		resp, ok := f.controlPoller.Response()
		require.True(t, ok, "responded PollResponse must not clear the successor latch")
		require.Equal(t, kind.correlationID(second), resp.CorrelationID)

		// Phase: the successor still reads its own OK.
		responded, err = second.PollResponse()
		require.NoError(t, err)
		require.True(t, responded, "successor OK must complete")
	})
}

func testAsyncHandleTryOffer[H asyncHandle](t *testing.T, kind asyncHandleKind[H]) {
	t.Helper()

	retriable := []struct {
		name   string
		status int64
	}{
		{name: "back_pressured", status: aeron.BackPressured},
		{name: "admin_action", status: aeron.AdminAction},
	}
	for _, tc := range retriable {
		t.Run("retriable_then_accepted/"+tc.name, func(t *testing.T) {
			f := newAsyncHandleFixture(t, kind)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.NoError(t, err, "retriable offer must not be terminal")
			require.False(t, offered, "retriable offer must not report accepted")
			require.Len(t, f.pub.offered, 1)
			f.requireOffered(t)

			f.pub.offerResult = 0
			offered, err = f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered, "accepted offer must report true")
			require.Len(t, f.pub.offered, 2)
			f.requireOffered(t)
		})
	}

	terminal := []struct {
		name   string
		status int64
	}{
		{name: "closed", status: aeron.Closed},
		{name: "max_position_exceeded", status: aeron.MaxPositionExceeded},
		{name: "not_connected", status: aeron.NotConnected},
		{name: "unsupported_message_length", status: aeron.UnsupportedMessageLength},
	}
	for _, tc := range terminal {
		t.Run("terminal/"+tc.name, func(t *testing.T) {
			f := newAsyncHandleFixture(t, kind)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.False(t, offered, "terminal offer must not report accepted")
			requireControlTerminal(t, err, tc.status)
			require.Len(t, f.pub.offered, 1)
			f.requireOffered(t)
		})
	}
}

func testAsyncHandleClose[H asyncHandle](t *testing.T, kind asyncHandleKind[H]) {
	t.Helper()

	t.Run("disarms_correlation_filter", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		other := f.okFor(f.handle)
		other.CorrelationID++
		otherWire := encodeControlResponseWire(t, other)

		f.deliver(t, otherWire)
		require.False(t, f.controlPoller.IsPollComplete(), "armed filter must drop other correlation")

		f.handle.Close()
		require.Equal(t, int64(0), f.controlPoller.awaitCorrelationID)

		f.deliver(t, otherWire)
		require.True(t, f.controlPoller.IsPollComplete(), "disarmed poller accepts any correlation")
	})

	t.Run("clears_latched_matching_terminal", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		f.latchResponse(t, f.okFor(f.handle))

		f.handle.Close()
		require.False(t, f.controlPoller.IsPollComplete(), "Close must clear matching terminal")
		require.Equal(t, int64(0), f.controlPoller.awaitCorrelationID)
		if kind.slotTracker != nil {
			require.Nil(t, f.client.pendingSignalTracker, "Close must empty the slot")
		}
	})

	t.Run("late_ok_latches_until_next_build", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		f.handle.Close()
		require.Equal(t, int64(0), f.controlPoller.awaitCorrelationID)

		f.deliverResponse(t, f.okFor(f.handle))
		require.True(t, f.controlPoller.IsPollComplete(), "late OK latches on disarmed poller")

		f.handle.Close()
		require.True(t, f.controlPoller.IsPollComplete(), "second Close must leave residual latch")

		f.newHandle(t)
		require.False(t, f.controlPoller.IsPollComplete(), "next build must clear residual latch")
	})

	t.Run("keeps_successor_filter", func(t *testing.T) {
		f := newAsyncHandleFixture(t, kind)
		first := f.handle
		second := f.newHandle(t)
		require.NotEqual(t, kind.correlationID(first), kind.correlationID(second))

		first.Close()
		require.Equal(
			t,
			kind.correlationID(second),
			f.controlPoller.awaitCorrelationID,
			"Close of prior handle must not disarm successor filter",
		)
		if kind.slotTracker != nil {
			require.Same(
				t,
				kind.slotTracker(second),
				f.client.pendingSignalTracker,
				"Close of prior handle must not empty successor slot",
			)
		}
	})
}
