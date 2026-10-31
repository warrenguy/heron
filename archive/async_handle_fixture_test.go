//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

type asyncHandle interface {
	TryOffer() (bool, error)
	Close()
}

type asyncHandleKind[H asyncHandle] struct {
	build           func(client *Client) H
	correlationID   func(handle H) int64
	relevantID      int64
	descriptorArmed bool
	slotTracker     func(handle H) RecordingSignalHandler
	requireOffered  func(t *testing.T, client *Client, handle H, wire []byte)
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
