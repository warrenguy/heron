//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncStartRecordingTestSubscriptionID int64 = 2002

type asyncStartRecordingFixture struct {
	h      *lifecycleHarness
	client *Client
	handle *AsyncStartRecording
}

func newAsyncStartRecordingFixture(t *testing.T) *asyncStartRecordingFixture {
	t.Helper()

	h := newLifecycleHarness(t)
	f := &asyncStartRecordingFixture{
		h:      h,
		client: h.newClient(),
	}
	f.handle = f.newHandle(t)
	return f
}

func (f *asyncStartRecordingFixture) newHandle(t *testing.T) *AsyncStartRecording {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := newAsyncStartRecordingForTest(f.client)
	require.Equal(t, prior+1, handle.request.correlationID)
	require.Equal(t, handle.request.correlationID, f.h.poller.awaitCorrelationID)
	return handle
}

// deliver appends wires to the control-response term, then runs one
// Client.Poll over them.
func (f *asyncStartRecordingFixture) deliver(t *testing.T, wires ...[]byte) {
	t.Helper()

	for _, wire := range wires {
		appendUnfragmentedWireToTerm(t, f.h.termBuffer, wire)
	}
	require.Equal(t, len(wires), f.client.Poll())
}

func (f *asyncStartRecordingFixture) responseFor(
	handle *AsyncStartRecording,
	code archivewire.ControlResponseCode,
	errorMessage []byte,
) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    handle.request.correlationID,
		RelevantID:       asyncStartRecordingTestSubscriptionID,
		Code:             code,
		Version:          520,
		ErrorMessage:     errorMessage,
	}
}

func (f *asyncStartRecordingFixture) okWire(t *testing.T, handle *AsyncStartRecording) []byte {
	t.Helper()
	return encodeControlResponseWire(
		t,
		f.responseFor(handle, archivewire.ControlResponseCode_OK, nil),
	)
}

// newAsyncStartRecordingForTest builds a handle with autoStop true on the
// lifecycle stream and channel.
func newAsyncStartRecordingForTest(client *Client) *AsyncStartRecording {
	return client.NewAsyncStartRecording(
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		true,
		[]byte(lifecycleRecordingChannel),
	)
}

// startSignalWireFor encodes the START signal that completes handle.
func startSignalWireFor(
	t *testing.T,
	handle *AsyncStartRecording,
	recordingID int64,
) []byte {
	t.Helper()
	return encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: handle.request.client.controlSessionID,
		CorrelationID:    handle.request.correlationID,
		RecordingID:      recordingID,
		SubscriptionID:   asyncStartRecordingTestSubscriptionID,
		Signal:           archivewire.RecordingSignal_START,
	})
}

func requirePollSignal(
	t *testing.T,
	handle *AsyncStartRecording,
	wantDone bool,
	msgAndArgs ...any,
) {
	t.Helper()

	done, err := handle.PollSignal()
	require.NoError(t, err, msgAndArgs...)
	require.Equal(t, wantDone, done, msgAndArgs...)
}

func TestAsyncStartRecording_PollSignal_startThroughClientPoll(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	offered, err := f.handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must consume the matching OK")
	require.Zero(t, f.handle.RecordingID())

	f.deliver(t, startSignalWireFor(t, f.handle, lifecycleRecordingID))
	requirePollSignal(t, f.handle, true, "START must complete the handle")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func (f *asyncStartRecordingFixture) requireOfferedStartRecording(
	t *testing.T,
	handle *AsyncStartRecording,
	wantAutoStop archivewire.BooleanType,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.StartRecordingRequest2Fields](
		t,
		f.h.pub.lastOffer(),
		archivewire.StartRecordingRequest2SbeTemplateID,
	)
	require.Equal(t, f.client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
	require.Equal(t, wantAutoStop, fields.AutoStop)
	require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)
}

func TestAsyncStartRecording_TryOffer_wireFields(t *testing.T) {
	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStartRecordingFixture(t)
			f.handle.Close()
			handle := f.client.NewAsyncStartRecording(
				archiveProxyTestRecordingStreamID,
				archivewire.SourceLocation_LOCAL,
				tc.autoStop,
				[]byte(lifecycleRecordingChannel),
			)

			offered, err := handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered)
			require.Len(t, f.h.pub.offered, 1)
			f.requireOfferedStartRecording(t, handle, tc.wantAutoStop)
		})
	}
}

func TestAsyncStartRecording_TryOffer_retriableThenAccepted(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "back_pressured", status: aeron.BackPressured},
		{name: "admin_action", status: aeron.AdminAction},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStartRecordingFixture(t)
			f.h.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.NoError(t, err, "retriable offer must not be terminal")
			require.False(t, offered, "retriable offer must not report accepted")
			require.Len(t, f.h.pub.offered, 1)
			f.requireOfferedStartRecording(t, f.handle, archivewire.BooleanType_TRUE)

			f.h.pub.offerResult = 0
			offered, err = f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered, "accepted offer must report true")
			require.Len(t, f.h.pub.offered, 2)
			f.requireOfferedStartRecording(t, f.handle, archivewire.BooleanType_TRUE)
		})
	}
}

func TestAsyncStartRecording_TryOffer_terminal(t *testing.T) {
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
			f := newAsyncStartRecordingFixture(t)
			f.h.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.False(t, offered, "terminal offer must not report accepted")
			requireControlTerminal(t, err, tc.status)
			require.Len(t, f.h.pub.offered, 1)
			f.requireOfferedStartRecording(t, f.handle, archivewire.BooleanType_TRUE)
		})
	}
}

func TestAsyncStartRecording_PollSignal_errorFirst(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	fields := f.responseFor(f.handle, archivewire.ControlResponseCode_ERROR, []byte("boom"))
	want := MapControlResponse(fields)

	f.deliver(t, encodeControlResponseWire(t, fields))
	for range 2 {
		done, err := f.handle.PollSignal()
		require.True(t, done, "matching error must complete")
		require.Equal(t, want, err)
		require.True(t, f.h.poller.IsPollComplete(), "error capture stays latched")
		require.Zero(t, f.handle.RecordingID())
	}

	f.handle.Close()
	require.False(t, f.h.poller.IsPollComplete(), "Close must clear the matching error")
}

func TestAsyncStartRecording_PollSignal_signalOverError(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	errorWire := encodeControlResponseWire(
		t,
		f.responseFor(f.handle, archivewire.ControlResponseCode_ERROR, []byte("late")),
	)

	f.deliver(t, errorWire, startSignalWireFor(t, f.handle, lifecycleRecordingID))
	require.True(t, f.h.poller.IsPollComplete())

	requirePollSignal(t, f.handle, true, "latched START must win over the matching error")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must clear the matching error")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncStartRecording_PollSignal_foreignCorrelationID(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	okFields := f.responseFor(f.handle, archivewire.ControlResponseCode_OK, nil)
	okFields.CorrelationID++
	signal := codecs.RecordingSignalEventFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    f.handle.request.correlationID + 1,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   asyncStartRecordingTestSubscriptionID,
		Signal:           archivewire.RecordingSignal_START,
	}

	f.deliver(t, encodeControlResponseWire(t, okFields), encodeRecordingSignalEventWire(t, signal))
	require.False(t, f.h.poller.IsPollComplete(), "armed filter must drop a foreign OK")
	require.Equal(t, 1, captured.calls, "session handler must receive the foreign START")
	require.False(t, f.handle.tracker.Started(), "tracker must ignore a foreign START")
	requirePollSignal(t, f.handle, false)
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncStartRecording_PollSignal_discardsStaleTerminal(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	stale := f.responseFor(f.handle, archivewire.ControlResponseCode_ERROR, []byte("stale"))
	stale.ControlSessionID++

	f.deliver(t, encodeControlResponseWire(t, stale))
	require.True(t, f.h.poller.IsPollComplete())
	requirePollSignal(t, f.handle, false, "stale terminal is not a match")
	require.False(t, f.h.poller.IsPollComplete(), "stale terminal must be discarded")

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must consume the matching OK")

	f.deliver(t, startSignalWireFor(t, f.handle, lifecycleRecordingID))
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncStartRecording_Close_disarmsCorrelationFilter(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	other := f.responseFor(f.handle, archivewire.ControlResponseCode_OK, nil)
	other.CorrelationID++
	otherWire := encodeControlResponseWire(t, other)

	f.deliver(t, otherWire)
	require.False(t, f.h.poller.IsPollComplete(), "armed filter must drop other correlation")

	f.handle.Close()
	require.Equal(t, int64(0), f.h.poller.awaitCorrelationID)

	f.deliver(t, otherWire)
	require.True(t, f.h.poller.IsPollComplete(), "disarmed poller accepts any correlation")
}

func TestAsyncStartRecording_Close_clearsLatchedMatchingTerminal(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	require.Same(t, f.handle.tracker, f.client.pendingSignalTracker)
	f.deliver(t, f.okWire(t, f.handle))
	require.True(t, f.h.poller.IsPollComplete())

	f.handle.Close()
	require.False(t, f.h.poller.IsPollComplete(), "Close must clear matching terminal")
	require.Equal(t, int64(0), f.h.poller.awaitCorrelationID)
	require.Nil(t, f.client.pendingSignalTracker, "Close must empty the slot")
}

func TestAsyncStartRecording_Close_lateOKLatchesUntilResetAwait(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	f.handle.Close()
	require.Equal(t, int64(0), f.h.poller.awaitCorrelationID)

	f.deliver(t, f.okWire(t, f.handle))
	require.True(t, f.h.poller.IsPollComplete(), "late OK latches on disarmed poller")

	f.handle.Close()
	require.True(t, f.h.poller.IsPollComplete(), "second Close must leave residual latch")

	next := f.newHandle(t)
	require.False(t, f.h.poller.IsPollComplete(), "next build must clear residual latch")
	require.Same(t, next.tracker, f.client.pendingSignalTracker)
}

func TestAsyncStartRecording_Close_keepsSuccessorFilterAndSlot(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	second := f.newHandle(t)
	require.NotEqual(t, first.request.correlationID, second.request.correlationID)

	first.Close()
	require.Equal(
		t,
		second.request.correlationID,
		f.h.poller.awaitCorrelationID,
		"Close of prior handle must not disarm successor filter",
	)
	require.Same(
		t,
		second.tracker,
		f.client.pendingSignalTracker,
		"Close of prior handle must not empty successor slot",
	)
}

func TestAsyncStartRecording_PollSignal_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.deliver(t, f.okWire(t, second))

	requirePollSignal(t, first, false, "closed handle must not match")
	require.True(t, f.h.poller.IsPollComplete(), "closed PollSignal must leave successor latch")
}

func TestAsyncStartRecording_closedDoesNotOfferOrMatch(t *testing.T) {
	var nilHandle *AsyncStartRecording
	offered, err := nilHandle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "nil TryOffer must not accept")
	requirePollSignal(t, nilHandle, false, "nil PollSignal must not match")
	require.Zero(t, nilHandle.RecordingID())
	require.NotPanics(t, nilHandle.Close)

	f := newAsyncStartRecordingFixture(t)
	f.handle.Close()

	offered, err = f.handle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "closed TryOffer must not accept")
	require.Empty(t, f.h.pub.offered, "closed TryOffer must not offer")

	f.deliver(t, f.okWire(t, f.handle), startSignalWireFor(t, f.handle, lifecycleRecordingID))
	require.False(t, f.handle.tracker.Started(), "closed handle tracker must not see START")
	requirePollSignal(t, f.handle, false, "closed PollSignal must not match")
	require.True(t, f.h.poller.IsPollComplete(), "closed PollSignal must not consume")
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncStartRecording_Close_insideDispatch(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	calls := 0
	f.client.SetRecordingSignalHandler(RecordingSignalHandlerFunc(
		func(codecs.RecordingSignalEventFields) {
			calls++
			f.handle.Close()
		},
	))

	f.deliver(t, startSignalWireFor(t, f.handle, lifecycleRecordingID))
	require.Equal(t, 1, calls)
	require.Nil(t, f.client.pendingSignalTracker, "Close inside the handler must empty the slot")
	require.False(t, f.handle.tracker.Started(), "closed handle tracker must not see START")
	requirePollSignal(t, f.handle, false)
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncStartRecording_NewAsyncStartRecording_insideDispatch(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	var rebuilt *AsyncStartRecording
	f.client.SetRecordingSignalHandler(RecordingSignalHandlerFunc(
		func(codecs.RecordingSignalEventFields) {
			if rebuilt == nil {
				rebuilt = newAsyncStartRecordingForTest(f.client)
			}
		},
	))

	f.deliver(t, startSignalWireFor(t, first, lifecycleRecordingID))
	require.NotNil(t, rebuilt)
	require.Same(t, rebuilt.tracker, f.client.pendingSignalTracker)
	require.False(t, rebuilt.tracker.Started(), "fresh correlation id must not match the signal")
	require.False(t, first.tracker.Started(), "displaced tracker must not see START")
	requirePollSignal(t, rebuilt, false)
	requirePollSignal(t, first, false)
	first.Close()
	rebuilt.Close()
	require.Nil(t, f.client.pendingSignalTracker)
}

func TestAsyncStartRecording_PollSignal_displacedHandle(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	second := f.newHandle(t)
	require.Same(t, second.tracker, f.client.pendingSignalTracker)

	// Phase: the displaced handle never sees its START.
	f.deliver(t, startSignalWireFor(t, first, lifecycleRecordingID))
	require.False(t, first.tracker.Started(), "displaced tracker must not see START")
	requirePollSignal(t, first, false)

	// Phase: a poll on the open older handle clears the newer latched OK.
	f.deliver(t, f.okWire(t, second))
	require.True(t, f.h.poller.IsPollComplete())
	requirePollSignal(t, first, false)
	require.False(t, f.h.poller.IsPollComplete(), "older poll must discard the newer OK")

	// Phase: the newer handle still completes on its own START.
	f.deliver(t, startSignalWireFor(t, second, lifecycleRecordingID+1))
	requirePollSignal(t, second, true)
	require.Equal(t, lifecycleRecordingID+1, second.RecordingID())
	require.Zero(t, first.RecordingID())
}

func TestAsyncStartRecording_PollSignal_latchedClearsOnlyOwnTerminal(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	f.deliver(t, startSignalWireFor(t, first, lifecycleRecordingID))
	requirePollSignal(t, first, true)

	second := f.newHandle(t)
	f.deliver(t, f.okWire(t, second))
	require.True(t, f.h.poller.IsPollComplete())

	requirePollSignal(t, first, true)
	require.True(t, f.h.poller.IsPollComplete(), "latched handle must keep the other latch")
	require.Equal(t, lifecycleRecordingID, first.RecordingID())
}
