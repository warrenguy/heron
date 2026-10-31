//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncExtendRecordingTestSubscriptionID int64 = 2003

type asyncExtendRecordingFixture struct {
	h      *lifecycleHarness
	client *Client
	handle *AsyncExtendRecording
}

func newAsyncExtendRecordingFixture(t *testing.T) *asyncExtendRecordingFixture {
	t.Helper()

	h := newLifecycleHarness(t)
	f := &asyncExtendRecordingFixture{
		h:      h,
		client: h.newClient(),
	}
	f.handle = f.newHandle(t)
	return f
}

func (f *asyncExtendRecordingFixture) newHandle(t *testing.T) *AsyncExtendRecording {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := newAsyncExtendRecordingForTest(f.client)
	require.Equal(t, prior+1, handle.request.correlationID)
	require.Equal(t, handle.request.correlationID, f.h.poller.awaitCorrelationID)
	require.Same(t, &handle.tracker, f.client.pendingSignalTracker)
	return handle
}

// deliver appends wires to the control-response term, then runs one
// Client.Poll over them.
func (f *asyncExtendRecordingFixture) deliver(t *testing.T, wires ...[]byte) {
	t.Helper()

	for _, wire := range wires {
		appendUnfragmentedWireToTerm(t, f.h.termBuffer, wire)
	}
	require.Equal(t, len(wires), f.client.Poll())
}

func (f *asyncExtendRecordingFixture) responseFor(
	handle *AsyncExtendRecording,
	code archivewire.ControlResponseCode,
	errorMessage []byte,
) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    handle.request.correlationID,
		RelevantID:       asyncExtendRecordingTestSubscriptionID,
		Code:             code,
		Version:          520,
		ErrorMessage:     errorMessage,
	}
}

func (f *asyncExtendRecordingFixture) okWire(t *testing.T, handle *AsyncExtendRecording) []byte {
	t.Helper()
	return encodeControlResponseWire(
		t,
		f.responseFor(handle, archivewire.ControlResponseCode_OK, nil),
	)
}

func (f *asyncExtendRecordingFixture) errorWire(
	t *testing.T,
	handle *AsyncExtendRecording,
	errorMessage string,
) []byte {
	t.Helper()
	return encodeControlResponseWire(
		t,
		f.responseFor(handle, archivewire.ControlResponseCode_ERROR, []byte(errorMessage)),
	)
}

func newAsyncExtendRecordingForTest(client *Client) *AsyncExtendRecording {
	return client.NewAsyncExtendRecording(
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		true,
		[]byte(lifecycleRecordingChannel),
	)
}

func (f *asyncExtendRecordingFixture) extendWire(t *testing.T) []byte {
	t.Helper()
	return extendSignalWireFor(t, f.handle, lifecycleRecordingID)
}

func extendSignalWireFor(
	t *testing.T,
	handle *AsyncExtendRecording,
	recordingID int64,
) []byte {
	t.Helper()
	return encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: handle.request.client.controlSessionID,
		CorrelationID:    handle.request.correlationID,
		RecordingID:      recordingID,
		SubscriptionID:   asyncExtendRecordingTestSubscriptionID,
		Position:         3003,
		Signal:           archivewire.RecordingSignal_EXTEND,
	})
}

// TestAsyncExtendRecording_PollSignal_extendThroughClientPoll asserts Poll
// feeds the open handle's tracker when Client has no session handler.
func TestAsyncExtendRecording_PollSignal_extendThroughClientPoll(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	require.Nil(t, f.client.signalHandler)
	offered, err := f.handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must consume the matching OK")
	require.Zero(t, f.handle.RecordingID())

	f.deliver(t, f.extendWire(t))
	require.True(t, f.handle.tracker.Extended())
	require.Zero(t, f.handle.RecordingID(), "RecordingID must wait for PollSignal")
	requirePollSignal(t, f.handle, true, "EXTEND must complete the handle")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())

	f.handle.Close()
	require.Nil(t, f.client.signalTarget(), "empty slot and nil handler must skip the decode")
}

func (f *asyncExtendRecordingFixture) requireOfferedExtendRecording(
	t *testing.T,
	handle *AsyncExtendRecording,
	wantAutoStop archivewire.BooleanType,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.ExtendRecordingRequest2Fields](
		t,
		f.h.pub.lastOffer(),
		archivewire.ExtendRecordingRequest2SbeTemplateID,
	)
	require.Equal(t, f.client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, lifecycleRecordingID, fields.RecordingID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
	require.Equal(t, wantAutoStop, fields.AutoStop)
	require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)
}

func TestAsyncExtendRecording_TryOffer_wireFields(t *testing.T) {
	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			f := &asyncExtendRecordingFixture{h: h, client: h.newClient()}
			handle := f.client.NewAsyncExtendRecording(
				lifecycleRecordingID,
				archiveProxyTestRecordingStreamID,
				archivewire.SourceLocation_LOCAL,
				tc.autoStop,
				[]byte(lifecycleRecordingChannel),
			)

			offered, err := handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered)
			require.Len(t, h.pub.offered, 1)
			f.requireOfferedExtendRecording(t, handle, tc.wantAutoStop)
		})
	}
}

func TestAsyncExtendRecording_PollSignal_errorFirst(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
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

func TestAsyncExtendRecording_PollSignal_signalOverError(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)

	f.deliver(t, f.errorWire(t, f.handle, "late"), f.extendWire(t))
	require.True(t, f.h.poller.IsPollComplete())

	requirePollSignal(t, f.handle, true, "latched EXTEND must win over the matching error")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must clear the matching error")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

// TestAsyncExtendRecording_PollSignal_errorAfterConsumedOK asserts a refusal
// that lands after the PollSignal that consumed the OK fails the handle.
func TestAsyncExtendRecording_PollSignal_errorAfterConsumedOK(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must consume the matching OK")

	fields := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		[]byte("invalid extension"),
	)
	f.deliver(t, encodeControlResponseWire(t, fields))
	done, err := f.handle.PollSignal()
	require.True(t, done, "post-OK error must complete")
	require.Equal(t, MapControlResponse(fields), err)
	require.True(t, f.h.poller.IsPollComplete(), "error capture stays latched")
	require.Zero(t, f.handle.RecordingID())

	f.handle.Close()
	require.False(t, f.h.poller.IsPollComplete(), "Close must clear the matching error")
}

// TestAsyncExtendRecording_PollSignal_errorInOKBatch asserts that an error
// appended behind the OK before the first Client.Poll reaches PollSignal once
// PollSignal consumes the OK.
func TestAsyncExtendRecording_PollSignal_errorInOKBatch(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	errorFields := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		[]byte("invalid extension"),
	)
	appendUnfragmentedWireToTerm(t, f.h.termBuffer, f.okWire(t, f.handle))
	appendUnfragmentedWireToTerm(t, f.h.termBuffer, encodeControlResponseWire(t, errorFields))

	// Poll 1 latches the OK. The error stays unread.
	require.Equal(t, 1, f.client.Poll())
	resp, ok := f.h.poller.Response()
	require.True(t, ok)
	require.Equal(t, archivewire.ControlResponseCode_OK, resp.Code)
	requirePollSignal(t, f.handle, false, "PollSignal must consume the OK")
	require.False(t, f.h.poller.IsPollComplete())

	// Poll 2 latches the error.
	require.Equal(t, 1, f.client.Poll())
	resp, ok = f.h.poller.Response()
	require.True(t, ok)
	require.Equal(t, archivewire.ControlResponseCode_ERROR, resp.Code)
	done, err := f.handle.PollSignal()
	require.True(t, done, "held error must complete")
	require.Equal(t, MapControlResponse(errorFields), err)
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_foreignCorrelationID(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	okFields := f.responseFor(f.handle, archivewire.ControlResponseCode_OK, nil)
	okFields.CorrelationID++
	signal := codecs.RecordingSignalEventFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    f.handle.request.correlationID + 1,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   asyncExtendRecordingTestSubscriptionID,
		Signal:           archivewire.RecordingSignal_EXTEND,
	}

	f.deliver(t, encodeControlResponseWire(t, okFields), encodeRecordingSignalEventWire(t, signal))
	require.False(t, f.h.poller.IsPollComplete(), "armed filter must drop a foreign OK")
	require.Equal(t, 1, captured.calls, "session handler must receive the foreign EXTEND")
	require.False(t, f.handle.tracker.Extended(), "tracker must ignore a foreign EXTEND")
	requirePollSignal(t, f.handle, false)
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_foreignRecordingID(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	f.deliver(t, extendSignalWireFor(t, f.handle, lifecycleRecordingID+1))
	require.Equal(t, 1, captured.calls, "session handler must receive the foreign EXTEND")
	require.False(t, f.handle.tracker.Extended(), "tracker must ignore another recording")
	requirePollSignal(t, f.handle, false)
	require.Zero(t, f.handle.RecordingID())

	f.deliver(t, f.extendWire(t))
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_discardsStaleTerminal(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	stale := f.responseFor(f.handle, archivewire.ControlResponseCode_ERROR, []byte("stale"))
	stale.ControlSessionID++

	f.deliver(t, encodeControlResponseWire(t, stale))
	require.True(t, f.h.poller.IsPollComplete())
	requirePollSignal(t, f.handle, false, "stale terminal is not a match")
	require.False(t, f.h.poller.IsPollComplete(), "stale terminal must be discarded")

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.h.poller.IsPollComplete(), "PollSignal must consume the matching OK")

	f.deliver(t, f.extendWire(t))
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncExtendRecording_Close_disarmsCorrelationFilter(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
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

func TestAsyncExtendRecording_Close_clearsLatchedMatchingTerminal(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	f.deliver(t, f.okWire(t, f.handle))
	require.True(t, f.h.poller.IsPollComplete())

	f.handle.Close()
	require.False(t, f.h.poller.IsPollComplete(), "Close must clear matching terminal")
	require.Equal(t, int64(0), f.h.poller.awaitCorrelationID)
	require.Nil(t, f.client.pendingSignalTracker, "Close must empty the slot")
}

func TestAsyncExtendRecording_Close_lateOKLatchesUntilResetAwait(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	f.handle.Close()
	require.Equal(t, int64(0), f.h.poller.awaitCorrelationID)

	f.deliver(t, f.okWire(t, f.handle))
	require.True(t, f.h.poller.IsPollComplete(), "late OK latches on disarmed poller")

	f.handle.Close()
	require.True(t, f.h.poller.IsPollComplete(), "second Close must leave residual latch")

	f.newHandle(t)
	require.False(t, f.h.poller.IsPollComplete(), "next build must clear residual latch")
}

func TestAsyncExtendRecording_Close_keepsSuccessorFilterAndSlot(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
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
		&second.tracker,
		f.client.pendingSignalTracker,
		"Close of prior handle must not empty successor slot",
	)
}

func TestAsyncExtendRecording_PollSignal_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.deliver(t, f.okWire(t, second))

	requirePollSignal(t, first, false, "closed handle must not match")
	require.True(t, f.h.poller.IsPollComplete(), "closed PollSignal must leave successor latch")
}

func TestAsyncExtendRecording_closedDoesNotOfferOrMatch(t *testing.T) {
	var nilHandle *AsyncExtendRecording
	offered, err := nilHandle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "nil TryOffer must not accept")
	requirePollSignal(t, nilHandle, false, "nil PollSignal must not match")
	require.Zero(t, nilHandle.RecordingID())
	require.NotPanics(t, nilHandle.Close)

	f := newAsyncExtendRecordingFixture(t)
	f.handle.Close()

	offered, err = f.handle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "closed TryOffer must not accept")
	require.Empty(t, f.h.pub.offered, "closed TryOffer must not offer")

	f.deliver(t, f.okWire(t, f.handle), f.extendWire(t))
	require.False(t, f.handle.tracker.Extended(), "closed handle tracker must not see EXTEND")
	requirePollSignal(t, f.handle, false, "closed PollSignal must not match")
	require.True(t, f.h.poller.IsPollComplete(), "closed PollSignal must not consume")
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncExtendRecording_Close_insideDispatch(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	calls := 0
	f.client.SetRecordingSignalHandler(RecordingSignalHandlerFunc(
		func(codecs.RecordingSignalEventFields) {
			calls++
			f.handle.Close()
		},
	))

	f.deliver(t, f.extendWire(t))
	require.Equal(t, 1, calls)
	require.Nil(t, f.client.pendingSignalTracker, "Close inside the handler must empty the slot")
	require.False(t, f.handle.tracker.Extended(), "closed handle tracker must not see EXTEND")
	requirePollSignal(t, f.handle, false)
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncExtendRecording_NewAsyncExtendRecording_insideDispatch(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	first := f.handle
	var rebuilt *AsyncExtendRecording
	f.client.SetRecordingSignalHandler(RecordingSignalHandlerFunc(
		func(codecs.RecordingSignalEventFields) {
			if rebuilt == nil {
				rebuilt = newAsyncExtendRecordingForTest(f.client)
			}
		},
	))

	f.deliver(t, extendSignalWireFor(t, first, lifecycleRecordingID))
	require.NotNil(t, rebuilt)
	require.Same(t, &rebuilt.tracker, f.client.pendingSignalTracker)
	require.False(t, rebuilt.tracker.Extended(), "fresh correlation id must not match the signal")
	require.False(t, first.tracker.Extended(), "displaced tracker must not see EXTEND")
	requirePollSignal(t, rebuilt, false)
	requirePollSignal(t, first, false)
	first.Close()
	rebuilt.Close()
	require.Nil(t, f.client.pendingSignalTracker)
}

func TestAsyncExtendRecording_PollSignal_displacedHandle(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	first := f.handle
	second := f.newHandle(t)

	// Phase: the displaced handle never sees its EXTEND.
	f.deliver(t, extendSignalWireFor(t, first, lifecycleRecordingID))
	require.False(t, first.tracker.Extended(), "displaced tracker must not see EXTEND")
	requirePollSignal(t, first, false)

	// Phase: the newer handle still completes on its own EXTEND.
	f.deliver(t, extendSignalWireFor(t, second, lifecycleRecordingID))
	requirePollSignal(t, second, true)
	require.Equal(t, lifecycleRecordingID, second.RecordingID())
	require.Zero(t, first.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_displacedByStartHandle(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	start := newAsyncStartRecordingForTest(f.client)
	require.Same(t, &start.tracker, f.client.pendingSignalTracker)

	f.deliver(t, f.extendWire(t))
	require.False(t, f.handle.tracker.Extended(), "displaced tracker must not see EXTEND")
	requirePollSignal(t, f.handle, false)

	f.handle.Close()
	require.Same(t, &start.tracker, f.client.pendingSignalTracker, "Close must keep the start slot")
	start.Close()
	require.Nil(t, f.client.pendingSignalTracker)
}

func TestAsyncExtendRecording_PollSignal_latchedClearsOnlyOwnTerminal(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	first := f.handle
	f.deliver(t, extendSignalWireFor(t, first, lifecycleRecordingID))
	requirePollSignal(t, first, true)

	second := f.newHandle(t)
	f.deliver(t, f.okWire(t, second))
	require.True(t, f.h.poller.IsPollComplete())

	requirePollSignal(t, first, true)
	require.True(t, f.h.poller.IsPollComplete(), "latched handle must keep the other latch")
	require.Equal(t, lifecycleRecordingID, first.RecordingID())
}
