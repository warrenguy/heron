//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncExtendRecordingTestSubscriptionID int64 = 2003

var asyncExtendRecordingKind = asyncHandleKind[*AsyncExtendRecording]{
	build:         newAsyncExtendRecordingForTest,
	correlationID: func(handle *AsyncExtendRecording) int64 { return handle.request.correlationID },
	relevantID:    asyncExtendRecordingTestSubscriptionID,
	slotTracker: func(handle *AsyncExtendRecording) RecordingSignalHandler {
		return &handle.tracker
	},
	requireOffered: func(t *testing.T, client *Client, handle *AsyncExtendRecording, wire []byte) {
		t.Helper()
		requireOfferedExtendRecording(t, client, handle, wire, archivewire.BooleanType_TRUE)
	},
}

func newAsyncExtendRecordingFixture(t *testing.T) *asyncHandleFixture[*AsyncExtendRecording] {
	t.Helper()
	return newAsyncHandleFixture(t, asyncExtendRecordingKind)
}

func newAsyncExtendRecordingForTest(client *Client) *AsyncExtendRecording {
	return newAsyncExtendRecordingForAutoStop(client, true)
}

func newAsyncExtendRecordingForAutoStop(client *Client, autoStop bool) *AsyncExtendRecording {
	return client.NewAsyncExtendRecording(
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		autoStop,
		[]byte(lifecycleRecordingChannel),
	)
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

func extendWire(t *testing.T, handle *AsyncExtendRecording) []byte {
	t.Helper()
	return extendSignalWireFor(t, handle, lifecycleRecordingID)
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
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must consume the matching OK")
	require.Zero(t, f.handle.RecordingID())

	f.deliver(t, extendWire(t, f.handle))
	require.True(t, f.handle.tracker.Extended())
	require.Zero(t, f.handle.RecordingID(), "RecordingID must wait for PollSignal")
	requirePollSignal(t, f.handle, true, "EXTEND must complete the handle")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())

	f.handle.Close()
	require.Nil(t, f.client.signalTarget(), "empty slot and nil handler must skip the decode")
}

func requireOfferedExtendRecording(
	t *testing.T,
	client *Client,
	handle *AsyncExtendRecording,
	wire []byte,
	wantAutoStop archivewire.BooleanType,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.ExtendRecordingRequest2Fields](
		t,
		wire,
		archivewire.ExtendRecordingRequest2SbeTemplateID,
	)
	require.Equal(t, client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, lifecycleRecordingID, fields.RecordingID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
	require.Equal(t, wantAutoStop, fields.AutoStop)
	require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.ExtendRecordingRequest2SbeBlockLength,
		len(lifecycleRecordingChannel),
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncExtendRecording_TryOffer_wireFields(t *testing.T) {
	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()
			handle := newAsyncExtendRecordingForAutoStop(client, tc.autoStop)

			offered, err := handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered)
			require.Len(t, h.pub.offered, 1)
			requireOfferedExtendRecording(t, client, handle, h.pub.lastOffer(), tc.wantAutoStop)
		})
	}
}

func TestAsyncExtendRecording_PollSignal_errorFirst(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	fields := f.errorFor(f.handle, "boom")
	want := MapControlResponse(fields)

	f.deliverResponse(t, fields)
	for range 2 {
		done, err := f.handle.PollSignal()
		require.True(t, done, "matching error must complete")
		require.Equal(t, want, err)
		require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")
		require.Zero(t, f.handle.RecordingID())
	}

	f.handle.Close()
	require.False(t, f.controlPoller.IsPollComplete(), "Close must clear the matching error")
}

func TestAsyncExtendRecording_PollSignal_signalOverError(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)

	f.deliver(
		t,
		f.errorWire(t, f.handle, "late"),
		extendWire(t, f.handle),
	)
	require.True(t, f.controlPoller.IsPollComplete())

	requirePollSignal(t, f.handle, true, "latched EXTEND must win over the matching error")
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must clear the matching error")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

// TestAsyncExtendRecording_PollSignal_errorAfterConsumedOK asserts a refusal
// that lands after the PollSignal that consumed the OK fails the handle.
func TestAsyncExtendRecording_PollSignal_errorAfterConsumedOK(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must consume the matching OK")

	fields := f.errorFor(f.handle, "invalid extension")
	f.deliverResponse(t, fields)
	done, err := f.handle.PollSignal()
	require.True(t, done, "post-OK error must complete")
	require.Equal(t, MapControlResponse(fields), err)
	require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")
	require.Zero(t, f.handle.RecordingID())

	f.handle.Close()
	require.False(t, f.controlPoller.IsPollComplete(), "Close must clear the matching error")
}

// TestAsyncExtendRecording_PollSignal_errorInOKBatch asserts that an error
// appended behind the OK before the first Client.Poll reaches PollSignal once
// PollSignal consumes the OK.
func TestAsyncExtendRecording_PollSignal_errorInOKBatch(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	errorFields := f.errorFor(f.handle, "invalid extension")
	appendUnfragmentedWireToTerm(t, f.harness.termBuffer, f.okWire(t, f.handle))
	appendUnfragmentedWireToTerm(t, f.harness.termBuffer, encodeControlResponseWire(t, errorFields))

	// The first Poll does not read the error.
	require.Equal(t, 1, f.client.Poll())
	resp, ok := f.controlPoller.Response()
	require.True(t, ok)
	require.Equal(t, archivewire.ControlResponseCode_OK, resp.Code)
	requirePollSignal(t, f.handle, false, "PollSignal must consume the OK")
	require.False(t, f.controlPoller.IsPollComplete())

	require.Equal(t, 1, f.client.Poll())
	resp, ok = f.controlPoller.Response()
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

	okFields := f.okFor(f.handle)
	okFields.CorrelationID++
	signal := codecs.RecordingSignalEventFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    f.handle.request.correlationID + 1,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   asyncExtendRecordingTestSubscriptionID,
		Signal:           archivewire.RecordingSignal_EXTEND,
	}

	f.deliver(t, encodeControlResponseWire(t, okFields), encodeRecordingSignalEventWire(t, signal))
	require.False(t, f.controlPoller.IsPollComplete(), "armed filter must drop a foreign OK")
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

	f.deliver(t, extendWire(t, f.handle))
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_discardsStaleTerminal(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	stale := f.errorFor(f.handle, "stale")
	stale.ControlSessionID++

	f.latchResponse(t, stale)
	requirePollSignal(t, f.handle, false, "stale terminal is not a match")
	require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must consume the matching OK")

	f.deliver(t, extendWire(t, f.handle))
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.deliver(t, f.okWire(t, second))

	requirePollSignal(t, first, false, "closed handle must not match")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollSignal must leave successor latch")
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
	require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

	f.deliver(t, f.okWire(t, f.handle), extendWire(t, f.handle))
	require.False(t, f.handle.tracker.Extended(), "closed handle tracker must not see EXTEND")
	requirePollSignal(t, f.handle, false, "closed PollSignal must not match")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollSignal must not consume")
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

	f.deliver(t, extendWire(t, f.handle))
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

	f.deliver(t, extendWire(t, first))
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

	f.deliver(t, extendWire(t, first))
	require.False(t, first.tracker.Extended(), "displaced tracker must not see EXTEND")
	requirePollSignal(t, first, false)

	f.deliver(t, extendWire(t, second))
	requirePollSignal(t, second, true)
	require.Equal(t, lifecycleRecordingID, second.RecordingID())
	require.Zero(t, first.RecordingID())
}

func TestAsyncExtendRecording_PollSignal_displacedByStartHandle(t *testing.T) {
	f := newAsyncExtendRecordingFixture(t)
	start := newAsyncStartRecordingForTest(f.client)
	require.Same(t, &start.tracker, f.client.pendingSignalTracker)

	f.deliver(t, extendWire(t, f.handle))
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
	f.deliver(t, extendWire(t, first))
	requirePollSignal(t, first, true)

	second := f.newHandle(t)
	f.latchResponse(t, f.okFor(second))

	requirePollSignal(t, first, true)
	require.True(t, f.controlPoller.IsPollComplete(), "latched handle must keep the other latch")
	require.Equal(t, lifecycleRecordingID, first.RecordingID())
}
