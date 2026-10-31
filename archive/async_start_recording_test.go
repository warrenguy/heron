//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncStartRecordingTestSubscriptionID int64 = 2002

var asyncStartRecordingKind = asyncHandleKind[*AsyncStartRecording]{
	build:         newAsyncStartRecordingForTest,
	correlationID: func(handle *AsyncStartRecording) int64 { return handle.request.correlationID },
	relevantID:    asyncStartRecordingTestSubscriptionID,
	slotTracker: func(handle *AsyncStartRecording) RecordingSignalHandler {
		return &handle.tracker
	},
	requireOffered: func(t *testing.T, client *Client, handle *AsyncStartRecording, wire []byte) {
		t.Helper()
		requireOfferedStartRecording(t, client, handle, wire, archivewire.BooleanType_TRUE)
	},
}

func newAsyncStartRecordingFixture(t *testing.T) *asyncHandleFixture[*AsyncStartRecording] {
	t.Helper()
	return newAsyncHandleFixture(t, asyncStartRecordingKind)
}

// newAsyncStartRecordingForTest builds a handle with autoStop true on the
// lifecycle stream and channel.
func newAsyncStartRecordingForTest(client *Client) *AsyncStartRecording {
	return newAsyncStartRecordingForAutoStop(client, true)
}

func newAsyncStartRecordingForAutoStop(client *Client, autoStop bool) *AsyncStartRecording {
	return client.NewAsyncStartRecording(
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		autoStop,
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

func startWire(t *testing.T, handle *AsyncStartRecording) []byte {
	t.Helper()
	return startSignalWireFor(t, handle, lifecycleRecordingID)
}

func requirePollSignal(
	t *testing.T,
	handle interface{ PollSignal() (bool, error) },
	wantDone bool,
	msgAndArgs ...any,
) {
	t.Helper()

	done, err := handle.PollSignal()
	require.NoError(t, err, msgAndArgs...)
	require.Equal(t, wantDone, done, msgAndArgs...)
}

// TestAsyncStartRecording_PollSignal_startThroughClientPoll asserts Poll
// feeds the open handle's tracker when Client has no session handler.
func TestAsyncStartRecording_PollSignal_startThroughClientPoll(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	require.Nil(t, f.client.signalHandler)
	offered, err := f.handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must consume the matching OK")
	require.Zero(t, f.handle.RecordingID())

	f.deliver(t, startWire(t, f.handle))
	require.True(t, f.handle.tracker.Started())
	requirePollSignal(t, f.handle, true, "START must complete the handle")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())

	f.handle.Close()
	require.Nil(t, f.client.signalTarget(), "empty slot and nil handler must skip the decode")
}

func requireOfferedStartRecording(
	t *testing.T,
	client *Client,
	handle *AsyncStartRecording,
	wire []byte,
	wantAutoStop archivewire.BooleanType,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.StartRecordingRequest2Fields](
		t,
		wire,
		archivewire.StartRecordingRequest2SbeTemplateID,
	)
	require.Equal(t, client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
	require.Equal(t, wantAutoStop, fields.AutoStop)
	require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StartRecordingRequest2SbeBlockLength,
		len(lifecycleRecordingChannel),
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStartRecording_TryOffer_wireFields(t *testing.T) {
	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()
			handle := newAsyncStartRecordingForAutoStop(client, tc.autoStop)

			offered, err := handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered)
			require.Len(t, h.pub.offered, 1)
			requireOfferedStartRecording(t, client, handle, h.pub.lastOffer(), tc.wantAutoStop)
		})
	}
}

func TestAsyncStartRecording_PollSignal_errorFirst(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
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

func TestAsyncStartRecording_PollSignal_signalOverError(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	f.deliver(
		t,
		f.errorWire(t, f.handle, "late"),
		startWire(t, f.handle),
	)
	require.True(t, f.controlPoller.IsPollComplete())

	requirePollSignal(t, f.handle, true, "latched START must win over the matching error")
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must clear the matching error")
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncStartRecording_PollSignal_foreignCorrelationID(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	okFields := f.okFor(f.handle)
	okFields.CorrelationID++
	signal := codecs.RecordingSignalEventFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    f.handle.request.correlationID + 1,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   asyncStartRecordingTestSubscriptionID,
		Signal:           archivewire.RecordingSignal_START,
	}

	f.deliver(t, encodeControlResponseWire(t, okFields), encodeRecordingSignalEventWire(t, signal))
	require.False(t, f.controlPoller.IsPollComplete(), "armed filter must drop a foreign OK")
	require.Equal(t, 1, captured.calls, "session handler must receive the foreign START")
	require.False(t, f.handle.tracker.Started(), "tracker must ignore a foreign START")
	requirePollSignal(t, f.handle, false)
	require.Zero(t, f.handle.RecordingID())
}

func TestAsyncStartRecording_PollSignal_discardsStaleTerminal(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	stale := f.errorFor(f.handle, "stale")
	stale.ControlSessionID++

	f.latchResponse(t, stale)
	requirePollSignal(t, f.handle, false, "stale terminal is not a match")
	require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

	f.deliver(t, f.okWire(t, f.handle))
	requirePollSignal(t, f.handle, false, "OK must not complete the handle")
	require.False(t, f.controlPoller.IsPollComplete(), "PollSignal must consume the matching OK")

	f.deliver(t, startWire(t, f.handle))
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

func TestAsyncStartRecording_PollSignal_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.deliver(t, f.okWire(t, second))

	requirePollSignal(t, first, false, "closed handle must not match")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollSignal must leave successor latch")
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
	require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

	f.deliver(t, f.okWire(t, f.handle), startWire(t, f.handle))
	require.False(t, f.handle.tracker.Started(), "closed handle tracker must not see START")
	requirePollSignal(t, f.handle, false, "closed PollSignal must not match")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollSignal must not consume")
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

	f.deliver(t, startWire(t, f.handle))
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

	f.deliver(t, startWire(t, first))
	require.NotNil(t, rebuilt)
	require.Same(t, &rebuilt.tracker, f.client.pendingSignalTracker)
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

	f.deliver(t, startWire(t, first))
	require.False(t, first.tracker.Started(), "displaced tracker must not see START")
	requirePollSignal(t, first, false)

	f.deliver(t, startSignalWireFor(t, second, lifecycleRecordingID+1))
	requirePollSignal(t, second, true)
	require.Equal(t, lifecycleRecordingID+1, second.RecordingID())
	require.Zero(t, first.RecordingID())
}

func TestAsyncStartRecording_PollSignal_latchedClearsOnlyOwnTerminal(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	first := f.handle
	f.deliver(t, startWire(t, first))
	requirePollSignal(t, first, true)

	second := f.newHandle(t)
	f.latchResponse(t, f.okFor(second))

	requirePollSignal(t, first, true)
	require.True(t, f.controlPoller.IsPollComplete(), "latched handle must keep the other latch")
	require.Equal(t, lifecycleRecordingID, first.RecordingID())
}
