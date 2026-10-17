//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncListRecordingTestRecordingID int64 = 42

type asyncListRecordingFixture struct {
	client           *Client
	pub              *stubControlPublication
	controlPoller    *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	handle           *AsyncListRecording
}

func newAsyncListRecordingFixture(t *testing.T) *asyncListRecordingFixture {
	t.Helper()

	pub, proxy := newTestProxy(Config{})
	controlPoller := NewControlResponsePoller(nil, 1)
	descriptorPoller := NewRecordingDescriptorPoller()
	f := &asyncListRecordingFixture{
		client: &Client{
			controlSessionID: controlResponseTestControlSessionID,
			proxy:            proxy,
			controlPoller:    controlPoller,
			descriptorPoller: descriptorPoller,
			aeronClient:      aeron.NewAsyncConnectTestHarness(t).Client,
		},
		pub:              pub,
		controlPoller:    controlPoller,
		descriptorPoller: descriptorPoller,
	}
	f.handle = f.newHandle(t)
	return f
}

func (f *asyncListRecordingFixture) newHandle(t *testing.T) *AsyncListRecording {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := f.client.NewAsyncListRecording(asyncListRecordingTestRecordingID)
	require.Equal(t, prior+1, handle.correlationID)
	require.Equal(t, handle.correlationID, f.controlPoller.awaitCorrelationID)
	require.Equal(t, handle.correlationID, f.descriptorPoller.awaitCorrelationID)
	return handle
}

// deliver routes wire through the session poll targets, as Client.Poll does.
func (f *asyncListRecordingFixture) deliver(t *testing.T, wire []byte) {
	t.Helper()

	invokeHandleControlFragment(t, f.client.sessionPollTargets(nil), wire)
}

func (f *asyncListRecordingFixture) descriptorFor(
	handle *AsyncListRecording,
) codecs.RecordingDescriptorFields {
	return codecs.RecordingDescriptorFields{
		ControlSessionID: f.client.controlSessionID,
		CorrelationID:    handle.correlationID,
		RecordingID:      asyncListRecordingTestRecordingID,
		StartPosition:    4096,
		StopPosition:     8192,
		StrippedChannel:  []byte("aeron:ipc"),
		OriginalChannel:  []byte("aeron:ipc?alias=log"),
		SourceIdentity:   []byte("source-identity"),
	}
}

func (f *asyncListRecordingFixture) deliverDescriptor(
	t *testing.T,
	fields codecs.RecordingDescriptorFields,
) {
	t.Helper()

	f.deliver(t, encodeRecordingDescriptorWire(t, fields))
}

func (f *asyncListRecordingFixture) requireOfferedListRecording(t *testing.T) {
	t.Helper()

	wire := f.pub.lastOffer()
	fields := requireProxyWireFields[codecs.ListRecordingRequestFields](
		t,
		wire,
		archivewire.ListRecordingRequestSbeTemplateID,
	)
	require.Equal(t, f.client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, f.handle.correlationID, fields.CorrelationID)
	require.Equal(t, asyncListRecordingTestRecordingID, fields.RecordingID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.ListRecordingRequestSbeBlockLength,
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func requirePollDescriptor(
	t *testing.T,
	handle *AsyncListRecording,
	wantDone bool,
	wantDescriptor codecs.RecordingDescriptorFields,
	wantErr error,
) {
	t.Helper()

	done, descriptor, err := handle.PollDescriptor()
	require.Equal(t, wantErr, err)
	require.Equal(t, wantDone, done)
	require.Equal(t, wantDescriptor, descriptor)
}

func (f *asyncListRecordingFixture) responseFor(
	handle *AsyncListRecording,
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

func (f *asyncListRecordingFixture) okFor(handle *AsyncListRecording) codecs.ControlResponseFields {
	return f.responseFor(handle, archivewire.ControlResponseCode_OK, 0, nil)
}

func (f *asyncListRecordingFixture) deliverResponse(
	t *testing.T,
	fields codecs.ControlResponseFields,
) {
	t.Helper()

	f.deliver(t, encodeControlResponseWire(t, fields))
}

func TestAsyncListRecording_TryOffer_wire(t *testing.T) {
	f := newAsyncListRecordingFixture(t)

	offered, err := f.handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered, "accepted offer must report true")
	require.Len(t, f.pub.offered, 1)
	f.requireOfferedListRecording(t)
}

func TestAsyncListRecording_TryOffer_retriableThenAccepted(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "back_pressured", status: aeron.BackPressured},
		{name: "admin_action", status: aeron.AdminAction},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncListRecordingFixture(t)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.NoError(t, err, "retriable offer must not be terminal")
			require.False(t, offered, "retriable offer must not report accepted")
			require.Len(t, f.pub.offered, 1)
			f.requireOfferedListRecording(t)

			f.pub.offerResult = 0
			offered, err = f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered, "accepted offer must report true")
			require.Len(t, f.pub.offered, 2)
			f.requireOfferedListRecording(t)
		})
	}
}

func TestAsyncListRecording_TryOffer_terminal(t *testing.T) {
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
			f := newAsyncListRecordingFixture(t)
			f.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.False(t, offered, "terminal offer must not report accepted")
			requireControlTerminal(t, err, tc.status)
			require.Len(t, f.pub.offered, 1)
			f.requireOfferedListRecording(t)
		})
	}
}

func TestAsyncListRecording_PollDescriptor_matchingDescriptorCompletes(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)

	want := f.descriptorFor(f.handle)
	f.deliverDescriptor(t, want)

	requirePollDescriptor(t, f.handle, true, want, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "descriptor must not latch the control poller")
}

func TestAsyncListRecording_PollDescriptor_OKThenDescriptor(t *testing.T) {
	f := newAsyncListRecordingFixture(t)

	// Phase: PollDescriptor consumes a matching OK and keeps waiting.
	f.deliverResponse(t, f.okFor(f.handle))
	require.True(t, f.controlPoller.IsPollComplete())
	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "matching OK must be consumed")

	// Phase: the descriptor completes the handle.
	want := f.descriptorFor(f.handle)
	f.deliverDescriptor(t, want)
	requirePollDescriptor(t, f.handle, true, want, nil)
	require.False(t, f.controlPoller.IsPollComplete())
}

func TestAsyncListRecording_PollDescriptor_descriptorWinsOverError(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	want := f.descriptorFor(f.handle)
	f.deliverDescriptor(t, want)
	f.deliverResponse(t, f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("late"),
	))
	require.True(t, f.controlPoller.IsPollComplete())

	requirePollDescriptor(t, f.handle, true, want, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "matching terminal must clear on completion")
}

func TestAsyncListRecording_PollDescriptor_errorResponses(t *testing.T) {
	cases := []struct {
		name          string
		code          archivewire.ControlResponseCode
		relevantID    int64
		errorMessage  []byte
		wantErrorCode int32
	}{
		{
			name:          "recording_unknown",
			code:          archivewire.ControlResponseCode_RECORDING_UNKNOWN,
			relevantID:    asyncListRecordingTestRecordingID,
			wantErrorCode: ErrorUnknownRecording,
		},
		{
			name:          "error",
			code:          archivewire.ControlResponseCode_ERROR,
			relevantID:    ErrorGeneric,
			errorMessage:  []byte("refused"),
			wantErrorCode: ErrorGeneric,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncListRecordingFixture(t)
			fields := f.responseFor(f.handle, tc.code, tc.relevantID, tc.errorMessage)
			f.deliverResponse(t, fields)

			want := MapControlResponse(fields)
			requirePollDescriptor(t, f.handle, true, codecs.RecordingDescriptorFields{}, want)
			code, ok := ExceptionCode(want)
			require.True(t, ok)
			require.Equal(t, tc.wantErrorCode, code)
			require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")
			require.False(t, f.descriptorPoller.IsPollComplete())
		})
	}
}

func TestAsyncListRecording_PollDescriptor_descriptorModeError(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	fields := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_RECORDING_UNKNOWN,
		asyncListRecordingTestRecordingID,
		nil,
	)
	invokeHandleControlFragment(
		t,
		f.client.listRecordingPollTargets(),
		encodeControlResponseWire(t, fields),
	)
	require.True(t, f.descriptorPoller.IsPollComplete())
	require.False(t, f.controlPoller.IsPollComplete())

	requirePollDescriptor(
		t,
		f.handle,
		true,
		codecs.RecordingDescriptorFields{},
		MapControlResponse(fields),
	)
}

func TestAsyncListRecording_PollDescriptor_foreignLeavesPending(t *testing.T) {
	f := newAsyncListRecordingFixture(t)

	foreignDescriptor := f.descriptorFor(f.handle)
	foreignDescriptor.CorrelationID++
	f.deliverDescriptor(t, foreignDescriptor)
	require.False(t, f.descriptorPoller.IsPollComplete(), "armed filter must drop other correlation")

	foreignResponse := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("foreign"),
	)
	foreignResponse.CorrelationID++
	f.deliverResponse(t, foreignResponse)
	require.False(t, f.controlPoller.IsPollComplete(), "armed filter must drop other correlation")

	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
}

func TestAsyncListRecording_PollDescriptor_discardsStaleTerminal(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	stale := f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		11,
		[]byte("stale"),
	)
	stale.ControlSessionID++
	f.deliverResponse(t, stale)
	require.True(t, f.controlPoller.IsPollComplete())

	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

	want := f.descriptorFor(f.handle)
	f.deliverDescriptor(t, want)
	requirePollDescriptor(t, f.handle, true, want, nil)
}

func TestAsyncListRecording_Close_disarmsBothFilters(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	f.handle.Close()
	require.Equal(t, int64(0), f.controlPoller.awaitCorrelationID)
	require.Equal(t, conductor.NullCorrelationID, f.descriptorPoller.awaitCorrelationID)

	f.deliverDescriptor(t, f.descriptorFor(f.handle))
	require.False(
		t,
		f.descriptorPoller.IsPollComplete(),
		"disarmed descriptor poller must match nothing",
	)
}

func TestAsyncListRecording_Close_clearsLatchedCaptures(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	f.deliverDescriptor(t, f.descriptorFor(f.handle))
	f.deliverResponse(t, f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("boom"),
	))
	require.True(t, f.descriptorPoller.IsPollComplete())
	require.True(t, f.controlPoller.IsPollComplete())

	f.handle.Close()
	require.False(t, f.descriptorPoller.IsPollComplete(), "Close must clear the descriptor capture")
	require.Equal(t, codecs.RecordingDescriptorFields{}, f.descriptorPoller.Descriptor())
	require.False(t, f.controlPoller.IsPollComplete(), "Close must clear matching terminal")

	f.handle.Close()
	require.Equal(t, conductor.NullCorrelationID, f.descriptorPoller.awaitCorrelationID)
}

func TestAsyncListRecording_Close_keepsSuccessorFilters(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	first := f.handle
	second := f.newHandle(t)
	require.NotEqual(t, first.correlationID, second.correlationID)
	want := f.descriptorFor(second)
	f.deliverDescriptor(t, want)

	first.Close()
	require.Equal(
		t,
		second.correlationID,
		f.controlPoller.awaitCorrelationID,
		"Close of prior handle must not disarm successor control filter",
	)
	require.Equal(
		t,
		second.correlationID,
		f.descriptorPoller.awaitCorrelationID,
		"Close of prior handle must not disarm successor descriptor filter",
	)
	requirePollDescriptor(t, second, true, want, nil)
}

func TestAsyncListRecording_PollDescriptor_openPriorIgnoresSuccessorDescriptor(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	first := f.handle
	second := f.newHandle(t)
	want := f.descriptorFor(second)
	f.deliverDescriptor(t, want)
	require.True(t, f.descriptorPoller.IsPollComplete())

	requirePollDescriptor(t, first, false, codecs.RecordingDescriptorFields{}, nil)
	requirePollDescriptor(t, second, true, want, nil)
}

func TestAsyncListRecording_closedDoesNotOfferOrMatch(t *testing.T) {
	var nilHandle *AsyncListRecording
	offered, err := nilHandle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "nil TryOffer must not accept")
	requirePollDescriptor(t, nilHandle, false, codecs.RecordingDescriptorFields{}, nil)
	nilHandle.Close()

	f := newAsyncListRecordingFixture(t)
	f.handle.Close()

	offered, err = f.handle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "closed TryOffer must not accept")
	require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

	// Phase: the closed handle ignores a successor's captures.
	successor := f.newHandle(t)
	f.deliverResponse(t, f.responseFor(
		successor,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("boom"),
	))
	f.deliverDescriptor(t, f.descriptorFor(successor))

	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
	require.True(
		t,
		f.descriptorPoller.IsPollComplete(),
		"closed handle must leave successor descriptor",
	)
	require.True(t, f.controlPoller.IsPollComplete(), "closed handle must leave successor response")
}
