//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncListRecordingTestRecordingID int64 = 42

var asyncListRecordingKind = asyncHandleKind[*AsyncListRecording]{
	build: func(client *Client) *AsyncListRecording {
		return client.NewAsyncListRecording(asyncListRecordingTestRecordingID)
	},
	correlationID: func(handle *AsyncListRecording) int64 {
		return handle.correlationID
	},
	relevantID:      0,
	descriptorArmed: true,
	requireOffered:  requireOfferedListRecording,
}

func newAsyncListRecordingFixture(t *testing.T) *asyncHandleFixture[*AsyncListRecording] {
	t.Helper()
	return newAsyncHandleFixture(t, asyncListRecordingKind)
}

func asyncListDescriptorFor(handle *AsyncListRecording) codecs.RecordingDescriptorFields {
	return codecs.RecordingDescriptorFields{
		ControlSessionID: handle.client.controlSessionID,
		CorrelationID:    handle.correlationID,
		RecordingID:      asyncListRecordingTestRecordingID,
		StartPosition:    4096,
		StopPosition:     8192,
		StrippedChannel:  []byte("aeron:ipc"),
		OriginalChannel:  []byte("aeron:ipc?alias=log"),
		SourceIdentity:   []byte("source-identity"),
	}
}

func deliverAsyncListDescriptor(
	t *testing.T,
	f *asyncHandleFixture[*AsyncListRecording],
	handle *AsyncListRecording,
) codecs.RecordingDescriptorFields {
	t.Helper()

	fields := asyncListDescriptorFor(handle)
	f.deliver(t, encodeRecordingDescriptorWire(t, fields))
	return fields
}

func requireOfferedListRecording(
	t *testing.T,
	client *Client,
	handle *AsyncListRecording,
	wire []byte,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.ListRecordingRequestFields](
		t,
		wire,
		archivewire.ListRecordingRequestSbeTemplateID,
	)
	require.Equal(t, client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.correlationID, fields.CorrelationID)
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

func TestAsyncListRecording_PollDescriptor_matchingDescriptorCompletes(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)

	want := deliverAsyncListDescriptor(t, f, f.handle)

	requirePollDescriptor(t, f.handle, true, want, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "descriptor must not latch the control poller")
}

func TestAsyncListRecording_PollDescriptor_OKThenDescriptor(t *testing.T) {
	f := newAsyncListRecordingFixture(t)

	f.latchResponse(t, f.okFor(f.handle))
	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "matching OK must be consumed")

	want := deliverAsyncListDescriptor(t, f, f.handle)
	requirePollDescriptor(t, f.handle, true, want, nil)
	require.False(t, f.controlPoller.IsPollComplete())
}

func TestAsyncListRecording_PollDescriptor_descriptorWinsOverError(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	want := deliverAsyncListDescriptor(t, f, f.handle)
	f.latchResponse(t, f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("late"),
	))

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

	foreignDescriptor := asyncListDescriptorFor(f.handle)
	foreignDescriptor.CorrelationID++
	f.deliver(t, encodeRecordingDescriptorWire(t, foreignDescriptor))
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
	f.latchResponse(t, stale)

	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
	require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

	want := deliverAsyncListDescriptor(t, f, f.handle)
	requirePollDescriptor(t, f.handle, true, want, nil)
}

func TestAsyncListRecording_Close_disarmsBothFilters(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	f.handle.Close()
	require.Equal(t, int64(0), f.controlPoller.awaitCorrelationID)
	require.Equal(t, conductor.NullCorrelationID, f.descriptorPoller.awaitCorrelationID)

	deliverAsyncListDescriptor(t, f, f.handle)
	require.False(
		t,
		f.descriptorPoller.IsPollComplete(),
		"disarmed descriptor poller must match nothing",
	)
}

func TestAsyncListRecording_Close_clearsLatchedCaptures(t *testing.T) {
	f := newAsyncListRecordingFixture(t)
	deliverAsyncListDescriptor(t, f, f.handle)
	f.latchResponse(t, f.responseFor(
		f.handle,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("boom"),
	))
	require.True(t, f.descriptorPoller.IsPollComplete())

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
	want := deliverAsyncListDescriptor(t, f, second)

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
	want := deliverAsyncListDescriptor(t, f, second)
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

	successor := f.newHandle(t)
	f.deliverResponse(t, f.responseFor(
		successor,
		archivewire.ControlResponseCode_ERROR,
		0,
		[]byte("boom"),
	))
	deliverAsyncListDescriptor(t, f, successor)

	requirePollDescriptor(t, f.handle, false, codecs.RecordingDescriptorFields{}, nil)
	require.True(
		t,
		f.descriptorPoller.IsPollComplete(),
		"closed handle must leave successor descriptor",
	)
	require.True(t, f.controlPoller.IsPollComplete(), "closed handle must leave successor response")
}
