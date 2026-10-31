//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const asyncStopRecordingByIdentityTestRecordingID int64 = 42

var asyncStopRecordingByIdentityKind = asyncHandleKind[*AsyncStopRecordingByIdentity]{
	build: func(client *Client) *AsyncStopRecordingByIdentity {
		return client.NewAsyncStopRecordingByIdentity(asyncStopRecordingByIdentityTestRecordingID)
	},
	correlationID: func(handle *AsyncStopRecordingByIdentity) int64 {
		return handle.correlationID
	},
	relevantID:     1,
	requireOffered: requireOfferedStopRecordingByIdentity,
}

func newAsyncStopRecordingByIdentityFixture(
	t *testing.T,
) *asyncHandleFixture[*AsyncStopRecordingByIdentity] {
	t.Helper()
	return newAsyncHandleFixture(t, asyncStopRecordingByIdentityKind)
}

func requireOfferedStopRecordingByIdentity(
	t *testing.T,
	client *Client,
	handle *AsyncStopRecordingByIdentity,
	wire []byte,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.StopRecordingByIdentityRequestFields](
		t,
		wire,
		archivewire.StopRecordingByIdentityRequestSbeTemplateID,
	)
	require.Equal(t, client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.correlationID, fields.CorrelationID)
	require.Equal(t, asyncStopRecordingByIdentityTestRecordingID, fields.RecordingID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopRecordingByIdentityRequestSbeBlockLength,
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStopRecordingByIdentity_PollResponse_matchingOKConsumed(t *testing.T) {
	cases := []struct {
		name        string
		relevantID  int64
		wantStopped bool
	}{
		{name: "stopped_active_recording", relevantID: 1, wantStopped: true},
		{name: "no_subscription_removed", relevantID: 0, wantStopped: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopRecordingByIdentityFixture(t)

			responded, err := f.handle.PollResponse()
			require.NoError(t, err)
			require.False(t, responded, "empty capture is not a response")
			require.False(t, f.handle.IsStopped(), "IsStopped must be false before completion")

			f.latchResponse(t, f.okWithRelevantID(f.handle, tc.relevantID))

			responded, err = f.handle.PollResponse()
			require.NoError(t, err)
			require.True(t, responded, "matching OK must complete")
			require.Equal(t, tc.wantStopped, f.handle.IsStopped())
			require.False(t, f.controlPoller.IsPollComplete(), "matching OK must be consumed")

			responded, err = f.handle.PollResponse()
			require.NoError(t, err)
			require.False(t, responded, "consumed capture must not match again")
			require.Equal(t, tc.wantStopped, f.handle.IsStopped())
		})
	}
}

func TestAsyncStopRecordingByIdentity_PollResponse_laterOKKeepsStopped(t *testing.T) {
	f := newAsyncStopRecordingByIdentityFixture(t)

	f.latchResponse(t, f.okFor(f.handle))
	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "first OK must complete")
	require.True(t, f.handle.IsStopped())

	f.latchResponse(t, f.okWithRelevantID(f.handle, 0))
	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "second OK must match the still-armed handle")
	require.True(t, f.handle.IsStopped(), "a later OK with relevantId 0 must not clear the latch")

	f.handle.Close()
	require.True(t, f.handle.IsStopped(), "Close must keep the latched result")
}

func TestAsyncStopRecordingByIdentity_PollResponse_errorResponses(t *testing.T) {
	cases := []struct {
		name          string
		relevantID    int64
		errorMessage  []byte
		wantErrorCode int32
	}{
		{
			name:          "unknown_recording",
			relevantID:    ErrorUnknownRecording,
			errorMessage:  []byte("unknown recording id: 42"),
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
			f := newAsyncStopRecordingByIdentityFixture(t)
			fields := f.responseFor(
				f.handle,
				archivewire.ControlResponseCode_ERROR,
				tc.relevantID,
				tc.errorMessage,
			)
			f.latchResponse(t, fields)

			want := MapControlResponse(fields)
			responded, err := f.handle.PollResponse()
			require.True(t, responded, "matching error must complete")
			require.Equal(t, want, err)
			code, ok := ExceptionCode(err)
			require.True(t, ok)
			require.Equal(t, tc.wantErrorCode, code)
			require.False(t, f.handle.IsStopped(), "error must not report stopped")
			require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")

			responded, err = f.handle.PollResponse()
			require.True(t, responded, "latched error remains visible")
			require.Equal(t, want, err)
			require.True(t, f.controlPoller.IsPollComplete(), "error capture stays latched")
		})
	}
}

func TestAsyncStopRecordingByIdentity_PollResponse_discardsStaleTerminal(t *testing.T) {
	f := newAsyncStopRecordingByIdentityFixture(t)
	stale := f.okFor(f.handle)
	stale.ControlSessionID++
	f.latchResponse(t, stale)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "stale terminal is not a match")
	require.False(t, f.handle.IsStopped(), "stale OK must not report stopped")
	require.False(t, f.controlPoller.IsPollComplete(), "stale terminal must be discarded")

	f.latchResponse(t, f.okFor(f.handle))
	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching response after stale discard")
	require.True(t, f.handle.IsStopped())
	require.False(t, f.controlPoller.IsPollComplete())
}

func TestAsyncStopRecordingByIdentity_PollResponse_closedDoesNotTouchSuccessor(t *testing.T) {
	f := newAsyncStopRecordingByIdentityFixture(t)
	first := f.handle
	first.Close()

	second := f.newHandle(t)
	f.latchResponse(t, f.okFor(second))

	responded, err := first.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed handle must not match")
	require.False(t, first.IsStopped(), "closed handle must not report stopped")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollResponse must leave successor latch")
}

func TestAsyncStopRecordingByIdentity_Close_lateOKDoesNotReportStopped(t *testing.T) {
	f := newAsyncStopRecordingByIdentityFixture(t)
	f.handle.Close()

	f.latchResponse(t, f.okFor(f.handle))
	require.False(t, f.handle.IsStopped(), "late OK must not report stopped on closed handle")

	f.handle.Close()
	require.False(t, f.handle.IsStopped(), "second Close must not report stopped")
}

func TestAsyncStopRecordingByIdentity_closedDoesNotOfferOrMatch(t *testing.T) {
	var nilHandle *AsyncStopRecordingByIdentity
	offered, err := nilHandle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "nil TryOffer must not accept")
	responded, err := nilHandle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "nil PollResponse must not match")
	require.False(t, nilHandle.IsStopped(), "nil IsStopped must be false")
	nilHandle.Close()

	f := newAsyncStopRecordingByIdentityFixture(t)
	f.handle.Close()

	offered, err = f.handle.TryOffer()
	require.NoError(t, err)
	require.False(t, offered, "closed TryOffer must not accept")
	require.Empty(t, f.pub.offered, "closed TryOffer must not offer")

	f.latchResponse(t, f.okFor(f.handle))

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "closed PollResponse must not match")
	require.False(t, f.handle.IsStopped(), "closed handle must not report stopped")
	require.True(t, f.controlPoller.IsPollComplete(), "closed PollResponse must not consume")
}
