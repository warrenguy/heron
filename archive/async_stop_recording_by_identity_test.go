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
		return handle.request.correlationID
	},
	relevantID:     1,
	requireOffered: requireOfferedStopRecordingByIdentity,
	isStopped:      (*AsyncStopRecordingByIdentity).IsStopped,
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
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, asyncStopRecordingByIdentityTestRecordingID, fields.RecordingID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopRecordingByIdentityRequestSbeBlockLength,
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStopRecordingByIdentity_PollResponse_matchingOKConsumed(t *testing.T) {
	cases := []struct {
		name            string
		relevantID      int64
		laterRelevantID int64
		wantStopped     bool
	}{
		{name: "stopped_active_recording", relevantID: 1, laterRelevantID: 0, wantStopped: true},
		{name: "no_subscription_removed", relevantID: 0, laterRelevantID: 1, wantStopped: false},
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

			// Phase: a later OK stays unread.
			f.latchResponse(t, f.okWithRelevantID(f.handle, tc.laterRelevantID))
			responded, err = f.handle.PollResponse()
			require.NoError(t, err)
			require.True(t, responded, "responded handle stays complete")
			require.True(t, f.controlPoller.IsPollComplete(), "responded handle must not read a later OK")
			require.Equal(t, tc.wantStopped, f.handle.IsStopped(), "a later OK must not change IsStopped")

			// Phase: Close clears the latch.
			f.handle.Close()
			require.False(t, f.controlPoller.IsPollComplete(), "Close must clear the matching latch")
			require.Equal(t, tc.wantStopped, f.handle.IsStopped(), "Close must keep the result")
		})
	}
}
