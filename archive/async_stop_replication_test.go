//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/archive/codecs"
	"luas.fast/archive/codecs/archivewire"
)

var asyncStopReplicationKind = asyncHandleKind[*AsyncStopReplication]{
	build: func(client *Client) *AsyncStopReplication {
		return client.NewAsyncStopReplication(asyncReplicateTestReplicationID)
	},
	correlationID: func(handle *AsyncStopReplication) int64 {
		return handle.request.correlationID
	},
	requireOffered: requireOfferedStopReplication,
	isStopped:      (*AsyncStopReplication).IsStopped,
}

func newAsyncStopReplicationFixture(t *testing.T) *asyncHandleFixture[*AsyncStopReplication] {
	t.Helper()
	return newAsyncHandleFixture(t, asyncStopReplicationKind)
}

func requireOfferedStopReplication(
	t *testing.T,
	client *Client,
	handle *AsyncStopReplication,
	wire []byte,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.StopReplicationRequestFields](
		t,
		wire,
		archivewire.StopReplicationRequestSbeTemplateID,
	)
	require.Equal(t, client.controlSessionID, fields.ControlSessionID)
	require.Equal(t, handle.request.correlationID, fields.CorrelationID)
	require.Equal(t, asyncReplicateTestReplicationID, fields.ReplicationID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopReplicationRequestSbeBlockLength,
	)
	require.True(t, lenOK)
	require.Len(t, wire, expectedLen)
}

func TestAsyncStopReplication_PollResponse_matchingOKConsumed(t *testing.T) {
	f := newAsyncStopReplicationFixture(t)

	responded, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, responded, "empty capture is not a response")
	require.False(t, f.handle.IsStopped(), "IsStopped must be false before completion")

	f.latchResponse(t, f.okFor(f.handle))

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "matching OK must complete")
	require.True(t, f.handle.IsStopped())
	require.False(t, f.controlPoller.IsPollComplete(), "matching OK must be consumed")

	responded, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, responded, "responded handle stays complete")
	require.True(t, f.handle.IsStopped())
}

func TestAsyncStopReplication_PollResponse_unknownReplicationConsumed(t *testing.T) {
	cases := []struct {
		name    string
		priorOK bool
	}{
		{name: "alone"},
		{name: "after_ok_not_read", priorOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncStopReplicationFixture(t)
			if tc.priorOK {
				f.latchResponse(t, f.okFor(f.handle))
				responded, err := f.handle.PollResponse()
				require.NoError(t, err)
				require.True(t, responded, "first OK must complete")
			}

			f.latchResponse(t, f.responseFor(
				f.handle,
				archivewire.ControlResponseCode_ERROR,
				ErrorUnknownReplication,
				[]byte("unknown replication"),
			))
			responded, err := f.handle.PollResponse()
			require.NoError(t, err, "unknown replication is an outcome")
			require.True(t, responded, "unknown replication must complete")
			require.Equal(
				t,
				tc.priorOK,
				f.controlPoller.IsPollComplete(),
				"only a responded handle leaves the later response latched",
			)
			require.Equal(t, tc.priorOK, f.handle.IsStopped())

			offered, err := f.handle.TryOffer()
			require.NoError(t, err)
			require.False(t, offered, "responded TryOffer must not accept")
			require.Empty(t, f.pub.offered, "responded TryOffer must not offer")

			f.handle.Close()
			require.False(t, f.controlPoller.IsPollComplete(), "Close must clear the matching latch")
			require.Equal(t, tc.priorOK, f.handle.IsStopped(), "Close must keep the result")
		})
	}
}
