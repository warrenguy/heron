//go:build bench

package harness

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster/client"
)

func TestNewAeronClusterWithEgressForBench_dispatchesMessage(t *testing.T) {
	payload := []byte("egress-bench-payload")

	var (
		calls            int
		clusterSessionID int64
		timestamp        int64
		delivered        []byte
	)
	handler := &client.EgressHandlers{
		MessageFunc: func(
			gotClusterSessionID int64,
			gotTimestamp int64,
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			_ *aeron.Header,
		) {
			calls++
			clusterSessionID = gotClusterSessionID
			timestamp = gotTimestamp
			delivered = bytes.Clone(buffer.Bytes()[offset : offset+length])
		},
	}

	ac, image := NewAeronClusterWithEgressForBench(payload, handler)

	// First poll: the frame reaches the handler.
	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, 1, calls)
	require.Equal(t, BenchClusterSessionID, clusterSessionID)
	require.Equal(t, benchEgressTimestamp, timestamp)
	require.Equal(t, payload, delivered)

	// Poll at the tail: the image holds one frame only.
	require.Equal(t, 0, ac.PollEgress())
	require.Equal(t, 1, calls)

	// Poll after the rewind: the same frame reaches the handler again.
	aeron.SetImageSubscriberPositionForBench(image, 0)
	require.Equal(t, 1, ac.PollEgress())
	require.Equal(t, 2, calls)
	require.Equal(t, payload, delivered)

	require.Equal(t, client.SessionStateConnected, ac.SessionState())
}
