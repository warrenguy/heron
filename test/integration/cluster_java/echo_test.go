//go:build integration && cluster_java

package cluster_java_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterEchoSmoke proves byte-identical echo against live Java EchoService.
func TestClusterEchoSmoke(t *testing.T) {
	handlers, echoed := newEchoCaptureHandlers()

	aeronClient, hadClientError, result := asyncConnectUntilResult(t, handlers)

	requireConnectSmokeResult(t, aeronClient, result)

	session := newConnectedClusterSession(t, result)
	requireConnectResultHandlesTransferred(t, result)

	pos := roundTripEcho(
		t, aeronClient, session, hadClientError, echoed, echoPingPayload,
		harness.ConnectedPollTimeout, echoSmokeTimeout,
	)
	require.False(t, hadClientError.Load(), "transport errors during echo smoke")

	t.Logf(
		"cluster echo smoke passed: clusterSessionId=%d offerPosition=%d echoLength=%d",
		result.ClusterSessionID, pos, len(*echoed),
	)
}
