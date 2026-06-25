//go:build integration && cluster_java

package cluster_java_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClusterConnectSmoke proves AsyncConnect session establishment against live Java cluster.
// Does not assert offer/egress delivery or service handler behavior.
func TestClusterConnectSmoke(t *testing.T) {
	aeronClient, hadClientError, result := asyncConnectUntilResult(t, nil)

	cleanupConnectResultHandles(t, result)

	requireConnectSmokeResult(t, aeronClient, result)
	require.False(t, hadClientError.Load(), "transport errors during connect smoke")

	t.Logf(
		"cluster connect smoke passed: clusterSessionId=%d leaderMemberId=%d leadershipTermId=%d",
		result.ClusterSessionID, result.LeaderMemberID, result.LeadershipTermID,
	)
}
