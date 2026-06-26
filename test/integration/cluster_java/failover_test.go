//go:build integration && cluster_java

package cluster_java_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterFailoverSmoke proves session continuity and byte-identical echo across leader kill.
func TestClusterFailoverSmoke(t *testing.T) {
	handlers, echoed := newEchoCaptureHandlers()

	hadClientError, aeronClient, md, result, composeDir := connectClusterUntilResult(
		t,
		clusterJavaFailoverComposeGate,
		clusterjava.FailoverHarnessClusterContext,
		failoverConnectTimeout,
		handlers,
	)
	defer md.AssertAlive(t)
	session := newConnectedClusterSession(t, result)
	requireConnectResultHandlesTransferred(t, result)

	sessionID := result.ClusterSessionID
	termBefore := session.LeadershipTermID()
	leaderBefore := session.LeaderMemberID()
	require.GreaterOrEqual(t, leaderBefore, int32(0))
	require.Less(
		t, int(leaderBefore), clusterjava.FailoverMemberCount,
		"fresh 3-node cluster must elect a valid leader "+
			"(not deterministic member 0 under simultaneous start)",
	)
	t.Logf("pre-failover: leader=%d term=%d session=%d", leaderBefore, termBefore, sessionID)

	roundTripEcho(
		t, aeronClient, session, hadClientError, echoed, echoPingPayload,
		harness.ConnectedPollTimeout, echoSmokeTimeout,
	)

	killClusterMemberContainer(t, leaderBefore, composeDir)

	pollUntilFailoverRecovered(t, aeronClient, session, hadClientError, PollConfig{
		KilledLeader: leaderBefore,
		SessionID:    sessionID,
		MinTerm:      termBefore + 1,
		Timeout:      failoverRecoveryTimeout,
	})

	*echoed = nil
	roundTripEcho(
		t, aeronClient, session, hadClientError, echoed, echoPostFailoverPayload,
		failoverPostRecoveryRoundTripTimeout, failoverPostRecoveryRoundTripTimeout,
	)
	require.False(t, hadClientError.Load())
}
