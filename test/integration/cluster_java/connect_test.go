//go:build integration && cluster_java

package cluster_java_test

import (
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	clientpkg "github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

const connectSmokeTimeout = 60 * time.Second

// TestClusterConnectSmoke proves AsyncConnect session establishment against live Java cluster.
// Does not assert offer/egress delivery or service handler behavior.
func TestClusterConnectSmoke(t *testing.T) {
	if os.Getenv("HERON_CLUSTER_JAVA_COMPOSE") != "1" {
		t.Skip("cluster java requires docker compose; run make test-cluster-java")
	}

	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)
	require.Equal(t, strconv.Itoa(logbuffer.TermMinLength), os.Getenv("AERON_IPC_TERM_BUFFER_LENGTH"))

	md := harness.JoinExternal(t, dir)
	md.AssertAlive(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)
	require.False(t, hadClientError.Load(), "transport errors before connect")

	ctx := clusterjava.HarnessClusterContext()
	ctx.AeronDir = dir
	ctx.Aeron = client
	// REQUIRED: NewAsyncConnect normalizes ctx; zero MessageTimeout becomes
	// DefaultMessageTimeout (5s). Override to 60s before connect for compose cold-start flakes.
	ctx.MessageTimeout = connectSmokeTimeout

	ac, err := clientpkg.NewAsyncConnect(ctx)
	require.NoError(t, err)
	harness.CleanupClose(t, "close async connect", ac.Close)

	result := pollAsyncConnectSmoke(t, client, ac, hadClientError, connectSmokeTimeout)

	// Channel-only IPC (single-node): concludeConnect closes non-leader member pubs;
	// only close handles transferred into ConnectResult. Do not iterate MemberIngressMap
	// or close result.Aeron (borrowed client; harness ConnectIPCClient cleanup).
	harness.CleanupClose(t, "close ingress publication", func() error {
		if result.IngressPublication == nil {
			return nil
		}
		return result.IngressPublication.Close()
	})
	harness.CleanupClose(t, "close egress subscription", func() error {
		if result.EgressSubscription == nil {
			return nil
		}
		return result.EgressSubscription.Close()
	})

	requireConnectSmokeResult(t, client, result)

	t.Logf(
		"cluster connect smoke passed: clusterSessionId=%d leaderMemberId=%d leadershipTermId=%d",
		result.ClusterSessionID, result.LeaderMemberID, result.LeadershipTermID,
	)
}

func pollAsyncConnectSmoke(
	t *testing.T,
	client *aeron.Client,
	ac *clientpkg.AsyncConnect,
	hadClientError *atomic.Bool,
	timeout time.Duration,
) *clientpkg.ConnectResult {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		require.False(t, hadClientError.Load(), "transport error during connect poll")
		client.DoWork()
		result, err := ac.Poll()
		if err != nil {
			t.Fatalf(
				"cluster connect failed: %v (state=%v step=%v hadClientError=%v)",
				err, ac.State(), ac.Step(), hadClientError.Load(),
			)
		}
		if result != nil {
			return result
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	t.Fatalf(
		"cluster connect poll timed out: state=%v step=%v hadClientError=%v",
		ac.State(), ac.Step(), hadClientError.Load(),
	)
	return nil
}

func requireConnectSmokeResult(
	t *testing.T,
	client *aeron.Client,
	result *clientpkg.ConnectResult,
) {
	t.Helper()

	require.Positive(t, result.ClusterSessionID)
	require.Equal(t, int32(clusterjava.MemberID), result.LeaderMemberID)
	// In a fresh single-node Aeron cluster, the first leadership term is 0.
	require.Equal(t, int64(0), result.LeadershipTermID)
	require.False(t, result.OwnsAeronClient)
	require.Same(t, client, result.Aeron)
	require.True(t, result.IngressPublication.IsConnected())
	require.True(t, result.EgressSubscription.IsConnected())
	require.NotNil(t, result.EgressImage)
	require.Equal(t, clusterjava.IngressChannel, result.Context.IngressChannel)
	require.Equal(t, clusterjava.EgressChannel, result.Context.EgressChannel)
	require.Equal(t, clusterjava.IngressStreamID, result.Context.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, result.Context.ResponseStreamID)
	require.Equal(t, clusterjava.IngressEndpoints, result.Context.IngressEndpoints)
}
