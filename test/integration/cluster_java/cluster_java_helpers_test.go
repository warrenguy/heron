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
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

const connectSmokeTimeout = 60 * time.Second

func requireComposeEnv(t *testing.T) (string, *atomic.Bool, *aeron.Client) {
	t.Helper()

	if os.Getenv("HERON_CLUSTER_JAVA_COMPOSE") != "1" {
		t.Skip("cluster java requires docker compose; run make test-cluster-java")
	}

	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)
	require.Equal(
		t,
		strconv.Itoa(clusterjava.TermMinLength),
		os.Getenv("AERON_IPC_TERM_BUFFER_LENGTH"),
	)

	md := harness.JoinExternal(t, dir)
	md.AssertAlive(t)
	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)
	require.False(t, hadClientError.Load(), "transport errors before connect")
	return dir, hadClientError, aeronClient
}

// Pass untyped nil for egressHandler, not (*client.EgressHandlers)(nil).
func clusterContextForSmoke(
	dir string,
	aeronClient *aeron.Client,
	egressHandler client.EgressHandler,
) client.ClusterContext {
	ctx := clusterjava.HarnessClusterContext()
	ctx.AeronDir = dir
	ctx.Aeron = aeronClient
	ctx.MessageTimeout = connectSmokeTimeout // compose cold-start; avoids 5s default
	ctx.EgressHandler = egressHandler
	return ctx
}

func asyncConnectUntilResult(
	t *testing.T,
	egressHandler client.EgressHandler,
) (*aeron.Client, *atomic.Bool, *client.ConnectResult) {
	t.Helper()

	dir, hadClientError, aeronClient := requireComposeEnv(t)
	ctx := clusterContextForSmoke(dir, aeronClient, egressHandler)

	ac, err := client.NewAsyncConnect(ctx)
	require.NoError(t, err)
	harness.CleanupClose(t, "close async connect", ac.Close)

	result := pollAsyncConnectUntilResult(t, aeronClient, ac, hadClientError)
	return aeronClient, hadClientError, result
}

func pollAsyncConnectUntilResult(
	t *testing.T,
	aeronClient *aeron.Client,
	ac *client.AsyncConnect,
	hadClientError *atomic.Bool,
) *client.ConnectResult {
	t.Helper()

	deadline := time.Now().Add(connectSmokeTimeout)
	for time.Now().Before(deadline) {
		require.False(t, hadClientError.Load(), "transport error during connect poll")
		aeronClient.DoWork()
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

func cleanupConnectResultHandles(t *testing.T, result *client.ConnectResult) {
	t.Helper()

	harness.CleanupClose(t, "close ingress publication", result.IngressPublication.Close)
	harness.CleanupClose(t, "close egress subscription", result.EgressSubscription.Close)
}

func requireConnectSmokeResult(
	t *testing.T,
	aeronClient *aeron.Client,
	result *client.ConnectResult,
) {
	t.Helper()

	require.Positive(t, result.ClusterSessionID)
	require.Equal(t, int32(clusterjava.MemberID), result.LeaderMemberID)
	require.Equal(t, int64(0), result.LeadershipTermID)
	require.False(t, result.OwnsAeronClient)
	require.Same(t, aeronClient, result.Aeron)
	require.True(t, result.IngressPublication.IsConnected())
	require.True(t, result.EgressSubscription.IsConnected())
	require.NotNil(t, result.EgressImage)
	require.Equal(t, clusterjava.IngressChannel, result.Context.IngressChannel)
	require.Equal(t, clusterjava.EgressChannel, result.Context.EgressChannel)
	require.Equal(t, clusterjava.IngressStreamID, result.Context.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, result.Context.ResponseStreamID)
	require.Equal(t, clusterjava.IngressEndpoints, result.Context.IngressEndpoints)
}
