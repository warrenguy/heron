//go:build integration && cluster_java

package cluster_java_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	agronaatomic "github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/test/integration/harness"
)

const echoSmokeTimeout = 30 * time.Second

var echoPingPayload = []byte("heron-cluster-echo-ping")

// TestClusterEchoSmoke proves byte-identical echo against live Java EchoService.
func TestClusterEchoSmoke(t *testing.T) {
	var echoed []byte
	handlers := &client.EgressHandlers{
		MessageFunc: func(
			_, _ int64,
			buf agronaatomic.AtomicBuffer,
			offset, length int,
			_ *aeron.Header,
		) {
			echoed = append([]byte(nil), buf.Slice(offset, length).Bytes()...)
		},
	}

	aeronClient, hadClientError, result := asyncConnectUntilResult(t, handlers)

	requireConnectSmokeResult(t, aeronClient, result)

	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	require.Nil(t, result.IngressPublication)
	require.Nil(t, result.EgressSubscription)
	require.Nil(t, result.EgressImage)

	harness.CleanupClose(t, "close aeron cluster", session.Close)

	require.Equal(t, client.SessionStateConnected, session.SessionState())

	pos := offerUntilPositive(t, aeronClient, session, echoPingPayload, hadClientError)
	pollEchoUntilMessage(t, aeronClient, session, hadClientError, &echoed)

	require.Equal(t, echoPingPayload, echoed)
	require.False(t, hadClientError.Load(), "transport errors during echo smoke")

	t.Logf(
		"cluster echo smoke passed: clusterSessionId=%d offerPosition=%d echoLength=%d",
		result.ClusterSessionID, pos, len(echoed),
	)
}

// offerUntilPositive spins session.Offer like harness.OfferWithRetry on
// BackPressured and AdminAction only. Cluster session-guard codes
// (NotConnected, Closed, etc.) fail immediately; IPC OfferWithRetry retries
// NotConnected. Checks hadClientError each spin.
func offerUntilPositive(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	payload []byte,
	hadClientError *atomic.Bool,
) int64 {
	t.Helper()

	deadline := time.Now().Add(harness.ConnectedPollTimeout)
	var lastStatus int64
	for time.Now().Before(deadline) {
		require.False(t, hadClientError.Load(), "transport error during offer")
		aeronClient.DoWork()
		position := session.Offer(payload)
		if position > 0 {
			require.False(t, hadClientError.Load(), "transport error during offer")
			return position
		}
		lastStatus = position
		switch position {
		case aeron.BackPressured:
			time.Sleep(aeron.DefaultIdleSleepDuration)
		case aeron.AdminAction:
		default:
			t.Fatalf(
				"cluster offer failed: status=%d sessionState=%v hadClientError=%v",
				position, session.SessionState(), hadClientError.Load(),
			)
		}
	}
	t.Fatalf(
		"cluster offer timed out: lastStatus=%d sessionState=%v hadClientError=%v",
		lastStatus, session.SessionState(), hadClientError.Load(),
	)
	return 0
}

func pollEchoUntilMessage(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	echoed *[]byte,
) {
	t.Helper()

	deadline := time.Now().Add(echoSmokeTimeout)
	for time.Now().Before(deadline) {
		require.False(t, hadClientError.Load(), "transport error during echo poll")
		aeronClient.DoWork()
		session.PollEgress()
		if len(*echoed) > 0 {
			require.False(t, hadClientError.Load(), "transport error during echo poll")
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	t.Fatalf(
		"cluster echo poll timed out: echoedLen=%d sessionState=%v hadClientError=%v "+
			"(check compose logs cluster for heron: EchoService offer exhausted)",
		len(*echoed), session.SessionState(), hadClientError.Load(),
	)
}
