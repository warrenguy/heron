//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/test/integration/harness"
)

// requireLogTopology asserts every live member reports logMDC as its log
// mode. Under MDC each follower holds its own log endpoint as its live
// destination. Its log adapter image carries the leader session.
func requireLogTopology(
	t testing.TB,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	logMDC bool,
) {
	t.Helper()

	leaderSession := leader.LogStreams().Publication().SessionID()

	for i, m := range set.Modules() {
		if m == nil {
			continue
		}

		topology := m.Topology()
		require.Equal(t, logMDC, topology.IsLogMDC(), "member %d IsLogMDC:\n%s", i, set.State())

		if !logMDC || m == leader {
			continue
		}

		require.Equal(
			t,
			topology.LogChannel(),
			m.Agent().LiveLogDestination(),
			"member %d live log destination:\n%s",
			i,
			set.State(),
		)

		image := m.LogAdapter().Image()
		require.NotNil(t, image, "member %d log adapter image:\n%s", i, set.State())
		require.Equal(
			t,
			leaderSession,
			image.SessionID(),
			"member %d log adapter image session:\n%s",
			i,
			set.State(),
		)
	}
}

// TestDurableCluster_catchesUpAFollowerOnAUnicastLog runs the follower
// catchup scenario with each member receiving the log at its own endpoint.
func TestDurableCluster_catchesUpAFollowerOnAUnicastLog(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(t, harness.DurableUnicastCatchupPortBase, "")
	runFollowerCatchup(t, set, true)
}

// TestDurableCluster_rejoinsTheExLeaderOnAUnicastLog runs the outgoing leader
// restart scenario with each member receiving the log at its own endpoint.
func TestDurableCluster_rejoinsTheExLeaderOnAUnicastLog(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(t, harness.DurableUnicastExLeaderPortBase, "")
	runOutgoingLeaderRestart(t, set, true)
}
