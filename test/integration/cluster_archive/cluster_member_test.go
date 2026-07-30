//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// Fixture smoke endpoints. Disjoint from three-node module tests that use
// ClusterModulePortBase 25000 and ClusterModuleLogEndpoint.
const (
	clusterMemberSetSmokePortBase    = 25300
	clusterMemberSetSmokeLogEndpoint = "239.255.12.9:25304"
)

// TestClusterMemberSet_opensAndClosesCleanly asserts a two-member set opens,
// every member holds every peer log image by session id, one Archive runs per
// member on distinct directories, and Close is idempotent.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestClusterMemberSet_opensAndClosesCleanly(t *testing.T) {
	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          2,
		PortBase:             clusterMemberSetSmokePortBase,
		LogMulticastEndpoint: clusterMemberSetSmokeLogEndpoint,
	})
	require.Len(t, set.Modules(), 2)
	require.Len(t, set.ArchiveNodes(), 2)
	require.NotEmpty(t, set.MembersString())

	nodes := set.ArchiveNodes()
	require.NotEmpty(t, nodes[0].Dir)
	require.NotEmpty(t, nodes[1].Dir)
	require.NotEqual(t, nodes[0].Dir, nodes[1].Dir)
	require.NotEmpty(t, nodes[0].ArchiveID)
	require.NotEmpty(t, nodes[1].ArchiveID)

	for i, m := range set.Modules() {
		archiveClient := m.ArchiveClient()
		require.NotNil(t, archiveClient, "member %d archive client", i)
		require.Positive(t, archiveClient.ControlSessionID(), "member %d control session", i)
	}

	require.True(t, set.AwaitLogImages(30*time.Second), "log images not ready:\n%s", set.State())

	for _, m := range set.Modules() {
		require.Equal(t, len(set.Modules()), m.LogStreams().Subscription().ImageCount())
	}

	// Short deadline bound with ok always false. Asserts the timeout path.
	require.False(t, set.PumpTransportUntil(time.Millisecond, func() bool { return false }))

	require.NoError(t, set.Close())
	require.NoError(t, set.Close())
}
