//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterMemberSet_opensAndClosesCleanly asserts a two-member set opens,
// every member holds every peer log image by session id, one Archive runs per
// member on distinct directories, and Close is idempotent.
func TestClusterMemberSet_opensAndClosesCleanly(t *testing.T) {
	t.Parallel()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          2,
		PortBase:             harness.ClusterMemberSmokePortBase,
		LogMulticastEndpoint: harness.ClusterMemberSmokeLogEndpoint,
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

// TestClusterMemberSet_stopAndRestartAffordance asserts harness stop and restart
// on one two-member set:
//   - out-of-range and occupied-slot checks
//   - stopped-slot accessor behaviour
//   - restart reuses the stored config
//   - one cleanup per slot
//
// One boot runs every step in order. A set per step would repeat the two-driver,
// two-Archive boot for read-only asserts.
func TestClusterMemberSet_stopAndRestartAffordance(t *testing.T) {
	t.Parallel()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          2,
		PortBase:             harness.ClusterMemberRestartPortBase,
		LogMulticastEndpoint: harness.ClusterMemberRestartLogEndpoint,
	})

	// Bounds:
	//   - out-of-range indexes and an occupied slot are refused
	//   - the set is untouched
	n := len(set.Modules())
	require.Error(t, set.StopMember(-1))
	require.Error(t, set.StopMember(n))
	require.Error(t, set.RestartMember(-1))
	require.Error(t, set.RestartMember(n))
	require.ErrorContains(t, set.RestartMember(1), "call StopMember first")
	require.Len(t, set.Modules(), n)
	require.NotNil(t, set.Module(0))
	require.NotNil(t, set.Module(1))

	// Image fan-in before stop. Survivor has connected peer work for steps below.
	require.True(t, set.AwaitLogImages(30*time.Second), "log images not ready:\n%s", set.State())
	survivor := set.Module(0)
	require.NotNil(t, survivor)
	dirBefore := set.ClusterDir(1)
	require.NotEmpty(t, dirBefore)
	nodeBefore := set.ArchiveNodes()[1]

	// Stop: the slot empties and a second stop is a no-op.
	require.NoError(t, set.StopMember(1))
	require.Nil(t, set.Module(1))
	require.NoError(t, set.StopMember(1))

	// Stopped slot:
	//   - positional accessors keep length
	//   - per-member walks skip empty slots
	modules := set.Modules()
	require.Len(t, modules, 2)
	require.NotNil(t, modules[0])
	require.Nil(t, modules[1])
	nodes := set.ArchiveNodes()
	require.Len(t, nodes, 2)
	require.NotNil(t, nodes[0])
	require.NotNil(t, nodes[1])
	require.Equal(t, dirBefore, set.ClusterDir(1))
	require.NotPanics(t, func() { _ = set.Leaders() })
	require.Len(t, set.LogSessionIDs(), 1)

	// Tick on the empty slot must not panic. Nil slots are skipped and the
	// surviving client's duty cycle still runs.
	require.NotPanics(t, func() {
		set.Tick()
		survivor.Client().DoWork()
	})

	// Restart:
	//   - slot refills against the stored config
	//   - cluster directory, media driver, and Archive node are the first boot's
	require.NoError(t, set.RestartMember(1))
	m := set.Module(1)
	require.NotNil(t, m)
	require.Equal(t, int32(1), m.ThisMemberID())
	require.Equal(t, dirBefore, set.ClusterDir(1))
	require.NotNil(t, m.RecordingLog())
	require.NotNil(t, m.Client())
	require.Same(t, nodeBefore, set.ArchiveNodes()[1])
	archiveClient := m.ArchiveClient()
	require.NotNil(t, archiveClient)
	require.Positive(t, archiveClient.ControlSessionID())

	// Replacement stays open. The slot Cleanup registered at boot closes it at
	// teardown and fails the test when that close errors.
}
