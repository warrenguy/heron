//go:build integration && archive_live

package cluster_archive_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// Launcher smoke control endpoints. Isolated high ports far from
// ClusterModulePortBase (25000) and cluster member-set smoke (25300).
// No t.Parallel: fixed host UDP control ports.
const (
	archiveNodeSmokeControl0 = "aeron:udp?endpoint=127.0.0.1:28001"
	archiveNodeSmokeControl1 = "aeron:udp?endpoint=127.0.0.1:28101"
)

// TestArchiveNode_startsAndReportsReady asserts one Archive server starts
// against a per-test media driver and writes its ready marker.
func TestArchiveNode_startsAndReportsReady(t *testing.T) {
	md := harness.Start(t)
	node := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: archiveNodeSmokeControl0,
	})

	require.NotEmpty(t, node.Dir)
	require.NotEqual(t, md.Dir, node.Dir)
	require.NotEmpty(t, node.ArchiveID)

	readyPath := filepath.Join(node.Dir, harness.ArchiveReadyMarker)
	data, err := os.ReadFile(readyPath)
	require.NoError(t, err)
	require.Equal(t, node.ArchiveID, strings.TrimSpace(string(data)))

	dirAfterStop := node.Dir
	node.Stop()
	require.Equal(t, dirAfterStop, node.Dir)
	require.DirExists(t, node.Dir)
}

// TestArchiveNode_twoArchivesDistinctDirsAndEndpoints asserts two Archive
// servers on two drivers get distinct data directories and both report ready
// under distinct control endpoints.
func TestArchiveNode_twoArchivesDistinctDirsAndEndpoints(t *testing.T) {
	md0 := harness.Start(t)
	md1 := harness.Start(t)

	node0 := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md0.Dir,
		ControlChannel: archiveNodeSmokeControl0,
	})
	node1 := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md1.Dir,
		ControlChannel: archiveNodeSmokeControl1,
	})

	require.NotEmpty(t, node0.Dir)
	require.NotEmpty(t, node1.Dir)
	require.NotEqual(t, node0.Dir, node1.Dir)
	require.NotEmpty(t, node0.ArchiveID)
	require.NotEmpty(t, node1.ArchiveID)
}
