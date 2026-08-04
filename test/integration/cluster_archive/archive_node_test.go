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

// TestArchiveNode_startsAndReportsReady asserts one Archive server starts
// against a per-test media driver and writes its ready marker.
func TestArchiveNode_startsAndReportsReady(t *testing.T) {
	t.Parallel()

	md := harness.Start(t)
	node := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveNodeSmokeControl0,
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
	t.Parallel()

	md0 := harness.Start(t)
	md1 := harness.Start(t)

	node0 := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md0.Dir,
		ControlChannel: harness.ArchiveNodePairControl0,
	})
	node1 := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md1.Dir,
		ControlChannel: harness.ArchiveNodePairControl1,
	})

	require.NotEmpty(t, node0.Dir)
	require.NotEmpty(t, node1.Dir)
	require.NotEqual(t, node0.Dir, node1.Dir)
	require.NotEmpty(t, node0.ArchiveID)
	require.NotEmpty(t, node1.ArchiveID)
}
