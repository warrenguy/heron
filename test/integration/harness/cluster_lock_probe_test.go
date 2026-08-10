//go:build integration

package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/cluster"
)

// clusterLockFileName is the exclusive lock basename under ClusterDir.
// OpenRecovery creates this path on first open.
const clusterLockFileName = "cluster.lock"

// TestClusterLockFileName_matchesRecoveryOpen asserts OpenRecovery still creates
// cluster.lock under ClusterDir. RestartMember reopens that path through
// OpenRecovery. A rename must fail here rather than as a Restart open against
// the wrong basename.
//
// Binds no ports and starts no driver. OpenRecovery touches the filesystem only.
func TestClusterLockFileName_matchesRecoveryOpen(t *testing.T) {
	t.Parallel()

	membersString, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID: 0,
		IngressHostnames: []string{"127.0.0.1"},
		ClusterHostnames: []string{"127.0.0.1"},
	})
	require.NoError(t, err)

	// Normalize first, as OpenConsensusModule does. OpenRecovery only validates.
	// Nil ArchiveLister is allowed: a fresh directory has an empty recording log.
	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: MemberClusterConfig(0, membersString, dir, t.TempDir()).Normalize(),
	})
	require.NoError(t, err)
	CleanupClose(t, "recovery", recovery.Close)

	require.FileExists(t, filepath.Join(dir, clusterLockFileName))
}

// TestWrapRestartOpenError asserts the release note appears only for
// EWOULDBLOCK. Every other open failure carries slot and ClusterDir without the
// note.
func TestWrapRestartOpenError(t *testing.T) {
	t.Parallel()

	const releaseNote = "StopMember must have released cluster.lock"

	cases := []struct {
		name     string
		index    int
		in       error
		wantHint bool
	}{
		{name: "ewouldblock", index: 1, in: unix.EWOULDBLOCK, wantHint: true},
		{name: "permission", index: 3, in: os.ErrPermission, wantHint: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			err := wrapRestartOpenError(tc.index, dir, tc.in)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.in)
			require.Contains(t, err.Error(), fmt.Sprintf("restart slot %d", tc.index))
			require.Contains(t, err.Error(), dir)
			if tc.wantHint {
				require.Contains(t, err.Error(), releaseNote)
				return
			}
			require.NotContains(t, err.Error(), releaseNote)
		})
	}
}
