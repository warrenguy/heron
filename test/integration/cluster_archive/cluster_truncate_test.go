//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterModule_wiresLogTruncateControl asserts a durable member set opens
// with truncate control wired. An active-recording truncate refuses through the
// wired control, not the no-control path.
//
// Full divergence re-elect coverage lives in unit and election package tests.
// Withhold/re-elect live shapes remain harness-sensitive.
func TestClusterModule_wiresLogTruncateControl(t *testing.T) {
	t.Parallel()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          3,
		PortBase:             harness.ClusterTruncatePortBase,
		LogMulticastEndpoint: harness.ClusterTruncateLogEndpoint,
	})
	require.Len(t, set.Modules(), 3)

	const imageBound = 30 * time.Second
	const electBound = 60 * time.Second
	requireAwait(t, set, "log image fan-in", imageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", electBound, set.AwaitElection)

	leaders := set.Leaders()
	require.Len(t, leaders, 1, "expected exactly one leader:\n%s", set.State())
	leader := leaders[0]

	// Refusal path: truncate while the leader log recording is active must
	// fail closed with control wired.
	require.True(
		t,
		leader.Agent().IsRecordingActive(),
		"leader log recording must be active:\n%s",
		set.State(),
	)
	err := leader.Agent().TruncateLogEntry(0, 0)
	require.ErrorContains(
		t,
		err,
		"cluster log recording is active",
		"active-recording truncate must refuse:\n%s",
		set.State(),
	)
}
