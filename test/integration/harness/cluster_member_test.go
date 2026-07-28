//go:build integration

package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Fixture smoke endpoints. Disjoint from three-node module tests that use
// ClusterModulePortBase 25000 and ClusterModuleLogEndpoint.
const (
	clusterMemberSetSmokePortBase    = 25300
	clusterMemberSetSmokeLogEndpoint = "239.255.12.9:25304"
)

// TestClusterMemberSet_opensAndClosesCleanly asserts a two-member set opens,
// every member holds every peer log image by session id, and Close is
// idempotent.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestClusterMemberSet_opensAndClosesCleanly(t *testing.T) {
	set := StartClusterMemberSet(t, ClusterMemberSetConfig{
		MemberCount:          2,
		PortBase:             clusterMemberSetSmokePortBase,
		LogMulticastEndpoint: clusterMemberSetSmokeLogEndpoint,
	})
	require.Len(t, set.Modules(), 2)
	require.NotEmpty(t, set.MembersString())

	require.True(t, set.AwaitLogImages(30*time.Second), "log images not ready:\n%s", set.State())

	for _, m := range set.Modules() {
		require.Equal(t, len(set.Modules()), m.LogStreams().Subscription().ImageCount())
	}

	// Short deadline bound with ok always false. Asserts the timeout path.
	require.False(t, set.PumpTransportUntil(time.Millisecond, func() bool { return false }))

	require.NoError(t, set.Close())
	require.NoError(t, set.Close())
}

// TestClusterMemberSet_rejectsInvalidConfig asserts StartClusterMemberSet fails
// closed on bad MemberCount, PortBase, or LogMulticastEndpoint before any
// media driver starts.
func TestClusterMemberSet_rejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  ClusterMemberSetConfig
	}{
		{
			name: "MemberCount zero",
			cfg: ClusterMemberSetConfig{
				MemberCount:          0,
				PortBase:             ClusterModulePortBase,
				LogMulticastEndpoint: "239.255.12.7:1",
			},
		},
		{
			name: "PortBase zero",
			cfg: ClusterMemberSetConfig{
				MemberCount:          1,
				PortBase:             0,
				LogMulticastEndpoint: "239.255.12.7:1",
			},
		},
		{
			name: "empty LogMulticastEndpoint",
			cfg: ClusterMemberSetConfig{
				MemberCount:          1,
				PortBase:             ClusterModulePortBase,
				LogMulticastEndpoint: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			capture := NewCaptureFailTB(t)
			set := StartClusterMemberSet(capture, tt.cfg)
			require.True(t, capture.Failed())
			require.Nil(t, set)
		})
	}
}
