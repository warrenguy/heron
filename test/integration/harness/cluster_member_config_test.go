//go:build integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClusterMemberSet_rejectsInvalidConfig asserts StartClusterMemberSet
// rejects zero MemberCount, zero PortBase, or empty LogMulticastEndpoint.
// Start fails before any media driver or Archive process starts.
func TestClusterMemberSet_rejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ClusterMemberSetConfig
		wantMsg string
	}{
		{
			name: "MemberCount zero",
			cfg: ClusterMemberSetConfig{
				MemberCount:          0,
				PortBase:             ClusterModulePortBase,
				LogMulticastEndpoint: "239.255.12.7:1",
			},
			wantMsg: "ClusterMemberSet: MemberCount must be >= 1",
		},
		{
			name: "PortBase zero",
			cfg: ClusterMemberSetConfig{
				MemberCount:          1,
				PortBase:             0,
				LogMulticastEndpoint: "239.255.12.7:1",
			},
			wantMsg: "ClusterMemberSet: PortBase must be positive",
		},
		{
			name: "empty LogMulticastEndpoint",
			cfg: ClusterMemberSetConfig{
				MemberCount:          1,
				PortBase:             ClusterModulePortBase,
				LogMulticastEndpoint: "",
			},
			wantMsg: "ClusterMemberSet: LogMulticastEndpoint must be non-empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			capture := NewCaptureFailTB(t)
			set := StartClusterMemberSet(capture, tt.cfg)
			require.True(t, capture.Failed())
			require.Nil(t, set)
			require.Contains(t, capture.lastMsg, tt.wantMsg)
		})
	}
}
