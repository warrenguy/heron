package cluster_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
)

func TestCalculatePort(t *testing.T) {
	t.Parallel()

	// Every case uses DefaultPortBase; other bases are covered by
	// TestCalculatePort_monotonic_rapid.
	tests := []struct {
		name     string
		memberID int32
		offset   int
		want     int
	}{
		{name: "member0 archive", memberID: 0, offset: cluster.ArchiveControlPortOffset, want: 9001},
		{name: "member0 ingress", memberID: 0, offset: cluster.ClientFacingPortOffset, want: 9002},
		{name: "member0 consensus", memberID: 0, offset: cluster.MemberFacingPortOffset, want: 9003},
		{name: "member0 log", memberID: 0, offset: cluster.LogPortOffset, want: 9004},
		{name: "member0 catchup", memberID: 0, offset: cluster.TransferPortOffset, want: 9005},
		{name: "member1 ingress", memberID: 1, offset: cluster.ClientFacingPortOffset, want: 9102},
		{name: "member2 log", memberID: 2, offset: cluster.LogPortOffset, want: 9204},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tt.want,
				cluster.CalculatePort(tt.memberID, cluster.DefaultPortBase, tt.offset),
			)
		})
	}
}

func TestBuildClusterMembersString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  cluster.MembersStringConfig
		want string
	}{
		{
			name: "single node localhost",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"localhost"},
				ClusterHostnames: []string{"localhost"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: goldenClusterMembersSingle,
		},
		{
			name: "three node cluster-N",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"cluster-0", "cluster-1", "cluster-2"},
				ClusterHostnames: []string{"cluster-0", "cluster-1", "cluster-2"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: goldenClusterMembersFailover,
		},
		{
			name: "three node shared multicast log",
			cfg: cluster.MembersStringConfig{
				IngressHostnames:  []string{"127.0.0.1", "127.0.0.1", "127.0.0.1"},
				ClusterHostnames:  []string{"127.0.0.1", "127.0.0.1", "127.0.0.1"},
				PortBase:          cluster.DefaultPortBase,
				SharedLogEndpoint: "239.255.12.1:9004",
			},
			want: goldenClusterMembersSharedLog,
		},
		{
			name: "zero PortBase defaults to DefaultPortBase",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"localhost"},
				ClusterHostnames: []string{"localhost"},
				PortBase:         0,
			},
			want: goldenClusterMembersSingle,
		},
		{
			// ids 5,6,7; ports from those member ids under DefaultPortBase.
			name: "non-zero StartingMemberID",
			cfg: cluster.MembersStringConfig{
				StartingMemberID: 5,
				IngressHostnames: []string{"host-a", "host-b", "host-c"},
				ClusterHostnames: []string{"host-a", "host-b", "host-c"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: "5,host-a:9502,host-a:9503,host-a:9504,host-a:9505,host-a:9501|" +
				"6,host-b:9602,host-b:9603,host-b:9604,host-b:9605,host-b:9601|" +
				"7,host-c:9702,host-c:9703,host-c:9704,host-c:9705,host-c:9701|",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := cluster.BuildClusterMembersString(tt.cfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBuildClusterMembersString_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  cluster.MembersStringConfig
		want string
	}{
		{
			name: "hostname length mismatch",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"a", "b"},
				ClusterHostnames: []string{"a"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: "cluster: IngressHostnames and ClusterHostnames must be the same length",
		},
		{
			name: "empty ingress hostname slice",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: nil,
				ClusterHostnames: []string{"a"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: "cluster: IngressHostnames and ClusterHostnames must not be empty",
		},
		{
			name: "empty cluster hostname slice",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"a"},
				ClusterHostnames: nil,
				PortBase:         cluster.DefaultPortBase,
			},
			want: "cluster: IngressHostnames and ClusterHostnames must not be empty",
		},
		{
			name: "both hostname slices empty",
			cfg: cluster.MembersStringConfig{
				PortBase: cluster.DefaultPortBase,
			},
			want: "cluster: IngressHostnames and ClusterHostnames must not be empty",
		},
		{
			name: "empty ingress hostname element",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{""},
				ClusterHostnames: []string{"a"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: "cluster: hostname must not be empty",
		},
		{
			name: "empty cluster hostname element",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"a"},
				ClusterHostnames: []string{""},
				PortBase:         cluster.DefaultPortBase,
			},
			want: "cluster: hostname must not be empty",
		},
		{
			name: "negative StartingMemberID",
			cfg: cluster.MembersStringConfig{
				StartingMemberID: -1,
				IngressHostnames: []string{"a"},
				ClusterHostnames: []string{"a"},
				PortBase:         cluster.DefaultPortBase,
			},
			want: "cluster: StartingMemberID must not be negative",
		},
		{
			// Member 6 ingress: 65000 + 6*100 + 2 = 65602 exceeds MaxValidPort.
			name: "port overflow",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: slices.Repeat([]string{"h"}, 10),
				ClusterHostnames: slices.Repeat([]string{"h"}, 10),
				PortBase:         65000,
			},
			want: "cluster: derived port 65602 is out of range",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := cluster.BuildClusterMembersString(tt.cfg)
			require.EqualError(t, err, tt.want)
			require.Empty(t, got)
		})
	}
}
