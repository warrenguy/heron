//go:build integration && cluster_java

package clusterjava_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

const (
	// Literal from cluster/client/config.go validateClusterIngressEndpoints.
	errUDPIngressEndpointWithMap = "cluster client: IngressChannel must not use ?endpoint= " +
		"when IngressEndpoints is configured"
)

// TestFailoverHarnessConstants asserts the failover golden table. The table
// hand-syncs with Java ClusterConstantsFailoverTest. Java ClusterConfig helpers
// are authoritative for topology strings.
//
// The Java parameterized table covers a subset of these constants on purpose.
// Java clusterConfigHelpers_matchFailoverGoldenStrings asserts the topology
// strings (members, ingress endpoints).
func TestFailoverHarnessConstants(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "FailoverMemberCount", got: clusterjava.FailoverMemberCount, want: 3},
		{
			name: "FailoverIngressEndpointHost",
			got:  clusterjava.FailoverIngressEndpointHost,
			want: "cluster-0",
		},
		{
			name: "FailoverIngressChannel",
			got:  clusterjava.FailoverIngressChannel,
			want: "aeron:udp?term-length=64k",
		},
		{
			name: "FailoverEgressChannel",
			got:  clusterjava.FailoverEgressChannel,
			want: "aeron:udp?endpoint=aeronmd:0|term-length=64k",
		},
		{
			name: "FailoverBaseDirPrefix",
			got:  clusterjava.FailoverBaseDirPrefix,
			want: "/dev/shm/heron-cluster-",
		},
		{
			name: "FailoverIngressEndpoints",
			got:  clusterjava.FailoverIngressEndpoints,
			want: "0=cluster-0:9002,1=cluster-1:9102,2=cluster-2:9202",
		},
		{
			name: "FailoverClusterMembersString",
			got:  clusterjava.FailoverClusterMembersString,
			want: "0,cluster-0:9002,cluster-0:9003,cluster-0:9004,cluster-0:9005," +
				"cluster-0:9001|1,cluster-1:9102,cluster-1:9103,cluster-1:9104," +
				"cluster-1:9105,cluster-1:9101|2,cluster-2:9202,cluster-2:9203,cluster-2:9204," +
				"cluster-2:9205,cluster-2:9201|",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}

	t.Run("FailoverIngressChannel_no_endpoint_query", func(t *testing.T) {
		require.NotContains(t, clusterjava.FailoverIngressChannel, "?endpoint=")
	})
}

func TestFailoverHarnessConfig_udpDefaults(t *testing.T) {
	cfg := clusterjava.FailoverHarnessConfig()

	require.Equal(t, clusterjava.FailoverIngressChannel, cfg.IngressChannel)
	require.Equal(t, clusterjava.FailoverEgressChannel, cfg.EgressChannel)
	require.Equal(t, clusterjava.FailoverIngressEndpoints, cfg.IngressEndpoints)
	require.Equal(t, clusterjava.FailoverNewLeaderTimeout, cfg.NewLeaderTimeout)
	require.Equal(t, clusterjava.FailoverNewLeaderTimeout, cfg.MessageTimeout)
	require.Empty(t, cfg.AeronDir)
	require.Empty(t, cfg.AeronConfig.AeronDir)
	require.Zero(t, cfg.IngressStreamID)
	require.Zero(t, cfg.ResponseStreamID)

	cfg = cfg.Normalize()

	require.Equal(t, clusterjava.IngressStreamID, cfg.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, cfg.ResponseStreamID)
	require.NoError(t, cfg.ValidateForConnect())
}

func TestFailoverHarnessConfig_validateForConnect(t *testing.T) {
	cfg := clusterjava.FailoverHarnessConfig()
	cfg.IngressChannel = "aeron:udp?endpoint=cluster-0:9002"
	cfg.EgressChannel = clusterjava.EgressChannel

	err := cfg.Normalize().ValidateForConnect()
	require.EqualError(t, err, errUDPIngressEndpointWithMap)
}

func TestFailoverBuildIngressChannel_memberEndpoint(t *testing.T) {
	const member1Endpoint = "cluster-1:9102"

	got, err := client.BuildIngressChannel(
		clusterjava.FailoverIngressChannel,
		member1Endpoint,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		clusterjava.FailoverIngressChannel+"|endpoint="+member1Endpoint,
		got,
	)
}

func TestBuildClusterMembersString_javaOracleParity(t *testing.T) {
	failoverHosts := []string{"cluster-0", "cluster-1", "cluster-2"}

	tests := []struct {
		name string
		cfg  cluster.MembersStringConfig
		want string
	}{
		{
			name: "FailoverClusterMembersString",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: failoverHosts,
				ClusterHostnames: failoverHosts,
				PortBase:         clusterjava.PortBase,
			},
			want: clusterjava.FailoverClusterMembersString,
		},
		{
			name: "ClusterMembersString",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: []string{"localhost"},
				ClusterHostnames: []string{"localhost"},
				PortBase:         clusterjava.PortBase,
			},
			want: clusterjava.ClusterMembersString,
		},
		{
			name: "MixedClusterMembersJavaLeader",
			cfg: cluster.MembersStringConfig{
				IngressHostnames: clusterjava.MixedMemberHostnames(),
				ClusterHostnames: clusterjava.MixedMemberHostnames(),
				PortBase:         clusterjava.PortBase,
			},
			want: clusterjava.MixedClusterMembersJavaLeader,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cluster.BuildClusterMembersString(tt.cfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
