//go:build integration && cluster_java

// Golden tables hand-synced with ClusterConstantsFailoverTest; Java ClusterConfig helpers
// are authoritative for topology strings.
//
// Java ClusterConstantsFailoverTest.parameterized goldens intentionally cover a subset
// of these constants; topology strings (members, ingress endpoints) are asserted in
// clusterConfigHelpers_matchFailoverGoldenStrings instead of the parameterized table.
package clusterjava_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

const (
	// Literal from cluster/client/context.go validateClusterIngressEndpoints.
	errUDPIngressEndpointWithMap = "cluster client: IngressChannel must not use ?endpoint= " +
		"when IngressEndpoints is configured"
)

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

func TestFailoverHarnessClusterContext_udpDefaults(t *testing.T) {
	clusterCtx := clusterjava.FailoverHarnessClusterContext()

	require.Equal(t, clusterjava.FailoverIngressChannel, clusterCtx.IngressChannel)
	require.Equal(t, clusterjava.FailoverEgressChannel, clusterCtx.EgressChannel)
	require.Equal(t, clusterjava.FailoverIngressEndpoints, clusterCtx.IngressEndpoints)
	require.Equal(t, clusterjava.FailoverNewLeaderTimeout, clusterCtx.NewLeaderTimeout)
	require.Equal(t, clusterjava.FailoverNewLeaderTimeout, clusterCtx.MessageTimeout)
	require.Empty(t, clusterCtx.AeronDir)
	require.Empty(t, clusterCtx.AeronContext.AeronDir)
	require.Zero(t, clusterCtx.IngressStreamID)
	require.Zero(t, clusterCtx.ResponseStreamID)

	clusterCtx = clusterCtx.Normalize()

	require.Equal(t, clusterjava.IngressStreamID, clusterCtx.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, clusterCtx.ResponseStreamID)
	require.NoError(t, clusterCtx.ValidateForConnect())
}

func TestFailoverHarnessClusterContext_validateForConnect(t *testing.T) {
	clusterCtx := clusterjava.FailoverHarnessClusterContext()
	clusterCtx.IngressChannel = "aeron:udp?endpoint=cluster-0:9002"
	clusterCtx.EgressChannel = clusterjava.EgressChannel

	err := clusterCtx.Normalize().ValidateForConnect()
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
