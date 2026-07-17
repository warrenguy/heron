//go:build integration && cluster_java

package clusterjava_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

func TestHarnessConstants(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "PortBase", got: clusterjava.PortBase, want: 9000},
		{name: "MemberID", got: clusterjava.MemberID, want: 0},
		{name: "MemberCount", got: clusterjava.MemberCount, want: 1},
		{name: "IngressEndpointPort", got: clusterjava.IngressEndpointPort, want: 9002},
		{name: "IngressStreamID", got: clusterjava.IngressStreamID, want: int32(101)},
		{name: "EgressStreamID", got: clusterjava.EgressStreamID, want: int32(102)},
		{name: "TermMinLength", got: clusterjava.TermMinLength, want: 65536},
		{name: "IngressEndpointHost", got: clusterjava.IngressEndpointHost, want: "localhost"},
		{name: "IngressChannel", got: clusterjava.IngressChannel, want: "aeron:ipc?term-length=64k"},
		{name: "EgressChannel", got: clusterjava.EgressChannel, want: "aeron:ipc?term-length=64k"},
		{name: "IngressEndpoints", got: clusterjava.IngressEndpoints, want: ""},
		{name: "ComposeAeronDir", got: clusterjava.ComposeAeronDir, want: "/dev/shm/aeron"},
		{name: "ComposeDir", got: clusterjava.ComposeDir, want: "/app/compose"},
		{
			name: "ComposeGateEnvKey",
			got:  clusterjava.ComposeGateEnvKey,
			want: "HERON_CLUSTER_JAVA_COMPOSE",
		},
		{
			name: "FailoverComposeGateEnvKey",
			got:  clusterjava.FailoverComposeGateEnvKey,
			want: "HERON_CLUSTER_JAVA_FAILOVER_COMPOSE",
		},
		{
			name: "ComposeSkipHint",
			got:  clusterjava.ComposeSkipHint,
			want: "cluster java requires docker compose; run make test-cluster-java",
		},
		{
			name: "FailoverComposeSkipHint",
			got:  clusterjava.FailoverComposeSkipHint,
			want: "failover requires docker compose; run make test-cluster-java-failover",
		},
		{
			name: "ComposeDirEnvKey",
			got:  clusterjava.ComposeDirEnvKey,
			want: "HERON_CLUSTER_JAVA_COMPOSE_DIR",
		},
		{
			name: "FailoverComposeProfileName",
			got:  clusterjava.FailoverComposeProfileName,
			want: "failover",
		},
		{name: "BaseDir", got: clusterjava.BaseDir, want: "/tmp/heron-cluster-0"},
		{
			name: "ClusterMembersString",
			got:  clusterjava.ClusterMembersString,
			want: "0,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001|",
		},
		{
			name: "IngressEndpointsString",
			got:  clusterjava.IngressEndpointsString,
			want: "0=localhost:9002",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func TestHarnessClusterContext_ipcDefaults(t *testing.T) {
	clusterCtx := clusterjava.HarnessClusterContext()

	require.Equal(t, clusterjava.IngressChannel, clusterCtx.IngressChannel)
	require.Equal(t, clusterjava.EgressChannel, clusterCtx.EgressChannel)
	require.Equal(t, clusterjava.IngressEndpoints, clusterCtx.IngressEndpoints)
	require.Empty(t, clusterCtx.AeronDir)
	require.Empty(t, clusterCtx.AeronContext.AeronDir)
	require.Zero(t, clusterCtx.IngressStreamID)
	require.Zero(t, clusterCtx.ResponseStreamID)

	clusterCtx = clusterCtx.Normalize()

	require.Equal(t, clusterjava.IngressStreamID, clusterCtx.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, clusterCtx.ResponseStreamID)
	require.NoError(t, clusterCtx.ValidateForConnect())
}
