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
	ctx := clusterjava.HarnessClusterContext()

	require.Equal(t, clusterjava.IngressChannel, ctx.IngressChannel)
	require.Equal(t, clusterjava.EgressChannel, ctx.EgressChannel)
	require.Equal(t, clusterjava.IngressEndpoints, ctx.IngressEndpoints)
	require.Empty(t, ctx.AeronDir)
	require.Empty(t, ctx.AeronContext.AeronDir)
	require.Zero(t, ctx.IngressStreamID)
	require.Zero(t, ctx.ResponseStreamID)

	ctx = ctx.Normalize()

	require.Equal(t, clusterjava.IngressStreamID, ctx.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, ctx.ResponseStreamID)
	require.NoError(t, ctx.ValidateForConnect())
}
