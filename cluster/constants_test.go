package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
)

func TestConstants(t *testing.T) {
	t.Parallel()

	// Stream IDs.
	require.Equal(t, int32(100), cluster.DefaultLogStreamID)
	require.Equal(t, int32(108), cluster.DefaultConsensusStreamID)
	require.Equal(t, int32(101), cluster.DefaultIngressStreamID)
	require.Equal(t, int32(102), cluster.DefaultEgressStreamID)
	require.Equal(t, int32(104), cluster.DefaultServiceStreamID)
	require.Equal(t, int32(105), cluster.DefaultConsensusModuleStreamID)
	require.Equal(t, int32(103), cluster.DefaultReplayStreamID)

	// Parity with client package defaults (test-only import).
	require.Equal(t, client.DefaultIngressStreamID, cluster.DefaultIngressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, cluster.DefaultEgressStreamID)

	// Port block layout.
	require.Equal(t, 9000, cluster.DefaultPortBase)
	require.Equal(t, 100, cluster.PortsPerNode)
	require.Equal(t, 1, cluster.ArchiveControlPortOffset)
	require.Equal(t, 2, cluster.ClientFacingPortOffset)
	require.Equal(t, 3, cluster.MemberFacingPortOffset)
	require.Equal(t, 4, cluster.LogPortOffset)
	require.Equal(t, 5, cluster.TransferPortOffset)

	// Channel templates.
	require.Equal(t, "aeron:udp?term-length=64k", cluster.DefaultConsensusChannel)
	require.Equal(t, "aeron:udp?term-length=64k", cluster.DefaultLogChannel)
	require.Equal(t, "aeron:udp?term-length=64k", cluster.DefaultIngressChannel)
	require.Equal(t, "aeron:ipc", cluster.DefaultReplayChannel)
	require.Equal(t, "aeron:ipc?term-length=128k", cluster.DefaultServiceControlChannel)

	// Port bounds.
	require.Equal(t, 1, cluster.MinValidPort)
	require.Equal(t, 65535, cluster.MaxValidPort)
}

func TestIsValidPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
		want bool
	}{
		{name: "zero", port: 0, want: false},
		{name: "min", port: cluster.MinValidPort, want: true},
		{name: "max", port: cluster.MaxValidPort, want: true},
		{name: "above max", port: cluster.MaxValidPort + 1, want: false},
		{name: "negative", port: -1, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, cluster.IsValidPort(tt.port))
		})
	}
}
