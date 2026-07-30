//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestConsensusPublications_openCloseOnLiveDriver opens peer consensus exclusive
// publications on the shared driver and checks bind plus close behaviour.
func TestConsensusPublications_openCloseOnLiveDriver(t *testing.T) {
	md := harness.SharedParallel(t)
	client, _ := harness.ConnectIPCClient(t, md)

	hosts := []string{"127.0.0.1", "127.0.0.1", "127.0.0.1"}
	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames: hosts,
		ClusterHostnames: hosts,
		PortBase:         24000,
	})
	require.NoError(t, err)

	topology, err := cluster.NewTopology(cluster.Config{
		MemberID:                      0,
		ClusterMembers:                members,
		ClusterDir:                    t.TempDir(),
		AeronDir:                      t.TempDir(),
		ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
		ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
	}.Normalize())
	require.NoError(t, err)

	publisher := election.NewAeronPublisher()
	pubs, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
		Adder:     client,
		Topology:  topology,
		Publisher: publisher,
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "consensus publications", pubs.Close)

	require.Equal(t, 2, pubs.PeerCount())

	peer1 := pubs.Publication(1)
	peer2 := pubs.Publication(2)
	require.NotNil(t, peer1)
	require.NotNil(t, peer2)

	require.Equal(t, topology.ConsensusStreamID(), peer1.StreamID())
	require.False(t, peer1.IsConnected())
	require.False(t, peer2.IsConnected())
	require.Same(t, peer1, publisher.Publication(1))
	require.Same(t, peer2, publisher.Publication(2))

	require.NoError(t, pubs.Close())
	require.True(t, peer1.IsClosed())
	require.True(t, peer2.IsClosed())
	require.Nil(t, pubs.Publication(1))
	require.Zero(t, pubs.PeerCount())
	require.Nil(t, publisher.Publication(1))
}
