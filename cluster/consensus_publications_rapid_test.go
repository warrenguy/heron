//go:build rapid

package cluster_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
)

func TestConsensusPublications_peerSetLifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(t)
		hosts := drawHostnames(t, memberCount)
		members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: hosts,
			ClusterHostnames: hosts,
			PortBase:         portBase,
		})
		require.NoError(t, err)

		thisIdx := rapid.IntRange(0, memberCount-1).Draw(t, "thisIdx")
		memberID := startID + int32(thisIdx)

		topology, err := cluster.NewTopology(topologyConfigMembers(memberID, members))
		require.NoError(t, err)

		// Model: all ids except self.
		model := make([]int32, 0, memberCount-1)
		for i := range memberCount {
			id := startID + int32(i)
			if id != memberID {
				model = append(model, id)
			}
		}
		slices.Sort(model)

		publisher := election.NewAeronPublisher()
		adder := &fakeAdder{failAt: -1}

		cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
			Adder:     adder,
			Topology:  topology,
			Publisher: publisher,
		})
		require.NoError(t, err)
		require.NotNil(t, cp)

		require.Equal(t, len(model), cp.PeerCount())
		require.Nil(t, cp.Publication(memberID))
		require.Nil(t, publisher.Publication(memberID))

		channels := topology.PeerConsensusChannels()
		streamID := topology.ConsensusStreamID()
		require.Len(t, adder.calls, len(model))

		var prev *aeron.ExclusivePublication
		for i, id := range model {
			pub := cp.Publication(id)
			require.NotNil(t, pub)
			require.Same(t, pub, publisher.Publication(id))
			if prev != nil {
				require.NotSame(t, prev, pub)
			}
			prev = pub

			require.Equal(t, streamID, adder.calls[i].streamID)
			require.Equal(t, channels[id], adder.calls[i].channel)
		}

		// Close: every handle closed; set inert; publisher unbound.
		require.NoError(t, cp.Close())
		for _, pub := range adder.pubs {
			require.True(t, pub.IsClosed())
		}
		require.Equal(t, 0, cp.PeerCount())
		for _, id := range model {
			require.Nil(t, cp.Publication(id))
			require.Nil(t, publisher.Publication(id))
		}
		require.Nil(t, cp.Publication(memberID))
		require.Nil(t, publisher.Publication(memberID))

		// Second Close: identical state, nil error.
		require.NoError(t, cp.Close())
		require.Equal(t, 0, cp.PeerCount())
		for _, id := range model {
			require.Nil(t, cp.Publication(id))
			require.Nil(t, publisher.Publication(id))
		}
	})
}

func TestConsensusPublications_addFailureRollsBack_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		// At least two members so a peer set exists and a failure index is meaningful.
		memberCount := rapid.IntRange(2, 7).Draw(t, "memberCount")
		startID := rapid.Int32Range(0, 50).Draw(t, "startingMemberID")
		maxID := int(startID) + memberCount - 1
		maxPortBase := cluster.MaxValidPort - maxID*cluster.PortsPerNode - cluster.TransferPortOffset
		require.GreaterOrEqual(t, maxPortBase, 1024)
		portBase := rapid.IntRange(1024, maxPortBase).Draw(t, "portBase")
		hosts := drawHostnames(t, memberCount)
		members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: hosts,
			ClusterHostnames: hosts,
			PortBase:         portBase,
		})
		require.NoError(t, err)

		thisIdx := rapid.IntRange(0, memberCount-1).Draw(t, "thisIdx")
		memberID := startID + int32(thisIdx)

		topology, err := cluster.NewTopology(topologyConfigMembers(memberID, members))
		require.NoError(t, err)

		peerIDs := make([]int32, 0, memberCount-1)
		for i := range memberCount {
			id := startID + int32(i)
			if id != memberID {
				peerIDs = append(peerIDs, id)
			}
		}
		slices.Sort(peerIDs)
		require.NotEmpty(t, peerIDs)

		failIdx := rapid.IntRange(0, len(peerIDs)-1).Draw(t, "failIdx")
		cause := errors.New("forced add failure")
		adder := &fakeAdder{failAt: failIdx, failErr: cause}
		publisher := election.NewAeronPublisher()

		cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
			Adder:     adder,
			Topology:  topology,
			Publisher: publisher,
		})
		require.Nil(t, cp)
		require.Error(t, err)

		failedID := peerIDs[failIdx]
		channels := topology.PeerConsensusChannels()
		want := fmt.Sprintf(
			"cluster: add consensus publication for member %d on %s: %s",
			failedID,
			channels[failedID],
			cause.Error(),
		)
		require.EqualError(t, err, want)
		require.ErrorIs(t, err, cause)

		// Exactly failIdx handles produced; every one closed.
		require.Len(t, adder.pubs, failIdx)
		for _, pub := range adder.pubs {
			require.True(t, pub.IsClosed())
		}

		// Publisher holds nothing for any drawn member id.
		for i := range memberCount {
			id := startID + int32(i)
			require.Nil(t, publisher.Publication(id))
		}
	})
}
