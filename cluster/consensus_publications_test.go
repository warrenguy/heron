package cluster_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
)

type addCall struct {
	streamID int32
	channel  string
}

type fakeAdder struct {
	calls     []addCall
	pubs      []*aeron.ExclusivePublication
	failAt    int // 0-based call index to fail; -1 means never
	failErr   error
	returnNil bool // return (nil, nil) on every call
}

func (f *fakeAdder) AddExclusivePublication(
	streamID int32,
	channel string,
) (*aeron.ExclusivePublication, error) {
	f.calls = append(f.calls, addCall{streamID: streamID, channel: channel})
	if f.returnNil {
		return nil, nil
	}
	idx := len(f.calls) - 1
	if f.failAt >= 0 && idx == f.failAt {
		err := f.failErr
		if err == nil {
			err = errors.New("add failed")
		}
		return nil, err
	}
	pub := new(aeron.ExclusivePublication)
	f.pubs = append(f.pubs, pub)
	return pub, nil
}

func threeMemberTopology(t *testing.T, thisID int32) *cluster.Topology {
	t.Helper()
	hosts := []string{"host-0", "host-1", "host-2"}
	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames: hosts,
		ClusterHostnames: hosts,
		PortBase:         9000,
	})
	require.NoError(t, err)

	topology, err := cluster.NewTopology(cluster.Config{
		MemberID:                      thisID,
		ClusterMembers:                members,
		ClusterDir:                    t.TempDir(),
		AeronDir:                      t.TempDir(),
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
	}.Normalize())
	require.NoError(t, err)
	return topology
}

func singleMemberTopology(t *testing.T) *cluster.Topology {
	t.Helper()
	hosts := []string{"host-0"}
	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames: hosts,
		ClusterHostnames: hosts,
		PortBase:         9000,
	})
	require.NoError(t, err)

	topology, err := cluster.NewTopology(cluster.Config{
		MemberID:                      0,
		ClusterMembers:                members,
		ClusterDir:                    t.TempDir(),
		AeronDir:                      t.TempDir(),
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
	}.Normalize())
	require.NoError(t, err)
	return topology
}

func TestOpenConsensusPublications_nilDeps(t *testing.T) {
	t.Parallel()

	topology := threeMemberTopology(t, 1)
	publisher := election.NewAeronPublisher()
	adder := &fakeAdder{failAt: -1}

	t.Run("nil adder", func(t *testing.T) {
		t.Parallel()
		cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
			Adder:     nil,
			Topology:  topology,
			Publisher: publisher,
		})
		require.EqualError(t, err, "cluster: exclusive publication adder is required")
		require.Nil(t, cp)
	})

	t.Run("nil topology", func(t *testing.T) {
		t.Parallel()
		cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
			Adder:     adder,
			Topology:  nil,
			Publisher: publisher,
		})
		require.EqualError(t, err, "cluster: topology is required")
		require.Nil(t, cp)
	})

	t.Run("nil publisher", func(t *testing.T) {
		t.Parallel()
		cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
			Adder:     adder,
			Topology:  topology,
			Publisher: nil,
		})
		require.EqualError(t, err, "cluster: consensus publisher is required")
		require.Nil(t, cp)
	})
}

func TestOpenConsensusPublications_threeMembers(t *testing.T) {
	t.Parallel()

	topology := threeMemberTopology(t, 1)
	publisher := election.NewAeronPublisher()
	adder := &fakeAdder{failAt: -1}

	cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
		Adder:     adder,
		Topology:  topology,
		Publisher: publisher,
	})
	require.NoError(t, err)
	require.NotNil(t, cp)
	t.Cleanup(func() { _ = cp.Close() })

	require.Equal(t, 2, cp.PeerCount())
	require.NotNil(t, cp.Publication(0))
	require.Nil(t, cp.Publication(1))
	require.NotNil(t, cp.Publication(2))

	// Self never bound on the shared publisher.
	require.Nil(t, publisher.Publication(1))

	// Bound handles are the added handles.
	require.Same(t, cp.Publication(0), publisher.Publication(0))
	require.Same(t, cp.Publication(2), publisher.Publication(2))

	// Distinct handles per peer.
	require.NotSame(t, cp.Publication(0), cp.Publication(2))

	// Add arguments; ascending member id: peer 0 then peer 2.
	channels := topology.PeerConsensusChannels()
	streamID := topology.ConsensusStreamID()
	require.Len(t, adder.calls, 2)
	require.Equal(t, streamID, adder.calls[0].streamID)
	require.Equal(t, channels[0], adder.calls[0].channel)
	require.Equal(t, streamID, adder.calls[1].streamID)
	require.Equal(t, channels[2], adder.calls[1].channel)
}

func TestOpenConsensusPublications_singleMember(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	publisher := election.NewAeronPublisher()
	adder := &fakeAdder{failAt: -1}

	cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
		Adder:     adder,
		Topology:  topology,
		Publisher: publisher,
	})
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, 0, cp.PeerCount())
	require.Empty(t, adder.calls)
	require.Nil(t, publisher.Publication(0))
	require.NoError(t, cp.Close())
}

func TestOpenConsensusPublications_addFailureSecondPeer(t *testing.T) {
	t.Parallel()

	topology := threeMemberTopology(t, 1)
	publisher := election.NewAeronPublisher()
	cause := errors.New("driver timeout")
	adder := &fakeAdder{failAt: 1, failErr: cause}

	cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
		Adder:     adder,
		Topology:  topology,
		Publisher: publisher,
	})
	require.Nil(t, cp)
	channels := topology.PeerConsensusChannels()
	want := fmt.Sprintf(
		"cluster: add consensus publication for member 2 on %s: %s",
		channels[2],
		cause.Error(),
	)
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, cause)

	// One publication handed out and closed on rollback.
	require.Len(t, adder.pubs, 1)
	require.True(t, adder.pubs[0].IsClosed())

	// Publisher untouched.
	require.Nil(t, publisher.Publication(0))
	require.Nil(t, publisher.Publication(2))
}

func TestOpenConsensusPublications_adderReturnsNilNil(t *testing.T) {
	t.Parallel()

	topology := threeMemberTopology(t, 1)
	publisher := election.NewAeronPublisher()
	adder := &fakeAdder{failAt: -1, returnNil: true}

	cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
		Adder:     adder,
		Topology:  topology,
		Publisher: publisher,
	})
	require.Nil(t, cp)
	channels := topology.PeerConsensusChannels()

	// Ascending peer ids for thisID=1 are 0 then 2; first add fails.
	want := fmt.Sprintf(
		"cluster: add consensus publication for member 0 on %s: adder returned no publication",
		channels[0],
	)
	require.EqualError(t, err, want)

	// First peer fails immediately; no successful pubs to roll back.
	require.Empty(t, adder.pubs)
	require.Nil(t, publisher.Publication(0))
	require.Nil(t, publisher.Publication(2))
}

func TestConsensusPublications_close(t *testing.T) {
	t.Parallel()

	topology := threeMemberTopology(t, 1)
	publisher := election.NewAeronPublisher()
	adder := &fakeAdder{failAt: -1}

	cp, err := cluster.OpenConsensusPublications(cluster.ConsensusPublicationsDeps{
		Adder:     adder,
		Topology:  topology,
		Publisher: publisher,
	})
	require.NoError(t, err)

	require.NoError(t, cp.Close())

	// Unbind then close: every recorded publication closed; publisher empty.
	for _, pub := range adder.pubs {
		require.True(t, pub.IsClosed())
	}
	require.Nil(t, publisher.Publication(0))
	require.Nil(t, publisher.Publication(1))
	require.Nil(t, publisher.Publication(2))
	require.Equal(t, 0, cp.PeerCount())
	require.Nil(t, cp.Publication(0))
	require.Nil(t, cp.Publication(2))

	// Idempotent.
	require.NoError(t, cp.Close())
	require.Equal(t, 0, cp.PeerCount())
}

func TestConsensusPublications_nilReceiver(t *testing.T) {
	t.Parallel()

	var cp *cluster.ConsensusPublications
	require.NoError(t, cp.Close())
	require.Nil(t, cp.Publication(0))
	require.Zero(t, cp.PeerCount())
}
