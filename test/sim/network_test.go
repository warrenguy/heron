package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestNetwork_memberIsolation(t *testing.T) {
	t.Parallel()

	cfg := Config{Seed: 7, MemberSpec: defaultThreeNodeMemberSpec}
	sim, err := NewElectionSim(t, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sim.Close()) })

	nodes := sim.Nodes()
	require.Len(t, nodes, 3)

	setMemberLogPositionForTest(nodes[0], 1, 999)
	require.Equal(t, int64(999), nodes[0].members[1].LogPosition)
	require.NotEqual(t, nodes[0].members[1].LogPosition, nodes[1].members[1].LogPosition)
}

func TestNetwork_focusNodeIDDropsNonFocusDelivery(t *testing.T) {
	t.Parallel()

	focus := int32(1)
	sim, err := NewElectionSim(t, Config{
		Seed:        9,
		MemberSpec:  defaultThreeNodeMemberSpec,
		FocusNodeID: &focus,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sim.Close()) })

	sim.Node(0).Publisher().CanvassPosition(
		election.PublicationTarget{MemberID: 2},
		0, 0, 0, 0,
	)
	require.NoError(t, sim.RunSteps(1))

	dropFound := false
	for _, e := range sim.Timeline() {
		if e.Kind == "drop" && e.To == 2 {
			dropFound = true
		}
		if e.Kind == "deliver" && e.To == 2 {
			require.Fail(t, "focus node 1 should not receive deliveries to node 2")
		}
	}
	require.True(t, dropFound)
	require.Equal(t, 0, sim.Node(2).Snapshot().InboundCount)
}

func TestNetwork_deliverUnknownTarget(t *testing.T) {
	t.Parallel()

	cfg := Config{Seed: 8, MemberSpec: defaultThreeNodeMemberSpec}
	sim, err := NewElectionSim(t, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sim.Close()) })

	ok := sim.network.Deliver(DeliverPayload{
		From: 0,
		To:   99,
		Msg: election.SentMessage{
			TemplateID: 1,
			Dst:        election.PublicationTarget{MemberID: 99},
		},
	})
	require.False(t, ok)
}

func setMemberLogPositionForTest(n *Node, memberID int32, pos int64) {
	for i := range n.members {
		if n.members[i].ID == memberID {
			n.members[i].LogPosition = pos
			return
		}
	}
}
