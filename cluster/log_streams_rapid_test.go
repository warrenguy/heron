//go:build rapid

package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
)

// TestLogStreams_openAndTeardown_rapid checks the open/bind/close roundtrip
// over arbitrary valid topologies: adds target the topology log channel and
// stream id for any member, and Close returns the agent to the unbound state.
// Fixed-input failure and rollback cases live in the table tests.
func TestLogStreams_openAndTeardown_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(t)
		thisIdx := rapid.IntRange(0, memberCount-1).Draw(t, "thisIdx")
		memberID := startID + int32(thisIdx)
		hosts := drawHostnames(t, memberCount)

		members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: hosts,
			ClusterHostnames: hosts,
			PortBase:         portBase,
		})
		require.NoError(t, err)

		topology, err := cluster.NewTopology(cluster.Config{
			MemberID:       memberID,
			ClusterMembers: members,
			ClusterDir:     "/tmp/cluster",
			AeronDir:       "/tmp/aeron",
		}.Normalize())
		require.NoError(t, err)

		adder := &fakeLogStreamAdder{}
		agent := consensus.NewAgent(consensus.AgentConfig{})
		logPublisher := consensus.NewLogPublisher()
		logAdapter := consensus.NewLogAdapter(0)

		ls, err := cluster.OpenLogStreams(cluster.LogStreamsDeps{
			Adder:        adder,
			Topology:     topology,
			Agent:        agent,
			LogPublisher: logPublisher,
			LogAdapter:   logAdapter,
		})
		require.NoError(t, err)
		require.NotNil(t, ls)

		require.Len(t, adder.calls, 2)
		require.Equal(t, "subscription", adder.calls[0].kind)
		require.Equal(t, "publication", adder.calls[1].kind)
		require.Equal(t, topology.LogStreamID(), adder.calls[0].streamID)
		require.Equal(t, topology.LogStreamID(), adder.calls[1].streamID)
		require.Equal(t, topology.LogChannel(), adder.calls[0].channel)
		require.Equal(t, topology.LogChannel(), adder.calls[1].channel)

		require.Equal(t, adder.lastPub.SessionID(), agent.AddLogPublication(0))
		require.Same(t, logPublisher, agent.AppendSource())
		require.Nil(t, logAdapter.Image())
		require.Same(t, adder.lastPub, logPublisher.Publication())
		require.Same(t, adder.lastPub, ls.Publication())
		require.Same(t, adder.lastSub, ls.Subscription())

		pub := adder.lastPub
		sub := adder.lastSub
		require.NoError(t, ls.Close())
		require.Equal(t, int32(-1), agent.AddLogPublication(0))
		require.Nil(t, agent.AppendSource())
		require.Nil(t, logPublisher.Publication())
		require.True(t, pub.IsClosed())
		require.True(t, sub.IsClosed())
		require.Nil(t, ls.Publication())
		require.Nil(t, ls.Subscription())
	})
}
