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
	rapid.Check(t, func(rt *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(rt)
		thisIdx := rapid.IntRange(0, memberCount-1).Draw(rt, "thisIdx")
		memberID := startID + int32(thisIdx)
		hosts := drawHostnames(rt, memberCount)

		members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: hosts,
			ClusterHostnames: hosts,
			PortBase:         portBase,
		})
		require.NoError(rt, err)

		topology, err := cluster.NewTopology(topologyConfigMembers(memberID, members))
		require.NoError(rt, err)

		adder := &fakeLogStreamAdder{}
		agent, logPublisher, logAdapter, deps := openLogStreamsDeps(t, topology, adder)

		ls, err := cluster.OpenLogStreams(deps)
		require.NoError(rt, err)
		require.NotNil(rt, ls)

		logChannel := topology.LogChannel()
		streamID := topology.LogStreamID()
		requireLogStreamOpenOrder(rt, adder, streamID)
		require.Equal(rt, logChannel, adder.calls[1].channel)
		require.Equal(rt, []string{logChannel}, adder.lastDestHook.Adds)
		require.Equal(rt, logChannel, agent.LiveLogDestination())

		sessionID, err := agent.AddLogPublication(0)
		require.NoError(rt, err)
		require.Equal(rt, adder.lastPub.SessionID(), sessionID)
		require.IsType(rt, (*consensus.LogAppendPosition)(nil), agent.AppendSource())
		require.IsType(rt, (*consensus.SubscriptionLogJoinSource)(nil), agent.LogJoinSource())
		require.Nil(rt, logAdapter.Image())
		require.Same(rt, adder.lastPub, logPublisher.Publication())
		require.Same(rt, adder.lastPub, ls.Publication())
		require.Same(rt, adder.lastSub, ls.Subscription())

		pub := adder.lastPub
		sub := adder.lastSub
		require.NoError(rt, ls.Close())
		requireAgentLogUnbound(rt, agent, logPublisher)
		require.True(rt, pub.IsClosed())
		require.True(rt, sub.IsClosed())
		require.Nil(rt, ls.Publication())
		require.Nil(rt, ls.Subscription())
	})
}
