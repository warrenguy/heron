//go:build rapid

package cluster_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
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

// Log publication reseed ops for TestLogStreams_reseedPublication_rapid.
const (
	reseedPublicationOK     = "reseed"
	reseedPublicationFailed = "reseed add failure"
	reseedPublicationClose  = "close"
)

// TestLogStreams_reseedPublication_rapid checks reseeds, failed reseed adds
// and Close over random sequences. The model tracks the bound state, whether
// Close ran, and every publication added.
func TestLogStreams_reseedPublication_rapid(t *testing.T) {
	t.Parallel()
	topology := sharedLogTopology(t, 0, "")
	cause := errors.New("driver refused reseed")
	rapid.Check(t, func(rt *rapid.T) {
		adder := &fakeLogStreamAdder{}
		_, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)
		ls, err := cluster.OpenLogStreams(deps)
		require.NoError(rt, err)

		added := []*aeron.ExclusivePublication{adder.lastPub}
		bound, closed := true, false
		ops := rapid.SampledFrom([]string{
			reseedPublicationOK,
			reseedPublicationFailed,
			reseedPublicationClose,
		})
		opCount := rapid.IntRange(0, 32).Draw(rt, "opCount")
		for range opCount {
			op := ops.Draw(rt, "op")
			callsBefore := len(adder.calls)

			if op == reseedPublicationClose {
				require.NoError(rt, ls.Close())
				bound, closed = false, true
			} else {
				frames := rapid.Int64Range(0, 1<<20).Draw(rt, "frames")
				seed := logPublicationSeedAt(frames * logbuffer.FrameAlignment)
				adder.failPubErr = nil
				if op == reseedPublicationFailed {
					adder.failPubErr = cause
				}
				err := ls.ReseedPublicationAt(
					adder,
					topology.LogChannel(),
					topology.LogStreamID(),
					seed,
				)
				switch {
				case closed:
					require.EqualError(rt, err, "cluster: log streams closed")
					require.Len(rt, adder.calls, callsBefore)
				case op == reseedPublicationFailed:
					require.ErrorIs(rt, err, cause)
					require.Len(rt, adder.calls, callsBefore+1)
					bound = false
				default:
					require.NoError(rt, err)
					require.Len(rt, adder.calls, callsBefore+1)
					added = append(added, adder.lastPub)
					bound = true
				}
			}

			require.Same(rt, ls.Publication(), logPublisher.Publication())
			superseded := added
			if bound {
				last := added[len(added)-1]
				require.Same(rt, last, ls.Publication())
				require.False(rt, last.IsClosed())
				superseded = added[:len(added)-1]
			} else {
				require.Nil(rt, ls.Publication())
			}
			for _, pub := range superseded {
				require.True(rt, pub.IsClosed())
			}
		}
		require.NoError(rt, ls.Close())
	})
}
