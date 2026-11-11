//go:build rapid && testhooks

package cluster_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/internal/testutil"
)

// TestLogStreams_openAndTeardown_rapid checks the open, bind, and close
// roundtrip over random shared and manual multi-destination-cast topologies.
// Close returns the agent to the unbound state. Close clears the leader log
// recording channel. The table tests cover the fixed-input failure and
// rollback cases.
func TestLogStreams_openAndTeardown_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		tb := testutil.IterationTB(t, rt)
		topology := drawLogTopology(rt)

		adder := &fakeLogStreamAdder{}
		agent, logPublisher, logAdapter, deps := openLogStreamsDeps(tb, topology, adder)

		ls, err := cluster.OpenLogStreams(deps)
		require.NoError(rt, err)
		require.NotNil(rt, ls)

		logChannel := topology.LogChannel()
		streamID := topology.LogStreamID()
		requireLogStreamOpenOrder(rt, adder, streamID)
		require.Equal(rt, []string{logChannel}, adder.lastDestHook.Adds)
		require.Equal(rt, logChannel, agent.LiveLogDestination())

		seen := map[string]struct{}{}
		noteChannelTagsFrom(tb, adder, 0, seen)
		wantJoin := requireOpenLogPublication(tb, topology, adder, agent)

		sessionID, err := agent.AddLogPublication(0)
		require.NoError(rt, err)
		require.Equal(rt, adder.lastPub.SessionID(), sessionID)
		require.IsType(rt, (*consensus.LogAppendPosition)(nil), agent.AppendSource())
		require.IsType(rt, (*consensus.SubscriptionLogJoinSource)(nil), agent.LogJoinSource())
		require.Nil(rt, logAdapter.Image())
		require.Same(rt, adder.lastPub, logPublisher.Publication())
		require.Same(rt, adder.lastPub, ls.Publication())
		require.Same(rt, adder.lastSub, ls.Subscription())

		joinPub := newJoinLogPublication(tb, agent)
		requireLoggedJoinChannel(tb, agent, joinPub, wantJoin)

		pub := adder.lastPub
		sub := adder.lastSub
		require.NoError(rt, ls.Close())
		requireAgentLogUnbound(rt, agent, logPublisher)
		require.Empty(rt, agent.LeaderLogRecordingChannel())
		require.True(rt, pub.IsClosed())
		require.True(rt, sub.IsClosed())
		require.Nil(rt, ls.Publication())
		require.Nil(rt, ls.Subscription())
		requireLoggedJoinChannel(tb, agent, joinPub, "aeron-spy:")
	})
}

// Log publication reseed ops for TestLogStreams_reseedPublication_rapid.
const (
	reseedPublicationOK         = "reseed"
	reseedPublicationFailed     = "reseed add failure"
	reseedPublicationDestFailed = "reseed destination failure"
	reseedPublicationClose      = "close"
)

// TestLogStreams_reseedPublication_rapid checks reseeds, failed publication
// adds, failed destination adds, and Close. The model tracks the bound
// publication and the leader log recording channel. Every channel tag stays
// unique.
func TestLogStreams_reseedPublication_rapid(t *testing.T) {
	t.Parallel()
	cause := errors.New("driver refused reseed")
	destCause := errors.New("driver refused destination")
	rapid.Check(t, func(rt *rapid.T) {
		tb := testutil.IterationTB(t, rt)
		topology := drawLogTopology(rt)
		mdc := topology.IsLogMDC()

		adder := &fakeLogStreamAdder{}
		agent, logPublisher, _, deps := openLogStreamsDeps(tb, topology, adder)
		ls, err := cluster.OpenLogStreams(deps)
		require.NoError(rt, err)

		seen := map[string]struct{}{}
		noteChannelTagsFrom(tb, adder, 0, seen)
		wantJoin := requireOpenLogPublication(tb, topology, adder, agent)
		leader := agent.LeaderLogRecordingChannel()

		added := []*aeron.ExclusivePublication{adder.lastPub}
		bound, closed := true, false
		joinPub := newJoinLogPublication(tb, agent)
		requireLoggedJoinChannel(tb, agent, joinPub, wantJoin)

		opNames := []string{
			reseedPublicationOK,
			reseedPublicationFailed,
			reseedPublicationClose,
		}
		if mdc {
			opNames = append(opNames, reseedPublicationDestFailed)
		}
		ops := rapid.SampledFrom(opNames)
		opCount := rapid.IntRange(0, 32).Draw(rt, "opCount")
		for range opCount {
			op := ops.Draw(rt, "op")
			callsBefore := len(adder.calls)
			adder.failPubErr = nil
			adder.failSendAt = 0
			adder.failSendErr = nil

			if op == reseedPublicationClose {
				require.NoError(rt, ls.Close())
				bound, closed = false, true
				leader = ""
				wantJoin = "aeron-spy:"
			} else {
				frames := rapid.Int64Range(0, 1<<20).Draw(rt, "frames")
				seed := logPublicationSeedAt(frames * logbuffer.FrameAlignment)
				if op == reseedPublicationFailed {
					adder.failPubErr = cause
				}
				if op == reseedPublicationDestFailed {
					adder.failSendAt = 1
					adder.failSendErr = destCause
				}
				err := ls.ReseedPublicationAt(
					adder,
					topology.LogPublicationChannel(),
					topology.LogStreamID(),
					seed,
				)
				noteChannelTagsFrom(tb, adder, callsBefore, seen)
				switch {
				case closed:
					require.EqualError(rt, err, "cluster: log streams closed")
					require.Len(rt, adder.calls, callsBefore)
				case op == reseedPublicationFailed:
					require.ErrorIs(rt, err, cause)
					require.Len(rt, adder.calls, callsBefore+1)
					bound = false
				case op == reseedPublicationDestFailed:
					require.ErrorIs(rt, err, destCause)
					require.Len(rt, adder.calls, callsBefore+1)
					require.True(rt, adder.lastPub.IsClosed())
					bound = false
				default:
					require.NoError(rt, err)
					require.Len(rt, adder.calls, callsBefore+1)
					added = append(added, adder.lastPub)
					bound = true
					channel := adder.calls[len(adder.calls)-1].channel
					leader = requireLogPublication(tb, topology, adder, channel)
					if mdc {
						wantJoin = "aeron-spy:" + leader
					}
				}
			}

			require.Same(rt, ls.Publication(), logPublisher.Publication())
			require.Equal(rt, leader, agent.LeaderLogRecordingChannel())
			superseded := added
			if bound {
				last := added[len(added)-1]
				require.Same(rt, last, ls.Publication())
				require.False(rt, last.IsClosed())
				superseded = added[:len(added)-1]
			} else {
				require.Nil(rt, ls.Publication())
				require.Nil(rt, logPublisher.Publication())
			}
			for _, pub := range superseded {
				require.True(rt, pub.IsClosed())
			}
			requireLoggedJoinChannel(tb, agent, joinPub, wantJoin)
		}
		require.NoError(rt, ls.Close())
		require.Empty(rt, agent.LeaderLogRecordingChannel())
	})
}

// drawLogTopology draws a shared or manual multi-destination-cast topology.
func drawLogTopology(rt *rapid.T) *cluster.Topology {
	rt.Helper()
	memberCount, startID, portBase := drawValidMembersPorts(rt)
	thisIdx := rapid.IntRange(0, memberCount-1).Draw(rt, "thisIdx")
	hosts := drawHostnames(rt, memberCount)
	mdc := memberCount >= 2 && rapid.Bool().Draw(rt, "mdc")

	cfg := cluster.MembersStringConfig{
		StartingMemberID: startID,
		IngressHostnames: hosts,
		ClusterHostnames: hosts,
		PortBase:         portBase,
	}
	if !mdc && memberCount >= 2 {
		cfg.SharedLogEndpoint = expectedEndpoint(
			hosts[0],
			startID,
			portBase,
			cluster.LogPortOffset,
		)
	}
	members, err := cluster.BuildClusterMembersString(cfg)
	require.NoError(rt, err)
	tp, err := cluster.NewTopology(topologyConfigMembers(startID+int32(thisIdx), members))
	require.NoError(rt, err)
	require.Equal(rt, mdc, tp.IsLogMDC())
	return tp
}

func requireOpenLogPublication(
	t testing.TB,
	topology *cluster.Topology,
	adder *fakeLogStreamAdder,
	agent *consensus.Agent,
) string {
	t.Helper()
	channel := adder.calls[1].channel
	leader := requireLogPublication(t, topology, adder, channel)
	require.Equal(t, leader, agent.LeaderLogRecordingChannel())

	if topology.IsLogMDC() {
		return "aeron-spy:" + leader
	}

	require.Equal(t, topology.LogChannel(), channel)
	return "aeron-spy:" + topology.LogChannel()
}

func requireLogPublication(
	t testing.TB,
	topology *cluster.Topology,
	adder *fakeLogStreamAdder,
	channel string,
) string {
	t.Helper()
	if !topology.IsLogMDC() {
		require.NotContains(t, channel, "tags=")
		require.Empty(t, adder.lastSendHook.Adds)
		return ""
	}

	requireManualMDCPublication(t, channel, topology)
	require.Equal(t, topology.LogPublicationDestinations(), adder.lastSendHook.Adds)
	return leaderTagChannel(t, channel)
}

func requireManualMDCPublication(t testing.TB, channel string, tp *cluster.Topology) {
	t.Helper()
	require.NotContains(t, channel, "endpoint=")
	require.Equal(t, 1, strings.Count(channel, "control-mode="))
	require.Contains(t, channel, "control-mode=manual")
	require.Contains(t, channel, "control="+wantControlEndpoint(t, tp.ThisMember().LogEndpoint))
	require.Contains(t, channel, "ssc=false")
	require.Contains(t, channel, "alias=log")
	require.Contains(t, channel, "tags=")
}

func leaderTagChannel(t testing.TB, channel string) string {
	t.Helper()
	idx := aeron.ChannelParamIndex(channel, "tags=", true)
	require.GreaterOrEqual(t, idx, 0)
	value := aeron.ChannelParamValue(channel, idx, "tags=")
	tag, _, ok := strings.Cut(value, ",")
	require.True(t, ok)
	require.NotEmpty(t, tag)
	return "aeron:udp?tags=" + tag + "|alias=log"
}

func noteChannelTagsFrom(
	t testing.TB,
	adder *fakeLogStreamAdder,
	from int,
	seen map[string]struct{},
) {
	t.Helper()
	for _, call := range adder.calls[from:] {
		noteChannelTags(t, call.channel, seen)
	}
}

func noteChannelTags(t testing.TB, channel string, seen map[string]struct{}) {
	t.Helper()
	idx := aeron.ChannelParamIndex(channel, "tags=", true)
	if idx < 0 {
		return
	}
	value := aeron.ChannelParamValue(channel, idx, "tags=")
	require.NotEmpty(t, value)
	for tag := range strings.SplitSeq(value, ",") {
		require.NotEmpty(t, tag)
		_, dup := seen[tag]
		require.False(t, dup)
		seen[tag] = struct{}{}
	}
}

func requireLoggedJoinChannel(
	t testing.TB,
	agent *consensus.Agent,
	pub *aeron.Publication,
	want string,
) {
	t.Helper()
	pos := pub.Position()
	require.NoError(t, agent.JoinLogAsLeader(1, 0, 7, false))
	require.Greater(t, pub.Position(), pos)
	got := decodeJoinLog(t, pub, int(pos))
	require.Equal(t, want, string(got.LogChannel))
}

// Log subscription reseed ops for TestLogStreams_reseedSubscription_rapid.
const (
	reseedSubscriptionOK            = "reseed"
	reseedSubscriptionAddFailed     = "reseed add failure"
	reseedSubscriptionLiveFailed    = "reseed recovery destination failure"
	reseedSubscriptionDoubleFailure = "reseed double failure"
	reseedSubscriptionClose         = "close"
)

// TestLogStreams_reseedSubscription_rapid checks reseeds, failed reseed and
// recovery adds, and Close over random sequences. The model tracks the bound
// state, whether Close ran, the agent live field, and every subscription added.
func TestLogStreams_reseedSubscription_rapid(t *testing.T) {
	t.Parallel()
	topology := sharedLogTopology(t, 0, "")
	liveURI := topology.LogChannel()
	cause := errors.New("driver refused reseed")
	destCause := errors.New("driver refused live destination")
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		adder := &fakeLogStreamAdder{}
		agent, _, _, deps := openLogStreamsDeps(t, topology, adder)
		ls, err := cluster.OpenLogStreams(deps)
		require.NoError(rt, err)

		added := []*aeron.Subscription{adder.lastSub}
		lastJoin := agent.LogJoinSource()
		bound, closed := true, false
		ops := rapid.SampledFrom([]string{
			reseedSubscriptionOK,
			reseedSubscriptionAddFailed,
			reseedSubscriptionLiveFailed,
			reseedSubscriptionDoubleFailure,
			reseedSubscriptionClose,
		})
		opCount := rapid.IntRange(0, 32).Draw(rt, "opCount")
		for range opCount {
			op := ops.Draw(rt, "op")
			callsBefore := len(adder.calls)
			addedBefore := len(added)
			wantLive := ""

			if op == reseedSubscriptionClose {
				require.NoError(rt, ls.Close())
				bound, closed = false, true
			} else {
				// Clear live as Ensure does before each reseed.
				agent.LiveLogDestinationSet("")
				adder.failNextSubscriptionAdds(0, nil)
				adder.failDestErr = nil
				switch op {
				case reseedSubscriptionAddFailed:
					adder.failNextSubscriptionAdds(1, cause)
				case reseedSubscriptionLiveFailed:
					adder.failNextSubscriptionAdds(1, cause)
					adder.failDestErr = destCause
				case reseedSubscriptionDoubleFailure:
					adder.failNextSubscriptionAdds(2, cause)
				}

				err := ls.ReseedSubscription(adder)
				switch {
				case closed:
					require.EqualError(rt, err, "cluster: log streams closed")
					require.Len(rt, adder.calls, callsBefore)
				case op == reseedSubscriptionOK:
					require.NoError(rt, err)
					require.Len(rt, adder.calls, callsBefore+1)
					added = append(added, adder.lastSub)
					bound = true
				default:
					require.ErrorIs(rt, err, cause)
					require.Len(rt, adder.calls, callsBefore+2)
					bound = op != reseedSubscriptionDoubleFailure
					if bound {
						added = append(added, adder.lastSub)
					}
					switch op {
					case reseedSubscriptionAddFailed:
						wantLive = liveURI
					case reseedSubscriptionLiveFailed:
						require.ErrorIs(rt, err, destCause)
					}
				}
				for _, c := range adder.calls[callsBefore:] {
					require.Equal(rt, "subscription", c.kind)
					requireManualLogSubscriptionChannel(rt, c.channel)
				}
			}

			superseded := added
			join := agent.LogJoinSource()
			if bound {
				last := added[len(added)-1]
				require.Same(rt, last, ls.Subscription())
				require.False(rt, last.IsClosed())
				require.IsType(rt, (*consensus.SubscriptionLogJoinSource)(nil), join)
				// A fresh bind rebinds the join source.
				if len(added) > addedBefore && lastJoin != nil {
					require.NotSame(rt, lastJoin, join)
				}
				superseded = added[:len(added)-1]
			} else {
				require.Nil(rt, ls.Subscription())
				require.Nil(rt, join)
			}
			lastJoin = join
			for _, sub := range superseded {
				require.True(rt, sub.IsClosed())
			}
			require.Equal(rt, wantLive, agent.LiveLogDestination())
		}
		require.NoError(rt, ls.Close())
	})
}
