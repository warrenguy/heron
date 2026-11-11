//go:build testhooks

package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/consensus"
)

// TestOpenLogStreams_setsLocalLogStream asserts OpenLogStreams wires the
// resolved publication channel and log stream id into the agent for leader
// JoinLog.
func TestOpenLogStreams_setsLocalLogStream(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, testLogChannelTemplate)
	adder := &fakeLogStreamAdder{}
	_, agent, _, _ := openLogStreamsOK(t, topology, adder)

	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy := consensus.NewServiceProxy()
	proxy.SetPublication(pub)
	agent.SetServiceProxy(proxy)

	const logPos int64 = 42
	require.NoError(t, agent.JoinLogAsLeader(1, logPos, 7, true))
	require.Equal(t, 1, agent.PendingServiceAckWaits())
	require.Equal(t, logPos, agent.ExpectedAckPosition())

	got := decodeJoinLog(t, pub, 0)
	require.Equal(t, topology.LogStreamID(), got.LogStreamID)
	// UDP log channel takes the spy prefix for leader JoinLog.
	require.Equal(t, "aeron-spy:"+topology.LogChannel(), string(got.LogChannel))
}

// TestOpenLogStreams_setsLeaderTagChannel asserts a manual
// multi-destination-cast log names the leader JoinLog and the LOCAL recording
// by the publication channel tag. A reseed draws the next tag.
func TestOpenLogStreams_setsLeaderTagChannel(t *testing.T) {
	t.Parallel()

	topology, err := cluster.NewTopology(
		topologyConfigMembers(0, goldenClusterMembersFailover),
	)
	require.NoError(t, err)
	adder := &fakeLogStreamAdder{}
	ls, agent, _, _ := openLogStreamsOK(t, topology, adder)

	const openLeader = "aeron:udp?tags=3|alias=log"
	require.Equal(t, openLeader, agent.LeaderLogRecordingChannel())
	requireJoinLogChannel(t, agent, "aeron-spy:"+openLeader, topology.LogStreamID())

	require.NoError(t, ls.ReseedPublicationAt(
		adder,
		topology.LogPublicationChannel(),
		topology.LogStreamID(),
		nil,
	))
	const reseedLeader = "aeron:udp?tags=5|alias=log"
	require.Equal(t, reseedLeader, agent.LeaderLogRecordingChannel())
	requireJoinLogChannel(t, agent, "aeron-spy:"+reseedLeader, topology.LogStreamID())
}

func requireJoinLogChannel(
	t testing.TB,
	agent *consensus.Agent,
	want string,
	streamID int32,
) {
	t.Helper()

	pub := newJoinLogPublication(t, agent)
	require.NoError(t, agent.JoinLogAsLeader(1, 0, 7, true))
	got := decodeJoinLog(t, pub, 0)
	require.Equal(t, streamID, got.LogStreamID)
	require.Equal(t, want, string(got.LogChannel))
}

func newJoinLogPublication(tb testing.TB, agent *consensus.Agent) *aeron.Publication {
	tb.Helper()
	pub := aeron.NewTestPublicationForHooksWithConnected(tb, true)
	proxy := consensus.NewServiceProxy()
	proxy.SetPublication(pub)
	agent.SetServiceProxy(proxy)
	return pub
}

// decodeJoinLog decodes the JoinLog frame at offset in term 0 of pub.
func decodeJoinLog(t testing.TB, pub *aeron.Publication, offset int) codecs.JoinLogFields {
	t.Helper()

	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
	require.Positive(t, frameLength)
	wire := termBuf.Bytes()[offset+logbuffer.OffsetFramePayload : offset+frameLength]
	got, ok := codecs.TryDecodeJoinLogAt(wire, 0)
	require.True(t, ok)

	return got
}

// Asserts Close clears the local log stream so a later JoinLog does not
// advertise the closed publication channel or stream id.
func TestOpenLogStreams_closeClearsLocalLogStream(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, testLogChannelTemplate)
	adder := &fakeLogStreamAdder{}
	ls, agent, _, _ := openLogStreamsOK(t, topology, adder)

	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy := consensus.NewServiceProxy()
	proxy.SetPublication(pub)
	agent.SetServiceProxy(proxy)

	require.NoError(t, ls.Close())

	// After Close the local stream is cleared. JoinLog still offers on the
	// bound proxy. Advertised stream id is 0. Empty base channel is not IPC, so
	// the channel is spy prefix only.
	const logPos int64 = 7
	require.NoError(t, agent.JoinLogAsLeader(1, logPos, 3, true))
	require.Equal(t, 1, agent.PendingServiceAckWaits())

	got := decodeJoinLog(t, pub, 0)
	require.Equal(t, int32(0), got.LogStreamID)
	require.Equal(t, "aeron-spy:", string(got.LogChannel))
}
