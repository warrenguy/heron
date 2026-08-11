//go:build testhooks

package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/consensus"
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

	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)
	wire := termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLength]
	got, ok := codecs.TryDecodeJoinLogAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, topology.LogStreamID(), got.LogStreamID)
	// UDP log channel takes the spy prefix for leader JoinLog.
	require.Equal(t, "aeron-spy:"+topology.LogChannel(), string(got.LogChannel))
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

	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)
	wire := termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLength]
	got, ok := codecs.TryDecodeJoinLogAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, int32(0), got.LogStreamID)
	require.Equal(t, "aeron-spy:", string(got.LogChannel))
}
