//go:build testhooks

package consensus

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func encodeServiceAckWire(t testing.TB, fields codecs.ServiceAckFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.ServiceAckSbeSchemaVersion)
	buf := make([]byte, codecs.ServiceAckEncodedLen())
	end, ok := codecs.EncodeServiceAck(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeSessionCloseEventWire(t testing.TB) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.SessionCloseEventSbeSchemaVersion)
	buf := make([]byte, codecs.SessionCloseEventEncodedLen())
	end, ok := codecs.EncodeSessionCloseEvent(buf, 0, actingVersion, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
	require.True(t, ok)
	return buf[:end]
}

func agentWithServiceIngress(serviceCount int) (*Agent, *QueueConsensusIngress) {
	return agentWithServiceIngressConfig(AgentConfig{ServiceCount: serviceCount})
}

func agentWithServiceIngressConfig(cfg AgentConfig) (*Agent, *QueueConsensusIngress) {
	a := NewAgent(cfg)
	return a, attachQueueServiceIngress(a)
}

// attachQueueServiceIngress wires a fresh queue service ingress onto a.
func attachQueueServiceIngress(a *Agent) *QueueConsensusIngress {
	q := &QueueConsensusIngress{}
	a.SetServiceIngress(q)
	return q
}

// agentWithServiceControl wires service ingress and a connected service proxy.
func agentWithServiceControl(
	t testing.TB,
	serviceCount int,
) (*Agent, *QueueConsensusIngress, *aeron.Publication) {
	t.Helper()
	a, q := agentWithServiceIngress(serviceCount)
	p, pub := boundServiceProxy(t)
	a.SetServiceProxy(p)
	return a, q, pub
}

// enqueueServiceAck queues one ServiceAck frame from serviceID. RelevantID is
// filler and is not asserted.
func enqueueServiceAck(
	t testing.TB,
	q *QueueConsensusIngress,
	logPosition, ackID int64,
	serviceID int32,
) {
	t.Helper()
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: logPosition,
		AckID:       ackID,
		RelevantID:  ackID + 1,
		ServiceID:   serviceID,
	}))
}

// decodeJoinLogFromPub decodes the first JoinLog frame from a test publication.
func decodeJoinLogFromPub(t *testing.T, pub *aeron.Publication) codecs.JoinLogFields {
	t.Helper()
	wire, _ := termWireAt(t, pub, 0)
	got, ok := codecs.TryDecodeJoinLogAt(wire, 0)
	require.True(t, ok)
	return got
}

// decodeRequestServiceAckFromPub decodes the first RequestServiceAck frame.
func decodeRequestServiceAckFromPub(
	t *testing.T,
	pub *aeron.Publication,
) codecs.RequestServiceAckFields {
	t.Helper()
	wire, _ := termWireAt(t, pub, 0)
	got, ok := codecs.TryDecodeRequestServiceAckAt(wire, 0)
	require.True(t, ok)
	return got
}

// prepareClearRunner calls PrepareForNewLeadership once per DoWork.
type prepareClearRunner struct {
	agent *Agent
}

func (r prepareClearRunner) DoWork(int64) int {
	_, _, _ = r.agent.PrepareForNewLeadership(0, 1)
	return 0
}

func (prepareClearRunner) HandleConsensusMessage([]byte, int) bool { return false }

func TestPollServiceControl_nilIngress_zero(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1})
	require.Nil(t, a.ServiceIngress())
	require.NotPanics(t, func() {
		require.Equal(t, 0, a.pollServiceControl())
	})
	require.Equal(t, 0, a.DoWork(1))
}

func TestSetServiceIngress_wireAndClear(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)
	require.Same(t, q, a.ServiceIngress())

	enqueueServiceAck(t, q, 10, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))

	a.SetServiceIngress(nil)
	require.Nil(t, a.ServiceIngress())
	require.Equal(t, 0, a.pollServiceControl())
}

func TestHandleServiceAck_happy(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)
	fields := codecs.ServiceAckFields{
		LogPosition: 42, Timestamp: 99, AckID: 0, RelevantID: 7, ServiceID: 0,
	}
	q.Enqueue(encodeServiceAckWire(t, fields))

	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())

	// Asserts check-only hasReached at the offered position matches the offered
	// fields and receive left ServiceAckFailure nil.
	ok, err := a.hasReachedServiceAcks(fields.LogPosition)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, a.ServiceAckFailure())
	// Depth unchanged after check-only hasReached.
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
}

func TestHandleServiceAck_receiveTimeMismatch(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)

	// First queued entry carries ack id 1 while expected id is 0.
	enqueueServiceAck(t, q, 10, 1, 0)
	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	first := a.ServiceAckFailure()
	require.ErrorIs(t, first, ErrServiceAckOutOfSequence)

	// Second mismatched receive keeps the first cause (marked once).
	enqueueServiceAck(t, q, 20, 2, 0)
	require.Equal(t, 1, a.DoWork(2))
	require.Equal(t, 2, a.ServiceAckQueueDepth(0))
	require.Same(t, first, a.ServiceAckFailure())
}

func TestHandleServiceAck_emptyLowerNoReceiveMark(t *testing.T) {
	t.Parallel()

	// Service 0 queue empty; mismatched ack arrives for service 1. Walk stops
	// at the empty lower-id queue: no failure marked at receive.
	a, q := agentWithServiceIngress(2)
	enqueueServiceAck(t, q, 10, 99, 1)
	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, 1, a.ServiceAckQueueDepth(1))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceAck_batchOrder(t *testing.T) {
	t.Parallel()

	// Multiple ServiceAcks in one poll batch. Offers keep arrival order.
	// The receive-time check compares the first queued entry against the
	// expected ack id of the service, not each incoming frame position.
	// Asserts a FIFO of several ack ids does not mark an out-of-sequence
	// failure.
	a, q := agentWithServiceIngress(1)
	for i := range 3 {
		q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
			LogPosition: int64(i + 1),
			AckID:       int64(i),
			RelevantID:  int64(i * 10),
			ServiceID:   0,
		}))
	}
	require.Equal(t, 3, a.pollServiceControl())
	require.Equal(t, 3, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
	// First queued entry is still ack id 0 at log position 1.
	ok, err := a.hasReachedServiceAcks(1)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestHandleServiceAck_multiGenRemoveBetween(t *testing.T) {
	t.Parallel()

	// Two acks with a remove between them. Asserts receive stays
	// clean and the next queued entry matches after advance.
	a, q := agentWithServiceIngress(1)
	enqueueServiceAck(t, q, 10, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())
	ok, err := a.hasReachedServiceAcks(10)
	require.True(t, ok)
	require.NoError(t, err)
	a.removeServiceAckHead()
	require.Equal(t, int64(1), a.ExpectedServiceAckID())

	enqueueServiceAck(t, q, 20, 1, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())
	ok, err = a.hasReachedServiceAcks(20)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
}

func TestHandleServiceAck_multiGenBufferedThenRemove(t *testing.T) {
	t.Parallel()

	// Two acks land before any remove. The queue holds both.
	// Asserts the first remove exposes the next queued entry for expected id 1.
	a, q := agentWithServiceIngress(1)
	enqueueServiceAck(t, q, 10, 0, 0)
	enqueueServiceAck(t, q, 20, 1, 0)
	require.Equal(t, 2, a.pollServiceControl())
	require.Equal(t, 2, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())

	ok, err := a.hasReachedServiceAcks(10)
	require.True(t, ok)
	require.NoError(t, err)
	a.removeServiceAckHead()
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))

	ok, err = a.hasReachedServiceAcks(20)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceAck_outOfRangeServiceID(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)

	// Pre-seed a mismatched first entry so a hostile out-of-range frame must
	// not fire detection on a refused offer.
	require.True(t, a.serviceAcks.offer(0, serviceAck{ackID: 99, logPosition: 10}))

	for _, serviceID := range []int32{5, -1} {
		enqueueServiceAck(t, q, 10, 0, serviceID)
	}
	require.Equal(t, 2, a.pollServiceControl())
	// Depth still only the pre-seeded entry. No failure from refused offers.
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceControlFragment_unknownTemplate(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)
	q.Enqueue(encodeSessionCloseEventWire(t))
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceControlFragment_undecodable(t *testing.T) {
	t.Parallel()

	const sessionID int64 = 1
	a, q := agentWithServiceIngress(1)
	s := seedLearnedOpen(t, a, sessionID, 1, 8)

	// Short buffer. Template peek fails.
	q.Enqueue([]byte{0x01, 0x02})
	// Wrong schema id. Template peek rejects.
	bad := make([]byte, 16)
	// blockLength=8, template=33, schema=0 (wrong), version=0
	bad[0] = 8
	bad[2] = 33
	q.Enqueue(bad)
	// Valid cluster schema and ServiceAck template. Truncated body makes
	// TryDecodeServiceAckAt return false after the template match.
	truncated := make([]byte, codecs.MessageHeaderEncodedLen())
	binary.LittleEndian.PutUint16(truncated[0:], clusterwire.ServiceAckSbeBlockLength)
	binary.LittleEndian.PutUint16(truncated[2:], clusterwire.ServiceAckSbeTemplateID)
	binary.LittleEndian.PutUint16(truncated[4:], clusterwire.MessageHeaderSbeSchemaID)
	binary.LittleEndian.PutUint16(truncated[6:], clusterwire.ServiceAckSbeSchemaVersion)
	q.Enqueue(truncated)
	// CloseSession frame cut to a 4-byte body. The decode fails.
	q.Enqueue(encodeCloseSessionWire(t, sessionID)[:codecs.MessageHeaderEncodedLen()+4])

	require.NotPanics(t, func() {
		require.Equal(t, 4, a.pollServiceControl())
	})
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, 1, a.OpenSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
}

func TestHandleServiceAck_electionLive(t *testing.T) {
	t.Parallel()

	// Acks still drain into queues while election is live (always polled).
	a, q := agentWithServiceIngressConfig(AgentConfig{
		ServiceCount: 1,
		Election:     &orderRunner{n: 0},
	})
	enqueueServiceAck(t, q, 10, 0, 0)
	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}

func TestSetServiceProxy_wireAndClear(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1})
	require.Nil(t, a.ServiceProxy())

	p, _ := boundServiceProxy(t)
	a.SetServiceProxy(p)
	require.Same(t, p, a.ServiceProxy())

	a.SetServiceProxy(nil)
	require.Nil(t, a.ServiceProxy())
}

func TestSetLocalLogStream(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{})
	a.SetLocalLogStream("aeron:udp?endpoint=localhost:0", 100)
	require.Equal(t, "aeron:udp?endpoint=localhost:0", a.localLogChannel)
	require.Equal(t, int32(100), a.localLogStreamID)
}

func TestJoinLogAsLeader_publishIPC(t *testing.T) {
	t.Parallel()

	const (
		memberID  int32 = 3
		logPos    int64 = 1000
		sessionID int32 = 7
		streamID  int32 = 100
		isStartup       = true
	)
	a, _, pub := agentWithServiceControl(t, 1)
	a.thisMemberID = memberID
	a.SetLocalLogStream("aeron:ipc?term-length=128k", streamID)

	require.NoError(t, a.JoinLogAsLeader(1, logPos, sessionID, isStartup))

	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, logPos, a.ExpectedAckPosition())
	require.False(t, a.HasPendingJoinLog())
	require.NoError(t, a.ServicePublishFailure())

	// IPC channel is not spy-prefixed.
	require.Equal(t, codecs.JoinLogFields{
		LogPosition:    logPos,
		MaxLogPosition: math.MaxInt64,
		MemberID:       memberID,
		LogSessionID:   sessionID,
		LogStreamID:    streamID,
		IsStartup:      clusterwire.BooleanType_TRUE,
		Role:           RoleLeader.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte("aeron:ipc?term-length=128k"),
	}, decodeJoinLogFromPub(t, pub))
}

func TestJoinLogAsLeader_publishUDP_spyPrefix(t *testing.T) {
	t.Parallel()

	const channel = "aeron:udp?endpoint=localhost:40123"
	a, _, pub := agentWithServiceControl(t, 1)
	a.SetLocalLogStream(channel, 100)

	require.NoError(t, a.JoinLogAsLeader(1, 50, 1, false))

	got := decodeJoinLogFromPub(t, pub)
	require.Equal(t, []byte(spyChannelPrefix+channel), got.LogChannel)
	require.Equal(t, clusterwire.BooleanType_FALSE, got.IsStartup)
	require.Equal(t, RoleLeader.Code(), got.Role)
}

func TestJoinLogAsFollower_publishFreshBind(t *testing.T) {
	t.Parallel()

	const (
		memberID  int32 = 4
		sessionID int32 = 11
		joinPos   int64 = 96
		streamID  int32 = 100
		channel         = "aeron:udp?endpoint=localhost:9"
	)
	a, _, adapter := agentWithLogJoin(t, sessionID, joinPos, AgentConfig{
		ServiceCount: 1,
		ThisMemberID: memberID,
	})
	p, pub := boundServiceProxy(t)
	a.SetServiceProxy(p)

	params := FollowerLogJoinParams{
		LogChannel:      channel,
		LogStreamID:     streamID,
		LogSessionID:    sessionID,
		LogJoinPosition: joinPos,
		IsLeaderStartup: true,
	}
	joined, err := a.JoinLogAsFollower(params, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, adapter.Image(), a.logAdapter.Image())

	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, joinPos, a.ExpectedAckPosition())
	require.False(t, a.HasPendingJoinLog())

	// Follower channel is unchanged (no spy prefix).
	require.Equal(t, codecs.JoinLogFields{
		LogPosition:    joinPos,
		MaxLogPosition: math.MaxInt64,
		MemberID:       memberID,
		LogSessionID:   sessionID,
		LogStreamID:    streamID,
		IsStartup:      clusterwire.BooleanType_TRUE,
		Role:           RoleFollower.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(channel),
	}, decodeJoinLogFromPub(t, pub))
}

func TestJoinLogAsFollower_boundRejoin_noPublish(t *testing.T) {
	t.Parallel()

	const sessionID int32 = 10
	a, image, adapter := agentWithLogJoin(t, sessionID, 0, AgentConfig{
		ServiceCount: 1,
	})
	adapter.SetImage(image)
	p, pub := boundServiceProxy(t)
	a.SetServiceProxy(p)
	posBefore := pub.Position()

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{
		LogSessionID:  sessionID,
		IsBoundRejoin: true,
	}, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Equal(t, posBefore, pub.Position())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.False(t, a.HasPendingJoinLog())
}

func TestJoinLog_noProxy_noPublish(t *testing.T) {
	t.Parallel()

	// No proxy: both publish paths match pre-proxy behaviour.
	a := NewAgent(AgentConfig{ServiceCount: 1})
	require.NoError(t, a.JoinLogAsLeader(1, 100, 1, true))
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.False(t, a.HasPendingJoinLog())
	require.Equal(t, int64(nullValue), a.ExpectedAckPosition())

	a2, _, _ := agentWithLogJoin(t, 10, 0, AgentConfig{ServiceCount: 1})
	joined, err := a2.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 10}, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Equal(t, 0, a2.PendingServiceAckWaits())
	require.False(t, a2.HasPendingJoinLog())
}

func TestJoinLog_pendingPublishRetry(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1, ThisMemberID: 1})
	// Not connected: JoinLog stores pending, queues no wait.
	disconnected := boundServiceProxyWithLimit(t, 0, false)
	a.SetServiceProxy(disconnected)
	a.SetLocalLogStream("aeron:ipc", 100)

	require.NoError(t, a.JoinLogAsLeader(1, 200, 5, false))
	require.True(t, a.HasPendingJoinLog())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.NoError(t, a.ServicePublishFailure())
	require.Equal(t, int64(200), a.pendingJoinLog.LogPosition)

	// Second join before retry replaces the pending fields.
	require.NoError(t, a.JoinLogAsLeader(2, 300, 6, true))
	require.True(t, a.HasPendingJoinLog())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(300), a.pendingJoinLog.LogPosition)
	require.Equal(t, clusterwire.BooleanType_TRUE, a.pendingJoinLog.IsStartup)

	// Connected publication: pending retry publishes and queues the wait.
	connected, pub := boundServiceProxy(t)
	a.SetServiceProxy(connected)
	require.Equal(t, 1, a.retryPendingJoinLog())
	require.False(t, a.HasPendingJoinLog())
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(300), a.ExpectedAckPosition())

	got := decodeJoinLogFromPub(t, pub)
	require.Equal(t, int64(300), got.LogPosition)
	require.Equal(t, int32(6), got.LogSessionID)
	require.Equal(t, clusterwire.BooleanType_TRUE, got.IsStartup)
}

func TestJoinLog_terminalPublishMarksOnce(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1})
	p, pub := boundServiceProxy(t)
	require.NoError(t, pub.Close())
	a.SetServiceProxy(p)
	a.SetLocalLogStream("aeron:ipc", 100)

	require.NoError(t, a.JoinLogAsLeader(1, 10, 1, false))
	first := a.ServicePublishFailure()
	require.Error(t, first)
	require.False(t, a.HasPendingJoinLog())
	require.Equal(t, 0, a.PendingServiceAckWaits())

	// Later terminal outcome keeps the first cause.
	require.NoError(t, a.JoinLogAsLeader(2, 20, 2, false))
	require.Same(t, first, a.ServicePublishFailure())
	require.Equal(t, 0, a.PendingServiceAckWaits())
}

func TestPrepareForNewLeadership_clearsPendingKeepsWaits(t *testing.T) {
	t.Parallel()

	a, _, _ := agentWithServiceControl(t, 1)
	a.SetLocalLogStream("aeron:ipc", 100)
	require.NoError(t, a.JoinLogAsLeader(1, 111, 1, false))
	require.Equal(t, 1, a.PendingServiceAckWaits())

	// Force a pending JoinLog on a disconnected proxy.
	a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
	require.NoError(t, a.JoinLogAsLeader(2, 222, 2, false))
	require.True(t, a.HasPendingJoinLog())
	// First wait still present; pending is separate.
	require.Equal(t, 1, a.PendingServiceAckWaits())

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, a.HasPendingJoinLog())
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(111), a.ExpectedAckPosition())
}

func TestServiceControlWork_waitConsume(t *testing.T) {
	t.Parallel()

	a, q, _ := agentWithServiceControl(t, 1)
	require.NoError(t, a.RequestServiceAck(42))
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(0), a.ExpectedServiceAckID())

	// Partial first entries leave the wait queued.
	require.Equal(t, 0, a.serviceControlWork())
	require.Equal(t, 1, a.PendingServiceAckWaits())

	enqueueServiceAck(t, q, 42, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, int64(nullValue), a.ExpectedAckPosition())
}

func TestServiceControlWork_oneWaitPerTick(t *testing.T) {
	t.Parallel()

	a, q, _ := agentWithServiceControl(t, 1)
	require.NoError(t, a.RequestServiceAck(10))
	require.NoError(t, a.RequestServiceAck(20))
	require.Equal(t, 2, a.PendingServiceAckWaits())

	// Both ack rounds present before any consume.
	enqueueServiceAck(t, q, 10, 0, 0)
	enqueueServiceAck(t, q, 20, 1, 0)
	require.Equal(t, 2, a.pollServiceControl())

	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(20), a.ExpectedAckPosition())
	require.Equal(t, int64(1), a.ExpectedServiceAckID())

	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
}

func TestServiceControlWork_FIFOJoinThenRequest(t *testing.T) {
	t.Parallel()

	a, q, _ := agentWithServiceControl(t, 1)
	a.SetLocalLogStream("aeron:ipc", 100)

	require.NoError(t, a.JoinLogAsLeader(1, 100, 1, false))
	require.NoError(t, a.RequestServiceAck(200))
	require.Equal(t, 2, a.PendingServiceAckWaits())
	require.Equal(t, int64(100), a.ExpectedAckPosition())

	// First round: JoinLog acks.
	enqueueServiceAck(t, q, 100, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, int64(200), a.ExpectedAckPosition())
	require.Equal(t, int64(1), a.ExpectedServiceAckID())

	// Second round: RequestServiceAck acks.
	enqueueServiceAck(t, q, 200, 1, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
}

func TestServiceControlWork_overlappingJoinLogs(t *testing.T) {
	t.Parallel()

	// Every delivered frame acked in order: both waits complete.
	a, q, _ := agentWithServiceControl(t, 1)
	a.SetLocalLogStream("aeron:ipc", 100)

	require.NoError(t, a.JoinLogAsLeader(1, 10, 1, false))
	require.NoError(t, a.JoinLogAsLeader(2, 20, 2, false))
	require.Equal(t, 2, a.PendingServiceAckWaits())

	enqueueServiceAck(t, q, 10, 0, 0)
	enqueueServiceAck(t, q, 20, 1, 0)
	require.Equal(t, 2, a.pollServiceControl())

	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, int64(20), a.ExpectedAckPosition())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())
}

func TestServiceControlWork_contractViolation_marksFailure(t *testing.T) {
	t.Parallel()

	// Service skips wait pos 10, acks only pos 20. Receive stays clean
	// (first entry matches expected id 0). Wait site marks failure once; both
	// waits stay queued.
	a, q, _ := agentWithServiceControl(t, 1)
	a.SetLocalLogStream("aeron:ipc", 100)

	require.NoError(t, a.JoinLogAsLeader(1, 10, 1, false))
	require.NoError(t, a.JoinLogAsLeader(2, 20, 2, false))

	enqueueServiceAck(t, q, 20, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())

	require.Equal(t, 0, a.serviceControlWork())
	require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
	require.Equal(t, 2, a.PendingServiceAckWaits())
	require.Equal(t, int64(10), a.ExpectedAckPosition())
	require.Equal(t, int64(0), a.ExpectedServiceAckID())
}

func TestServiceControlWork_failureConsult_pendingStillRetries(t *testing.T) {
	t.Parallel()

	a, q, _ := agentWithServiceControl(t, 1)
	a.serviceAckFailure = ErrServiceAckOutOfSequence

	// Seed a wait via white-box so the consume half has something to skip.
	a.serviceAckWaits = append(a.serviceAckWaits, 10)
	enqueueServiceAck(t, q, 10, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())

	// Pending JoinLog still retries under the mark.
	a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
	a.SetLocalLogStream("aeron:ipc", 100)
	pending := sampleJoinLogFields()
	pending.LogPosition = 20
	pending.LogSessionID = 1
	pending.IsStartup = clusterwire.BooleanType_FALSE
	a.publishOrQueueJoinLog(pending)
	require.True(t, a.HasPendingJoinLog())

	connected, pub := boundServiceProxy(t)
	a.SetServiceProxy(connected)
	// Pending retry still runs under the mark. The frame publishes.
	require.Equal(t, 1, a.retryPendingJoinLog())
	require.False(t, a.HasPendingJoinLog())
	// Wait not consumed under the mark.
	require.Equal(t, 0, a.serviceControlWork())
	// No new wait under the mark. Marked waits would never drain.
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(10), a.ExpectedAckPosition())
	require.Equal(t, int64(0), a.ExpectedServiceAckID())
	require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
	require.Positive(t, pub.Position())
}

// Asserts DoWork retries pending only after the election branch, so Prepare
// can drop a superseded unpublished JoinLog before any offer.
func TestDoWork_prepareClearsPendingBeforeRetry(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1, ThisMemberID: 1})
	disconnected := boundServiceProxyWithLimit(t, 0, false)
	a.SetServiceProxy(disconnected)
	a.SetLocalLogStream("aeron:ipc", 100)
	require.NoError(t, a.JoinLogAsLeader(1, 500, 9, false))
	require.True(t, a.HasPendingJoinLog())

	connected, pub := boundServiceProxy(t)
	a.SetServiceProxy(connected)

	// Election.DoWork clears pending via Prepare before the post-branch retry.
	a.election = prepareClearRunner{agent: a}
	require.Equal(t, 0, a.DoWork(1))
	require.False(t, a.HasPendingJoinLog())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(0), pub.Position())
}

func TestServiceControlWork_waitSiteMismatch(t *testing.T) {
	t.Parallel()

	// Mismatched first entry stopped earlier in the walk: empty lower queue at
	// receive, filled later with a pair that matches receive-time (first-entry
	// position) but not the first wait position.
	a, q, _ := agentWithServiceControl(t, 2)
	require.NoError(t, a.RequestServiceAck(10))

	// Service 1 first, wrong for wait pos 10 but service 0 empty: no receive mark.
	enqueueServiceAck(t, q, 20, 0, 1)
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())

	// Service 0 fills with the same (0, 20) pair. Receive-time checks
	// hasReached(20): both first entries match at 20. No receive mark.
	enqueueServiceAck(t, q, 20, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())

	// Wait site checks position 10: mismatch marks once; wait stays.
	require.Equal(t, 0, a.serviceControlWork())
	require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(0), a.ExpectedServiceAckID())

	// Second mismatch keeps the first cause.
	first := a.ServiceAckFailure()
	require.Equal(t, 0, a.serviceControlWork())
	require.Same(t, first, a.ServiceAckFailure())
}

func TestRequestServiceAck_success(t *testing.T) {
	t.Parallel()

	a, _, pub := agentWithServiceControl(t, 1)
	const logPos int64 = 4242
	require.NoError(t, a.RequestServiceAck(logPos))
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, logPos, a.ExpectedAckPosition())

	got := decodeRequestServiceAckFromPub(t, pub)
	require.Equal(t, logPos, got.LogPosition)
}

func TestRequestServiceAck_unbound(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1})
	err := a.RequestServiceAck(1)
	require.ErrorIs(t, err, ErrServiceControlUnbound)
	require.Equal(t, 0, a.PendingServiceAckWaits())
}

func TestRequestServiceAck_failureMarked(t *testing.T) {
	t.Parallel()

	a, _, _ := agentWithServiceControl(t, 1)
	a.serviceAckFailure = ErrServiceAckOutOfSequence
	err := a.RequestServiceAck(1)
	require.ErrorIs(t, err, ErrServiceAckOutOfSequence)
	require.Equal(t, 0, a.PendingServiceAckWaits())
}

func TestRequestServiceAck_publishFailure(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1})
	a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
	err := a.RequestServiceAck(1)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrServiceControlUnbound)
	require.Equal(t, 0, a.PendingServiceAckWaits())
	// RequestServiceAck does not mark servicePublishFailure (JoinLog path only).
	require.NoError(t, a.ServicePublishFailure())
}

func TestServiceControlWork_emptyAndNilProxy(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 0})
	require.Equal(t, 0, a.serviceControlWork())
	require.Equal(t, 0, a.DoWork(1))
}

func TestSpyPrefixLogChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "ipc bare", in: "aeron:ipc", want: "aeron:ipc"},
		{name: "ipc params", in: "aeron:ipc?term-length=128k", want: "aeron:ipc?term-length=128k"},
		{
			name: "udp spy",
			in:   "aeron:udp?endpoint=localhost:0",
			want: spyChannelPrefix + "aeron:udp?endpoint=localhost:0",
		},
		{name: "empty", in: "", want: spyChannelPrefix},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, spyPrefixLogChannel(tc.in))
		})
	}
}
