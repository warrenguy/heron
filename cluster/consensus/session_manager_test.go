//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func sampleConnectFieldsForSession(correlationID int64) codecs.SessionConnectRequestFields {
	fields := sampleConnectFields()
	fields.CorrelationID = correlationID
	return fields
}

// leaderWithLogPublisher returns a settled leader with p and leadershipTermID.
func leaderWithLogPublisher(p *LogPublisher, leadershipTermID int64) *Agent {
	return NewAgent(AgentConfig{
		Role:             RoleLeader,
		LeadershipTermID: leadershipTermID,
		LogPublisher:     p,
	})
}

// decodeSessionOpenAtTermOffset decodes one SessionOpenEvent DATA frame at
// termOffset. Returns the fields and the next aligned term offset.
func decodeSessionOpenAtTermOffset(
	t *testing.T,
	pub *aeron.ExclusivePublication,
	termOffset int,
) (codecs.SessionOpenEventFields, int) {
	t.Helper()
	wire, next := termWireAt(t, pub, termOffset)
	fields, ok := codecs.TryDecodeSessionOpenEventAt(wire, 0)
	require.True(t, ok)
	return fields, next
}

// requireClientSessionFields asserts one registry session holds the connect
// fields, state, and accept clock.
func requireClientSessionFields(
	t *testing.T,
	s *clientSession,
	fields codecs.SessionConnectRequestFields,
	state sessionState,
	lastActivityNs int64,
) {
	t.Helper()
	require.Equal(t, state, s.state)
	require.Equal(t, fields.CorrelationID, s.correlationID)
	require.Equal(t, fields.ResponseStreamID, s.responseStreamID)
	require.Equal(t, fields.ResponseChannel, s.responseChannel)
	require.Equal(t, lastActivityNs, s.lastActivityNs)
}

func requireSessionOpenFields(
	t *testing.T,
	got codecs.SessionOpenEventFields,
	termID, corr, sessionID, timestamp int64,
	streamID int32,
	channel []byte,
) {
	t.Helper()
	require.Equal(t, termID, got.LeadershipTermID)
	require.Equal(t, corr, got.CorrelationID)
	require.Equal(t, sessionID, got.ClusterSessionID)
	require.Equal(t, timestamp, got.Timestamp)
	require.Equal(t, streamID, got.ResponseStreamID)
	require.Equal(t, channel, got.ResponseChannel)
	require.Empty(t, got.EncodedPrincipal)
}

func TestSessionManager_addPendingConnect_idsAndFields(t *testing.T) {
	m := newSessionManager()
	const nowNs int64 = 1_000

	for i, corr := range []int64{10, 20, 30} {
		fields := sampleConnectFieldsForSession(corr)
		s := m.addPendingConnect(fields, nowNs+int64(i))
		require.NotNil(t, s)
		require.Equal(t, int64(i+1), s.id)
		requireClientSessionFields(t, s, fields, sessionPending, nowNs+int64(i))
		require.Zero(t, s.openedLogPosition)
	}
	require.Equal(t, 3, m.pendingCount())
	require.Equal(t, 0, m.openCount())
	require.Len(t, m.byID, 3)
}

func TestSessionManager_addPendingConnect_cap(t *testing.T) {
	m := newSessionManager()
	fields := sampleConnectFieldsForSession(1)
	for range maxConcurrentSessions {
		s := m.addPendingConnect(fields, 1)
		require.NotNil(t, s)
	}
	require.Equal(t, maxConcurrentSessions, m.pendingCount())
	require.Len(t, m.byID, maxConcurrentSessions)

	// Cap: next add returns nil. Registry stays untouched.
	beforeNext := m.nextSessionID
	s := m.addPendingConnect(fields, 2)
	require.Nil(t, s)
	require.Equal(t, maxConcurrentSessions, m.pendingCount())
	require.Len(t, m.byID, maxConcurrentSessions)
	require.Equal(t, beforeNext, m.nextSessionID)
	require.Equal(t, 0, m.openCount())

	// Cap is open plus pending, not pending alone. markOpen all; refuse still holds.
	for id := int64(1); id <= int64(maxConcurrentSessions); id++ {
		require.True(t, m.markOpen(id, id*100))
	}
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, maxConcurrentSessions, m.openCount())
	require.Len(t, m.byID, maxConcurrentSessions)
	beforeNext = m.nextSessionID
	require.Nil(t, m.addPendingConnect(fields, 3))
	require.Equal(t, beforeNext, m.nextSessionID)
	require.Equal(t, maxConcurrentSessions, m.openCount())
	require.Len(t, m.byID, maxConcurrentSessions)

	// Mix of open and pending still at the cap.
	for id := int64(1); id <= int64(maxConcurrentSessions/2); id++ {
		require.True(t, m.remove(id))
	}
	require.Equal(t, maxConcurrentSessions/2, m.openCount())
	for range maxConcurrentSessions / 2 {
		require.NotNil(t, m.addPendingConnect(fields, 4))
	}
	require.Equal(t, maxConcurrentSessions/2, m.pendingCount())
	require.Len(t, m.byID, maxConcurrentSessions)
	beforeNext = m.nextSessionID
	require.Nil(t, m.addPendingConnect(fields, 5))
	require.Equal(t, beforeNext, m.nextSessionID)
	require.Equal(t, maxConcurrentSessions/2, m.pendingCount())
	require.Equal(t, maxConcurrentSessions/2, m.openCount())
}

func TestSessionManager_markOpen(t *testing.T) {
	m := newSessionManager()
	s := m.addPendingConnect(sampleConnectFieldsForSession(7), 50)
	require.NotNil(t, s)

	const logPos int64 = 4096
	require.True(t, m.markOpen(s.id, logPos))
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, logPos, s.openedLogPosition)
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, 1, m.openCount())

	// Unknown id: false. Registry untouched.
	require.False(t, m.markOpen(99, 1))
	require.Equal(t, 1, m.openCount())

	// Second markOpen: not pending, false. Position stays untouched.
	require.False(t, m.markOpen(s.id, 9999))
	require.Equal(t, logPos, s.openedLogPosition)
	require.Equal(t, sessionOpen, s.state)
}

func TestSessionManager_remove(t *testing.T) {
	m := newSessionManager()
	pending := m.addPendingConnect(sampleConnectFieldsForSession(1), 1)
	open := m.addPendingConnect(sampleConnectFieldsForSession(2), 2)
	require.True(t, m.markOpen(open.id, 100))

	require.True(t, m.remove(pending.id))
	_, ok := m.sessionByID(pending.id)
	require.False(t, ok)
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, 1, m.openCount())

	require.True(t, m.remove(open.id))
	_, ok = m.sessionByID(open.id)
	require.False(t, ok)
	require.Equal(t, 0, m.openCount())

	require.False(t, m.remove(999))
}

func TestSessionManager_sessionByID(t *testing.T) {
	m := newSessionManager()
	s := m.addPendingConnect(sampleConnectFieldsForSession(1), 1)
	got, ok := m.sessionByID(s.id)
	require.True(t, ok)
	require.Same(t, s, got)

	_, ok = m.sessionByID(999)
	require.False(t, ok)
}

func TestAcceptSessionConnect_steadyLeader(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader})
	fields := sampleConnectFieldsForSession(99)
	a.acceptSessionConnect(fields, 1_000)
	require.Equal(t, 1, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	requireClientSessionFields(t, s, fields, sessionPending, 1_000)
}

func TestSessionWork_happy(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 11)
	fields := sampleConnectFieldsForSession(22)
	a.acceptSessionConnect(fields, 1)
	const nowNs int64 = 44
	require.Equal(t, 1, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())

	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Positive(t, s.openedLogPosition)

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 11, 22, 1, nowNs, fields.ResponseStreamID, fields.ResponseChannel,
	)
	require.Equal(t, s.openedLogPosition, pub.Position())
}

func TestSessionWork_backPressureHold(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	a := leaderWithLogPublisher(p, 0)
	a.acceptSessionConnect(sampleConnectFieldsForSession(1), 1)

	for range 5 {
		require.Equal(t, 0, a.sessionWork(10))
		require.Equal(t, 1, a.PendingSessionCount())
		require.Equal(t, 0, a.OpenSessionCount())
	}
}

func TestSessionWork_retryClears(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	a := leaderWithLogPublisher(p, 3)
	fields := sampleConnectFieldsForSession(9)
	a.acceptSessionConnect(fields, 1)
	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 1, a.PendingSessionCount())

	// Rebind an unlimited connected publication. Next tick opens.
	p2, pub := boundLogPublisher(t)
	a.SetLogPublisher(p2)
	const nowNs int64 = 77
	require.Equal(t, 1, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 3, 9, 1, nowNs, fields.ResponseStreamID, fields.ResponseChannel,
	)
}

// TestSessionWork_closedPublicationHoldsQueue asserts a closed publication is
// publication-wide, not per-session. The walk stops at the head. The whole
// pending queue stays in accept order for a rebound publication.
func TestSessionWork_closedPublicationHoldsQueue(t *testing.T) {
	p, pub := boundLogPublisher(t)
	require.NoError(t, pub.Close())
	a := leaderWithLogPublisher(p, 0)
	a.acceptSessionConnect(sampleConnectFieldsForSession(1), 1)
	a.acceptSessionConnect(sampleConnectFieldsForSession(2), 2)

	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 2, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, int64(1), a.sessions.pending[0].id)
	require.Equal(t, int64(2), a.sessions.pending[1].id)

	// Rebind a live publication. Both sessions open in accept order.
	p2, _ := boundLogPublisher(t)
	a.SetLogPublisher(p2)
	require.Equal(t, 2, a.sessionWork(20))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 2, a.OpenSessionCount())
}

// TestSessionWork_headMissingFromRegistry asserts the markOpen-false path. The
// head is deleted from byID while it stays on the pending list. Registry
// methods never produce that desync on their own.
func TestSessionWork_headMissingFromRegistry(t *testing.T) {
	p, _ := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 0)
	a.acceptSessionConnect(sampleConnectFieldsForSession(1), 1)
	a.acceptSessionConnect(sampleConnectFieldsForSession(2), 2)

	// Force registry desync for the fail-closed path.
	delete(a.sessions.byID, 1)

	require.Equal(t, 2, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	s2, ok := a.sessions.sessionByID(2)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s2.state)
}

// TestSessionWork_terminalHeadMissingFromRegistry asserts the remove-false
// path on a per-session terminal append.
func TestSessionWork_terminalHeadMissingFromRegistry(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 0)

	// Head response channel exceeds MaxPayloadLength. Append returns terminal
	// UnsupportedMessageLength.
	emptyFixed, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
	)
	require.True(t, ok)
	headFields := sampleConnectFieldsForSession(1)
	headFields.ResponseChannel = make([]byte, pub.MaxPayloadLength()-emptyFixed+1)
	a.acceptSessionConnect(headFields, 1)

	// Force registry desync for the fail-closed path.
	delete(a.sessions.byID, 1)

	require.Equal(t, 1, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
}

func TestSessionWork_fifoStopOnRetriable(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	a := leaderWithLogPublisher(p, 0)
	a.acceptSessionConnect(sampleConnectFieldsForSession(1), 1)
	a.acceptSessionConnect(sampleConnectFieldsForSession(2), 2)
	require.Equal(t, 2, a.PendingSessionCount())

	require.Equal(t, 0, a.sessionWork(10))
	// Head stays at the front. Second never opens before first.
	require.Equal(t, 2, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, int64(1), a.sessions.pending[0].id)
	require.Equal(t, int64(2), a.sessions.pending[1].id)
}

func TestSessionWork_fifoTwoOpenSameTick(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 7)
	f1 := sampleConnectFieldsForSession(1)
	f2 := sampleConnectFieldsForSession(2)
	a.acceptSessionConnect(f1, 1)
	a.acceptSessionConnect(f2, 2)
	const nowNs int64 = 50
	require.Equal(t, 2, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 2, a.OpenSessionCount())

	// Frames land on the term buffer in accept order.
	d1, next := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(t, d1, 7, 1, 1, nowNs, f1.ResponseStreamID, f1.ResponseChannel)
	d2, _ := decodeSessionOpenAtTermOffset(t, pub, next)
	requireSessionOpenFields(t, d2, 7, 2, 2, nowNs, f2.ResponseStreamID, f2.ResponseChannel)

	s1, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s1.state)
	s2, ok := a.sessions.sessionByID(2)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s2.state)
	require.Less(t, s1.openedLogPosition, s2.openedLogPosition)
}

func TestSessionWork_terminalHeadContinuesToTail(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 4)

	// Head response channel exceeds MaxPayloadLength. Append returns terminal
	// UnsupportedMessageLength.
	emptyFixed, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
	)
	require.True(t, ok)
	oversize := make([]byte, pub.MaxPayloadLength()-emptyFixed+1)
	headFields := sampleConnectFieldsForSession(1)
	headFields.ResponseChannel = oversize
	tailFields := sampleConnectFieldsForSession(2)
	a.acceptSessionConnect(headFields, 1)
	a.acceptSessionConnect(tailFields, 2)

	const nowNs int64 = 60
	// Terminal remove of head, then open of tail.
	require.Equal(t, 2, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	_, headOK := a.sessions.sessionByID(1)
	require.False(t, headOK)
	s2, ok := a.sessions.sessionByID(2)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s2.state)

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 4, 2, 2, nowNs, tailFields.ResponseStreamID, tailFields.ResponseChannel,
	)
}

func TestSessionWork_openThenRetriableMidWalk(t *testing.T) {
	// Limit allows exactly one SessionOpen frame from position 0. Second hits
	// back-pressure mid-walk.
	channel := []byte("ch")
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, len(channel), 0,
	)
	require.True(t, ok)
	aligned := logbuffer.AlignFrameLength(logbuffer.DataHeaderLength + wireLen)
	p := boundLogPublisherWithLimit(t, int64(aligned), true)

	a := leaderWithLogPublisher(p, 2)
	f1 := sampleConnectFieldsForSession(1)
	f1.ResponseChannel = channel
	f2 := sampleConnectFieldsForSession(2)
	f2.ResponseChannel = channel
	a.acceptSessionConnect(f1, 1)
	a.acceptSessionConnect(f2, 2)

	const nowNs int64 = 70
	// One open (work 1). Second stays pending under retriable stop.
	require.Equal(t, 1, a.sessionWork(nowNs))
	require.Equal(t, 1, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	require.Equal(t, int64(2), a.sessions.pending[0].id)
	s1, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s1.state)
}

// TestSessionWork_guardTable asserts every sessionWork early return leaves the
// pending session untouched.
func TestSessionWork_guardTable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		agent func(t *testing.T) *Agent
	}{
		{
			name: "off_leader",
			agent: func(t *testing.T) *Agent {
				p, _ := boundLogPublisher(t)
				return NewAgent(AgentConfig{Role: RoleFollower, LogPublisher: p})
			},
		},
		{
			name: "election_live_leader",
			agent: func(t *testing.T) *Agent {
				p, _ := boundLogPublisher(t)
				return NewAgent(AgentConfig{
					Role:         RoleLeader,
					Election:     &recordingRunner{},
					LogPublisher: p,
				})
			},
		},
		{
			name: "nil_publisher",
			agent: func(t *testing.T) *Agent {
				a := NewAgent(AgentConfig{Role: RoleLeader})
				require.Nil(t, a.logPublisher)
				return a
			},
		},
		{
			name: "unbound_publisher",
			agent: func(t *testing.T) *Agent {
				p := NewLogPublisher()
				require.Nil(t, p.Publication())
				return leaderWithLogPublisher(p, 0)
			},
		},
		{
			name: "publication_cleared_after_bind",
			agent: func(t *testing.T) *Agent {
				p, _ := boundLogPublisher(t)
				p.SetPublication(nil)
				return leaderWithLogPublisher(p, 0)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := tc.agent(t)
			a.acceptSessionConnect(sampleConnectFieldsForSession(1), 1)
			require.Equal(t, 0, a.sessionWork(10))
			require.Equal(t, 1, a.PendingSessionCount())
			require.Equal(t, 0, a.OpenSessionCount())
		})
	}
}

// TestSessionWork_noPending asserts the empty-pending early return.
func TestSessionWork_noPending(t *testing.T) {
	p, _ := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 0)
	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
}

func TestAcceptSessionConnect_handlerOverrides(t *testing.T) {
	a, q, handler := leaderWithClientIngress(nil)
	q.Enqueue(encodeSessionConnectWire(t, sampleConnectFields()))
	require.Equal(t, 1, a.pollClientIngress(1_000))
	require.Len(t, handler.calls, 1)
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
}

// TestDoWork_connectAppendsSameTick asserts a connect polled this tick appends
// this tick. One DoWork on a settled leader with a bound log publisher and nil
// SessionConnectHandler runs pollClientIngress then sessionWork.
func TestDoWork_connectAppendsSameTick(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(p, 9)
	q := &QueueConsensusIngress{}
	a.SetClientIngress(q)
	// Nil SessionConnectHandler runs the product accept.
	fields := sampleConnectFields()
	q.Enqueue(encodeSessionConnectWire(t, fields))

	const nowNs int64 = 12345
	_ = a.DoWork(nowNs)

	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	requireClientSessionFields(t, s, fields, sessionOpen, nowNs)

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 9, fields.CorrelationID, 1, nowNs,
		fields.ResponseStreamID, fields.ResponseChannel,
	)
}
