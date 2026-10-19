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

// validConnectFields returns connect fields with the current protocol version.
// sampleConnectFields already sets that version. The name documents intent for
// session open and accept tests across this package.
func validConnectFields(correlationID int64) codecs.SessionConnectRequestFields {
	fields := sampleConnectFields()
	fields.CorrelationID = correlationID
	return fields
}

// leaderWithLogPublisher returns a settled leader with p and leadershipTermID.
// Wires an immediate connected egress adder so pending sessions can open.
func leaderWithLogPublisher(
	t testing.TB,
	p *LogPublisher,
	leadershipTermID int64,
) *Agent {
	t.Helper()
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		LeadershipTermID: leadershipTermID,
		LogPublisher:     p,
	})
	wireImmediateEgress(t, a)
	return a
}

// wireImmediateEgress installs an adder that resolves to a connected unlimited
// concurrent publication on the first poll.
func wireImmediateEgress(t testing.TB, a *Agent) {
	t.Helper()
	a.SetEgressPublicationAdder(withImmediateConnected(t))
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

func addPending(
	m *sessionManager,
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) *clientSession {
	return m.addPendingConnect(m.allocateSessionID(), fields, nowNs)
}

func TestSessionState_String(t *testing.T) {
	tests := []struct {
		state sessionState
		want  string
	}{
		{sessionPending, "pending"},
		{sessionChallenged, "challenged"},
		{sessionAuthenticated, "authenticated"},
		{sessionRejected, "rejected"},
		{sessionOpen, "open"},
		{sessionClosing, "closing"},
		{sessionState(-1), "unknown"},
		{sessionState(len(sessionStateNames)), "unknown"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.state.String())
	}
}

func TestSessionManager_addPendingConnect_idsAndFields(t *testing.T) {
	m := newSessionManager()
	const nowNs int64 = 1_000

	for i, corr := range []int64{10, 20, 30} {
		fields := validConnectFields(corr)
		s := addPending(m, fields, nowNs+int64(i))
		require.NotNil(t, s)
		require.Equal(t, int64(i+1), s.id)
		requireClientSessionFields(t, s, fields, sessionPending, nowNs+int64(i))
		require.Zero(t, s.openedLogPosition)
		require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	}
	require.Equal(t, 3, m.pendingCount())
	require.Equal(t, 0, m.openCount())
	require.Len(t, m.byID, 3)
	require.Empty(t, m.okPending)
}

func TestSessionManager_addPendingConnect_cap(t *testing.T) {
	m := newSessionManager()
	fields := validConnectFields(1)
	for range maxConcurrentSessions {
		s := addPending(m, fields, 1)
		require.NotNil(t, s)
	}
	require.Equal(t, maxConcurrentSessions, m.pendingCount())
	require.Len(t, m.byID, maxConcurrentSessions)

	// Cap: next add returns nil. Registry stays untouched. allocate still advances.
	beforeNext := m.nextSessionID
	id := m.allocateSessionID()
	s := m.addPendingConnect(id, fields, 2)
	require.Nil(t, s)
	require.Equal(t, maxConcurrentSessions, m.pendingCount())
	require.Len(t, m.byID, maxConcurrentSessions)
	require.Equal(t, beforeNext+1, m.nextSessionID)
	require.Equal(t, 0, m.openCount())

	// Cap is open plus pending, not pending alone. Authenticate then markOpen
	// all. Refuse still holds.
	for id := int64(1); id <= int64(maxConcurrentSessions); id++ {
		require.True(t, m.markAuthenticated(id, nil))
		require.True(t, m.markOpen(id, id*100))
	}
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, maxConcurrentSessions, m.openCount())
	require.Len(t, m.byID, maxConcurrentSessions)
	require.Len(t, m.okPending, maxConcurrentSessions)
	beforeNext = m.nextSessionID
	require.Nil(t, m.addPendingConnect(m.allocateSessionID(), fields, 3))
	require.Equal(t, beforeNext+1, m.nextSessionID)
	require.Equal(t, maxConcurrentSessions, m.openCount())
	require.Len(t, m.byID, maxConcurrentSessions)

	// Mix of open and pending still at the cap.
	for id := int64(1); id <= int64(maxConcurrentSessions/2); id++ {
		require.True(t, m.remove(id))
	}
	require.Equal(t, maxConcurrentSessions/2, m.openCount())
	require.Len(t, m.okPending, maxConcurrentSessions/2)
	for range maxConcurrentSessions / 2 {
		require.NotNil(t, addPending(m, fields, 4))
	}
	require.Equal(t, maxConcurrentSessions/2, m.pendingCount())
	require.Len(t, m.byID, maxConcurrentSessions)
	beforeNext = m.nextSessionID
	require.Nil(t, m.addPendingConnect(m.allocateSessionID(), fields, 5))
	require.Equal(t, beforeNext+1, m.nextSessionID)
	require.Equal(t, maxConcurrentSessions/2, m.pendingCount())
	require.Equal(t, maxConcurrentSessions/2, m.openCount())
}

func TestSessionManager_markOpen(t *testing.T) {
	m := newSessionManager()
	s := addPending(m, validConnectFields(7), 50)
	require.NotNil(t, s)

	// Pending (unauthenticated) markOpen fails.
	require.False(t, m.markOpen(s.id, 1))
	require.Equal(t, sessionPending, s.state)
	require.Equal(t, 1, m.pendingCount())

	require.True(t, m.markAuthenticated(s.id, []byte("prin")))
	const logPos int64 = 4096
	require.True(t, m.markOpen(s.id, logPos))
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, logPos, s.openedLogPosition)
	require.Equal(t, []byte("prin"), s.encodedPrincipal)
	require.Len(t, m.okPending, 1)
	require.Same(t, s, m.okPending[0])
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, 1, m.openCount())

	// Unknown id: false. Registry untouched.
	require.False(t, m.markOpen(99, 1))
	require.Equal(t, 1, m.openCount())
	require.Len(t, m.okPending, 1)

	// Second markOpen: not authenticated, false. Position stays untouched.
	require.False(t, m.markOpen(s.id, 9999))
	require.Equal(t, logPos, s.openedLogPosition)
	require.Equal(t, sessionOpen, s.state)
	require.Len(t, m.okPending, 1)
	require.Same(t, s, m.okPending[0])

	m.setOKPending(s, false)
	require.Empty(t, m.okPending)
	// Second clear is a no-op when s is already off okPending.
	m.setOKPending(s, false)
	require.Empty(t, m.okPending)
}

func TestSessionManager_authMutators(t *testing.T) {
	m := newSessionManager()
	s := addPending(m, validConnectFields(1), 10)
	require.NotNil(t, s)

	// Allowed edges from pending.
	require.True(t, m.markChallenged(s.id))
	require.Equal(t, sessionChallenged, s.state)
	// Re-challenge stays challenged.
	require.True(t, m.markChallenged(s.id))
	require.Equal(t, sessionChallenged, s.state)

	require.True(t, m.refreshChallenged(s.id, 55, 20))
	require.Equal(t, int64(55), s.correlationID)
	require.Equal(t, int64(20), s.lastActivityNs)
	require.Equal(t, sessionChallenged, s.state)

	require.True(t, m.markAuthenticated(s.id, []byte("p")))
	require.Equal(t, sessionAuthenticated, s.state)
	require.Equal(t, []byte("p"), s.encodedPrincipal)
	// Authenticated stays on the walk list until markOpen.
	require.Equal(t, 1, m.pendingCount())

	// Unknown id. Wrong-state edges live in TestSessionManager_illegalEdges.
	require.False(t, m.markChallenged(99))
	require.False(t, m.markAuthenticated(99, nil))
	require.False(t, m.markRejected(99))
	require.False(t, m.refreshChallenged(99, 1, 1))
	require.False(t, m.markOpen(99, 1))

	// pending to authenticated. pending to rejected.
	s2 := addPending(m, validConnectFields(2), 30)
	require.True(t, m.markAuthenticated(s2.id, nil))
	require.Equal(t, sessionAuthenticated, s2.state)
	require.Nil(t, s2.encodedPrincipal)

	s3 := addPending(m, validConnectFields(3), 40)
	require.True(t, m.markRejected(s3.id))
	require.Equal(t, sessionRejected, s3.state)

	// challenged to rejected.
	s4 := addPending(m, validConnectFields(4), 50)
	require.True(t, m.markChallenged(s4.id))
	require.True(t, m.markRejected(s4.id))
	require.Equal(t, sessionRejected, s4.state)
}

// TestSessionManager_illegalEdges asserts every wrong-state mutator returns
// false with the registry untouched.
func TestSessionManager_illegalEdges(t *testing.T) {
	type edge struct {
		name string
		// setup places one session in the named start state under id 1.
		setup func(t *testing.T, m *sessionManager) *clientSession
		// call runs the wrong-state mutator against the setup session.
		call func(m *sessionManager, s *clientSession) bool
	}

	seedPending := func(t *testing.T, m *sessionManager) *clientSession {
		t.Helper()
		return addPending(m, validConnectFields(1), 100)
	}
	seedChallenged := func(t *testing.T, m *sessionManager) *clientSession {
		t.Helper()
		s := seedPending(t, m)
		require.True(t, m.markChallenged(s.id))
		return s
	}
	seedAuthenticated := func(t *testing.T, m *sessionManager) *clientSession {
		t.Helper()
		s := seedPending(t, m)
		require.True(t, m.markAuthenticated(s.id, []byte("prin")))
		return s
	}
	seedRejected := func(t *testing.T, m *sessionManager) *clientSession {
		t.Helper()
		s := seedPending(t, m)
		require.True(t, m.markRejected(s.id))
		return s
	}
	seedOpen := func(t *testing.T, m *sessionManager) *clientSession {
		t.Helper()
		s := seedAuthenticated(t, m)
		require.True(t, m.markOpen(s.id, 4096))
		return s
	}

	callChallenged := func(m *sessionManager, s *clientSession) bool {
		return m.markChallenged(s.id)
	}
	callRejected := func(m *sessionManager, s *clientSession) bool {
		return m.markRejected(s.id)
	}
	// Every row asserts false with the registry untouched. Mutator arguments
	// never reach stored state.
	callAuthenticate := func(m *sessionManager, s *clientSession) bool {
		return m.markAuthenticated(s.id, []byte("x"))
	}
	callRefresh := func(m *sessionManager, s *clientSession) bool {
		return m.refreshChallenged(s.id, 9, 200)
	}
	callOpen := func(m *sessionManager, s *clientSession) bool {
		return m.markOpen(s.id, 4096)
	}

	cases := []edge{
		// pending
		{
			name:  "pending_refreshChallenged",
			setup: seedPending,
			call:  callRefresh,
		},
		{
			name:  "pending_markOpen",
			setup: seedPending,
			call:  callOpen,
		},
		// challenged
		{
			name:  "challenged_markOpen",
			setup: seedChallenged,
			call:  callOpen,
		},
		// authenticated
		{
			name:  "authenticated_markChallenged",
			setup: seedAuthenticated,
			call:  callChallenged,
		},
		{
			name:  "authenticated_markRejected",
			setup: seedAuthenticated,
			call:  callRejected,
		},
		{
			name:  "authenticated_refreshChallenged",
			setup: seedAuthenticated,
			call:  callRefresh,
		},
		{
			name:  "authenticated_markAuthenticated",
			setup: seedAuthenticated,
			call:  callAuthenticate,
		},
		// rejected
		{
			name:  "rejected_markChallenged",
			setup: seedRejected,
			call:  callChallenged,
		},
		{
			name:  "rejected_markAuthenticated",
			setup: seedRejected,
			call:  callAuthenticate,
		},
		{
			name:  "rejected_markRejected",
			setup: seedRejected,
			call:  callRejected,
		},
		{
			name:  "rejected_refreshChallenged",
			setup: seedRejected,
			call:  callRefresh,
		},
		{
			name:  "rejected_markOpen",
			setup: seedRejected,
			call:  callOpen,
		},
		// open
		{
			name:  "open_markChallenged",
			setup: seedOpen,
			call:  callChallenged,
		},
		{
			name:  "open_markAuthenticated",
			setup: seedOpen,
			call:  callAuthenticate,
		},
		{
			name:  "open_markRejected",
			setup: seedOpen,
			call:  callRejected,
		},
		{
			name:  "open_refreshChallenged",
			setup: seedOpen,
			call:  callRefresh,
		},
		{
			name:  "open_markOpen",
			setup: seedOpen,
			call:  callOpen,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newSessionManager()
			s := tc.setup(t, m)
			require.NotNil(t, s)
			stateBefore := s.state
			corrBefore := s.correlationID
			activityBefore := s.lastActivityNs
			prinBefore := append([]byte(nil), s.encodedPrincipal...)
			posBefore := s.openedLogPosition
			pendingBefore := m.pendingCount()
			openBefore := m.openCount()
			okBefore := len(m.okPending)

			require.False(t, tc.call(m, s))
			require.Equal(t, stateBefore, s.state)
			require.Equal(t, corrBefore, s.correlationID)
			require.Equal(t, activityBefore, s.lastActivityNs)
			require.Equal(t, prinBefore, s.encodedPrincipal)
			require.Equal(t, posBefore, s.openedLogPosition)
			require.Equal(t, pendingBefore, m.pendingCount())
			require.Equal(t, openBefore, m.openCount())
			require.Len(t, m.okPending, okBefore)
		})
	}
}

func TestSessionManager_markClosing(t *testing.T) {
	m := newSessionManager()
	pending := addPending(m, validConnectFields(1), 1)
	open := addPending(m, validConnectFields(2), 2)
	require.True(t, m.markAuthenticated(open.id, nil))
	require.True(t, m.markOpen(open.id, 100))
	require.Len(t, m.all, 2)
	require.Equal(t, 1, m.openCount())
	require.Equal(t, 0, m.closingSessionCount())

	// Pending refuses.
	require.False(t, m.markClosing(pending.id, clusterwire.CloseReason_CLIENT_ACTION))
	require.Equal(t, sessionPending, pending.state)
	require.Equal(t, 0, m.closingSessionCount())

	// Unknown refuses.
	require.False(t, m.markClosing(99, clusterwire.CloseReason_TIMEOUT))

	// Open succeeds.
	require.True(t, m.markClosing(open.id, clusterwire.CloseReason_TIMEOUT))
	require.Equal(t, sessionClosing, open.state)
	require.Equal(t, clusterwire.CloseReason_TIMEOUT, open.closeReason)
	require.Equal(t, 0, m.openCount())
	require.Equal(t, 1, m.closingSessionCount())
	require.Empty(t, m.okPending)
	require.Len(t, m.all, 2)

	// Closing takes the new reason only.
	require.True(t, m.markClosing(open.id, clusterwire.CloseReason_CLIENT_ACTION))
	require.Equal(t, sessionClosing, open.state)
	require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, open.closeReason)
	require.Equal(t, 1, m.closingSessionCount())

	// remove maintains all and closingCount.
	require.True(t, m.remove(open.id))
	require.Equal(t, 0, m.closingSessionCount())
	require.Len(t, m.all, 1)
	require.Equal(t, pending.id, m.all[0].id)
}

func TestSessionManager_removeOrphan(t *testing.T) {
	m := newSessionManager()
	open := addPending(m, validConnectFields(1), 1)
	require.True(t, m.markAuthenticated(open.id, nil))
	require.True(t, m.markOpen(open.id, 100))
	require.True(t, m.markClosing(open.id, clusterwire.CloseReason_TIMEOUT))
	require.Equal(t, 1, m.closingSessionCount())
	require.Len(t, m.all, 1)

	// Force byID miss for the Agent fail-closed path.
	delete(m.byID, open.id)
	require.Equal(t, -1, m.openCount())

	m.removeOrphan(open)
	require.Equal(t, 0, m.closingSessionCount())
	require.Equal(t, 0, m.openCount())
	require.Empty(t, m.all)

	// Second call clamps closingCount. Counters stay zero.
	m.removeOrphan(open)
	require.Equal(t, 0, m.closingSessionCount())
	require.Equal(t, 0, m.openCount())

	m.removeOrphan(nil)
}

func TestSessionManager_remove(t *testing.T) {
	m := newSessionManager()
	pending := addPending(m, validConnectFields(1), 1)
	open := addPending(m, validConnectFields(2), 2)
	require.True(t, m.markAuthenticated(open.id, nil))
	require.True(t, m.markOpen(open.id, 100))
	require.Len(t, m.okPending, 1)
	require.Same(t, open, m.okPending[0])
	require.Len(t, m.all, 2)

	require.True(t, m.remove(pending.id))
	_, ok := m.sessionByID(pending.id)
	require.False(t, ok)
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, 1, m.openCount())
	require.Len(t, m.okPending, 1)
	require.Len(t, m.all, 1)

	require.True(t, m.remove(open.id))
	_, ok = m.sessionByID(open.id)
	require.False(t, ok)
	require.Equal(t, 0, m.openCount())
	require.Empty(t, m.okPending)
	require.Empty(t, m.all)

	require.False(t, m.remove(999))

	// remove drops walk-list for challenged, authenticated, and rejected.
	for _, tc := range []struct {
		name string
		mark func(int64) bool
	}{
		{"challenged", func(id int64) bool { return m.markChallenged(id) }},
		{"authenticated", func(id int64) bool { return m.markAuthenticated(id, nil) }},
		{"rejected", func(id int64) bool { return m.markRejected(id) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := addPending(m, validConnectFields(10), 1)
			require.True(t, tc.mark(s.id))
			require.Equal(t, 1, m.pendingCount())
			require.True(t, m.remove(s.id))
			require.Equal(t, 0, m.pendingCount())
			_, ok := m.sessionByID(s.id)
			require.False(t, ok)
		})
	}
}

func TestSessionManager_sessionByID(t *testing.T) {
	m := newSessionManager()
	s := addPending(m, validConnectFields(1), 1)
	got, ok := m.sessionByID(s.id)
	require.True(t, ok)
	require.Same(t, s, got)

	_, ok = m.sessionByID(999)
	require.False(t, ok)
}

func TestAcceptSessionConnect_steadyLeader(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader})
	wireImmediateEgress(t, a)
	fields := validConnectFields(99)
	a.acceptSessionConnect(fields, 1_000)
	require.Equal(t, 1, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	requireClientSessionFields(t, s, fields, sessionPending, 1_000)
	require.NotEqual(t, int64(nullValue), s.egressAddCorrelationID)
}

func TestSessionWork_happy(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 11)
	fields := validConnectFields(22)
	a.acceptSessionConnect(fields, 1)
	const nowNs int64 = 44
	// open + OK
	require.Equal(t, 2, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())

	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Positive(t, s.openedLogPosition)
	require.Empty(t, a.sessions.okPending)

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 11, 22, 1, nowNs, fields.ResponseStreamID, fields.ResponseChannel,
	)
	require.Equal(t, s.openedLogPosition, pub.Position())
}

func TestSessionWork_backPressureHold(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	a := leaderWithLogPublisher(t, p, 0)
	a.acceptSessionConnect(validConnectFields(1), 1)

	for range 5 {
		require.Equal(t, 0, a.sessionWork(10))
		require.Equal(t, 1, a.PendingSessionCount())
		require.Equal(t, 0, a.OpenSessionCount())
	}
}

func TestSessionWork_retryClears(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	a := leaderWithLogPublisher(t, p, 3)
	fields := validConnectFields(9)
	a.acceptSessionConnect(fields, 1)
	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 1, a.PendingSessionCount())

	// Rebind an unlimited connected publication. Next tick opens + OK.
	p2, pub := boundLogPublisher(t)
	a.SetLogPublisher(p2)
	const nowNs int64 = 77
	require.Equal(t, 2, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 3, 9, 1, nowNs, fields.ResponseStreamID, fields.ResponseChannel,
	)
}

// TestSessionWork_closedPublicationHoldsQueue asserts a closed publication is
// publication-wide, not per-session. The walk stops. The whole pending queue
// stays for a rebound publication.
func TestSessionWork_closedPublicationHoldsQueue(t *testing.T) {
	p, pub := boundLogPublisher(t)
	require.NoError(t, pub.Close())
	a := leaderWithLogPublisher(t, p, 0)
	a.acceptSessionConnect(validConnectFields(1), 1)
	a.acceptSessionConnect(validConnectFields(2), 2)

	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 2, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, int64(1), a.sessions.pending[0].id)
	require.Equal(t, int64(2), a.sessions.pending[1].id)

	// Rebind a live publication. Both sessions open.
	p2, _ := boundLogPublisher(t)
	a.SetLogPublisher(p2)
	// 2 opens + 2 OKs
	require.Equal(t, 4, a.sessionWork(20))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 2, a.OpenSessionCount())
}

// TestSessionWork_headMissingFromRegistry asserts the markOpen-false path
// under byID desync. The head is authenticated then deleted from byID while
// the head stays on pending. Registry methods never produce that desync
// alone. Asserts fail-closed remove drops secondary indexes so the head
// leaves all.
func TestSessionWork_headMissingFromRegistry(t *testing.T) {
	p, _ := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 0)
	a.acceptSessionConnect(validConnectFields(1), 1)
	a.acceptSessionConnect(validConnectFields(2), 2)

	head, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	ready, _, _ := a.pollSessionEgress(head)
	require.True(t, ready)
	require.True(t, a.sessions.markAuthenticated(1, nil))

	// Force registry desync for the fail-closed path.
	delete(a.sessions.byID, 1)

	// Session 1 fail-closed drop (1) + session 2 open+OK (2).
	require.Equal(t, 3, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	s2, ok := a.sessions.sessionByID(2)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s2.state)
	require.Len(t, a.sessions.all, 1)
	require.Same(t, s2, a.sessions.all[0])
	require.Nil(t, head.egressPublication)
}

// TestSessionWork_terminalHeadMissingFromRegistry asserts the byID-desync path
// on a per-session terminal append. releaseSessionEgress runs from the session
// pointer even when byID remove is false.
func TestSessionWork_terminalHeadMissingFromRegistry(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 0)

	// Head response channel exceeds MaxMessageLength. Append returns terminal
	// UnsupportedMessageLength.
	emptyFixed, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
	)
	require.True(t, ok)
	headFields := validConnectFields(1)
	headFields.ResponseChannel = make([]byte, pub.MaxMessageLength()-emptyFixed+1)
	a.acceptSessionConnect(headFields, 1)
	head, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	// Resolve egress so the desync path has a publication to close.
	ready, _, _ := a.pollSessionEgress(head)
	require.True(t, ready)
	require.NotNil(t, head.egressPublication)
	egressPub := head.egressPublication
	// Authenticate so the walk reaches append. Does not stop on unresolved auth.
	require.True(t, a.sessions.markAuthenticated(1, nil))

	// Force registry desync for the fail-closed path.
	delete(a.sessions.byID, 1)

	require.Equal(t, 1, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.Nil(t, head.egressPublication)
	require.True(t, egressPub.IsClosed())
}

func TestSessionWork_fifoStopOnRetriable(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	a := leaderWithLogPublisher(t, p, 0)
	a.acceptSessionConnect(validConnectFields(1), 1)
	a.acceptSessionConnect(validConnectFields(2), 2)
	require.Equal(t, 2, a.PendingSessionCount())

	require.Equal(t, 0, a.sessionWork(10))
	// Both stay pending under log back-pressure.
	require.Equal(t, 2, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, int64(1), a.sessions.pending[0].id)
	require.Equal(t, int64(2), a.sessions.pending[1].id)
}

func TestSessionWork_fifoTwoOpenSameTick(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 7)
	f1 := validConnectFields(1)
	f2 := validConnectFields(2)
	a.acceptSessionConnect(f1, 1)
	a.acceptSessionConnect(f2, 2)
	const nowNs int64 = 50
	// 2 opens + 2 OKs
	require.Equal(t, 4, a.sessionWork(nowNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 2, a.OpenSessionCount())

	// Frames land on the term buffer in accept order when both are ready.
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
	a := leaderWithLogPublisher(t, p, 4)

	// Head response channel exceeds MaxMessageLength. Append returns terminal
	// UnsupportedMessageLength.
	emptyFixed, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
	)
	require.True(t, ok)
	oversize := make([]byte, pub.MaxMessageLength()-emptyFixed+1)
	headFields := validConnectFields(1)
	headFields.ResponseChannel = oversize
	tailFields := validConnectFields(2)
	a.acceptSessionConnect(headFields, 1)
	a.acceptSessionConnect(tailFields, 2)

	const nowNs int64 = 60
	// Terminal remove of head, then open + OK of tail.
	require.Equal(t, 3, a.sessionWork(nowNs))
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

	a := leaderWithLogPublisher(t, p, 2)
	f1 := validConnectFields(1)
	f1.ResponseChannel = channel
	f2 := validConnectFields(2)
	f2.ResponseChannel = channel
	a.acceptSessionConnect(f1, 1)
	a.acceptSessionConnect(f2, 2)

	const nowNs int64 = 70
	// One open + OK (work 2). Second stays pending under retriable stop.
	require.Equal(t, 2, a.sessionWork(nowNs))
	require.Equal(t, 1, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	require.Equal(t, int64(2), a.sessions.pending[0].id)
	s1, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s1.state)
}

// TestSessionWork_guardTable asserts every sessionWork early return leaves the
// pending session untouched when the deadline has not expired.
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
				return NewAgent(AgentConfig{Role: RoleLeader, LogPublisher: p})
			},
		},
		{
			name: "publication_cleared_after_bind",
			agent: func(t *testing.T) *Agent {
				p, _ := boundLogPublisher(t)
				p.SetPublication(nil)
				return NewAgent(AgentConfig{Role: RoleLeader, LogPublisher: p})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := tc.agent(t)
			wireImmediateEgress(t, a)
			// Product accept requires leader role. Seed the registry directly for
			// off-leader and election-live rows.
			fields := validConnectFields(1)
			if a.role == RoleLeader && a.election == nil {
				a.acceptSessionConnect(fields, 1)
			} else {
				s := addPending(a.sessions, fields, 1)
				require.NotNil(t, s)
				require.NoError(t, a.startSessionEgress(s))
			}
			require.Equal(t, 0, a.sessionWork(10))
			require.Equal(t, 1, a.PendingSessionCount())
			require.Equal(t, 0, a.OpenSessionCount())
		})
	}
}

// TestSessionWork_noPending asserts the empty-pending path returns 0 with no
// open work.
func TestSessionWork_noPending(t *testing.T) {
	p, _ := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 0)
	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
}

func TestAcceptSessionConnect_handlerOverrides(t *testing.T) {
	a, q, handler := leaderWithClientIngress(nil)
	wireImmediateEgress(t, a)
	q.Enqueue(encodeSessionConnectWire(t, validConnectFields(1)))
	require.Equal(t, 1, a.pollClientIngress(1_000))
	require.Len(t, handler.calls, 1)
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())
	require.Empty(t, a.egressEvents)
}

// TestDoWork_connectAppendsSameTick asserts a connect polled this tick appends
// this tick. One DoWork on a settled leader with a bound log publisher and nil
// SessionConnectHandler runs pollClientIngress then sessionWork.
func TestDoWork_connectAppendsSameTick(t *testing.T) {
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 9)
	q := &QueueClientIngress{}
	a.SetClientIngress(q)
	// Nil SessionConnectHandler runs acceptSessionConnect.
	// CorrelationID 99 matches the sampleConnectFields default used before.
	fields := validConnectFields(99)
	q.Enqueue(encodeSessionConnectWire(t, fields))

	const nowNs int64 = 12345
	_ = a.DoWork(nowNs)

	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.OpenSessionCount())
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	requireClientSessionFields(t, s, fields, sessionOpen, nowNs)
	require.Empty(t, a.sessions.okPending)

	decoded, _ := decodeSessionOpenAtTermOffset(t, pub, 0)
	requireSessionOpenFields(
		t, decoded, 9, fields.CorrelationID, 1, nowNs,
		fields.ResponseStreamID, fields.ResponseChannel,
	)
}

func TestSessionManager_learnSessionOpen_insertAndCount(t *testing.T) {
	m := newSessionManager()
	fields := sampleLearnOpenFields(5)
	m.learnSessionOpen(fields, 128, 50)

	require.Equal(t, 1, m.openCount())
	require.Equal(t, 0, m.pendingCount())
	s, ok := m.sessionByID(5)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, fields.ResponseChannel, s.responseChannel)
	require.Equal(t, fields.ResponseStreamID, s.responseStreamID)
	require.Equal(t, int64(128), s.openedLogPosition)
	require.Equal(t, int64(50), s.lastActivityNs)
	require.Nil(t, s.encodedPrincipal)
	require.Equal(t, int64(6), m.nextSessionID)
	require.Len(t, m.all, 1)
	require.Empty(t, m.okPending)
}

func TestSessionManager_learnSessionOpen_closingBecomesOpen(t *testing.T) {
	m := newSessionManager()
	s := addPending(m, validConnectFields(1), 1)
	require.True(t, m.markAuthenticated(s.id, []byte("prin")))
	require.True(t, m.markOpen(s.id, 10))
	require.True(t, m.markClosing(s.id, clusterwire.CloseReason_TIMEOUT))
	require.Equal(t, 1, m.closingSessionCount())
	require.Equal(t, 0, m.openCount())

	m.learnSessionOpen(sampleLearnOpenFields(s.id), 20, 3)
	require.Equal(t, 0, m.closingSessionCount())
	require.Equal(t, 1, m.openCount())
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, []byte("prin"), s.encodedPrincipal)
	require.Equal(t, int64(20), s.openedLogPosition)
}

func TestSessionManager_learnSessionOpen_ignoresAcceptCap(t *testing.T) {
	m := newSessionManager()
	fields := validConnectFields(1)
	for range maxConcurrentSessions {
		require.NotNil(t, addPending(m, fields, 1))
	}
	require.Nil(t, m.addPendingConnect(m.allocateSessionID(), fields, 2))

	learned := sampleLearnOpenFields(500)
	m.learnSessionOpen(learned, 9, 3)
	require.Len(t, m.byID, maxConcurrentSessions+1)
	s, ok := m.sessionByID(500)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, int64(501), m.nextSessionID)
}
