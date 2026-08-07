package consensus

import (
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
)

// maxConcurrentSessions caps open plus pending sessions. Connects over the
// cap drop until a session is removed. Bounds registry memory under a connect
// flood.
const maxConcurrentSessions = 100

// sessionState is the client session lifecycle state.
type sessionState int

const (
	// sessionPending is an accepted connect awaiting SessionOpenEvent append.
	sessionPending sessionState = iota

	// sessionOpen means SessionOpenEvent is on the log. openedLogPosition holds
	// the append position.
	sessionOpen
)

// clientSession is one client session on the leader.
//
// Lifetime: created at accept. Removed on a per-session terminal append
// failure.
type clientSession struct {
	id                int64
	correlationID     int64
	responseStreamID  int32
	responseChannel   []byte
	state             sessionState
	openedLogPosition int64

	// lastActivityNs is stamped at accept. No keepalive or timeout path
	// refreshes lastActivityNs.
	lastActivityNs int64
}

// sessionManager is the client session registry.
//
// Concurrency: invoker-only behind Agent.DoWork. No locks.
type sessionManager struct {
	byID          map[int64]*clientSession
	pending       []*clientSession
	nextSessionID int64
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		byID:          make(map[int64]*clientSession),
		nextSessionID: 1,
	}
}

// addPendingConnect allocates the next session id. Stores a pending session
// built from the decoded connect fields.
//
// Returns: nil at the capacity cap. On nil the registry is untouched.
func (m *sessionManager) addPendingConnect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) *clientSession {
	if len(m.byID) >= maxConcurrentSessions {
		return nil
	}
	id := m.nextSessionID
	m.nextSessionID++
	s := &clientSession{
		id:               id,
		correlationID:    fields.CorrelationID,
		responseStreamID: fields.ResponseStreamID,
		responseChannel:  fields.ResponseChannel,
		state:            sessionPending,
		lastActivityNs:   nowNs,
	}
	m.byID[id] = s
	m.pending = append(m.pending, s)
	return s
}

// markOpen moves a pending session to open. Stores logPosition as
// openedLogPosition. Removes the pending entry.
//
// Returns: false when the id is unknown or the session is not pending. On
// false the registry is untouched.
func (m *sessionManager) markOpen(sessionID, logPosition int64) bool {
	s, ok := m.byID[sessionID]
	if !ok || s.state != sessionPending {
		return false
	}
	s.state = sessionOpen
	s.openedLogPosition = logPosition
	m.removePending(sessionID)
	return true
}

// remove deletes the session in any state. Removes the pending entry when
// present.
//
// Returns: false when the id is unknown.
func (m *sessionManager) remove(sessionID int64) bool {
	s, ok := m.byID[sessionID]
	if !ok {
		return false
	}
	if s.state == sessionPending {
		m.removePending(sessionID)
	}
	delete(m.byID, sessionID)
	return true
}

func (m *sessionManager) sessionByID(sessionID int64) (*clientSession, bool) {
	s, ok := m.byID[sessionID]
	return s, ok
}

func (m *sessionManager) pendingCount() int {
	return len(m.pending)
}

func (m *sessionManager) openCount() int {
	// Every pending entry is in byID with state pending.
	return len(m.byID) - len(m.pending)
}

// removePending drops the pending entry for sessionID when present.
func (m *sessionManager) removePending(sessionID int64) {
	i := slices.IndexFunc(m.pending, func(s *clientSession) bool {
		return s.id == sessionID
	})
	if i >= 0 {
		m.pending = slices.Delete(m.pending, i, i+1)
	}
}

// acceptSessionConnect stores a pending session for a leader-accepted connect.
// Over-cap connects drop without a session entry.
func (a *Agent) acceptSessionConnect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) {
	a.sessions.addPendingConnect(fields, nowNs)
}

// sessionWork appends SessionOpenEvent for pending sessions on a leader.
//
// Behaviour: walks pending sessions in accept order.
//   - A returned position marks the session open at that position.
//   - A retriable code stops the walk. The session stays pending for the next
//     tick.
//   - A publication-wide terminal code (Closed, MaxPositionExceeded) stops the
//     walk. The session stays pending for a rebound publication.
//   - A per-session terminal code removes the session. The walk continues to
//     the next pending session.
//   - markOpen or remove false on the head drops that pending entry fail
//     closed so the duty cycle cannot hang.
//
// Concurrency: invoker-only. Callers must invoke sessionWork from the
// Agent.DoWork election-nil branch.
//
// Returns: the count of sessions opened or removed this call. Stops early and
// returns the work so far when a retriable or publication-wide terminal code
// appears. Returns 0 when no session is pending, an election is live, the role
// is not leader, or the log publisher is nil or unbound.
func (a *Agent) sessionWork(nowNs int64) int {
	// Empty pending is the common every-tick case. Return before the role and
	// publisher loads.
	if len(a.sessions.pending) == 0 {
		return 0
	}
	if a.election != nil || a.role != RoleLeader {
		return 0
	}
	publisher := a.logPublisher
	if publisher == nil || publisher.Publication() == nil {
		return 0
	}
	work := 0
	for len(a.sessions.pending) > 0 {
		session := a.sessions.pending[0]
		position := publisher.AppendSessionOpen(
			a.leadershipTermID,
			session.correlationID,
			session.id,
			nowNs,
			session.responseStreamID,
			session.responseChannel,
			nil,
		)
		if position > 0 {
			if !a.sessions.markOpen(session.id, position) {
				// Head not pending in byID. Drop pending head so the duty cycle
				// cannot hang on the same entry.
				a.sessions.removePending(session.id)
			}
			work++
			continue
		}
		if aeron.IsOfferRetriable(position) {
			// Back-pressure applies to the whole batch. Stopping keeps log open
			// order equal to accept order.
			return work
		}
		if position == aeron.Closed || position == aeron.MaxPositionExceeded {
			// Publication-wide terminal. Every later append fails the same way.
			// Stopping keeps one closed or exhausted publication from dropping
			// the whole pending queue in a single tick.
			return work
		}
		if !a.sessions.remove(session.id) {
			// Unknown head. Drop pending entry fail closed.
			a.sessions.removePending(session.id)
		}
		work++
	}
	return work
}

// PendingSessionCount returns the number of accepted sessions awaiting
// SessionOpenEvent append.
func (a *Agent) PendingSessionCount() int {
	return a.sessions.pendingCount()
}

// OpenSessionCount returns the number of sessions marked open.
func (a *Agent) OpenSessionCount() int {
	return a.sessions.openCount()
}
