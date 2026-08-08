package consensus

import (
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// maxConcurrentSessions caps open plus pending sessions. Connects over the
// cap receive SessionEvent ERROR until a session is removed. Bounds registry
// memory under a connect flood.
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
// Lifetime: created at accept. Removed on any of:
//   - offer-add error (fail-closed)
//   - per-session terminal append failure
//   - egress poll terminal
//   - pending deadline reclaim
type clientSession struct {
	id                int64
	correlationID     int64
	responseStreamID  int32
	responseChannel   []byte
	state             sessionState
	openedLogPosition int64

	// lastActivityNs is set at accept. No keepalive or timeout path refreshes
	// lastActivityNs. Pending deadline reclaim reads this field.
	lastActivityNs int64

	// egressAddCorrelationID is the in-flight async publication add, or
	// nullValue when none is in flight.
	egressAddCorrelationID int64

	// egressPublication is the per-session egress, nil until the async add
	// resolves. The Agent owns Close.
	egressPublication *aeron.Publication
}

// sessionManager is the client session registry.
//
// Concurrency: invoker-only behind Agent.DoWork. No locks.
//
// The registry is pure state. It stores publication pointers and correlation
// ids but never calls them. All I/O lives on the Agent.
type sessionManager struct {
	byID    map[int64]*clientSession
	pending []*clientSession
	// okPending holds open sessions awaiting SessionEvent OK, in markOpen order.
	okPending     []*clientSession
	nextSessionID int64
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		byID:          make(map[int64]*clientSession),
		nextSessionID: 1,
	}
}

// allocateSessionID returns the next session id and advances the allocator.
// Every leader connect consumes an id, including rejects.
func (m *sessionManager) allocateSessionID() int64 {
	id := m.nextSessionID
	m.nextSessionID++
	return id
}

// addPendingConnect stores a pending session under id built from the decoded
// connect fields.
//
// Returns: nil at the capacity cap. On nil the registry is untouched and id is
// not stored.
func (m *sessionManager) addPendingConnect(
	id int64,
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) *clientSession {
	if len(m.byID) >= maxConcurrentSessions {
		return nil
	}
	s := &clientSession{
		id:                     id,
		correlationID:          fields.CorrelationID,
		responseStreamID:       fields.ResponseStreamID,
		responseChannel:        fields.ResponseChannel,
		state:                  sessionPending,
		lastActivityNs:         nowNs,
		egressAddCorrelationID: nullValue,
	}
	m.byID[id] = s
	m.pending = append(m.pending, s)
	return s
}

// okPendingIndex returns the index of s in okPending, or -1 when absent.
func (m *sessionManager) okPendingIndex(s *clientSession) int {
	return slices.IndexFunc(m.okPending, func(x *clientSession) bool {
		return x == s
	})
}

// setOKPending adds or drops s on okPending. No-op when membership already
// matches pending. Sole mutation path for the SessionEvent OK backlog.
func (m *sessionManager) setOKPending(s *clientSession, pending bool) {
	i := m.okPendingIndex(s)
	inSlice := i >= 0
	if pending == inSlice {
		return
	}
	if pending {
		m.okPending = append(m.okPending, s)
		return
	}
	m.okPending = slices.Delete(m.okPending, i, i+1)
}

// markOpen moves a pending session to open. Stores logPosition as
// openedLogPosition. Appends the session to okPending in markOpen order so
// okPendingWork can send SessionEvent OK. Removes the pending entry.
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
	m.setOKPending(s, true)
	m.removePending(sessionID)
	return true
}

// remove deletes the session in any state. Removes the pending entry when
// present. Removes the session from okPending when present.
//
// Returns: false when the id is unknown.
func (m *sessionManager) remove(sessionID int64) bool {
	s, ok := m.byID[sessionID]
	if !ok {
		return false
	}
	m.setOKPending(s, false)
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

// acceptSessionConnect runs the product leader accept path.
//
// Behaviour: allocates a session id for every connect. Protocol-major mismatch
// and over-cap connects queue SessionEvent ERROR. A valid accept stores a
// pending session and starts the async egress publication add.
func (a *Agent) acceptSessionConnect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) {
	id := a.sessions.allocateSessionID()
	ours := codecs.ClusterProtocolSemanticVersion()
	if cnc.MajorVersion(fields.Version) != cnc.MajorVersion(ours) {
		a.enqueueEgressEvent(
			id,
			fields,
			clusterwire.EventCode_ERROR,
			invalidVersionDetail(fields.Version, ours),
			nowNs,
		)
		return
	}
	session := a.sessions.addPendingConnect(id, fields, nowNs)
	if session == nil {
		a.enqueueEgressEvent(
			id,
			fields,
			clusterwire.EventCode_ERROR,
			sessionLimitDetail,
			nowNs,
		)
		return
	}
	a.startSessionEgress(session)
}

// sessionWork runs on a settled member:
//  1. egressEventWork - any-role deadline reclaim and REDIRECT/ERROR drain
//  2. sessionPendingWork - leader pending walk (egress gate, append, markOpen)
//  3. okPendingWork - leader SessionEvent OK backlog drain
//
// Concurrency: invoker-only. Callers must invoke sessionWork from the
// [Agent.DoWork] election-nil branch. sessionPendingWork re-checks election
// and role. sessionPendingWork and okPendingWork run only for the leader.
//
// Returns: the sum of work units from each phase. Returns 0 when an election
// is live.
func (a *Agent) sessionWork(nowNs int64) int {
	if a.election != nil {
		return 0
	}
	work := a.egressEventWork(nowNs)
	if a.role != RoleLeader {
		return work
	}
	work += a.sessionPendingWork(nowNs)
	work += a.okPendingWork()
	return work
}

// sessionPendingWork appends SessionOpenEvent for ready pending sessions on a
// leader.
//
// Behaviour: walks every pending session each tick.
//   - Polls egress until the publication is connected with AvailableWindow > 0.
//     A not-done poll that contacts the adder counts as work.
//   - A returned position marks the session open at that position and appends
//     the session to okPending for the SessionEvent OK backlog.
//   - A retriable log code stops the walk. The session stays pending.
//   - A publication-wide terminal code (Closed, MaxPositionExceeded) stops the
//     walk. The session stays pending for a rebound publication.
//   - A per-session terminal code removes the session with resources. The walk
//     continues.
//   - markOpen false releases egress and removes the pending entry so the duty
//     cycle cannot hang.
//
// Returns: work units for not-done egress polls, sessions opened, and sessions
// removed this call. Stops early on a retriable or publication-wide terminal
// code and returns the work so far.
//
// Returns 0 when no session is pending, an election is live, the role is not
// leader, or the log publisher is nil or unbound.
func (a *Agent) sessionPendingWork(nowNs int64) int {
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
	for i := 0; i < len(a.sessions.pending); {
		session := a.sessions.pending[i]
		if session.egressPublication == nil {
			ready, removed, pollWork := a.pollSessionEgress(session)
			work += pollWork
			if removed {
				// Terminal remove already dropped the pending entry. Stay at i.
				work++
				continue
			}
			if !ready {
				// Still in flight or nil adder.
				i++
				continue
			}
		}
		// ready or already stored: egressPublication is non-nil.
		pub := session.egressPublication
		if !pub.IsConnected() || pub.AvailableWindow() <= 0 {
			i++
			continue
		}
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
				// Not pending in byID. Drop resources and pending entry so the
				// duty cycle cannot hang on the same entry.
				a.releaseSessionEgress(session)
				a.sessions.removePending(session.id)
			}
			// Entry left the pending slice; stay at i.
			work++
			continue
		}
		if aeron.IsOfferRetriable(position) {
			// Log back-pressure is publication-wide. Stopping keeps the queue.
			return work
		}
		if position == aeron.Closed || position == aeron.MaxPositionExceeded {
			// Publication-wide terminal. Hold the queue for a rebound
			// publication.
			return work
		}
		// Per-session terminal. Remove with resources; entry left pending.
		a.removeSessionWithResources(session)
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
