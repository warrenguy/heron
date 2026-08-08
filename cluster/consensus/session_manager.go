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
	// sessionPending is an accepted connect. Egress connect or the
	// authenticator grant is outstanding.
	sessionPending sessionState = iota

	// sessionChallenged means the Challenge frame was sent. ChallengeResponse
	// and the authenticator grant are outstanding.
	sessionChallenged

	// sessionAuthenticated means the authenticator granted the session.
	// encodedPrincipal holds the granted principal. SessionOpenEvent append is
	// outstanding.
	sessionAuthenticated

	// sessionRejected means the authenticator rejected the session. The walk
	// moves the session to the egress-event queue.
	sessionRejected

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
//   - authentication reject handoff
type clientSession struct {
	id                int64
	correlationID     int64
	responseStreamID  int32
	responseChannel   []byte
	state             sessionState
	openedLogPosition int64

	// lastActivityNs is set at accept. ChallengeResponse refresh updates the
	// field. Pending deadline reclaim reads the field.
	lastActivityNs int64

	// encodedPrincipal is the authenticator-granted principal, cloned at
	// Authenticate. Empty until granted. Appended on SessionOpenEvent.
	encodedPrincipal []byte

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

// authPhaseSession returns the session when id is pending or challenged.
//
// Returns: nil, false when the id is unknown or the state is not pending or
// challenged. On false the registry is untouched.
func (m *sessionManager) authPhaseSession(sessionID int64) (*clientSession, bool) {
	s, ok := m.byID[sessionID]
	if !ok || (s.state != sessionPending && s.state != sessionChallenged) {
		return nil, false
	}
	return s, true
}

// markChallenged moves a pending or challenged session to challenged.
//
// Behaviour: sets state to challenged. Challenged-to-challenged is the allowed
// re-challenge edge.
//
// Returns: false when the id is unknown or the state is not pending or
// challenged. On false the registry is untouched.
func (m *sessionManager) markChallenged(sessionID int64) bool {
	s, ok := m.authPhaseSession(sessionID)
	if !ok {
		return false
	}
	s.state = sessionChallenged
	return true
}

// markAuthenticated moves a pending or challenged session to authenticated.
// Stores encodedPrincipal on the session.
//
// Returns: false when the id is unknown or the state is not pending or
// challenged. On false the registry is untouched.
func (m *sessionManager) markAuthenticated(sessionID int64, encodedPrincipal []byte) bool {
	s, ok := m.authPhaseSession(sessionID)
	if !ok {
		return false
	}
	s.state = sessionAuthenticated
	s.encodedPrincipal = encodedPrincipal
	return true
}

// markRejected moves a pending or challenged session to rejected.
//
// Returns: false when the id is unknown or the state is not pending or
// challenged. On false the registry is untouched.
func (m *sessionManager) markRejected(sessionID int64) bool {
	s, ok := m.authPhaseSession(sessionID)
	if !ok {
		return false
	}
	s.state = sessionRejected
	return true
}

// refreshChallenged stores correlationID and lastActivityNs for a challenged
// session. State stays challenged. Later SessionEvent OK carries the adopted
// correlation id.
//
// Returns: false when the id is unknown or the session is not challenged. On
// false the registry is untouched.
func (m *sessionManager) refreshChallenged(sessionID, correlationID, nowNs int64) bool {
	s, ok := m.byID[sessionID]
	if !ok || s.state != sessionChallenged {
		return false
	}
	s.correlationID = correlationID
	s.lastActivityNs = nowNs
	return true
}

// markOpen moves an authenticated session to open.
//
// Behaviour:
//   - stores logPosition as openedLogPosition
//   - appends the session to okPending in markOpen order for SessionEvent OK
//   - removes the walk-list entry
//
// Returns: false when the id is unknown or the session is not authenticated. On
// false the registry is untouched.
func (m *sessionManager) markOpen(sessionID, logPosition int64) bool {
	s, ok := m.byID[sessionID]
	if !ok || s.state != sessionAuthenticated {
		return false
	}
	s.state = sessionOpen
	s.openedLogPosition = logPosition
	m.setOKPending(s, true)
	m.removePending(sessionID)
	return true
}

// remove deletes the session in any state. Drops the walk-list entry for every
// non-open state. Removes the session from okPending when present.
//
// Returns: false when the id is unknown.
func (m *sessionManager) remove(sessionID int64) bool {
	s, ok := m.byID[sessionID]
	if !ok {
		return false
	}
	m.setOKPending(s, false)
	if s.state != sessionOpen {
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
	// Walk-list holds every non-open session. Open sessions leave the list.
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
// Behaviour:
//   - allocates a session id for every connect
//   - protocol-major mismatch and over-cap queue SessionEvent ERROR; those
//     connects never reach the authenticator
//   - valid accept stores a pending session
//   - delivers connect credentials to the authenticator
//   - starts the async egress publication add
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
	a.authenticator.HandleConnectRequest(session.id, fields.EncodedCredentials, nowNs)
	a.startSessionEgress(session)
}

// sessionWork runs on a settled member:
//  1. egressEventWork - any-role deadline reclaim and REDIRECT/ERROR/reject
//     drain
//  2. sessionPendingWork - leader pre-open walk (egress, auth, append,
//     markOpen)
//  3. okPendingWork - leader SessionEvent OK backlog drain
//
// Concurrency: invoker-only. Callers must invoke sessionWork from the
// [Agent.DoWork] election-nil branch. sessionPendingWork re-checks election
// and role. sessionPendingWork and okPendingWork run only for the leader.
//
// Returns: the sum of work units from each step. Returns 0 when an election is
// live.
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

// sessionPendingWork advances pre-open sessions on a leader.
//
// Behaviour: walks every walk-list session each tick.
//   - Polls egress until the publication is connected. A not-done poll that
//     contacts the adder counts as work.
//   - Once connected, consults the authenticator for pending and challenged
//     sessions.
//   - Rejected sessions move to the egress-event queue.
//   - Sessions not yet authenticated wait for the next tick.
//   - Authenticated sessions with AvailableWindow > 0 append SessionOpenEvent
//     with the granted principal.
//   - A returned position marks the session open and appends the session to
//     okPending for the SessionEvent OK backlog.
//   - A retriable log code stops the walk. The session stays on the walk list.
//   - A publication-wide terminal code (Closed, MaxPositionExceeded) stops the
//     walk. The session stays for a rebound publication.
//   - A per-session terminal code removes the session with resources. The walk
//     continues.
//   - markOpen false releases egress and removes the walk-list entry so the
//     duty cycle cannot hang.
//
// Returns: work units for not-done egress polls, reject handoffs, sessions
// opened, and sessions removed this call. Stops early on a retriable or
// publication-wide terminal code and returns the work so far.
//
// Returns 0 when no session is on the walk list, an election is live, the role
// is not leader, or the log publisher is nil or unbound.
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
				// Terminal remove already dropped the walk-list entry. Stay at i.
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
		if !pub.IsConnected() {
			i++
			continue
		}
		switch a.advanceSessionAuth(session, nowNs) {
		case authAdvanceDropped:
			// Reject handoff left the walk list. Stay at i.
			work++
			continue
		case authAdvanceWait:
			// Authenticator has not resolved. Handler repeats next tick.
			i++
			continue
		case authAdvanceAppend:
		}
		if pub.AvailableWindow() <= 0 {
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
			session.encodedPrincipal,
		)
		if position > 0 {
			if !a.sessions.markOpen(session.id, position) {
				// Not authenticated in byID. Drop resources and the walk-list
				// entry so the duty cycle cannot hang on the same entry.
				a.releaseSessionEgress(session)
				a.sessions.removePending(session.id)
			}
			// Entry left the walk list; stay at i.
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
		// Per-session terminal. Remove with resources; entry left the walk list.
		a.removeSessionWithResources(session)
		work++
	}
	return work
}

// PendingSessionCount returns the number of accepted sessions still on the
// pre-open walk list.
// States on the list: pending, challenged, authenticated, or rejected.
func (a *Agent) PendingSessionCount() int {
	return a.sessions.pendingCount()
}

// OpenSessionCount returns the number of sessions marked open.
func (a *Agent) OpenSessionCount() int {
	return a.sessions.openCount()
}
