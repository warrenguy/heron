package consensus

import (
	"bytes"
	"math"
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// maxConcurrentSessions caps live registry entries (pending, open, closing, and
// other pre-open states). Connects over the cap receive SessionEvent ERROR
// until a session is removed. Bounds registry memory under a connect flood.
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

	// sessionClosing means the close reason is decided and the
	// SessionCloseEvent append is outstanding. closeReason holds the reason.
	// The egress publication is already closed for CLIENT_ACTION. It stays up
	// for TIMEOUT until the CLOSED event sends.
	sessionClosing
)

var sessionStateNames = [...]string{
	"pending",
	"challenged",
	"authenticated",
	"rejected",
	"open",
	"closing",
}

// String returns the diagnostic name for this session state. The method
// returns unknown when the value is out of range.
func (s sessionState) String() string {
	if s < 0 || int(s) >= len(sessionStateNames) {
		return "unknown"
	}
	return sessionStateNames[s]
}

// clientSession is one client session on the leader.
//
// Lifetime: created at accept. Removed on any of:
//   - offer-add error (fail-closed)
//   - per-session terminal append failure
//   - egress poll terminal
//   - pending deadline reclaim
//   - authentication reject handoff
//   - client close or idle timeout after SessionCloseEvent append
type clientSession struct {
	id                int64
	correlationID     int64
	responseStreamID  int32
	responseChannel   []byte
	state             sessionState
	openedLogPosition int64

	// lastActivityNs is set at accept. ChallengeResponse, keepalive, and
	// session message success refresh the field. Pending deadline reclaim and
	// the open-session idle sweep read the field.
	lastActivityNs int64

	// closeReason is the decided close reason, valid in sessionClosing.
	closeReason clusterwire.CloseReason

	// encodedPrincipal is the authenticator-granted principal, cloned at
	// Authenticate. Empty until granted. Appended on SessionOpenEvent.
	encodedPrincipal []byte

	// egressAddCorrelationID is the in-flight async publication add, or
	// nullValue when none is in flight.
	egressAddCorrelationID int64

	// egressPublication is the per-session egress, nil until the async add
	// resolves. The Agent owns Close.
	egressPublication *aeron.Publication

	// newLeaderEventPending marks a session that must receive NewLeaderEvent
	// from this member as the new leader. Set at ElectionComplete on the
	// leader. Cleared on send, on terminal egress, on close or remove, and
	// at PrepareForNewLeadership.
	newLeaderEventPending bool
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
	okPending []*clientSession
	// all holds every live session in accept order. The close sweep walks all
	// front to back.
	all []*clientSession
	// closingCount is the number of sessions in sessionClosing.
	closingCount  int
	nextSessionID int64

	// uncommittedCloses holds each SessionCloseEvent this member appended as
	// leader above commit, in append order. Each entry owns the removed
	// session until sweepUncommittedCloses or clearUncommittedCloses drops it.
	uncommittedCloses []uncommittedClose
}

// uncommittedClose is one SessionCloseEvent the leader appended above commit.
type uncommittedClose struct {
	// logPosition is the claim end position of the SessionCloseEvent.
	logPosition int64

	// session is the closed session. The registry no longer holds it.
	session *clientSession
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

// reseedSessionIDFloor raises nextSessionID so the next allocate sits strictly
// above id. A call with id below the current floor is a silent no-op. id equal
// to math.MaxInt64 is a silent no-op so id+1 cannot wrap.
func (m *sessionManager) reseedSessionIDFloor(id int64) {
	if id == math.MaxInt64 {
		return
	}
	m.nextSessionID = max(m.nextSessionID, id+1)
}

// learnSessionOpen inserts or updates an open session from a committed
// SessionOpenEvent.
//
// Behaviour: stores cloned response params, the frame position, and nowNs.
// Always reseeds the session id allocator above the learned id. Egress
// fields stay untouched. The call is idempotent under redelivery. Does not
// create egress or queue events.
//
// Concurrency: invoker-only.
func (m *sessionManager) learnSessionOpen(
	fields codecs.SessionOpenEventFields,
	logPosition, nowNs int64,
) {
	id := fields.ClusterSessionID
	m.reseedSessionIDFloor(id)

	channel := bytes.Clone(fields.ResponseChannel)
	s, ok := m.byID[id]
	if !ok {
		s = &clientSession{
			id:                     id,
			egressAddCorrelationID: nullValue,
		}
		m.byID[id] = s
		m.all = append(m.all, s)
	} else if s.state != sessionOpen {
		if s.state == sessionClosing && m.closingCount > 0 {
			m.closingCount--
		}
		m.removePending(id)
	}

	s.state = sessionOpen
	s.correlationID = fields.CorrelationID
	s.responseStreamID = fields.ResponseStreamID
	s.responseChannel = channel
	s.openedLogPosition = logPosition
	s.lastActivityNs = nowNs
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
	m.all = append(m.all, s)
	return s
}

// okPendingIndex returns the index of s in okPending, or -1 when absent.
func (m *sessionManager) okPendingIndex(s *clientSession) int {
	return slices.Index(m.okPending, s)
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
// Behaviour: stores logPosition as openedLogPosition. Queues SessionEvent OK
// in markOpen order. Drops the walk-list entry.
//
// Returns: false when the id is unknown or the session is not authenticated.
// On false the registry is untouched.
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

// markNewLeaderPending sets newLeaderEventPending on session.
//
// Behaviour: no-op when session is nil or not open. The mark implies state
// open.
//
// Concurrency: invoker-only.
func (m *sessionManager) markNewLeaderPending(session *clientSession) {
	if session == nil || session.state != sessionOpen {
		return
	}
	session.newLeaderEventPending = true
}

// markClosing moves an open session to closing and stores the reason.
//
// Behaviour: clears okPending and newLeaderEventPending for the session when
// present. Maintains closingCount.
//
// Returns: false when the id is unknown or the session is not open. On false
// the registry is untouched.
func (m *sessionManager) markClosing(
	sessionID int64,
	reason clusterwire.CloseReason,
) bool {
	s, ok := m.byID[sessionID]
	if !ok || s.state != sessionOpen {
		return false
	}
	s.state = sessionClosing
	s.closeReason = reason
	s.newLeaderEventPending = false
	m.closingCount++
	m.setOKPending(s, false)
	return true
}

// remove deletes the session in any state. Drops the walk-list entry for every
// non-open state. Removes the session from okPending and all when present.
// Decrements closingCount when the session was closing.
//
// Returns: false when the id is unknown.
func (m *sessionManager) remove(sessionID int64) bool {
	s, ok := m.byID[sessionID]
	if !ok {
		return false
	}
	m.dropSessionIndexes(s)
	delete(m.byID, sessionID)
	return true
}

// removeOrphan drops okPending, pending, and all for a session pointer when
// byID already lacks the id. Decrements closingCount when the session is
// closing.
//
// Callers must use this only on fail-closed paths that hold a session pointer
// outside byID. Does not delete byID.
func (m *sessionManager) removeOrphan(s *clientSession) {
	if s == nil {
		return
	}
	m.dropSessionIndexes(s)
}

// dropSessionIndexes clears okPending, newLeaderEventPending, pending, and
// all for s. When s is closing, decrements closingCount and clamps at zero.
// Does not delete byID.
func (m *sessionManager) dropSessionIndexes(s *clientSession) {
	s.newLeaderEventPending = false
	m.setOKPending(s, false)
	if s.state != sessionOpen {
		m.removePending(s.id)
	}
	if s.state == sessionClosing && m.closingCount > 0 {
		m.closingCount--
	}
	m.removeFromAll(s.id)
}

func (m *sessionManager) sessionByID(sessionID int64) (*clientSession, bool) {
	s, ok := m.byID[sessionID]
	return s, ok
}

func (m *sessionManager) pendingCount() int {
	return len(m.pending)
}

func (m *sessionManager) openCount() int {
	// Walk-list holds every pre-open session. Closing sessions leave both the
	// walk list and the open count.
	return len(m.byID) - len(m.pending) - m.closingCount
}

func (m *sessionManager) closingSessionCount() int {
	return m.closingCount
}

// trackUncommittedClose appends session to uncommittedCloses at the claim
// end position of its SessionCloseEvent.
//
// Concurrency: invoker-only.
func (m *sessionManager) trackUncommittedClose(position int64, session *clientSession) {
	m.uncommittedCloses = append(m.uncommittedCloses, uncommittedClose{
		logPosition: position,
		session:     session,
	})
}

// sweepUncommittedCloses drops the uncommittedCloses prefix at or below
// position. The delete keeps the capacity and clears the vacated slots, so
// the swept sessions can be freed.
//
// Concurrency: invoker-only.
func (m *sessionManager) sweepUncommittedCloses(position int64) {
	n := 0
	for n < len(m.uncommittedCloses) && m.uncommittedCloses[n].logPosition <= position {
		n++
	}
	m.uncommittedCloses = slices.Delete(m.uncommittedCloses, 0, n)
}

// clearUncommittedCloses drops every uncommittedCloses entry. The clear keeps
// the capacity and nils the vacated slots, so the held sessions can be freed.
//
// Concurrency: invoker-only.
func (m *sessionManager) clearUncommittedCloses() {
	clear(m.uncommittedCloses)
	m.uncommittedCloses = m.uncommittedCloses[:0]
}

// deleteSessionByID drops the entry with sessionID from sessions when present.
func deleteSessionByID(sessions []*clientSession, sessionID int64) []*clientSession {
	i := slices.IndexFunc(sessions, func(s *clientSession) bool {
		return s.id == sessionID
	})
	if i < 0 {
		return sessions
	}
	return slices.Delete(sessions, i, i+1)
}

// removeFromAll drops the all-list entry for sessionID when present.
func (m *sessionManager) removeFromAll(sessionID int64) {
	m.all = deleteSessionByID(m.all, sessionID)
}

// removePending drops the pending entry for sessionID when present.
func (m *sessionManager) removePending(sessionID int64) {
	m.pending = deleteSessionByID(m.pending, sessionID)
}

// acceptSessionConnect accepts a connect on the leader when
// sessionConnectHandler is nil.
//
// Behaviour:
//   - Protocol-major mismatch queues SessionEvent ERROR.
//   - Over-cap queues SessionEvent ERROR.
//   - Those connects never reach the authenticator.
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
	if err := a.startSessionEgress(session); err != nil {
		a.removeSessionWithResources(session)
	}
}

// sessionWork runs settled session duty. Event drain runs first. Queued
// REDIRECT and ERROR events send before NewLeaderEvent.
//
// Concurrency: invoker-only. Callers must invoke sessionWork from the
// [Agent.DoWork] election-nil branch. sessionPendingWork re-checks election
// and role. Leader-only steps run only for the leader.
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
	work += a.newLeaderEventWork()
	work += a.sessionPendingWork(nowNs)
	work += a.sessionCloseWork(nowNs)
	work += a.okPendingWork()
	return work
}

// canAppendLog reports whether a bound open log publication backs the session
// append paths.
//
// Behaviour: returns false when the log publisher is nil, holds no publication,
// or holds a closed publication. A bound open publication that back-pressures
// still returns true. Close paths must call canAppendLog before marking a
// session closing. A mark without an appendable log strands the session in
// closing on a leader.
func (a *Agent) canAppendLog() bool {
	publisher := a.logPublisher
	if publisher == nil {
		return false
	}
	publication := publisher.Publication()
	return publication != nil && !publication.IsClosed()
}

// sessionCloseWork marks idle open sessions closing with TIMEOUT, retries
// SessionCloseEvent appends for every closing session, and removes on success.
//
// Behaviour: walks a.sessions.all front to back. Pre-open states are skipped.
// An open session past lastActivityNs plus sessionTimeoutNs marks closing
// TIMEOUT then appends when the log publisher is bound. When the publisher is
// nil or unbound, the open session is left untouched so it cannot enter a
// stuck closing state. A closing session retries the append. Append success
// sends SessionEvent CLOSED when egress still holds (TIMEOUT path), then
// removes the session.
//
// Returns: work units for timeout marks and successful close removes.
//
// Concurrency: invoker-only. Leader only. Callers must invoke from sessionWork.
func (a *Agent) sessionCloseWork(nowNs int64) int {
	// Skip TIMEOUT mark when SessionCloseEvent cannot append: avoid stuck
	// closing without a log publication. Back-pressure still marks and retries.
	canMarkTimeout := a.canAppendLog()
	timeoutNs := a.sessionTimeoutNs
	work := 0
	for i := 0; i < len(a.sessions.all); {
		session := a.sessions.all[i]
		switch session.state {
		case sessionOpen:
			if !canMarkTimeout || nowNs <= session.lastActivityNs+timeoutNs {
				i++
				continue
			}
			if !a.sessions.markClosing(session.id, clusterwire.CloseReason_TIMEOUT) {
				i++
				continue
			}
			work++
		case sessionClosing:
			// Retry failed client-action and timeout appends.
		case sessionPending, sessionChallenged, sessionAuthenticated, sessionRejected:
			// Pre-open lifetimes belong to any-role deadline reclaim.
			i++
			continue
		}
		if a.appendSessionCloseAndRemove(session, nowNs) {
			// Entry left all; stay at i.
			work++
			continue
		}
		i++
	}
	return work
}

// appendSessionCloseAndRemove appends SessionCloseEvent and tears down the
// session on success.
//
// Behaviour:
//   - AppendSessionClose false leaves the session closing for the next tick.
//   - Append true: trackUncommittedClose at the claim end position.
//   - Then, when the session still holds an egress publication (TIMEOUT
//     path), sends SessionEvent CLOSED once with detail closeReason.String()
//     best effort.
//   - Then removeSessionWithResources.
//
// Returns: true when the session was removed.
func (a *Agent) appendSessionCloseAndRemove(session *clientSession, nowNs int64) bool {
	if session == nil {
		return false
	}
	publisher := a.logPublisher
	if publisher == nil {
		return false
	}
	position, ok := publisher.AppendSessionClose(
		a.leadershipTermID,
		session.id,
		nowNs,
		session.closeReason,
	)
	if !ok {
		return false
	}
	a.sessions.trackUncommittedClose(position, session)
	if session.egressPublication != nil {
		// TIMEOUT path: CLOSED best effort. Client-action already released egress.
		_ = a.sendSessionEvent(
			session.egressPublication,
			session.id,
			session.correlationID,
			a.announcedLeaderMemberID(),
			clusterwire.EventCode_CLOSED,
			[]byte(session.closeReason.String()),
		)
	}
	a.removeSessionWithResources(session)
	return true
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
				// byID lacks an authenticated session. removeSessionWithResources
				// drops egress and secondary indexes so all and closingCount stay
				// consistent under byID desync. Matches the per-session terminal
				// append path.
				a.removeSessionWithResources(session)
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
// Closing sessions are excluded.
func (a *Agent) OpenSessionCount() int {
	return a.sessions.openCount()
}

// ClosingSessionCount returns the number of sessions marked closing.
func (a *Agent) ClosingSessionCount() int {
	return a.sessions.closingSessionCount()
}
