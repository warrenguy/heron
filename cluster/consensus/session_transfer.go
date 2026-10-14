package consensus

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

var _ sessionEventSink = (*Agent)(nil)

// handleSessionOpenEvent learns a committed SessionOpenEvent into the registry.
//
// Behaviour: inserts or updates the session to open. Always reseeds the
// session id allocator above the learned id. Does not create egress or queue
// events.
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
func (a *Agent) handleSessionOpenEvent(
	fields codecs.SessionOpenEventFields,
	logPosition int64,
) {
	if a == nil || a.sessions == nil {
		return
	}
	a.sessions.learnSessionOpen(fields, logPosition, a.logPollNowNs)
}

// handleSessionCloseEvent learns a committed SessionCloseEvent.
//
// Behaviour: when the id is present, releases local egress and removes the
// session. An unknown id is a silent no-op. Close reason is not stored.
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
func (a *Agent) handleSessionCloseEvent(fields codecs.SessionCloseEventFields) {
	if a == nil || a.sessions == nil {
		return
	}
	session, ok := a.sessions.sessionByID(fields.ClusterSessionID)
	if !ok {
		return
	}
	a.removeSessionWithResources(session)
}

// drainLogBacklog polls the log adapter under boundPosition until the adapter
// position reaches the bound or stops advancing.
//
// Behaviour:
//   - A nil or unbound adapter drains nothing.
//   - The drain does not advance commit.
//   - A PAD frame or slack gap at the end of the term advances the position
//     with no fragment. The drain continues past both.
//   - A closed image or an image with no more data ends the drain.
//   - Callers must set logPollNowNs before the drain so session learn stores
//     the current duty clock.
//
// Concurrency: invoker-only.
func (a *Agent) drainLogBacklog(boundPosition int64) {
	adapter := a.logAdapter
	if adapter == nil || adapter.Image() == nil {
		return
	}
	for {
		position := adapter.Position()
		if position >= boundPosition {
			return
		}
		adapter.Poll(boundPosition)
		if adapter.Position() == position {
			return
		}
	}
}

// rebindOpenSessions marks every open session new-leader-pending and starts
// an egress add when no publication and no in-flight add exist.
//
// Behaviour: refreshes lastActivityNs to nowNs. Pre-open and closing sessions
// stay untouched. Does not send NewLeaderEvent.
//
// Concurrency: invoker-only. Leader ElectionComplete only.
func (a *Agent) rebindOpenSessions(nowNs int64) {
	for _, session := range a.sessions.all {
		if session.state != sessionOpen {
			continue
		}
		a.sessions.markNewLeaderPending(session)
		session.lastActivityNs = nowNs
		if session.egressPublication == nil && session.egressAddCorrelationID == nullValue {
			_ = a.startSessionEgress(session)
		}
	}
}

// newLeaderEventWork sends NewLeaderEvent for each open session marked
// new-leader-pending.
//
// Behaviour:
//   - Polls the in-flight add until the publication is connected, then
//     offers the event.
//   - A vanished add, a terminal add, a closed publication, or a terminal
//     send drops the handle and clears the mark. The session stays open.
//   - Does not start a new add.
//
// Concurrency: invoker-only. Leader only. Callers must invoke from sessionWork
// after egressEventWork.
//
// Returns: work units for add polls, fail-closed clears, terminal sends,
// and successful sends.
func (a *Agent) newLeaderEventWork() int {
	work := 0
	for _, session := range a.sessions.all {
		if session.state != sessionOpen || !session.newLeaderEventPending {
			continue
		}
		pub, state, pollWork := a.resolveSessionEgress(session)
		work += pollWork
		switch state {
		case egressAddTerminal, egressAddVanished:
			a.failNewLeaderEgress(session)
			work++
			continue
		case egressAddReady:
		case egressAddNone, egressAddInFlight:
			continue
		}
		if pub.IsClosed() {
			a.failNewLeaderEgress(session)
			work++
			continue
		}
		if !pub.IsConnected() {
			continue
		}
		switch a.sendNewLeaderEvent(session) {
		case sendOutcomeRetriable:
		case sendOutcomeSent:
			session.newLeaderEventPending = false
			work++
		case sendOutcomeTerminal:
			a.failNewLeaderEgress(session)
			work++
		}
	}
	return work
}

// failNewLeaderEgress releases the session egress handle and clears the
// new-leader-pending mark. The session stays open.
func (a *Agent) failNewLeaderEgress(session *clientSession) {
	a.releaseSessionEgress(session)
	session.newLeaderEventPending = false
}

const newLeaderEventActingVersion = uint16(clusterwire.NewLeaderEventSbeSchemaVersion)

// sendNewLeaderEvent encodes one NewLeaderEvent and offers it under
// [aeron.SendAttempts]. session.egressPublication must be non-nil.
//
// Behaviour: fields are the current leadership term, the session id, this
// member id, and the stored ingress-endpoints string.
//
// Returns:
//   - sendOutcomeSent on a positive stream position
//   - sendOutcomeRetriable when every attempt returned a retriable code
//   - sendOutcomeTerminal on oversize endpoints, encode failure, or a
//     terminal offer code
func (a *Agent) sendNewLeaderEvent(session *clientSession) sendOutcome {
	pub := session.egressPublication
	endpoints := []byte(a.ingressEndpoints)
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.NewLeaderEventSbeBlockLength,
		len(endpoints),
	)
	if !ok || wireLen > pub.MaxPayloadLength() {
		return sendOutcomeTerminal
	}
	a.ensureEgressScratch(wireLen)
	fields := codecs.NewLeaderEventFields{
		LeadershipTermID: a.leadershipTermID,
		ClusterSessionID: session.id,
		LeaderMemberID:   a.thisMemberID,
		IngressEndpoints: endpoints,
	}
	end, ok := codecs.EncodeNewLeaderEvent(
		a.egressScratch,
		0,
		newLeaderEventActingVersion,
		fields,
	)
	if !ok {
		return sendOutcomeTerminal
	}
	return a.offerEgressScratch(pub, end)
}

// releaseSessionsForNewTerm releases per-session egress and clears
// new-leader-pending and okPending. Registry entries stay.
//
// Behaviour: walks every session. Cancels in-flight adds and closes
// publications. Does not drop the REDIRECT/ERROR queue. Idempotent. A
// follower walk is a no-op on empty egress fields.
//
// Concurrency: invoker-only. [Agent.PrepareForNewLeadership] calls this
// before the recording stop. [Agent.CloseSessionEgress] also calls this.
func (a *Agent) releaseSessionsForNewTerm() {
	for _, session := range a.sessions.all {
		a.releaseSessionEgress(session)
		session.newLeaderEventPending = false
	}
	a.sessions.okPending = a.sessions.okPending[:0]
}
