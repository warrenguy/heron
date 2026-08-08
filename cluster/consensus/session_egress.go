package consensus

import (
	"fmt"
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// egressSendAttempts caps SessionEvent and Challenge offer retries per call.
const egressSendAttempts = 3

// defaultSessionTimeoutNs bounds pre-open session egress and queued egress
// events.
//
// A pending session whose egress never connects drops at the deadline. A
// REDIRECT, ERROR, or AUTHENTICATION_REJECTED entry that cannot send also
// drops at the deadline. Reclaim frees the publications. Reclaim runs in the
// any-role settled seat. Leadership loss does not leave pending-session
// publications unreclaimed.
const defaultSessionTimeoutNs int64 = 10_000_000_000

// maxPendingEgressEvents caps the REDIRECT and ERROR queue. Over-cap connects
// drop without an event. The cap bounds publication adds under a connect flood
// on any member.
const maxPendingEgressEvents = 100

// sessionLimitDetail is the ERROR detail for an over-cap connect.
const sessionLimitDetail = "concurrent session limit"

// sessionEventActingVersion is the SessionEvent acting version for product
// egress (full block including version and leaderHeartbeatTimeoutNs).
const sessionEventActingVersion = uint16(clusterwire.SessionEventSbeSchemaVersion)

// sessionEventBlockLength is the SessionEvent block length at the acting
// version. The acting version is constant, so the length is fixed at package
// init.
var sessionEventBlockLength = codecs.SessionEventBlockLength(sessionEventActingVersion)

// challengeActingVersion is the Challenge acting version for product egress.
const challengeActingVersion = uint16(clusterwire.ChallengeSbeSchemaVersion)

// EgressPublicationAdder registers concurrent egress publications for session
// connect and redirect paths.
//
// Behaviour:
//   - OfferAddPublication starts an async add and returns its correlation id.
//   - PollAddPublication runs one poll tick.
//   - CancelPending abandons an in-flight add.
//
// As-built [*aeron.Client] may retry a full to-driver ring with idle sleep
// until DriverTimeout. Callers must treat OfferAddPublication as cold-path
// only.
//
// Concurrency: invoker-only. Call from [Agent.DoWork].
//
// [*aeron.Client] satisfies this interface as-built.
type EgressPublicationAdder interface {
	OfferAddPublication(streamID int32, channel string) (int64, error)
	PollAddPublication(correlationID int64) (*aeron.Publication, bool, error)
	CancelPending(correlationID int64)
}

// egressEvent is one queued SessionEvent send with its own short-lived
// publication. Stream and channel are used only at offer time and are not
// stored on the entry.
type egressEvent struct {
	clusterSessionID int64
	correlationID    int64
	code             clusterwire.EventCode
	detail           []byte
	addCorrelationID int64
	publication      *aeron.Publication
	deadlineNs       int64
}

// SetEgressPublicationAdder wires or clears the egress publication adder.
//
// Behaviour: nil leaves accepted sessions pending with no egress add. Those
// sessions drop at the session deadline. Redirect and reject entries drop at
// queue time when the adder is nil.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetEgressPublicationAdder(adder EgressPublicationAdder) {
	a.egressAdder = adder
}

// SetIngressEndpoints stores the cluster ingress-endpoints string used as
// SessionEvent REDIRECT detail ("0=host:port,1=host:port").
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetIngressEndpoints(endpoints string) {
	a.ingressEndpoints = endpoints
}

// CloseSessionEgress cancels in-flight egress adds and closes every session
// and queued-event publication.
//
// Behaviour: sessions stay in the registry. Transport handles are closed and
// cleared. Clears okPending so a post-close DoWork does not retry OK sends
// without publications. Idempotent.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) CloseSessionEgress() {
	for _, s := range a.sessions.byID {
		a.releaseSessionEgress(s)
		a.sessions.setOKPending(s, false)
	}
	for _, e := range a.egressEvents {
		a.releaseEgressEvent(e)
	}
	a.egressEvents = a.egressEvents[:0]
}

// queueSessionRedirect enqueues a REDIRECT SessionEvent for a settled
// non-leader with a known leader. clusterSessionID is nullValue. The send path
// reads the leader member id from announcedLeaderMemberID.
func (a *Agent) queueSessionRedirect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) {
	a.enqueueEgressEvent(
		nullValue,
		fields,
		clusterwire.EventCode_REDIRECT,
		a.ingressEndpoints,
		nowNs,
	)
}

// canEnqueueEgressEvent reports whether a REDIRECT or ERROR entry may join the
// egress event queue. True when the adder is non-nil and the queue is under
// maxPendingEgressEvents.
func (a *Agent) canEnqueueEgressEvent() bool {
	return a.egressAdder != nil && len(a.egressEvents) < maxPendingEgressEvents
}

// appendEgressEvent admits one built entry to the egress event queue. Sole
// admission path for the queue. Every producer shares the cap rule.
//
// Returns: false at maxPendingEgressEvents. On false the entry is not queued.
//
// Ownership: on true the queue owns the entry publication and in-flight add. On
// false the caller must release the entry.
func (a *Agent) appendEgressEvent(e *egressEvent) bool {
	if len(a.egressEvents) >= maxPendingEgressEvents {
		return false
	}
	a.egressEvents = append(a.egressEvents, e)
	return true
}

// enqueueEgressEvent queues one REDIRECT or ERROR SessionEvent send. Copies
// detail to bytes only after the cap, adder, and offer checks pass. A dropped
// event allocates nothing.
func (a *Agent) enqueueEgressEvent(
	clusterSessionID int64,
	fields codecs.SessionConnectRequestFields,
	code clusterwire.EventCode,
	detail string,
	nowNs int64,
) {
	if !a.canEnqueueEgressEvent() {
		return
	}
	addCorr, err := a.egressAdder.OfferAddPublication(
		fields.ResponseStreamID,
		string(fields.ResponseChannel),
	)
	if err != nil {
		return
	}
	e := &egressEvent{
		clusterSessionID: clusterSessionID,
		correlationID:    fields.CorrelationID,
		code:             code,
		detail:           []byte(detail),
		addCorrelationID: addCorr,
		deadlineNs:       nowNs + defaultSessionTimeoutNs,
	}
	if !a.appendEgressEvent(e) {
		a.releaseEgressEvent(e)
	}
}

// egressEventWork reclaims expired pending-session egress and drains the
// REDIRECT/ERROR queue. Runs on every settled member (any role).
//
// Returns: work units for reclaims and successful event sends.
func (a *Agent) egressEventWork(nowNs int64) int {
	return a.reclaimExpiredPendingSessions(nowNs) + a.drainEgressEvents(nowNs)
}

// reclaimExpiredPendingSessions removes pending sessions past the session
// timeout. Cancels in-flight adds and closes publications. Any role.
func (a *Agent) reclaimExpiredPendingSessions(nowNs int64) int {
	work := 0
	// Index walk. Removals shrink the slice.
	for i := 0; i < len(a.sessions.pending); {
		s := a.sessions.pending[i]
		if nowNs-s.lastActivityNs < defaultSessionTimeoutNs {
			i++
			continue
		}
		a.removeSessionWithResources(s)
		work++
	}
	return work
}

// drainEgressEvents polls and sends queued REDIRECT/ERROR events.
//
// Behaviour:
//   - Drops an entry at the deadline when the publication never connects or the
//     send stays retriable.
//   - Drops immediately on a terminal poll-add error or a terminal offer
//     (Closed, MaxPositionExceeded, oversize detail, encode failure).
//   - Holds the entry for the next tick on a retriable offer.
//
// Returns: work units for deadline drops, terminal poll drops, terminal sends,
// not-done polls that contact the adder, and successful sends.
func (a *Agent) drainEgressEvents(nowNs int64) int {
	work := 0
	leaderMemberID := a.announcedLeaderMemberID()
	for i := 0; i < len(a.egressEvents); {
		e := a.egressEvents[i]
		if nowNs >= e.deadlineNs {
			a.dropEgressEventAt(i)
			work++
			continue
		}
		if e.publication == nil {
			ready, terminal, pollWork := a.pollEgressEventPublication(e)
			work += pollWork
			if terminal {
				a.dropEgressEventAt(i)
				work++
				continue
			}
			if !ready {
				i++
				continue
			}
		}
		switch a.sendSessionEvent(
			e.publication,
			e.clusterSessionID,
			e.correlationID,
			leaderMemberID,
			e.code,
			e.detail,
		) {
		case sendOutcomeRetriable:
			i++
			continue
		case sendOutcomeSent, sendOutcomeTerminal:
			a.dropEgressEventAt(i)
			work++
		}
	}
	return work
}

// dropEgressEventAt releases resources for the entry at i and removes it.
// Callers must not advance i after a drop; the next entry shifts into i.
func (a *Agent) dropEgressEventAt(i int) {
	a.releaseEgressEvent(a.egressEvents[i])
	a.egressEvents = slices.Delete(a.egressEvents, i, i+1)
}

// pollEgressAdd runs one PollAddPublication tick for *addCorr.
//
// Clears *addCorr when the poll completes (success or terminal).
//
// Returns:
//   - pub when the add succeeded
//   - terminal when the poll completed with an error or a nil publication
//   - pollWork is 1 when the adder was contacted and the add is still in flight
func (a *Agent) pollEgressAdd(
	addCorr *int64,
) (pub *aeron.Publication, terminal bool, pollWork int) {
	if a.egressAdder == nil || *addCorr == nullValue {
		return nil, false, 0
	}
	pub, done, err := a.egressAdder.PollAddPublication(*addCorr)
	if !done {
		return nil, false, 1
	}
	*addCorr = nullValue
	if err != nil || pub == nil {
		return nil, true, 0
	}
	return pub, false, 0
}

// pollEgressEventPublication polls one async add for a queued event.
//
// Returns:
//   - ready when the publication is stored
//   - terminal when the poll completed with an error or a nil publication
//   - pollWork is 1 when the adder was contacted and the add is still in flight
func (a *Agent) pollEgressEventPublication(e *egressEvent) (ready, terminal bool, pollWork int) {
	pub, terminal, pollWork := a.pollEgressAdd(&e.addCorrelationID)
	if terminal {
		return false, true, 0
	}
	if pub == nil {
		return false, false, pollWork
	}
	e.publication = pub
	return true, false, 0
}

// okPendingWork sends SessionEvent OK for each session on okPending. Walks
// okPending in markOpen order.
//
// Behaviour:
//   - A successful send clears the okPending entry. The session stays open.
//   - A retriable offer holds the entry for the next tick. A permanently
//     NotConnected publication holds forever under IsOfferRetriable. No
//     deadline walks okPending.
//   - A terminal offer (Closed, MaxPositionExceeded) releases the dead egress
//     handle and clears the okPending entry. The session stays open in the
//     registry because SessionOpenEvent is already on the log.
//
// Returns: work units for successful OK sends and terminal clear/release.
func (a *Agent) okPendingWork() int {
	if len(a.sessions.okPending) == 0 {
		return 0
	}
	work := 0
	leaderMemberID := a.announcedLeaderMemberID()
	for i := 0; i < len(a.sessions.okPending); {
		s := a.sessions.okPending[i]
		// Defensive: markOpen is the sole appender and state never reverts.
		// egressPublication is nil only after CloseSessionEgress, which also
		// clears okPending. Remove a broken entry so the duty cycle cannot hang.
		if s.state != sessionOpen || s.egressPublication == nil {
			a.sessions.setOKPending(s, false)
			continue
		}
		switch a.sendSessionEvent(
			s.egressPublication,
			s.id,
			s.correlationID,
			leaderMemberID,
			clusterwire.EventCode_OK,
			nil,
		) {
		case sendOutcomeRetriable:
			i++
			continue
		case sendOutcomeTerminal:
			// Session stays open: SessionOpenEvent is already on the log. Release
			// the dead handle so later ticks do not re-offer into Closed.
			a.releaseSessionEgress(s)
			a.sessions.setOKPending(s, false)
			work++
		case sendOutcomeSent:
			a.sessions.setOKPending(s, false)
			work++
		}
	}
	return work
}

// startSessionEgress offers an async publication add for a pending session.
//
// Behaviour: offer error removes the session fail-closed. Nil adder leaves the
// session pending for deadline reclaim.
func (a *Agent) startSessionEgress(session *clientSession) {
	if a.egressAdder == nil {
		return
	}
	addCorr, err := a.egressAdder.OfferAddPublication(
		session.responseStreamID,
		string(session.responseChannel),
	)
	if err != nil {
		a.removeSessionWithResources(session)
		return
	}
	session.egressAddCorrelationID = addCorr
}

// pollSessionEgress polls the in-flight async add for a pending session.
//
// Returns:
//   - ready when egressPublication is non-nil and ready to gate
//   - removed when a terminal poll called removeSessionWithResources
//   - pollWork 1 when the adder was contacted and the add is still in flight
//     (DoWork stays active while adds resolve)
func (a *Agent) pollSessionEgress(session *clientSession) (ready, removed bool, pollWork int) {
	if session.egressPublication != nil {
		return true, false, 0
	}
	pub, terminal, pollWork := a.pollEgressAdd(&session.egressAddCorrelationID)
	if terminal {
		a.removeSessionWithResources(session)
		return false, true, 0
	}
	if pub == nil {
		return false, false, pollWork
	}
	session.egressPublication = pub
	return true, false, 0
}

// sendOutcome is the result of one SessionEvent encode and offer attempt.
type sendOutcome int

const (
	// sendOutcomeSent means Offer returned a positive stream position.
	sendOutcomeSent sendOutcome = iota

	// sendOutcomeRetriable means the offer budget exhausted on retriable codes
	// only (BackPressured, AdminAction, NotConnected). The caller holds the
	// entry and retries.
	sendOutcomeRetriable

	// sendOutcomeTerminal means oversize detail, encode failure, or a
	// non-retriable offer code (Closed, MaxPositionExceeded,
	// UnsupportedMessageLength). The caller must not retry the same
	// publication.
	sendOutcomeTerminal
)

// ensureEgressScratch grows a.egressScratch to at least wireLen and sets the
// length to wireLen.
func (a *Agent) ensureEgressScratch(wireLen int) {
	if cap(a.egressScratch) < wireLen {
		a.egressScratch = make([]byte, wireLen)
	}
	a.egressScratch = a.egressScratch[:wireLen]
}

// offerEgressScratch offers a.egressScratch[0:end] with the
// egressSendAttempts budget.
//
// Returns:
//   - sendOutcomeSent on a positive stream position
//   - sendOutcomeRetriable when every attempt returned a retriable code
//   - sendOutcomeTerminal on a non-retriable offer code
func (a *Agent) offerEgressScratch(pub *aeron.Publication, end int) sendOutcome {
	for range egressSendAttempts {
		position := pub.OfferWithOffset(a.egressScratch, 0, end)
		if position > 0 {
			return sendOutcomeSent
		}
		if !aeron.IsOfferRetriable(position) {
			return sendOutcomeTerminal
		}
	}
	return sendOutcomeRetriable
}

// sendSessionEvent encodes one SessionEvent and offers it with the
// egressSendAttempts budget. pub must be non-nil.
//
// Returns:
//   - sendOutcomeSent on a positive stream position
//   - sendOutcomeRetriable when every attempt returned a retriable code
//   - sendOutcomeTerminal on oversize detail, encode failure, or a terminal
//     offer code
//
// OK paths pass a nil detail and a fixed small frame. Oversize and encode
// failure are unreachable for OK. ERROR and REDIRECT detail paths still map
// those cases to terminal.
func (a *Agent) sendSessionEvent(
	pub *aeron.Publication,
	clusterSessionID int64,
	correlationID int64,
	leaderMemberID int32,
	code clusterwire.EventCode,
	detail []byte,
) sendOutcome {
	wireLen, ok := codecs.ClusterWireMessageLen(sessionEventBlockLength, len(detail))
	if !ok || wireLen > pub.MaxPayloadLength() {
		return sendOutcomeTerminal
	}
	a.ensureEgressScratch(wireLen)
	fields := codecs.SessionEventFields{
		ClusterSessionID:         clusterSessionID,
		CorrelationID:            correlationID,
		LeadershipTermID:         a.leadershipTermID,
		LeaderMemberID:           leaderMemberID,
		Code:                     code,
		Version:                  codecs.ClusterProtocolSemanticVersion(),
		LeaderHeartbeatTimeoutNs: a.leaderHeartbeatTimeoutNs,
		Detail:                   detail,
	}
	end, ok := codecs.EncodeSessionEvent(
		a.egressScratch,
		0,
		sessionEventActingVersion,
		sessionEventBlockLength,
		fields,
	)
	if !ok {
		return sendOutcomeTerminal
	}
	return a.offerEgressScratch(pub, end)
}

// sendSessionChallenge encodes one Challenge frame. Offers the frame under the
// egressSendAttempts budget.
//
// Returns:
//   - sendOutcomeSent on a positive stream position
//   - sendOutcomeRetriable when every attempt returned a retriable code
//   - sendOutcomeTerminal on nil publication, oversize challenge, encode
//     failure, or a terminal offer code
func (a *Agent) sendSessionChallenge(
	session *clientSession,
	encodedChallenge []byte,
) sendOutcome {
	pub := session.egressPublication
	if pub == nil {
		return sendOutcomeTerminal
	}
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.ChallengeSbeBlockLength,
		len(encodedChallenge),
	)
	if !ok || wireLen > pub.MaxPayloadLength() {
		return sendOutcomeTerminal
	}
	a.ensureEgressScratch(wireLen)
	fields := codecs.ChallengeFields{
		CorrelationID:    session.correlationID,
		ClusterSessionID: session.id,
		EncodedChallenge: encodedChallenge,
	}
	end, ok := codecs.EncodeChallenge(
		a.egressScratch,
		0,
		challengeActingVersion,
		fields,
	)
	if !ok {
		return sendOutcomeTerminal
	}
	return a.offerEgressScratch(pub, end)
}

// removeSessionWithResources cancels an in-flight egress add, closes the
// publication, and removes the session from the registry.
//
// Behaviour: always releases egress from the session pointer first so a byID
// desync cannot leak a publication or in-flight add. When the id is unknown in
// byID, drops a stray pending entry fail closed when present.
func (a *Agent) removeSessionWithResources(session *clientSession) {
	if session == nil {
		return
	}
	a.releaseSessionEgress(session)
	if !a.sessions.remove(session.id) {
		a.sessions.removePending(session.id)
	}
}

// releaseSessionEgress cancels an in-flight add and closes the publication.
// Clears egress fields. Does not remove the session from the registry.
func (a *Agent) releaseSessionEgress(s *clientSession) {
	if s == nil {
		return
	}
	a.cancelEgressAdd(&s.egressAddCorrelationID)
	closeEgressPublication(&s.egressPublication)
}

// releaseEgressEvent cancels an in-flight add and closes the publication for a
// queued event entry.
func (a *Agent) releaseEgressEvent(e *egressEvent) {
	if e == nil {
		return
	}
	a.cancelEgressAdd(&e.addCorrelationID)
	closeEgressPublication(&e.publication)
}

// cancelEgressAdd abandons an in-flight add when *addCorr is not nullValue.
// Clears *addCorr. No-op when the adder is nil.
func (a *Agent) cancelEgressAdd(addCorr *int64) {
	if *addCorr == nullValue {
		return
	}
	if a.egressAdder != nil {
		a.egressAdder.CancelPending(*addCorr)
	}
	*addCorr = nullValue
}

// announcedLeaderMemberID returns the member id stamped as leader on outgoing
// SessionEvents. Returns this member when the agent is leader. Otherwise
// returns the leader hint.
//
// The role check is required. JoinLogAsLeader clears leaderMemberID to
// nullMemberID on the leader itself.
func (a *Agent) announcedLeaderMemberID() int32 {
	if a.role == RoleLeader {
		return a.thisMemberID
	}
	return a.leaderMemberID
}

// closeEgressPublication closes *pub when non-nil and clears the pointer.
func closeEgressPublication(pub **aeron.Publication) {
	if *pub == nil {
		return
	}
	_ = (*pub).Close()
	*pub = nil
}

// invalidVersionDetail builds the ERROR detail for a protocol-major mismatch.
func invalidVersionDetail(clientVersion, clusterVersion int32) string {
	return fmt.Sprintf(
		"invalid client version %s, cluster is %s",
		cnc.FormatSemanticVersion(clientVersion),
		cnc.FormatSemanticVersion(clusterVersion),
	)
}
