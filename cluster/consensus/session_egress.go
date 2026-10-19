package consensus

import (
	"fmt"
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// DefaultSessionTimeoutNs is the default session timeout (10s).
//
// Bounds pre-open session egress, queued egress events, challenge waits, and
// open-session idle close. Zero config values take this default.
const DefaultSessionTimeoutNs int64 = 10_000_000_000

// maxPendingEgressEvents caps the REDIRECT and ERROR queue. Over-cap connects
// drop without an event. The cap bounds publication adds under a connect flood
// on any member.
const maxPendingEgressEvents = 100

// sessionLimitDetail is the ERROR detail for an over-cap connect.
const sessionLimitDetail = "concurrent session limit"

// sessionEventActingVersion is the SessionEvent acting version (full block
// including version and leaderHeartbeatTimeoutNs).
const sessionEventActingVersion = uint16(clusterwire.SessionEventSbeSchemaVersion)

// sessionEventBlockLength is the SessionEvent block length at the acting
// version. The acting version is constant, so the length is fixed at package
// init.
var sessionEventBlockLength = codecs.SessionEventBlockLength(sessionEventActingVersion)

// challengeActingVersion is the Challenge acting version.
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
// cleared. Clears okPending and new-leader-pending so a post-close DoWork
// does not retry those sends without publications. Drops the REDIRECT/ERROR
// queue. Idempotent.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) CloseSessionEgress() {
	a.releaseSessionsForNewTerm()
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
		eventEgressPublicationChannel(fields.ResponseChannel),
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
		deadlineNs:       nowNs + a.sessionTimeoutNs,
	}
	if !a.appendEgressEvent(e) {
		a.releaseEgressEvent(e)
	}
}

// egressEventWork reclaims timed-out pre-open sessions, then drains the
// REDIRECT and ERROR queue. Runs on every settled member for any role.
//
// Returns: work units for reclaims and successful event sends.
func (a *Agent) egressEventWork(nowNs int64) int {
	return a.reclaimTimedOutSessions(nowNs) + a.drainEgressEvents(nowNs)
}

// reclaimTimedOutSessions removes each pre-open session when
// nowNs - lastActivityNs >= sessionTimeoutNs. The exact deadline counts, as on
// the pending and egress-event deadline paths. Runs on any role.
//
// Closing sessions stay. A committed SessionCloseEvent removes one on a
// non-leader. sessionCloseWork appends the close on a leader.
func (a *Agent) reclaimTimedOutSessions(nowNs int64) int {
	work := 0
	timeout := a.sessionTimeoutNs
	// Pending reclaim uses >= timeout so the exact deadline is included. Matches
	// pending and egress-event deadline paths. Index walk; removals shrink the
	// slice.
	for i := 0; i < len(a.sessions.pending); {
		s := a.sessions.pending[i]
		if nowNs-s.lastActivityNs < timeout {
			i++
			continue
		}
		a.removeSessionWithResources(s)
		work++
	}
	return work
}

// drainEgressEvents polls and sends queued REDIRECT and ERROR events.
//
// Behaviour:
//   - Drops the entry at the deadline when the publication never connects or
//     the send stays retriable.
//   - Drops the entry immediately on a terminal poll-add error or a terminal
//     offer (Closed, MaxPositionExceeded, oversize detail, encode failure).
//   - Holds a vanished not-done poll-add until the deadline. A vanished
//     poll-add is not terminal.
//   - Holds the entry for the next duty cycle on a retriable offer.
//
// Returns: work units for deadline drops, terminal poll drops, terminal
// sends, not-done polls that contact the adder, and successful sends.
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

// egressAddPoll is the result of one PollAddPublication tick.
type egressAddPoll int

const (
	// egressAddNone means no adder or no in-flight correlation id.
	egressAddNone egressAddPoll = iota

	// egressAddInFlight means the adder was contacted and the add is still
	// pending.
	egressAddInFlight

	// egressAddReady means the add completed with a publication.
	egressAddReady

	// egressAddTerminal means the add completed with an error or a nil
	// publication.
	egressAddTerminal

	// egressAddVanished means a not-done PollAddPublication returned a
	// non-nil err. The pending add is gone or the client is not ready. The
	// poll clears *addCorr.
	egressAddVanished
)

// pollEgressAdd runs one PollAddPublication tick for *addCorr.
//
// Behaviour: the poll clears *addCorr on ready, terminal, or vanished. A
// vanished add is a not-done poll with a non-nil err. The next caller must
// not poll that correlation id.
//
// Returns:
//   - pub and egressAddReady when the add succeeded
//   - egressAddTerminal when the poll completed with an error or a nil
//     publication
//   - egressAddVanished when a not-done poll returns a non-nil err
//   - egressAddInFlight and pollWork 1 when the add is still pending
//   - egressAddNone when the adder is nil or *addCorr is nullValue
func (a *Agent) pollEgressAdd(
	addCorr *int64,
) (pub *aeron.Publication, state egressAddPoll, pollWork int) {
	if a.egressAdder == nil || *addCorr == nullValue {
		return nil, egressAddNone, 0
	}
	pub, done, err := a.egressAdder.PollAddPublication(*addCorr)
	if !done {
		if err != nil {
			*addCorr = nullValue
			return nil, egressAddVanished, 0
		}
		return nil, egressAddInFlight, 1
	}
	*addCorr = nullValue
	if err != nil || pub == nil {
		return nil, egressAddTerminal, 0
	}
	return pub, egressAddReady, 0
}

// resolveSessionEgress returns the session egress publication when one is
// stored or when the in-flight add becomes ready.
//
// Behaviour: a stored publication returns egressAddReady with no poll. A
// ready poll stores the publication on the session. The call does not
// remove the session. The call does not start a new add.
func (a *Agent) resolveSessionEgress(
	session *clientSession,
) (pub *aeron.Publication, state egressAddPoll, pollWork int) {
	if session.egressPublication != nil {
		return session.egressPublication, egressAddReady, 0
	}
	pub, state, pollWork = a.pollEgressAdd(&session.egressAddCorrelationID)
	if state == egressAddReady {
		session.egressPublication = pub
	}
	return pub, state, pollWork
}

// pollEgressEventPublication polls one async add for a queued event.
//
// Behaviour: a vanished add is not terminal. The caller holds the entry
// until the deadline.
//
// Returns:
//   - ready when the publication is stored
//   - terminal when the poll completed with an error or a nil publication
//   - pollWork 1 when the adder was contacted and the add is still in flight
func (a *Agent) pollEgressEventPublication(e *egressEvent) (ready, terminal bool, pollWork int) {
	pub, state, pollWork := a.pollEgressAdd(&e.addCorrelationID)
	switch state {
	case egressAddTerminal:
		return false, true, 0
	case egressAddReady:
		e.publication = pub
		return true, false, 0
	case egressAddNone, egressAddInFlight, egressAddVanished:
		return false, false, pollWork
	}
	return false, false, pollWork
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

const sessionEgressLingerParam = "linger="

// eventEgressLingerNs is 0. REDIRECT and ERROR publications must not linger
// on a follower that later becomes leader.
const eventEgressLingerNs = "0"

// eventEgressPublicationChannel builds the REDIRECT or ERROR publication URI.
// Appends linger=0 when linger is absent.
func eventEgressPublicationChannel(responseChannel []byte) string {
	channel := string(responseChannel)
	if aeron.ChannelParamIndex(channel, sessionEgressLingerParam, true) >= 0 {
		return channel
	}
	return aeron.AppendChannelParam(channel, sessionEgressLingerParam, eventEgressLingerNs)
}

// startSessionEgress offers an async publication add for a session.
//
// Behaviour: a nil adder stores no add. The nil-adder path returns nil.
// Offer success stores the correlation id. Offer error returns the error.
// The session stays untouched.
//
// Returns an error when: OfferAddPublication fails.
func (a *Agent) startSessionEgress(session *clientSession) error {
	if a.egressAdder == nil {
		return nil
	}
	addCorr, err := a.egressAdder.OfferAddPublication(
		session.responseStreamID,
		string(session.responseChannel),
	)
	if err != nil {
		return err
	}
	session.egressAddCorrelationID = addCorr
	return nil
}

// pollSessionEgress polls the in-flight async add for a pending session.
//
// Behaviour: a vanished or terminal add is fail-closed. The call removes
// the session so first-connect cannot hold a dead id.
//
// Returns:
//   - ready when egressPublication is non-nil and ready to gate
//   - removed when a terminal or vanished add runs
//     removeSessionWithResources
//   - pollWork 1 when the adder was contacted and the add is still in
//     flight (DoWork stays active while adds resolve)
func (a *Agent) pollSessionEgress(session *clientSession) (ready, removed bool, pollWork int) {
	_, state, pollWork := a.resolveSessionEgress(session)
	switch state {
	case egressAddTerminal, egressAddVanished:
		a.removeSessionWithResources(session)
		return false, true, 0
	case egressAddReady:
		return true, false, 0
	case egressAddNone, egressAddInFlight:
		return false, false, pollWork
	}
	return false, false, pollWork
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

// offerEgressScratch offers a.egressScratch[:end] under [aeron.SendAttempts].
//
// Returns:
//   - sendOutcomeSent on a positive stream position
//   - sendOutcomeRetriable when every attempt returned a retriable code
//   - sendOutcomeTerminal on a non-retriable offer code
func (a *Agent) offerEgressScratch(pub *aeron.Publication, end int) sendOutcome {
	position := aeron.OfferAttempts(pub, a.egressScratch[:end], aeron.SendAttempts)
	if position > 0 {
		return sendOutcomeSent
	}
	if aeron.IsOfferRetriable(position) {
		return sendOutcomeRetriable
	}
	return sendOutcomeTerminal
}

// sendSessionEvent encodes one SessionEvent and offers it under
// [aeron.SendAttempts]. pub must be non-nil.
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

// sendSessionChallenge encodes one Challenge frame and offers it under
// [aeron.SendAttempts].
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
// Behaviour:
//   - Always releases egress from the session pointer first. A byID desync
//     must not leak a publication or in-flight add.
//   - When remove finds the id in byID, drops the registry entry as usual.
//   - When the id is unknown in byID, fail-closed removeOrphan drops
//     secondary indexes and adjusts closingCount.
//
// Registry methods never produce a byID desync alone. Fail-closed cleanup
// stops sessionCloseWork from re-appending forever on a stale all entry and
// keeps ClosingSessionCount and OpenSessionCount consistent.
func (a *Agent) removeSessionWithResources(session *clientSession) {
	if session == nil {
		return
	}
	a.releaseSessionEgress(session)
	if !a.sessions.remove(session.id) {
		a.sessions.removeOrphan(session)
	}
}

// removeSessionWithCloseEvent sends SessionEvent CLOSED with detail when the
// session holds an egress publication. Best effort. Then removes the session
// with resources.
func (a *Agent) removeSessionWithCloseEvent(
	session *clientSession,
	leaderMemberID int32,
	detail []byte,
) {
	if session.egressPublication != nil {
		_ = a.sendSessionEvent(
			session.egressPublication,
			session.id,
			session.correlationID,
			leaderMemberID,
			clusterwire.EventCode_CLOSED,
			detail,
		)
	}
	a.removeSessionWithResources(session)
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
