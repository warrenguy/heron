package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// defaultClientIngressFragmentLimit is the max fragments drained per
// [Agent.pollClientIngress] poll. Shares the consensus ingress batch size.
const defaultClientIngressFragmentLimit = defaultConsensusIngressFragmentLimit

// ClientIngress polls one duty-cycle batch of whole client ingress messages.
// Fragmented frames arrive reassembled.
//
// Poll returns the work count. onFragment runs once per whole message (buffer
// and offset at MessageHeader) and returns the poll action.
// [aeron.ControlledAbort] holds the current message for redelivery on a later
// Poll. Other actions follow [aeron.ControlledFragmentHandler].
//
// onFragment receives one whole message as a bounded slice. Implementations
// pass a message-exact view at offset 0. len(buf) is the message end. Trailing
// term bytes after the frame must not be included.
// [codecs.SessionMessagePayloadFromLayout] treats the remainder of buf past the
// fixed block as app payload.
//
// Implementations: [QueueClientIngress], [SubscriptionClientIngress].
//
// Concurrency: invoker-only. Called from [Agent.DoWork].
type ClientIngress interface {
	Poll(
		fragmentLimit int,
		onFragment func(buf []byte, offset int) aeron.ControlledAction,
	) int
}

// SessionConnectHandler receives one decoded SessionConnectRequest from the
// leader client ingress dispatch.
//
// Behaviour:
//   - The agent calls the handler only on a settled leader (election settled,
//     role leader).
//   - Nil runs acceptSessionConnect. A non-nil handler replaces
//     acceptSessionConnect.
//   - Redirect on a settled non-leader is not behind this seam.
//   - Var field slices in fields are copies. The handler may retain them.
//   - nowNs is the duty-cycle clock of the poll that delivered the frame.
//   - Handlers must not log EncodedCredentials raw.
//
// Concurrency: invoker-only. Called from [Agent.DoWork].
type SessionConnectHandler interface {
	HandleSessionConnect(fields codecs.SessionConnectRequestFields, nowNs int64)
}

// SetClientIngress replaces the client ingress poller.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
//
// Nil is allowed. [Agent.pollClientIngress] returns 0 until rewired.
func (a *Agent) SetClientIngress(ingress ClientIngress) { a.clientIngress = ingress }

// ClientIngress returns the wired client [ClientIngress], or nil if unset.
func (a *Agent) ClientIngress() ClientIngress { return a.clientIngress }

// SetSessionConnectHandler wires or clears the [SessionConnectHandler].
//
// Behaviour:
//   - Nil runs acceptSessionConnect.
//   - A non-nil handler replaces acceptSessionConnect on the leader only.
//   - Redirect is not behind this seam.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetSessionConnectHandler(handler SessionConnectHandler) {
	a.sessionConnectHandler = handler
}

// Frames stay in the image while an election is live or the leader is
// outside ACTIVE ([Agent.isLeaderHeld]).
func (a *Agent) pollClientIngress(nowNs int64) int {
	if a.election != nil {
		return 0
	}
	if a.isLeaderHeld() {
		if a.moduleState == ModuleStateSnapshot {
			a.refreshSessionActivity(nowNs)
		}
		return 0
	}
	if a.clientIngress == nil {
		return 0
	}
	a.clientIngressPollNowNs = nowNs
	return a.clientIngress.Poll(defaultClientIngressFragmentLimit, a.clientIngressHandler)
}

func (a *Agent) handleClientIngressFragment(
	buf []byte,
	offset int,
) aeron.ControlledAction {
	layout, ok := codecs.ClusterMessageLayoutAt(
		buf, offset, codecs.ClusterDecodeMaxVarLen,
	)
	if !ok {
		return aeron.ControlledContinue
	}
	nowNs := a.clientIngressPollNowNs
	switch layout.TemplateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		return a.handleSessionMessage(buf, layout, nowNs)
	case clusterwire.SessionKeepAliveSbeTemplateID:
		a.handleSessionKeepAlive(buf, layout, nowNs)
	case clusterwire.SessionCloseRequestSbeTemplateID:
		a.handleSessionCloseRequest(buf, layout, nowNs)
	case clusterwire.SessionConnectRequestSbeTemplateID:
		// Settled non-leader: drop before decode when REDIRECT cannot enqueue.
		// REDIRECT enqueue is the only non-leader outcome. Nil leader hint, nil
		// adder, or a full queue drops the event after decode. Connect flood cost
		// stays low.
		redirectOnly := a.role != RoleLeader
		if redirectOnly && (a.leaderMemberID == nullMemberID || !a.canEnqueueEgressEvent()) {
			return aeron.ControlledContinue
		}
		if fields, ok := codecs.TryDecodeSessionConnectRequestAt(buf, offset); ok {
			a.handleSessionConnect(fields, nowNs)
		}
	case clusterwire.ChallengeResponseSbeTemplateID:
		// Only the leader holds pending sessions. No ClusterBackup forward of
		// ChallengeResponse.
		if a.role != RoleLeader {
			return aeron.ControlledContinue
		}
		if fields, ok := codecs.TryDecodeChallengeResponseAt(buf, offset); ok {
			a.handleChallengeResponse(fields, nowNs)
		}
	}
	return aeron.ControlledContinue
}

// handleSessionConnect dispatches a decoded connect. pollClientIngress checks
// the settled state. handleClientIngressFragment checks the leader hint.
//
// Behaviour:
//   - A non-leader queues SessionEvent REDIRECT.
//   - On the leader, nil sessionConnectHandler runs acceptSessionConnect.
//   - A non-nil handler replaces acceptSessionConnect.
func (a *Agent) handleSessionConnect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) {
	if a.role != RoleLeader {
		a.queueSessionRedirect(fields, nowNs)
		return
	}
	if a.sessionConnectHandler != nil {
		a.sessionConnectHandler.HandleSessionConnect(fields, nowNs)
		return
	}
	a.acceptSessionConnect(fields, nowNs)
}

// openLeaderSession returns the open session when this node is leader for
// leadershipTermID. False on term mismatch, non-leader role, unknown id, or
// non-open state.
func (a *Agent) openLeaderSession(
	sessionID int64,
	leadershipTermID int64,
) (*clientSession, bool) {
	if leadershipTermID != a.leadershipTermID || a.role != RoleLeader {
		return nil, false
	}
	session, ok := a.sessions.sessionByID(sessionID)
	if !ok || session.state != sessionOpen {
		return nil, false
	}
	return session, true
}

// handleSessionMessage gates and appends one client session message.
//
// Behaviour: term mismatch, non-leader, unknown session, and non-open session
// drop the message. A returned position refreshes session activity. A
// non-positive append aborts the poll so the message redelivers next tick.
// UnsupportedMessageLength is deterministic for the message length and drops
// fail-closed instead of aborting forever.
//
// The log frame carries the current term and the duty-cycle clock, not the
// client's stamped values.
func (a *Agent) handleSessionMessage(
	buf []byte,
	layout codecs.ClusterMessageLayout,
	nowNs int64,
) aeron.ControlledAction {
	fields, ok := codecs.SessionMessageHeaderFieldsFromLayout(buf, layout)
	if !ok {
		return aeron.ControlledContinue
	}
	session, ok := a.openLeaderSession(fields.ClusterSessionID, fields.LeadershipTermID)
	if !ok {
		return aeron.ControlledContinue
	}
	payload := codecs.SessionMessagePayloadFromLayout(buf, layout)
	if payload == nil {
		return aeron.ControlledContinue
	}
	position := a.logPublisher.AppendMessage(
		a.leadershipTermID, session.id, nowNs, payload,
	)
	if position > 0 {
		session.lastActivityNs = nowNs
		return aeron.ControlledContinue
	}
	if position == aeron.UnsupportedMessageLength {
		return aeron.ControlledContinue
	}
	return aeron.ControlledAbort
}

// handleSessionKeepAlive refreshes activity for an open session.
//
// Behaviour: term mismatch, non-leader, unknown session, and non-open session
// are silent. No frame is appended and no egress is sent.
func (a *Agent) handleSessionKeepAlive(
	buf []byte,
	layout codecs.ClusterMessageLayout,
	nowNs int64,
) {
	fields, ok := codecs.SessionKeepAliveFieldsFromLayout(buf, layout)
	if !ok {
		return
	}
	session, ok := a.openLeaderSession(fields.ClusterSessionID, fields.LeadershipTermID)
	if !ok {
		return
	}
	session.lastActivityNs = nowNs
}

// handleSessionCloseRequest starts a client-action close.
//
// Behaviour:
//   - Term mismatch, non-leader, unknown session, and non-open session drop the
//     request. A closing session refuses a second request.
//   - canAppendLog false drops the request and leaves the session open (nil
//     publisher, unbound publication, or closed publication).
//   - A match marks the session closing with CLIENT_ACTION, closes the session
//     egress immediately, and appends SessionCloseEvent inline.
//   - Append success removes the session. Append failure leaves the session
//     closing; sessionCloseWork retries.
func (a *Agent) handleSessionCloseRequest(
	buf []byte,
	layout codecs.ClusterMessageLayout,
	nowNs int64,
) {
	fields, ok := codecs.SessionCloseRequestFieldsFromLayout(buf, layout)
	if !ok {
		return
	}
	session, ok := a.openLeaderSession(fields.ClusterSessionID, fields.LeadershipTermID)
	if !ok {
		return
	}

	// Skip the mark when canAppendLog is false so the session cannot enter stuck
	// closing without a SessionCloseEvent. Matches the sessionCloseWork TIMEOUT
	// gate. A bound open publication that back-pressures still marks and retries.
	if !a.canAppendLog() {
		return
	}
	if !a.sessions.markClosing(session.id, clusterwire.CloseReason_CLIENT_ACTION) {
		return
	}
	// Disconnect egress before the append. No SessionEvent CLOSED for
	// client-action close.
	a.releaseSessionEgress(session)
	a.appendSessionCloseAndRemove(session, nowNs)
}
