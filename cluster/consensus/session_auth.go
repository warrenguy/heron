package consensus

import (
	"bytes"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// maxEncodedPrincipalLength caps the principal an authenticator may attach to a
// session (upstream MAX_ENCODED_PRINCIPAL_LENGTH). An oversized principal
// rejects the session fail-closed.
const maxEncodedPrincipalLength = 4 * 1024

// sessionRejectedDetailBytes is the shared AUTHENTICATION_REJECTED detail
// slice (upstream SESSION_REJECTED_MSG). Send paths only read the slice.
var sessionRejectedDetailBytes = []byte("session failed authentication")

// Authenticator decides whether a client session may open.
//
// Behaviour:
//   - HandleConnectRequest delivers the connect credentials.
//   - HandleConnectedSession runs every duty tick while the session egress is
//     connected and the session is neither challenged, authenticated, nor
//     rejected.
//   - HandleChallengedSession runs every duty tick while the session is
//     challenged and the egress is connected.
//   - Connected and challenged handlers may call Challenge, Authenticate, or
//     Reject on the proxy.
//   - Handlers may defer. The call repeats next tick until the session deadline
//     reclaims the connect.
//   - Implementations must not block. Every call runs on the duty cycle.
//   - Implementations must not log encodedCredentials raw.
//   - Principals land on the durable cluster log. Implementations must not put
//     secrets in the principal.
//   - Challenge bytes travel on the client response channel. Treat those bytes
//     as public.
//   - nowNs is Unix nanoseconds on the module clock. Upstream Authenticator uses
//     milliseconds.
//
// Concurrency: invoker-only, from [Agent.DoWork].
//
// Lifetime: credential slices are decoded copies. The authenticator may retain
// the slices. A [SessionProxy] is valid only for the duration of the call.
// Implementations must not retain the proxy.
type Authenticator interface {
	HandleConnectRequest(sessionID int64, encodedCredentials []byte, nowNs int64)
	HandleChallengeResponse(sessionID int64, encodedCredentials []byte, nowNs int64)
	HandleConnectedSession(proxy SessionProxy, nowNs int64)
	HandleChallengedSession(proxy SessionProxy, nowNs int64)
}

// SessionProxy lets an [Authenticator] resolve one session during a
// HandleConnectedSession or HandleChallengedSession call.
//
// Behaviour:
//   - Challenge sends the Challenge frame on the session egress. Marks the
//     session challenged when the send succeeds.
//   - A Challenge from a challenged session sends a further frame. The session
//     stays challenged (multi-round exchange, upstream parity).
//   - Challenge returns false when the send fails. The session state stays
//     unchanged. The handler runs again next tick.
//   - Authenticate stores a clone of encodedPrincipal. Marks the session
//     authenticated. Nil encodedPrincipal keeps the empty principal.
//   - Authenticate returns false when the principal exceeds
//     maxEncodedPrincipalLength. The session is rejected instead.
//   - Reject marks the session rejected. The agent sends
//     AUTHENTICATION_REJECTED and drops the session.
type SessionProxy interface {
	SessionID() int64
	Challenge(encodedChallenge []byte) bool
	Authenticate(encodedPrincipal []byte) bool
	Reject()
}

// acceptAllAuthenticator is the nil-config default (upstream
// DefaultAuthenticatorSupplier parity).
//
// Behaviour: grants every connected and challenged session an empty principal.
// Never challenges. Never rejects. Not sufficient for real credential
// authentication. Wire [SimpleAuthenticator] for credential checks.
type acceptAllAuthenticator struct{}

func (acceptAllAuthenticator) HandleConnectRequest(int64, []byte, int64) {}

func (acceptAllAuthenticator) HandleChallengeResponse(int64, []byte, int64) {}

func (acceptAllAuthenticator) HandleConnectedSession(proxy SessionProxy, _ int64) {
	proxy.Authenticate(nil)
}

func (acceptAllAuthenticator) HandleChallengedSession(proxy SessionProxy, _ int64) {
	proxy.Authenticate(nil)
}

var _ Authenticator = acceptAllAuthenticator{}

// sessionAuthProxy is the reused SessionProxy the Agent hands to the
// authenticator.
//
// Lifetime: valid only during the handler call.
type sessionAuthProxy struct {
	agent   *Agent
	session *clientSession
}

func (p *sessionAuthProxy) SessionID() int64 {
	return p.session.id
}

// Challenge sends the Challenge frame. Marks the session challenged when the
// send succeeds.
//
// Returns false when the send fails. The session state stays unchanged.
func (p *sessionAuthProxy) Challenge(encodedChallenge []byte) bool {
	if p.agent.sendSessionChallenge(p.session, encodedChallenge) != sendOutcomeSent {
		return false
	}
	return p.agent.sessions.markChallenged(p.session.id)
}

// Authenticate marks the session authenticated with a cloned principal.
//
// Returns false when the principal exceeds maxEncodedPrincipalLength. Rejects
// the session in that case.
func (p *sessionAuthProxy) Authenticate(encodedPrincipal []byte) bool {
	if len(encodedPrincipal) > maxEncodedPrincipalLength {
		p.Reject()
		return false
	}
	return p.agent.sessions.markAuthenticated(p.session.id, bytes.Clone(encodedPrincipal))
}

// Reject marks the session rejected for the egress-event handoff.
func (p *sessionAuthProxy) Reject() {
	p.agent.sessions.markRejected(p.session.id)
}

// proxyFor binds the reused sessionAuthProxy to session for one handler call.
// agent is set once in NewAgent.
func (a *Agent) proxyFor(session *clientSession) SessionProxy {
	a.sessionAuthProxy.session = session
	return &a.sessionAuthProxy
}

// authAdvance is the outcome of one authenticator consultation. The pre-open
// walk uses the value to move its index.
type authAdvance int

const (
	// authAdvanceWait means the authenticator has not resolved the session. The
	// entry stays on the walk list. The walk advances to the next index.
	authAdvanceWait authAdvance = iota

	// authAdvanceAppend means the session is authenticated. The walk may append
	// SessionOpenEvent with the granted principal.
	authAdvanceAppend

	// authAdvanceDropped means the session left the walk list. The walk must
	// stay at the same index.
	authAdvanceDropped
)

// advanceSessionAuth consults the authenticator for one connected pre-open
// session.
//
// Behaviour:
//   - A pending session runs HandleConnectedSession.
//   - A challenged session runs HandleChallengedSession.
//   - Handlers may challenge, authenticate, or reject through the proxy.
//   - A rejected session moves to the egress-event queue and leaves the walk
//     list.
//   - The proxy binding clears before return. The Agent does not retain the
//     session past the call.
//
// Concurrency: invoker-only, from [Agent.sessionPendingWork].
//
// Returns:
//   - authAdvanceDropped after reject handoff
//   - authAdvanceWait when the session is not authenticated
//   - authAdvanceAppend when the session is authenticated
func (a *Agent) advanceSessionAuth(session *clientSession, nowNs int64) authAdvance {
	switch session.state {
	case sessionPending:
		a.authenticator.HandleConnectedSession(a.proxyFor(session), nowNs)
	case sessionChallenged:
		a.authenticator.HandleChallengedSession(a.proxyFor(session), nowNs)
	case sessionAuthenticated, sessionRejected, sessionOpen, sessionClosing:
	}
	a.sessionAuthProxy.session = nil
	if session.state == sessionRejected {
		a.queueRejectedSession(session)
		return authAdvanceDropped
	}
	if session.state != sessionAuthenticated {
		return authAdvanceWait
	}
	return authAdvanceAppend
}

// handleChallengeResponse forwards a challenge response to the authenticator
// for a matched challenged session.
//
// Behaviour:
//   - Drops the response when the session id is unknown or the session is not
//     challenged.
//   - For a matched challenged session, adopts the response correlation id.
//     Refreshes lastActivityNs. Calls HandleChallengeResponse.
//   - Grant or reject lands on a later HandleChallengedSession call.
func (a *Agent) handleChallengeResponse(
	fields codecs.ChallengeResponseFields,
	nowNs int64,
) {
	if !a.sessions.refreshChallenged(
		fields.ClusterSessionID,
		fields.CorrelationID,
		nowNs,
	) {
		return
	}
	a.authenticator.HandleChallengeResponse(
		fields.ClusterSessionID,
		fields.EncodedCredentials,
		nowNs,
	)
}

// queueRejectedSession moves a rejected session onto the egress-event queue.
// Removes the session from the registry.
//
// Behaviour:
//   - Detaches the session egress publication. Places the publication on the
//     queue entry when capacity remains.
//   - No second publication add runs.
//   - At the queue cap, the publication closes and the reject drops. The client
//     recovers by timeout.
//   - The entry deadline is lastActivityNs plus the session timeout.
//
// Ownership: after enqueue, the queue entry owns the publication. The session
// does not own the publication after detach. At the queue cap, this method
// releases the entry before drop.
func (a *Agent) queueRejectedSession(session *clientSession) {
	e := &egressEvent{
		clusterSessionID: session.id,
		correlationID:    session.correlationID,
		code:             clusterwire.EventCode_AUTHENTICATION_REJECTED,
		detail:           sessionRejectedDetailBytes,
		addCorrelationID: nullValue,
		publication:      session.egressPublication,
		deadlineNs:       session.lastActivityNs + a.sessionTimeoutNs,
	}
	session.egressPublication = nil
	if !a.appendEgressEvent(e) {
		a.releaseEgressEvent(e)
	}
	a.removeSessionWithResources(session)
}
