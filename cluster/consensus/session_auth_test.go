//go:build testhooks

package consensus

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// scriptedAuthenticator records every authenticator call and plays per-session
// actions. Each state edge is drivable without real credentials.
type scriptedAuthenticator struct {
	connectCalls    []authCall
	challengeResp   []authCall
	connectedCalls  []int64
	challengedCalls []int64

	// actions maps session id to the next action sequence for connected and
	// challenged handlers. Empty means no proxy call.
	actions map[int64][]authAction
}

// authCall is one HandleConnectRequest or HandleChallengeResponse record.
type authCall struct {
	sessionID   int64
	credentials []byte
	nowNs       int64
}

type authActionKind int

const (
	authActionNone authActionKind = iota
	authActionChallenge
	authActionAuthenticate
	authActionReject
)

type authAction struct {
	kind      authActionKind
	challenge []byte
	principal []byte
}

func newScriptedAuthenticator() *scriptedAuthenticator {
	return &scriptedAuthenticator{
		actions: make(map[int64][]authAction),
	}
}

func (a *scriptedAuthenticator) enqueue(sessionID int64, acts ...authAction) {
	a.actions[sessionID] = append(a.actions[sessionID], acts...)
}

func (a *scriptedAuthenticator) HandleConnectRequest(
	sessionID int64,
	encodedCredentials []byte,
	nowNs int64,
) {
	a.connectCalls = append(a.connectCalls, authCall{
		sessionID:   sessionID,
		credentials: bytes.Clone(encodedCredentials),
		nowNs:       nowNs,
	})
}

func (a *scriptedAuthenticator) HandleChallengeResponse(
	sessionID int64,
	encodedCredentials []byte,
	nowNs int64,
) {
	a.challengeResp = append(a.challengeResp, authCall{
		sessionID:   sessionID,
		credentials: bytes.Clone(encodedCredentials),
		nowNs:       nowNs,
	})
}

func (a *scriptedAuthenticator) HandleConnectedSession(proxy SessionProxy, _ int64) {
	a.connectedCalls = append(a.connectedCalls, proxy.SessionID())
	a.play(proxy)
}

func (a *scriptedAuthenticator) HandleChallengedSession(proxy SessionProxy, _ int64) {
	a.challengedCalls = append(a.challengedCalls, proxy.SessionID())
	a.play(proxy)
}

func (a *scriptedAuthenticator) play(proxy SessionProxy) {
	id := proxy.SessionID()
	acts := a.actions[id]
	if len(acts) == 0 {
		return
	}
	act := acts[0]
	a.actions[id] = acts[1:]
	switch act.kind {
	case authActionChallenge:
		proxy.Challenge(act.challenge)
	case authActionAuthenticate:
		proxy.Authenticate(act.principal)
	case authActionReject:
		proxy.Reject()
	}
}

var _ Authenticator = (*scriptedAuthenticator)(nil)

// leaderWithAuth returns leaderWithLogPublisher with auth installed.
func leaderWithAuth(
	t testing.TB,
	auth Authenticator,
	p *LogPublisher,
	leadershipTermID int64,
) *Agent {
	t.Helper()
	a := leaderWithLogPublisher(t, p, leadershipTermID)
	a.authenticator = auth
	return a
}

// leaderWithAuthEgress returns a settled leader with auth, a bound log
// publisher, and a scripted connected egress publication (zero pending polls).
func leaderWithAuthEgress(
	t testing.TB,
	auth Authenticator,
	leadershipTermID int64,
	thisMemberID int32,
) (
	a *Agent,
	logPub *aeron.ExclusivePublication,
	adder *scriptedEgressAdder,
	egressPub *aeron.Publication,
) {
	t.Helper()
	p, logPub := boundLogPublisher(t)
	adder, egressPub = scriptedConnectedPub(t, 0)
	a = leaderWithEgress(t, p, leadershipTermID, thisMemberID, adder)
	a.authenticator = auth
	return a, logPub, adder, egressPub
}

// drainAuthRejectWork runs sessionWork once. If the reject event is still
// queued, runs a second tick to send the event.
func drainAuthRejectWork(t *testing.T, a *Agent, firstNs, secondNs int64) {
	t.Helper()
	work := a.sessionWork(firstNs)
	require.GreaterOrEqual(t, work, 1)
	if len(a.egressEvents) > 0 {
		require.Equal(t, 1, a.sessionWork(secondNs))
	}
}

func decodeChallengeAtTermOffset(
	t *testing.T,
	pub *aeron.Publication,
	termOffset int,
) (codecs.ChallengeFields, int) {
	t.Helper()
	return decodeClusterFieldsAtTermOffset[codecs.ChallengeFields](
		t,
		pub,
		termOffset,
		clusterwire.ChallengeSbeTemplateID,
	)
}

func TestAcceptSessionConnect_callsAuthenticator(t *testing.T) {
	auth := newScriptedAuthenticator()
	a := leaderWithAuth(t, auth, nil, 0)
	fields := validConnectFields(42)
	fields.EncodedCredentials = []byte{0xAB, 0xCD}
	const nowNs int64 = 1_000
	a.acceptSessionConnect(fields, nowNs)

	require.Len(t, auth.connectCalls, 1)
	require.Equal(t, int64(1), auth.connectCalls[0].sessionID)
	require.Equal(t, []byte{0xAB, 0xCD}, auth.connectCalls[0].credentials)
	require.Equal(t, nowNs, auth.connectCalls[0].nowNs)

	// Registry does not retain credentials.
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Nil(t, s.encodedPrincipal)
	require.Equal(t, sessionPending, s.state)
}

func TestAcceptSessionConnect_versionAndCapSkipAuthenticator(t *testing.T) {
	auth := newScriptedAuthenticator()
	a := leaderWithAuth(t, auth, nil, 0)

	// Version major mismatch.
	badVer := validConnectFields(1)
	badVer.Version = cnc.ComposeSemanticVersion(1, 0, 0)
	a.acceptSessionConnect(badVer, 10)
	require.Empty(t, auth.connectCalls)
	require.Equal(t, 0, a.PendingSessionCount())

	// Cap refuse: valid accepts call the authenticator. Over-cap does not.
	fillSessionsToCap(a, 20)
	connectsAtCap := len(auth.connectCalls)
	require.Equal(t, maxConcurrentSessions, connectsAtCap)
	a.acceptSessionConnect(validConnectFields(99), 30)
	require.Len(t, auth.connectCalls, connectsAtCap)
	require.Equal(t, maxConcurrentSessions, a.PendingSessionCount())
}

func TestAcceptSessionConnect_redirectSkipsAuthenticator(t *testing.T) {
	auth := newScriptedAuthenticator()
	a := followerWithEgress(t, withImmediateConnected(t), 3)
	a.authenticator = auth
	a.SetIngressEndpoints("0=host:1")
	a.handleSessionConnect(validConnectFields(1), 100)
	require.Empty(t, auth.connectCalls)
	require.Len(t, a.egressEvents, 1)
}

func TestAcceptSessionConnect_handlerReplacesAuthenticator(t *testing.T) {
	auth := newScriptedAuthenticator()
	handler := &recordingSessionConnectHandler{}
	a := leaderWithAuth(t, auth, nil, 0)
	a.SetSessionConnectHandler(handler)
	a.handleSessionConnect(validConnectFields(1), 50)
	require.Empty(t, auth.connectCalls)
	require.Len(t, handler.calls, 1)
	require.Equal(t, 0, a.PendingSessionCount())
}

func TestSessionPendingWork_egressNotConnected_noAuthConsult(t *testing.T) {
	auth := newScriptedAuthenticator()
	p, _ := boundLogPublisher(t)
	a := leaderWithEgress(t, p, 1, 0, neverReadyEgressAdder())
	a.authenticator = auth
	auth.enqueue(1, authAction{kind: authActionAuthenticate})
	a.acceptSessionConnect(validConnectFields(1), 1)

	// Not-done poll only. No connected consult.
	require.Equal(t, 1, a.sessionWork(10))
	require.Empty(t, auth.connectedCalls)
	require.Equal(t, 1, a.PendingSessionCount())
}

func TestSessionPendingWork_connectedConsultRepeats(t *testing.T) {
	auth := newScriptedAuthenticator()
	// Do nothing for two ticks, then authenticate.
	auth.enqueue(
		1,
		authAction{kind: authActionNone},
		authAction{kind: authActionNone},
		authAction{kind: authActionAuthenticate, principal: []byte("user")},
	)
	p, logPub := boundLogPublisher(t)
	a := leaderWithAuth(t, auth, p, 5)
	a.acceptSessionConnect(validConnectFields(7), 1)

	require.Equal(t, 0, a.sessionWork(10)) // none action
	require.Equal(t, 1, a.PendingSessionCount())
	require.Len(t, auth.connectedCalls, 1)

	require.Equal(t, 0, a.sessionWork(11)) // none action again
	require.Len(t, auth.connectedCalls, 2)

	// Authenticate + open + OK
	require.Equal(t, 2, a.sessionWork(12))
	require.Len(t, auth.connectedCalls, 3)
	requireSessionCounts(t, a, 0, 1)

	decoded, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Equal(t, []byte("user"), decoded.EncodedPrincipal)
}

func TestSessionAuth_challengeRound(t *testing.T) {
	auth := newScriptedAuthenticator()
	challengeBytes := []byte("nonce-1")
	principal := []byte("alice")
	// Challenge on connect, then grant on HandleChallengedSession after response.
	auth.enqueue(
		1,
		authAction{kind: authActionChallenge, challenge: challengeBytes},
		authAction{kind: authActionAuthenticate, principal: principal},
	)

	a, logPub, _, egressPub := leaderWithAuthEgress(t, auth, 3, 1)

	fields := validConnectFields(22)
	const acceptNs int64 = 100
	a.acceptSessionConnect(fields, acceptNs)

	// Challenge send. No SessionOpen.
	require.Equal(t, 0, a.sessionWork(acceptNs))
	require.Equal(t, 1, a.PendingSessionCount())
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionChallenged, s.state)
	require.Empty(t, auth.challengedCalls)

	ch, next := decodeChallengeAtTermOffset(t, egressPub, 0)
	require.Equal(t, int64(22), ch.CorrelationID)
	require.Equal(t, int64(1), ch.ClusterSessionID)
	require.Equal(t, challengeBytes, ch.EncodedChallenge)
	// No SessionOpen on the log yet.
	require.Zero(t, logPub.Position())

	// ChallengeResponse adopts correlation and refreshes activity.
	const respCorr int64 = 9001
	const respNs int64 = 200
	respCreds := []byte("resp-secret")
	a.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:      respCorr,
		ClusterSessionID:   1,
		EncodedCredentials: respCreds,
	}, respNs)
	require.Len(t, auth.challengeResp, 1)
	require.Equal(t, int64(1), auth.challengeResp[0].sessionID)
	require.Equal(t, respCreds, auth.challengeResp[0].credentials)
	require.Equal(t, respNs, auth.challengeResp[0].nowNs)
	require.Equal(t, respCorr, s.correlationID)
	require.Equal(t, respNs, s.lastActivityNs)

	// Next tick: HandleChallengedSession authenticates, open + OK.
	require.Equal(t, 2, a.sessionWork(respNs))
	requireSessionCounts(t, a, 0, 1)
	require.Len(t, auth.challengedCalls, 1)

	open, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Equal(t, principal, open.EncodedPrincipal)
	require.Equal(t, respCorr, open.CorrelationID)

	// OK carries the response correlation id.
	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, next)
	requireSessionEventOK(t, evt, 1, respCorr, 3, 1, a.leaderHeartbeatTimeoutNs)
}

func TestSessionAuth_challengeSendFailure(t *testing.T) {
	auth := newScriptedAuthenticator()
	// Every connected consult tries to challenge until send succeeds.
	auth.enqueue(
		1,
		authAction{kind: authActionChallenge, challenge: []byte("c")},
		authAction{kind: authActionChallenge, challenge: []byte("c")},
		authAction{kind: authActionChallenge, challenge: []byte("c2")},
		authAction{kind: authActionAuthenticate, principal: []byte("ok")},
	)
	p, _ := boundLogPublisher(t)
	adder := newScriptedEgressAdder()
	// Limit 0: challenge offer is retriable.
	zeroWin := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
	adder.setNextPub(zeroWin, 0)
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.authenticator = auth
	a.acceptSessionConnect(validConnectFields(1), 1)

	// Challenge fails. State stays pending.
	require.Equal(t, 0, a.sessionWork(10))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionPending, s.state)
	require.Len(t, auth.connectedCalls, 1)

	// Still pending on next tick (limit still 0).
	require.Equal(t, 0, a.sessionWork(11))
	require.Equal(t, sessionPending, s.state)
	require.Len(t, auth.connectedCalls, 2)

	// Raise limit: challenge succeeds.
	s.egressPublication = aeron.NewTestPublicationForHooksWithConnected(t, true)
	require.Equal(t, 0, a.sessionWork(12))
	require.Equal(t, sessionChallenged, s.state)

	// Grant and open on HandleChallengedSession.
	require.Equal(t, 2, a.sessionWork(13))
	requireSessionCounts(t, a, 0, 1)
}

func TestSessionAuth_reChallenge(t *testing.T) {
	auth := newScriptedAuthenticator()
	// First connect: challenge. After each response: re-challenge, then grant.
	auth.enqueue(
		1,
		authAction{kind: authActionChallenge, challenge: []byte("r1")},
		authAction{kind: authActionChallenge, challenge: []byte("r2")},
		authAction{kind: authActionAuthenticate, principal: []byte("final")},
	)

	a, logPub, _, egressPub := leaderWithAuthEgress(t, auth, 1, 2)
	a.acceptSessionConnect(validConnectFields(10), 1)

	require.Equal(t, 0, a.sessionWork(1))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionChallenged, s.state)
	ch1, off1 := decodeChallengeAtTermOffset(t, egressPub, 0)
	require.Equal(t, []byte("r1"), ch1.EncodedChallenge)
	require.Equal(t, int64(10), ch1.CorrelationID)

	a.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:      20,
		ClusterSessionID:   1,
		EncodedCredentials: []byte("x"),
	}, 2)
	require.Equal(t, int64(20), s.correlationID)

	// Re-challenge with latest correlation id.
	require.Equal(t, 0, a.sessionWork(2))
	require.Equal(t, sessionChallenged, s.state)
	ch2, off2 := decodeChallengeAtTermOffset(t, egressPub, off1)
	require.Equal(t, []byte("r2"), ch2.EncodedChallenge)
	require.Equal(t, int64(20), ch2.CorrelationID)

	a.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:      30,
		ClusterSessionID:   1,
		EncodedCredentials: []byte("y"),
	}, 3)

	require.Equal(t, 2, a.sessionWork(3))
	requireSessionCounts(t, a, 0, 1)
	open, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Equal(t, []byte("final"), open.EncodedPrincipal)
	require.Equal(t, int64(30), open.CorrelationID)
	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, off2)
	requireSessionEventOK(t, evt, 1, 30, 1, 2, a.leaderHeartbeatTimeoutNs)
}

func TestSessionAuth_challengeResponseMismatches(t *testing.T) {
	auth := newScriptedAuthenticator()
	auth.enqueue(1, authAction{kind: authActionChallenge, challenge: []byte("c")})
	p, _ := boundLogPublisher(t)
	a := leaderWithAuth(t, auth, p, 0)
	a.acceptSessionConnect(validConnectFields(1), 1)
	require.Equal(t, 0, a.sessionWork(1))

	// Unknown session id.
	a.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:    2,
		ClusterSessionID: 99,
	}, 10)
	require.Empty(t, auth.challengeResp)

	// Not challenged (fresh pending session).
	auth2 := newScriptedAuthenticator()
	// Do nothing so state stays pending.
	a2 := leaderWithAuth(t, auth2, p, 0)
	a2.acceptSessionConnect(validConnectFields(1), 1)
	// Resolve egress without auth resolution: force no action.
	s2, ok := a2.sessions.sessionByID(1)
	require.True(t, ok)
	ready, _, _ := a2.pollSessionEgress(s2)
	require.True(t, ready)
	a2.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:    2,
		ClusterSessionID: 1,
	}, 10)
	require.Empty(t, auth2.challengeResp)
}

func TestSessionAuth_challengeTimeoutReclaim(t *testing.T) {
	auth := newScriptedAuthenticator()
	auth.enqueue(1, authAction{kind: authActionChallenge, challenge: []byte("c")})
	a, _, _, egressPub := leaderWithAuthEgress(t, auth, 0, 0)
	const startNs int64 = 1_000
	a.acceptSessionConnect(validConnectFields(1), startNs)
	require.Equal(t, 0, a.sessionWork(startNs))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionChallenged, s.state)
	require.False(t, egressPub.IsClosed())

	// Past defaultSessionTimeoutNs from lastActivityNs (accept time, no refresh).
	require.Equal(t, 1, a.sessionWork(startNs+defaultSessionTimeoutNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.True(t, egressPub.IsClosed())
}

// TestSessionAuth_challengeResponseRefreshRestartsTimeout asserts
// ChallengeResponse refresh restarts the reclaim timeout from the refresh clock.
func TestSessionAuth_challengeResponseRefreshRestartsTimeout(t *testing.T) {
	auth := newScriptedAuthenticator()
	// Challenge once. Stay challenged on later ticks (no further actions).
	auth.enqueue(1, authAction{kind: authActionChallenge, challenge: []byte("c")})
	a, _, _, egressPub := leaderWithAuthEgress(t, auth, 0, 0)

	const acceptNs int64 = 1_000
	a.acceptSessionConnect(validConnectFields(1), acceptNs)
	require.Equal(t, 0, a.sessionWork(acceptNs))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionChallenged, s.state)

	// Refresh activity midway to the original accept deadline.
	const refreshNs int64 = acceptNs + defaultSessionTimeoutNs/2
	a.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:      77,
		ClusterSessionID:   1,
		EncodedCredentials: []byte("resp"),
	}, refreshNs)
	require.Equal(t, refreshNs, s.lastActivityNs)
	require.Equal(t, int64(77), s.correlationID)

	// Original accept deadline has passed. Refresh restarted the timeout.
	pastAcceptDeadline := acceptNs + defaultSessionTimeoutNs
	require.Greater(t, pastAcceptDeadline, refreshNs)
	// sessionWork still consults the challenged handler (no action). Does not
	// reclaim before refresh+timeout.
	_ = a.sessionWork(pastAcceptDeadline)
	require.Equal(t, 1, a.PendingSessionCount())
	_, still := a.sessions.sessionByID(1)
	require.True(t, still)
	require.False(t, egressPub.IsClosed())

	// One tick before refresh deadline: still present.
	justBefore := refreshNs + defaultSessionTimeoutNs - 1
	_ = a.sessionWork(justBefore)
	require.Equal(t, 1, a.PendingSessionCount())

	// At refresh+timeout: reclaim.
	require.Equal(t, 1, a.sessionWork(refreshNs+defaultSessionTimeoutNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.True(t, egressPub.IsClosed())
}

func TestSessionAuth_authenticateClonesPrincipal(t *testing.T) {
	auth := newScriptedAuthenticator()
	mutable := []byte("principal-v1")
	auth.enqueue(1, authAction{kind: authActionAuthenticate, principal: mutable})

	p, logPub := boundLogPublisher(t)
	a := leaderWithAuth(t, auth, p, 1)
	a.acceptSessionConnect(validConnectFields(9), 1)

	require.Equal(t, 2, a.sessionWork(1))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, []byte("principal-v1"), s.encodedPrincipal)

	// Mutate the caller's buffer after Authenticate cloned the principal.
	// Session and SessionOpenEvent keep the original principal bytes.
	copy(mutable, []byte("principal-XX"))
	require.Equal(t, []byte("principal-v1"), s.encodedPrincipal)

	open, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Equal(t, []byte("principal-v1"), open.EncodedPrincipal)
}

func TestSessionAuth_simpleAuthenticator_validAndInvalid(t *testing.T) {
	creds := []byte("secret")
	principal := []byte("alice")
	auth := NewSimpleAuthenticator([]SimplePrincipal{{
		EncodedPrincipal:   principal,
		EncodedCredentials: creds,
	}})

	// Valid credentials open with principal.
	a, logPub, _, egressPub := leaderWithAuthEgress(t, auth, 2, 4)
	good := validConnectFields(11)
	good.EncodedCredentials = creds
	a.acceptSessionConnect(good, 1)
	require.Equal(t, 2, a.sessionWork(1))
	requireSessionCounts(t, a, 0, 1)
	open, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Equal(t, principal, open.EncodedPrincipal)
	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, 0)
	requireSessionEventOK(t, evt, 1, 11, 2, 4, a.leaderHeartbeatTimeoutNs)

	// Invalid credentials reject on a fresh agent.
	a2, logPub2, adder2, egressPub2 := leaderWithAuthEgress(t, auth, 2, 4)
	bad := validConnectFields(12)
	bad.EncodedCredentials = []byte("wrong")
	a2.acceptSessionConnect(bad, 1)
	// Reject handoff is one work unit. Drain may need a second tick.
	drainAuthRejectWork(t, a2, 1, 2)
	require.Equal(t, 0, a2.PendingSessionCount())
	require.Empty(t, a2.sessions.byID)
	require.Empty(t, a2.egressEvents)
	require.Zero(t, logPub2.Position())
	rej, _ := decodeSessionEventAtTermOffset(t, egressPub2, 0)
	require.Equal(t, int64(1), rej.ClusterSessionID)
	require.Equal(t, int64(12), rej.CorrelationID)
	require.Equal(t, clusterwire.EventCode_AUTHENTICATION_REJECTED, rej.Code)
	require.Equal(t, sessionRejectedDetailBytes, rej.Detail)
	require.True(t, egressPub2.IsClosed())
	// Exactly one publication add for the connect-reject round.
	require.Len(t, adder2.offers, 1)
}

func TestSessionAuth_nilPrincipalGrant(t *testing.T) {
	// Default accept-all (nil config).
	p, logPub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, 1)
	a.acceptSessionConnect(validConnectFields(5), 1)
	require.Equal(t, 2, a.sessionWork(1))
	open, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Empty(t, open.EncodedPrincipal)
}

func TestSessionAuth_oversizedPrincipalRejects(t *testing.T) {
	auth := newScriptedAuthenticator()
	oversized := make([]byte, maxEncodedPrincipalLength+1)
	auth.enqueue(1, authAction{kind: authActionAuthenticate, principal: oversized})

	a, logPub, _, egressPub := leaderWithAuthEgress(t, auth, 0, 0)
	a.acceptSessionConnect(validConnectFields(3), 1)

	drainAuthRejectWork(t, a, 1, 2)
	require.Empty(t, a.sessions.byID)
	require.Zero(t, logPub.Position())
	rej, _ := decodeSessionEventAtTermOffset(t, egressPub, 0)
	require.Equal(t, clusterwire.EventCode_AUTHENTICATION_REJECTED, rej.Code)
	require.Equal(t, sessionRejectedDetailBytes, rej.Detail)
}

func TestSessionAuth_rejectAtEventQueueCap(t *testing.T) {
	auth := newScriptedAuthenticator()
	auth.enqueue(1, authAction{kind: authActionReject})

	p, _ := boundLogPublisher(t)
	a := NewAgent(AgentConfig{
		Role:           RoleFollower,
		LeaderMemberID: 1,
		LogPublisher:   p,
		Authenticator:  auth,
	})
	a.SetIngressEndpoints("0=x")
	// Pre-fill event queue to cap with never-ready redirect adds.
	a.SetEgressPublicationAdder(neverReadyEgressAdder())
	for range maxPendingEgressEvents {
		a.queueSessionRedirect(validConnectFields(1), 1)
	}
	require.Len(t, a.egressEvents, maxPendingEgressEvents)

	// Switch to leader with a resolved publication for the reject path.
	a.role = RoleLeader
	a.leaderMemberID = nullMemberID
	a.SetEgressPublicationAdder(withImmediateConnected(t))
	a.acceptSessionConnect(validConnectFields(9), 1)
	require.Same(t, auth, a.authenticator)

	pubBefore := a.sessions.pending[0]
	ready, _, _ := a.pollSessionEgress(pubBefore)
	require.True(t, ready)
	egressPub := pubBefore.egressPublication
	require.NotNil(t, egressPub)

	// Walk: reject, queue at cap closes publication and drops.
	require.Equal(t, 1, a.sessionPendingWork(1))
	require.Empty(t, a.sessions.byID)
	require.Len(t, a.egressEvents, maxPendingEgressEvents)
	require.True(t, egressPub.IsClosed())
}

func TestSessionAuth_rejectFromChallenged(t *testing.T) {
	auth := newScriptedAuthenticator()
	auth.enqueue(
		1,
		authAction{kind: authActionChallenge, challenge: []byte("c")},
		authAction{kind: authActionReject},
	)

	a, logPub, _, egressPub := leaderWithAuthEgress(t, auth, 0, 0)
	a.acceptSessionConnect(validConnectFields(4), 1)
	require.Equal(t, 0, a.sessionWork(1))
	require.Equal(t, sessionChallenged, a.sessions.pending[0].state)

	a.handleChallengeResponse(codecs.ChallengeResponseFields{
		CorrelationID:      50,
		ClusterSessionID:   1,
		EncodedCredentials: []byte("bad"),
	}, 2)

	drainAuthRejectWork(t, a, 2, 3)
	require.Empty(t, a.sessions.byID)
	require.Zero(t, logPub.Position())
	// Challenge frame is first on the egress publication. Reject follows.
	ch, next := decodeChallengeAtTermOffset(t, egressPub, 0)
	require.Equal(t, []byte("c"), ch.EncodedChallenge)
	rej, _ := decodeSessionEventAtTermOffset(t, egressPub, next)
	require.Equal(t, clusterwire.EventCode_AUTHENTICATION_REJECTED, rej.Code)
	require.Equal(t, int64(50), rej.CorrelationID)
}

func TestNewAgent_authenticatorWiring(t *testing.T) {
	// Nil config installs accept-all.
	a := NewAgent(AgentConfig{Role: RoleLeader})
	require.NotNil(t, a.authenticator)
	_, ok := a.authenticator.(acceptAllAuthenticator)
	require.True(t, ok)

	// Custom authenticator is retained.
	auth := newScriptedAuthenticator()
	a2 := NewAgent(AgentConfig{Role: RoleLeader, Authenticator: auth})
	require.Same(t, auth, a2.authenticator)
}

func TestSessionAuth_proxyAuthenticatePrincipalLength(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader})
	s := addPending(a.sessions, validConnectFields(1), 1)
	proxy := a.proxyFor(s)
	require.False(t, proxy.Authenticate(make([]byte, maxEncodedPrincipalLength+1)))
	require.Equal(t, sessionRejected, s.state)

	// Boundary principal length succeeds.
	s2 := addPending(a.sessions, validConnectFields(2), 1)
	proxy2 := a.proxyFor(s2)
	require.True(t, proxy2.Authenticate(make([]byte, maxEncodedPrincipalLength)))
	require.Equal(t, sessionAuthenticated, s2.state)
	require.Len(t, s2.encodedPrincipal, maxEncodedPrincipalLength)
}
