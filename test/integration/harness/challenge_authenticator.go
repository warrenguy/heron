package harness

import (
	"bytes"

	"github.com/warrenguy/heron/cluster/consensus"
)

// ChallengeAuthenticator grants one principal after one challenge round.
//
// Concurrency: invoker-only, from Agent.DoWork.
//
// Each member has one instance.
//
// Lifetime: a failed challenge send leaves the session for the next
// duty cycle. HandleConnectedSession and HandleChallengedSession delete
// the session entry after Authenticate or Reject.
type ChallengeAuthenticator struct {
	credentials []byte
	challenge   []byte
	response    []byte
	principal   []byte

	sessions map[int64]*challengeSession

	challengesSent int
	responsesSeen  int
}

type challengeSession struct {
	credentialsOK bool
	response      []byte
	sawResponse   bool
}

// Clones every slice. The caller may reuse the inputs after return.
func NewChallengeAuthenticator(
	credentials, challenge, response, principal []byte,
) *ChallengeAuthenticator {
	return &ChallengeAuthenticator{
		credentials: bytes.Clone(credentials),
		challenge:   bytes.Clone(challenge),
		response:    bytes.Clone(response),
		principal:   bytes.Clone(principal),
		sessions:    map[int64]*challengeSession{},
	}
}

// Returns the number of successful challenge sends.
func (a *ChallengeAuthenticator) ChallengesSent() int {
	return a.challengesSent
}

func (a *ChallengeAuthenticator) ResponsesSeen() int {
	return a.responsesSeen
}

func (a *ChallengeAuthenticator) HandleConnectRequest(
	sessionID int64,
	encodedCredentials []byte,
	_ int64,
) {
	state := a.session(sessionID)
	state.credentialsOK = bytes.Equal(a.credentials, encodedCredentials)
}

func (a *ChallengeAuthenticator) HandleChallengeResponse(
	sessionID int64,
	encodedCredentials []byte,
	_ int64,
) {
	state := a.session(sessionID)
	state.response = bytes.Clone(encodedCredentials)
	state.sawResponse = true
	a.responsesSeen++
}

func (a *ChallengeAuthenticator) HandleConnectedSession(proxy consensus.SessionProxy, _ int64) {
	sessionID := proxy.SessionID()
	state := a.sessions[sessionID]
	if state == nil || !state.credentialsOK {
		proxy.Reject()
		delete(a.sessions, sessionID)
		return
	}
	if proxy.Challenge(a.challenge) {
		a.challengesSent++
	}
}

func (a *ChallengeAuthenticator) HandleChallengedSession(proxy consensus.SessionProxy, _ int64) {
	sessionID := proxy.SessionID()
	state := a.sessions[sessionID]
	if state == nil || !state.sawResponse {
		return
	}
	if bytes.Equal(state.response, a.response) {
		proxy.Authenticate(a.principal)
	} else {
		proxy.Reject()
	}
	delete(a.sessions, sessionID)
}

func (a *ChallengeAuthenticator) session(sessionID int64) *challengeSession {
	state := a.sessions[sessionID]
	if state == nil {
		state = &challengeSession{}
		a.sessions[sessionID] = state
	}
	return state
}

var _ consensus.Authenticator = (*ChallengeAuthenticator)(nil)
