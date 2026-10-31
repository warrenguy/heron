package harness

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeSessionProxy struct {
	id              int64
	challengeResult bool
	challengeCalls  int
	lastChallenge   []byte
	authCalls       int
	lastPrincipal   []byte
	rejectCalls     int
}

func (p *fakeSessionProxy) SessionID() int64 { return p.id }

func (p *fakeSessionProxy) Challenge(encodedChallenge []byte) bool {
	p.challengeCalls++
	p.lastChallenge = bytes.Clone(encodedChallenge)
	return p.challengeResult
}

func (p *fakeSessionProxy) Authenticate(encodedPrincipal []byte) bool {
	p.authCalls++
	p.lastPrincipal = bytes.Clone(encodedPrincipal)
	return true
}

func (p *fakeSessionProxy) Reject() {
	p.rejectCalls++
}

func TestChallengeAuthenticator(t *testing.T) {
	t.Parallel()

	credentials := []byte("credentials")
	challenge := []byte("challenge")
	response := []byte("response")
	principal := []byte("principal")

	t.Run("credentials mismatch rejects", func(t *testing.T) {
		t.Parallel()
		auth := NewChallengeAuthenticator(credentials, challenge, response, principal)

		// The bad-credential session holds a matching response. A reject that
		// keeps the entry would authenticate in HandleChallengedSession.
		auth.HandleConnectRequest(7, []byte("other"), 1)
		auth.HandleChallengeResponse(7, response, 2)
		require.Equal(t, 1, auth.ResponsesSeen())
		proxy := &fakeSessionProxy{id: 7}
		auth.HandleConnectedSession(proxy, 3)
		require.Equal(t, 1, proxy.rejectCalls)
		require.Zero(t, proxy.challengeCalls)
		require.Zero(t, auth.ChallengesSent())
		auth.HandleChallengedSession(proxy, 4)
		require.Equal(t, 1, proxy.rejectCalls)
		require.Zero(t, proxy.authCalls)
	})

	t.Run("challenge send failure retries", func(t *testing.T) {
		t.Parallel()
		auth := NewChallengeAuthenticator(credentials, challenge, response, principal)
		auth.HandleConnectRequest(1, credentials, 10)
		proxy := &fakeSessionProxy{id: 1, challengeResult: false}
		auth.HandleConnectedSession(proxy, 11)
		require.Equal(t, 1, proxy.challengeCalls)
		require.Equal(t, challenge, proxy.lastChallenge)
		require.Zero(t, auth.ChallengesSent())
		require.Zero(t, proxy.rejectCalls)
		proxy.challengeResult = true
		auth.HandleConnectedSession(proxy, 12)
		require.Equal(t, 2, proxy.challengeCalls)
		require.Equal(t, 1, auth.ChallengesSent())
		require.Zero(t, proxy.rejectCalls)
	})

	t.Run("no response defers", func(t *testing.T) {
		t.Parallel()
		auth := NewChallengeAuthenticator(credentials, challenge, response, principal)
		proxy := connectedProxy(t, auth, credentials, challenge, 4)
		auth.HandleChallengedSession(proxy, 20)
		require.Zero(t, proxy.authCalls)
		require.Zero(t, proxy.rejectCalls)
		require.Zero(t, auth.ResponsesSeen())
		auth.HandleChallengeResponse(proxy.id, response, 21)
		require.Equal(t, 1, auth.ResponsesSeen())
		auth.HandleChallengedSession(proxy, 22)
		require.Equal(t, 1, proxy.authCalls)
		require.Equal(t, principal, proxy.lastPrincipal)
	})

	t.Run("matching response authenticates with the principal", func(t *testing.T) {
		t.Parallel()
		storedCredentials := bytes.Clone(credentials)
		storedChallenge := bytes.Clone(challenge)
		storedResponse := bytes.Clone(response)
		storedPrincipal := bytes.Clone(principal)
		auth := NewChallengeAuthenticator(
			storedCredentials,
			storedChallenge,
			storedResponse,
			storedPrincipal,
		)
		storedCredentials[0] = 'X'
		storedChallenge[0] = 'X'
		storedResponse[0] = 'X'
		storedPrincipal[0] = 'X'
		proxy := connectedProxy(t, auth, credentials, challenge, 8)
		seen := bytes.Clone(response)
		auth.HandleChallengeResponse(proxy.id, seen, 30)
		seen[0] = 'Y'
		auth.HandleChallengedSession(proxy, 31)
		require.Equal(t, 1, proxy.authCalls)
		require.Equal(t, principal, proxy.lastPrincipal)
		require.Zero(t, proxy.rejectCalls)
		require.Equal(t, 1, auth.ChallengesSent())
		require.Equal(t, 1, auth.ResponsesSeen())
		auth.HandleChallengedSession(proxy, 32)
		require.Equal(t, 1, proxy.authCalls)
	})

	t.Run("wrong response rejects", func(t *testing.T) {
		t.Parallel()
		auth := NewChallengeAuthenticator(credentials, challenge, response, principal)
		proxy := connectedProxy(t, auth, credentials, challenge, 9)
		auth.HandleChallengeResponse(proxy.id, []byte("wrong"), 40)
		auth.HandleChallengedSession(proxy, 41)
		require.Equal(t, 1, proxy.rejectCalls)
		require.Zero(t, proxy.authCalls)
		require.Equal(t, 1, auth.ResponsesSeen())
		auth.HandleChallengedSession(proxy, 42)
		require.Equal(t, 1, proxy.rejectCalls)
	})
}

func connectedProxy(
	t *testing.T,
	auth *ChallengeAuthenticator,
	credentials, challenge []byte,
	sessionID int64,
) *fakeSessionProxy {
	t.Helper()
	auth.HandleConnectRequest(sessionID, credentials, 1)
	proxy := &fakeSessionProxy{id: sessionID, challengeResult: true}
	auth.HandleConnectedSession(proxy, 2)
	require.Equal(t, 1, proxy.challengeCalls)
	require.Equal(t, challenge, proxy.lastChallenge)
	require.Equal(t, 1, auth.ChallengesSent())
	return proxy
}
