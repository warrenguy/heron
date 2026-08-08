//go:build testhooks

package consensus

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingSessionProxy records SessionProxy calls for authenticator tests.
type recordingSessionProxy struct {
	sessionID      int64
	challenges     [][]byte
	principals     [][]byte
	rejectCount    int
	authenticateOK bool
}

func newRecordingSessionProxy(sessionID int64) *recordingSessionProxy {
	return &recordingSessionProxy{
		sessionID:      sessionID,
		authenticateOK: true,
	}
}

func (p *recordingSessionProxy) SessionID() int64 { return p.sessionID }

func (p *recordingSessionProxy) Challenge(encodedChallenge []byte) bool {
	p.challenges = append(p.challenges, bytes.Clone(encodedChallenge))
	return true
}

func (p *recordingSessionProxy) Authenticate(encodedPrincipal []byte) bool {
	p.principals = append(p.principals, bytes.Clone(encodedPrincipal))
	return p.authenticateOK
}

func (p *recordingSessionProxy) Reject() {
	p.rejectCount++
}

func requireAuthGranted(t *testing.T, proxy *recordingSessionProxy, principal []byte) {
	t.Helper()
	require.Equal(t, 0, proxy.rejectCount)
	require.Equal(t, [][]byte{principal}, proxy.principals)
}

func requireAuthRejected(t *testing.T, proxy *recordingSessionProxy) {
	t.Helper()
	require.Equal(t, 1, proxy.rejectCount)
	require.Empty(t, proxy.principals)
}

func TestAcceptAllAuthenticator(t *testing.T) {
	t.Parallel()

	auth := acceptAllAuthenticator{}
	auth.HandleConnectRequest(1, []byte("creds"), 100)
	auth.HandleChallengeResponse(1, []byte("resp"), 200)

	connected := newRecordingSessionProxy(1)
	auth.HandleConnectedSession(connected, 300)
	require.Empty(t, connected.challenges)
	requireAuthGranted(t, connected, nil)

	challenged := newRecordingSessionProxy(2)
	auth.HandleChallengedSession(challenged, 400)
	require.Empty(t, challenged.challenges)
	requireAuthGranted(t, challenged, nil)
}

func TestSimpleAuthenticator_table(t *testing.T) {
	t.Parallel()

	aliceCreds := []byte("alice-secret")
	bobCreds := []byte("bob-secret")
	alicePrincipal := []byte("alice")
	bobPrincipal := []byte("bob")

	aliceAuth := func() *SimpleAuthenticator {
		return NewSimpleAuthenticator([]SimplePrincipal{
			{EncodedPrincipal: alicePrincipal, EncodedCredentials: aliceCreds},
		})
	}

	t.Run("multi_principal_each_grants_own", func(t *testing.T) {
		t.Parallel()
		a := NewSimpleAuthenticator([]SimplePrincipal{
			{EncodedPrincipal: alicePrincipal, EncodedCredentials: aliceCreds},
			{EncodedPrincipal: bobPrincipal, EncodedCredentials: bobCreds},
		})

		a.HandleConnectRequest(10, aliceCreds, 1)
		aliceProxy := newRecordingSessionProxy(10)
		a.HandleConnectedSession(aliceProxy, 2)
		requireAuthGranted(t, aliceProxy, alicePrincipal)

		a.HandleConnectRequest(11, bobCreds, 3)
		bobProxy := newRecordingSessionProxy(11)
		a.HandleConnectedSession(bobProxy, 4)
		requireAuthGranted(t, bobProxy, bobPrincipal)
	})

	t.Run("same_credentials_two_sessions", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()

		a.HandleConnectRequest(20, aliceCreds, 1)
		a.HandleConnectRequest(21, aliceCreds, 2)

		p20 := newRecordingSessionProxy(20)
		p21 := newRecordingSessionProxy(21)
		a.HandleConnectedSession(p20, 3)
		a.HandleConnectedSession(p21, 4)

		requireAuthGranted(t, p20, alicePrincipal)
		requireAuthGranted(t, p21, alicePrincipal)
	})

	t.Run("empty_credentials_no_empty_entry_rejects", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()

		a.HandleConnectRequest(30, nil, 1)
		proxy := newRecordingSessionProxy(30)
		a.HandleConnectedSession(proxy, 2)
		requireAuthRejected(t, proxy)
	})

	t.Run("grant_removes_map_entry", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()

		a.HandleConnectRequest(40, aliceCreds, 1)
		first := newRecordingSessionProxy(40)
		a.HandleConnectedSession(first, 2)
		requireAuthGranted(t, first, alicePrincipal)

		// Second HandleConnectedSession for the same id has no grant entry.
		second := newRecordingSessionProxy(40)
		a.HandleConnectedSession(second, 3)
		requireAuthRejected(t, second)
	})

	t.Run("authenticate_false_removes_grant", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()

		a.HandleConnectRequest(41, aliceCreds, 1)
		first := newRecordingSessionProxy(41)
		first.authenticateOK = false
		a.HandleConnectedSession(first, 2)
		requireAuthGranted(t, first, alicePrincipal)

		// Grant removed after Authenticate returns false. Second tick Rejects.
		second := newRecordingSessionProxy(41)
		a.HandleConnectedSession(second, 3)
		requireAuthRejected(t, second)
	})

	t.Run("challenge_response_noop", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()
		a.HandleConnectRequest(99, aliceCreds, 1)
		a.HandleChallengeResponse(99, []byte("ignored"), 2)

		// No proxy resolution from HandleChallengeResponse. Grant still works.
		proxy := newRecordingSessionProxy(99)
		a.HandleConnectedSession(proxy, 3)
		require.Empty(t, proxy.challenges)
		requireAuthGranted(t, proxy, alicePrincipal)
	})

	t.Run("challenged_session_rejects", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()
		proxy := newRecordingSessionProxy(50)
		a.HandleChallengedSession(proxy, 1)
		requireAuthRejected(t, proxy)
		require.Empty(t, proxy.challenges)
	})

	t.Run("wrong_credentials_reject", func(t *testing.T) {
		t.Parallel()
		a := aliceAuth()
		a.HandleConnectRequest(60, []byte("nope"), 1)
		proxy := newRecordingSessionProxy(60)
		a.HandleConnectedSession(proxy, 2)
		requireAuthRejected(t, proxy)
	})

	t.Run("empty_principal_grant", func(t *testing.T) {
		t.Parallel()
		creds := []byte("empty-principal-creds")
		a := NewSimpleAuthenticator([]SimplePrincipal{
			{EncodedPrincipal: nil, EncodedCredentials: creds},
		})
		a.HandleConnectRequest(70, creds, 1)
		proxy := newRecordingSessionProxy(70)
		a.HandleConnectedSession(proxy, 2)
		requireAuthGranted(t, proxy, nil)
	})

	t.Run("duplicate_credentials_last_wins", func(t *testing.T) {
		t.Parallel()
		creds := []byte("shared")
		firstPrincipal := []byte("first")
		lastPrincipal := []byte("last")
		a := NewSimpleAuthenticator([]SimplePrincipal{
			{EncodedPrincipal: firstPrincipal, EncodedCredentials: creds},
			{EncodedPrincipal: lastPrincipal, EncodedCredentials: creds},
		})
		a.HandleConnectRequest(80, creds, 1)
		proxy := newRecordingSessionProxy(80)
		a.HandleConnectedSession(proxy, 2)
		requireAuthGranted(t, proxy, lastPrincipal)
	})

	t.Run("clone_isolation", func(t *testing.T) {
		t.Parallel()
		creds := []byte("iso-creds")
		principal := []byte("iso-principal")
		input := []SimplePrincipal{
			{EncodedPrincipal: principal, EncodedCredentials: creds},
		}
		a := NewSimpleAuthenticator(input)

		// Mutate constructor inputs after New. Table still matches originals.
		// input[0] aliases creds and principal. Flip all bytes once.
		for i := range creds {
			creds[i] ^= 0xFF
		}
		for i := range principal {
			principal[i] ^= 0xFF
		}

		matchCreds := []byte("iso-creds")
		wantPrincipal := []byte("iso-principal")
		a.HandleConnectRequest(90, matchCreds, 1)
		proxy := newRecordingSessionProxy(90)
		a.HandleConnectedSession(proxy, 2)
		requireAuthGranted(t, proxy, wantPrincipal)

		// Mutating the Authenticate-recorded principal must not affect the
		// table for a later connect with the same credentials.
		proxy.principals[0][0] ^= 0xFF
		a.HandleConnectRequest(91, matchCreds, 3)
		second := newRecordingSessionProxy(91)
		a.HandleConnectedSession(second, 4)
		requireAuthGranted(t, second, wantPrincipal)
	})

	t.Run("empty_credentials_table_entry_grants", func(t *testing.T) {
		t.Parallel()
		a := NewSimpleAuthenticator([]SimplePrincipal{
			{EncodedPrincipal: []byte("anon"), EncodedCredentials: nil},
		})
		a.HandleConnectRequest(100, nil, 1)
		proxy := newRecordingSessionProxy(100)
		a.HandleConnectedSession(proxy, 2)
		requireAuthGranted(t, proxy, []byte("anon"))
	})
}
