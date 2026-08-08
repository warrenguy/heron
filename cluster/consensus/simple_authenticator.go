package consensus

import (
	"bytes"
	"slices"
)

// SimplePrincipal pairs an encoded principal with the credentials that grant
// that principal.
type SimplePrincipal struct {
	EncodedPrincipal   []byte
	EncodedCredentials []byte
}

// SimpleAuthenticator maps connect credentials to a principal (upstream
// SimpleAuthenticator parity).
//
// Behaviour:
//   - HandleConnectRequest compares credentials with bytes.Equal. Comparison
//     is not constant-time (upstream Arrays.equals parity). A match grants the
//     paired table principal under the session id. Duplicate credential rows:
//     last table entry wins (upstream credentials map put order). Empty
//     connect credentials match only a table entry with empty
//     EncodedCredentials. An empty-credentials match grants the paired
//     principal.
//   - HandleConnectedSession authenticates the stored principal through the
//     proxy. Removes the map entry after Authenticate returns, true or false.
//     Authenticate false ends the grant. The proxy already rejected the
//     session. Missing map entry rejects.
//   - HandleChallengeResponse is a silent no-op. This authenticator never
//     challenges. The path is unreachable in product use.
//   - HandleChallengedSession rejects the session fail-closed. Upstream
//     throws. Heron must not panic the duty cycle.
//   - Never challenges.
//
// Concurrency: invoker-only, from [Agent.DoWork].
//
// Lifetime: connect credentials are compared and discarded. A grant aliases
// the table principal. The constructor cloned the table. No method mutates the
// principal. The proxy clones again before the principal reaches the session.
// HandleConnectedSession invokes Authenticate, then deletes the map entry. An
// abandoned session id retains a map entry until process end. Abandoned means
// credentials matched and HandleConnectedSession never ran. Upstream-shaped.
// No reclaim hook on this type.
type SimpleAuthenticator struct {
	principals []SimplePrincipal
	// granted maps session id to the table principal after a credential match.
	granted map[int64][]byte
}

// NewSimpleAuthenticator builds a credentials-table authenticator.
//
// Behaviour: byte-matching connect credentials grant the paired principal.
// Non-matching connects are rejected. Never challenges.
//
// Construction clones every table slice. Callers may mutate the input after
// return.
func NewSimpleAuthenticator(principals []SimplePrincipal) *SimpleAuthenticator {
	table := make([]SimplePrincipal, len(principals))
	for i, p := range principals {
		table[i] = SimplePrincipal{
			EncodedPrincipal:   bytes.Clone(p.EncodedPrincipal),
			EncodedCredentials: bytes.Clone(p.EncodedCredentials),
		}
	}
	return &SimpleAuthenticator{
		principals: table,
		granted:    make(map[int64][]byte),
	}
}

// HandleConnectRequest grants the table principal on credential match. No match
// keeps the session ungranted. Last matching row wins.
func (a *SimpleAuthenticator) HandleConnectRequest(
	sessionID int64,
	encodedCredentials []byte,
	_ int64,
) {
	// Reverse scan. First hit from the back is the last matching table entry
	// (last wins).
	for _, p := range slices.Backward(a.principals) {
		if bytes.Equal(p.EncodedCredentials, encodedCredentials) {
			a.granted[sessionID] = p.EncodedPrincipal
			return
		}
	}
}

// HandleChallengeResponse is a silent no-op. SimpleAuthenticator never
// challenges.
func (a *SimpleAuthenticator) HandleChallengeResponse(int64, []byte, int64) {}

// HandleConnectedSession authenticates a previously granted principal. Rejects
// when no grant exists. Always deletes the grant map entry after Authenticate.
func (a *SimpleAuthenticator) HandleConnectedSession(proxy SessionProxy, _ int64) {
	sessionID := proxy.SessionID()
	principal, ok := a.granted[sessionID]
	if !ok {
		proxy.Reject()
		return
	}
	proxy.Authenticate(principal)
	delete(a.granted, sessionID)
}

// HandleChallengedSession rejects the session fail-closed. SimpleAuthenticator
// never challenges.
func (a *SimpleAuthenticator) HandleChallengedSession(proxy SessionProxy, _ int64) {
	proxy.Reject()
}

var _ Authenticator = (*SimpleAuthenticator)(nil)
