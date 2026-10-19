//go:build testhooks

package consensus

// OpenSessionLogPositionForTest returns the log position at which the open
// session sessionID opened. ok is false when no open session holds the id.
func (a *Agent) OpenSessionLogPositionForTest(sessionID int64) (logPosition int64, ok bool) {
	session, found := a.sessions.sessionByID(sessionID)
	if !found || session.state != sessionOpen {
		return 0, false
	}
	return session.openedLogPosition, true
}

// NextSessionIDForTest returns the next cluster session id the registry
// allocates.
func (a *Agent) NextSessionIDForTest() int64 {
	return a.sessions.nextSessionID
}
