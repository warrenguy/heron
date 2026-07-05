package client

import (
	"fmt"

	"github.com/warrenguy/heron/aeron"
)

// SessionState mirrors Java AeronCluster.State leadership-recovery values.
type SessionState int

const (
	SessionStateConnected SessionState = iota
	SessionStateAwaitNewLeader
	SessionStateAwaitNewLeaderConnection
	SessionStatePendingClose
)

var sessionStateNames = [...]string{
	"CONNECTED",
	"AWAIT_NEW_LEADER",
	"AWAIT_NEW_LEADER_CONNECTION",
	"PENDING_CLOSE",
}

// String returns the Java AeronCluster state name for diagnostics.
func (state SessionState) String() string {
	if state < 0 || int(state) >= len(sessionStateNames) {
		return fmt.Sprintf("SessionState(%d)", state)
	}
	return sessionStateNames[state]
}

// SessionState returns the current leadership-recovery session state.
//
// Nil receiver returns SessionStateConnected (no session constructed). Use
// IsClosed() for terminal lifecycle, not SessionState() alone. PENDING_CLOSE is
// a pending terminal state: ingress MaxPositionExceeded or a session Closed event may
// set it while IsClosed() is still false until PollEgress runs pollStateChanges and
// completes Close(). After Close, sessionState is not updated; SessionState() may
// retain any pre-close value (e.g. CONNECTED or PENDING_CLOSE).
func (ac *AeronCluster) SessionState() SessionState {
	if ac == nil {
		return SessionStateConnected
	}
	return ac.sessionState
}

func sessionPollsTransportDisconnect(state SessionState) bool {
	return state == SessionStateConnected || state == SessionStateAwaitNewLeaderConnection
}

func (ac *AeronCluster) sessionTerminallyClosing() bool {
	return ac.closed || ac.sessionState == SessionStatePendingClose
}

// enterAwaitNewLeaderConnection sets AWAIT_NEW_LEADER_CONNECTION with a
// MessageTimeout deadline.
func (ac *AeronCluster) enterAwaitNewLeaderConnection() {
	ac.sessionState = SessionStateAwaitNewLeaderConnection
	ac.stateDeadlineNs = ac.nanoClock() + ac.ctx.MessageTimeout.Nanoseconds()
}

// handleDisconnected clears ingressPublication, closes the leader member-map
// publication when present, and enters AWAIT_NEW_LEADER with a NewLeaderTimeout
// deadline. Mirrors Java AeronCluster.onDisconnected.
func (ac *AeronCluster) handleDisconnected() {
	if ac.closed {
		return
	}
	if ac.ingressPublication != nil {
		_ = ac.ingressPublication.Close()
		ac.ingressPublication = nil
	}
	ingressMemberID := leaderIngressMemberID(ac.memberMap, ac.leaderMemberID)
	if leader, ok := ac.memberMap.Lookup(ingressMemberID); ok {
		leader.closePublication(ac.aeron)
	}
	ac.enterAwaitNewLeader()
}

// enterAwaitNewLeader sets AWAIT_NEW_LEADER with a NewLeaderTimeout deadline.
func (ac *AeronCluster) enterAwaitNewLeader() {
	ac.sessionState = SessionStateAwaitNewLeader
	ac.stateDeadlineNs = ac.nanoClock() + ac.ctx.NewLeaderTimeout.Nanoseconds()
}

// enterPendingClose sets PENDING_CLOSE with no deadline; the next pollStateChanges
// call closes it without consulting stateDeadlineNs.
func (ac *AeronCluster) enterPendingClose() {
	ac.sessionState = SessionStatePendingClose
	ac.stateDeadlineNs = 0
}

// enterConnected returns to CONNECTED and clears the recovery deadline.
func (ac *AeronCluster) enterConnected() {
	ac.sessionState = SessionStateConnected
	ac.stateDeadlineNs = 0
}

// pollStateChanges applies timeout and pending-close transitions. stateDeadlineNs==0
// skips AWAIT_* timeout (PENDING_CLOSE uses zero deadline intentionally); production
// paths arm deadlines via enterAwaitNewLeader and applyLeaderIngressRebuild.
// PENDING_CLOSE and expired AWAIT_* deadlines call Close(); mirrors Java pollStateChanges.
func (ac *AeronCluster) pollStateChanges() int {
	if ac.closed {
		return 0
	}
	switch ac.sessionState {
	case SessionStatePendingClose:
	case SessionStateAwaitNewLeader, SessionStateAwaitNewLeaderConnection:
		if ac.stateDeadlineNs == 0 || ac.nanoClock() < ac.stateDeadlineNs {
			return 0
		}
	default:
		return 0
	}
	_ = ac.Close()
	return 1
}

// pollTransportDisconnect runs from PollEgress while transport health is polled.
//
// Active only in CONNECTED and AWAIT_NEW_LEADER_CONNECTION. When ingressPublication
// is disconnected, CONNECTED calls trackIngressPublicationResult(NotConnected)
// (-> handleDisconnected); recovery calls handleDisconnected directly. When
// ingressPublication is nil during AWAIT_NEW_LEADER_CONNECTION, a closed leader
// publication in the member map also enters AWAIT_NEW_LEADER. A closed tracked
// egress image without replacement enters AWAIT_NEW_LEADER. Re-checks
// sessionPollsTransportDisconnect after ingress handling may change state.
func (ac *AeronCluster) pollTransportDisconnect(trackedImageClosed bool) int {
	if !sessionPollsTransportDisconnect(ac.sessionState) {
		return 0
	}
	work := 0
	if ac.ingressPublication != nil && !ac.ingressPublication.IsConnected() {
		if ac.sessionState == SessionStateConnected {
			ac.trackIngressPublicationResult(aeron.NotConnected)
		} else {
			ac.handleDisconnected()
		}
		work++
	} else if ac.recoveryLeaderIngressDisconnected() {
		ac.handleDisconnected()
		work++
	}
	// Closed tracked image with no open replacement: Java onDisconnected. When
	// tryAttachEgressImage binds another subscription image, session stays CONNECTED.
	if trackedImageClosed && ac.egressImage == nil &&
		sessionPollsTransportDisconnect(ac.sessionState) {
		ac.handleDisconnected()
		work++
	}
	return work
}
