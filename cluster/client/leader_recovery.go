package client

import (
	"fmt"

	"github.com/warrenguy/heron/aeron"
)

// handleNewLeader rebuilds the member ingress map for a leadership change. Called from
// egress dispatch before user EgressHandler.NewLeader.
func (ac *AeronCluster) handleNewLeader(
	clusterSessionID int64,
	leadershipTermID int64,
	leaderMemberID int32,
	ingressEndpoints []byte,
	egressImage *aeron.Image,
) error {
	if clusterSessionID != ac.clusterSessionID {
		return fmt.Errorf(
			"cluster client: new leader cluster session id mismatch: got %d want %d",
			clusterSessionID,
			ac.clusterSessionID,
		)
	}
	return ac.applyLeaderIngressRebuild(
		leaderMemberID,
		ingressEndpoints,
		egressImage,
		leadershipTermID,
	)
}

// handleRedirect rebuilds the member ingress map for a live SessionEvent REDIRECT.
// Unlike handleNewLeader, leadershipTermID is not taken from the event; Java connect
// REDIRECT does not advance the term id on redirect detail alone.
func (ac *AeronCluster) handleRedirect(
	leaderMemberID int32,
	detail []byte,
	egressImage *aeron.Image,
) error {
	return ac.applyLeaderIngressRebuild(leaderMemberID, detail, egressImage, ac.leadershipTermID)
}

// applyLeaderIngressRebuild is the shared apply path for handleNewLeader and handleRedirect.
// leadershipTermID is supplied by the caller: NewLeaderEvent value for handleNewLeader,
// current session term for handleRedirect. Session ids and term are updated before apply
// so metadata matches Java onNewLeader ordering even when apply fails after pub close.
// Sync apply failure leaves AWAIT_NEW_LEADER with updated term/leader and swapped map.
func (ac *AeronCluster) applyLeaderIngressRebuild(
	leaderMemberID int32,
	endpoints []byte,
	egressImage *aeron.Image,
	leadershipTermID int64,
) error {
	if ac.sessionTerminallyClosing() {
		return nil
	}

	plan, err := planMemberIngressMapRebuild(ac.memberMap, leaderMemberID, endpoints)
	if err != nil {
		return err
	}

	ac.enterAwaitNewLeaderConnection()
	ac.leadershipTermID = leadershipTermID
	ac.leaderMemberID = leaderMemberID

	ac.ingressPublication = nil

	newMap, _, err := applyMemberIngressMapRebuild(ac.aeron, ac.ctx, ac.memberMap, plan)
	ac.memberMap = newMap
	if err != nil {
		ac.enterAwaitNewLeader()
	} else {
		if ac.egressAdapter != nil {
			ac.egressAdapter.Clear()
		}
		if egressImage != nil && !egressImage.IsClosed() {
			ac.egressImage = egressImage
		}
	}
	return err
}

// recoveryLeaderIngress returns the member-map leader ingress entry for
// AWAIT_NEW_LEADER_CONNECTION recovery callers.
func (ac *AeronCluster) recoveryLeaderIngress() (*MemberIngress, bool) {
	ingressMemberID := leaderIngressMemberID(ac.memberMap, ac.leaderMemberID)
	leader, err := ac.memberMap.Require(ingressMemberID)
	if err != nil {
		return nil, false
	}
	return leader, true
}

// Publication vs connectedPublication asymmetry during AWAIT_NEW_LEADER_CONNECTION:
// pollLeaderRecovery offers keep-alives only via connectedPublication() (open and
// connected).
//
// recoveryLeaderIngressDisconnected reports a closed leader publication in the
// member map during AWAIT_NEW_LEADER_CONNECTION while ingressPublication is still
// nil. !IsConnected during first UDP connect is normal; pollLeaderRecovery waits
// until MessageTimeout. Closed leader pubs mirror Java onDisconnected.
func (ac *AeronCluster) recoveryLeaderIngressDisconnected() bool {
	if ac.sessionState != SessionStateAwaitNewLeaderConnection || ac.ingressPublication != nil {
		return false
	}
	leader, ok := ac.recoveryLeaderIngress()
	if !ok {
		return false
	}
	pub := leader.Publication
	return pub != nil && pub.IsClosed()
}

// pollLeaderRecovery advances async ingress registration and sends the first
// SessionKeepAlive on the new leader publication during AWAIT_NEW_LEADER_CONNECTION.
//
// Returns 0 (no PollEgress work) when:
//   - session state is not AWAIT_NEW_LEADER_CONNECTION;
//   - recoveryLeaderIngress lookup fails;
//   - leader.connectedPublication() is nil (pub missing, closed, or not connected);
//   - offerSessionKeepAlive returns a retriable code (BackPressured, AdminAction);
//   - failIfIngressUnrecoverable reports an error and stateDeadlineNs has expired
//     (pollStateChanges closes the session on the same PollEgress spin).
//
// Returns 1 (counts as PollEgress work) when:
//   - failIfIngressUnrecoverable reports an error before the deadline and enters
//     AWAIT_NEW_LEADER;
//   - offerSessionKeepAlive returns a non-retriable code, including success (>0),
//     Closed, and NotConnected (Closed/NotConnected do not alter state here).
//
// Cross-spin callers must invoke client.DoWork before each PollEgress retry when
// keep-alive returns retriable codes.
//
// Recovery bypasses app session guards; not public SendKeepAlive.
func (ac *AeronCluster) pollLeaderRecovery() int {
	if ac.sessionState != SessionStateAwaitNewLeaderConnection {
		return 0
	}
	pollPendingMemberIngressRegistration(ac.memberMap, ac.ctx, ac.aeron)
	if err := ac.memberMap.failIfIngressUnrecoverable(); err != nil {
		if ac.stateDeadlineNs != 0 && ac.nanoClock() >= ac.stateDeadlineNs {
			return 0 // pollStateChanges closes on expired MessageTimeout
		}
		ac.enterAwaitNewLeader()
		return 1
	}

	leader, ok := ac.recoveryLeaderIngress()
	if !ok {
		return 0
	}
	pub := leader.connectedPublication()
	if pub == nil {
		return 0
	}

	pos := ac.offerSessionKeepAlive(pub)
	if pos > 0 {
		ac.ingressPublication = pub
		ac.refreshIngressSessionLimits(pub)
	}
	if IngressRetriableCode(pos) {
		return 0
	}
	return 1
}

// refreshIngressSessionLimits resizes ingress scratch for the connected leader pub.
// Called when the new leader ingress publication first connects during recovery.
func (ac *AeronCluster) refreshIngressSessionLimits(pub IngressPublication) {
	ac.ingressScratch, ac.ingressMaxFrameLen = ingressScratchForPub(pub, ac.ingressScratch)
}
