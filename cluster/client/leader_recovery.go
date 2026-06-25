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

// recoveryLeaderIngressDisconnected reports a settled leader publication in the
// member map that is no longer connected during AWAIT_NEW_LEADER_CONNECTION.
func (ac *AeronCluster) recoveryLeaderIngressDisconnected() bool {
	if ac.sessionState != SessionStateAwaitNewLeaderConnection {
		return false
	}
	ingressMemberID := leaderIngressMemberID(ac.memberMap, ac.leaderMemberID)
	leader, err := ac.memberMap.Require(ingressMemberID)
	if err != nil {
		return false
	}
	pub := leader.Publication
	return pub != nil && !pub.IsConnected()
}

// leaderIngressForRecovery is the recovery ingress entrypoint during
// AWAIT_NEW_LEADER_CONNECTION. Bypasses app session guards and
// leaderIngressPublication; pollLeaderRecovery drives reconnect via
// offerSessionKeepAlive on the member-map leader publication.
func (ac *AeronCluster) leaderIngressForRecovery() IngressPublication {
	ingressMemberID := leaderIngressMemberID(ac.memberMap, ac.leaderMemberID)
	leader, err := ac.memberMap.Require(ingressMemberID)
	if err != nil {
		return nil
	}
	return leader.connectedPublication()
}

// pollLeaderRecovery advances async ingress registration and sends the first
// SessionKeepAlive on the new leader publication during AWAIT_NEW_LEADER_CONNECTION.
// One offerSessionKeepAlive attempt per PollEgress spin; BackPressured or
// AdminAction returns zero work so the next PollEgress spin retries after caller
// DoWork. Not public SendKeepAlive; recovery bypasses app session guards.
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

	pub := ac.leaderIngressForRecovery()
	if pub == nil {
		return 0
	}

	pos := ac.offerSessionKeepAlive(pub)
	if pos > 0 {
		ac.ingressPublication = pub
		ac.refreshIngressSessionLimits(pub)
	}
	// Count one recovery spin unless backpressured/admin-action retry.
	if pos != aeron.BackPressured && pos != aeron.AdminAction {
		return 1
	}
	return 0
}

// refreshIngressSessionLimits resizes ingress scratch for the connected leader pub.
// Called when the new leader ingress publication first connects during recovery.
func (ac *AeronCluster) refreshIngressSessionLimits(pub IngressPublication) {
	ac.ingressScratch, ac.ingressMaxFrameLen = ingressScratchForPub(pub, ac.ingressScratch)
}
