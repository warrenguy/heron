package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

// leaderIngressMemberID returns the member map key for leader ingress operations.
// Channel-only maps always use synthetic member 0 regardless of wire LeaderMemberID.
func leaderIngressMemberID(m MemberIngressMap, leaderMemberID int32) int32 {
	if m.IsChannelOnly() {
		return channelOnlyMemberID
	}
	return leaderMemberID
}

type memberIngressMapPlan struct {
	newMap          MemberIngressMap
	newLeader       *MemberIngress
	ingressMemberID int32
	reusedPub       IngressPublication
}

// planMemberIngressMapRebuild parses endpoints and validates the rebuild without
// touching oldMap. Safe to call from handleNewLeader pre-validate.
func planMemberIngressMapRebuild(
	oldMap MemberIngressMap,
	leaderMemberID int32,
	endpoints []byte,
) (memberIngressMapPlan, error) {
	newMap, err := ParseIngressEndpointsBytes(endpoints)
	if err != nil {
		return memberIngressMapPlan{}, err
	}
	newMap = EnsureChannelOnlyMemberIngressMap(newMap)

	ingressMemberID := leaderIngressMemberID(newMap, leaderMemberID)
	oldIngressMemberID := leaderIngressMemberID(oldMap, leaderMemberID)

	oldLeader, _ := oldMap.Lookup(oldIngressMemberID)

	newLeader, err := newMap.Require(ingressMemberID)
	if err != nil {
		return memberIngressMapPlan{}, err
	}

	plan := memberIngressMapPlan{
		newMap:          newMap,
		newLeader:       newLeader,
		ingressMemberID: ingressMemberID,
	}
	if oldLeader != nil {
		plan.reusedPub = oldLeader.reusablePublicationForEndpoint(newLeader.Endpoint)
	}
	return plan, nil
}

// applyMemberIngressMapRebuild executes a validated plan: closes non-reused oldMap
// pubs, assigns newMap, wires reused leader pub or starts asyncOfferAdd for new leader.
func applyMemberIngressMapRebuild(
	client *aeron.Client,
	ctx ClusterContext,
	oldMap MemberIngressMap,
	plan memberIngressMapPlan,
) (newMap MemberIngressMap, leaderPub IngressPublication, err error) {
	for memberID, ingress := range oldMap {
		if ingress == nil {
			continue
		}
		if plan.reusedPub != nil && memberID == plan.ingressMemberID {
			ingress.RegistrationID = conductor.NullCorrelationID
			ingress.Publication = nil
			ingress.PublicationErr = nil
			continue
		}
		ingress.closePublication(client)
	}

	newMap = plan.newMap
	leader := plan.newLeader
	if plan.reusedPub != nil {
		leader.Publication = plan.reusedPub
		return newMap, plan.reusedPub, nil
	}
	if err := leader.asyncOfferAdd(ctx, client, newMap); err != nil {
		return newMap, nil, err
	}
	return newMap, nil, nil
}
