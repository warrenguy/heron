package client

import (
	"fmt"
)

// MemberIngress holds one cluster member's ingress endpoint. Publication registration
// fields are added with async connect wiring.
//
// Not safe for concurrent use.
type MemberIngress struct {
	MemberID int32
	Endpoint string
}

// MemberIngressMap maps cluster memberId to ingress state. Java endpointByIdMap /
// memberByIdMap parity.
//
// Built at connect and leadership recovery from parsed endpoints. Callers must
// replace the map on leader change rather than mutating entries across goroutines.
// Typical cardinality is 1-5 members; memory is O(members x endpoint string length).
type MemberIngressMap map[int32]*MemberIngress

// Lookup returns the ingress entry for memberID.
//
// Nil or empty map returns (nil, false).
func (m MemberIngressMap) Lookup(memberID int32) (*MemberIngress, bool) {
	ingress, ok := m[memberID]
	if !ok || ingress == nil {
		return nil, false
	}
	return ingress, true
}

// Require returns the ingress entry for the given cluster member id.
//
// Nil or empty map, or a member id not present in the map, returns an error.
func (m MemberIngressMap) Require(memberID int32) (*MemberIngress, error) {
	ingress, ok := m.Lookup(memberID)
	if !ok {
		return nil, fmt.Errorf("cluster client: member id %d not in ingress map", memberID)
	}
	return ingress, nil
}

// ChannelFor returns the Aeron ingress channel URI for offering to memberID.
//
// Delegates to Require, then BuildIngressChannel with ctx.IngressChannel and the
// member endpoint. Missing member ids return the Require error; channel substitution
// errors propagate from BuildIngressChannel.
func (m MemberIngressMap) ChannelFor(
	ctx ClusterContext,
	memberID int32,
) (string, error) {
	ingress, err := m.Require(memberID)
	if err != nil {
		return "", err
	}
	return BuildIngressChannel(ctx.IngressChannel, ingress.Endpoint)
}
