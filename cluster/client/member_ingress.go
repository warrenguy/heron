package client

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

// MemberIngress holds one cluster member's ingress endpoint and async connect
// registration state.
//
// Not safe for concurrent use.
type MemberIngress struct {
	MemberID int32
	Endpoint string

	// Async connect registration state. RegistrationID is conductor.NullCorrelationID
	// when idle.
	RegistrationID int64
	Publication    IngressPublication
	PublicationErr error
}

// channelOnlyMemberID is the synthetic member id for connect when IngressEndpoints
// is unset. The entry uses IngressChannel directly (empty Endpoint).
const channelOnlyMemberID int32 = 0

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
	if ingress.Endpoint == "" {
		return ctx.IngressChannel, nil
	}
	return BuildIngressChannel(ctx.IngressChannel, ingress.Endpoint)
}

// IsChannelOnly reports whether m is the single synthetic entry used when
// IngressEndpoints is unset at connect.
func (m MemberIngressMap) IsChannelOnly() bool {
	if len(m) != 1 {
		return false
	}
	ingress, ok := m.Lookup(channelOnlyMemberID)
	return ok && ingress.Endpoint == ""
}

// EnsureChannelOnly returns a one-entry map for unset IngressEndpoints.
func EnsureChannelOnlyMemberIngressMap(m MemberIngressMap) MemberIngressMap {
	if len(m) > 0 {
		return m
	}
	return MemberIngressMap{
		channelOnlyMemberID: &MemberIngress{
			MemberID:       channelOnlyMemberID,
			RegistrationID: conductor.NullCorrelationID,
		},
	}
}

// ingressExclusive reports whether connect should use exclusive ingress publications.
// Nil IngressExclusive defaults to true (Java isIngressExclusive default).
func ingressExclusive(ctx ClusterContext) bool {
	return ctx.IngressExclusive == nil || *ctx.IngressExclusive
}

// asyncOfferAdd resolves the per-member channel via ChannelFor on memberMap rather
// than a pre-built ingressChannel argument.
func (ingress *MemberIngress) asyncOfferAdd(
	ctx ClusterContext,
	client *aeron.Client,
	memberMap MemberIngressMap,
) error {
	if ingress == nil {
		return errors.New("cluster client: nil member ingress")
	}
	channel, err := memberMap.ChannelFor(ctx, ingress.MemberID)
	if err != nil {
		return err
	}
	return ingress.asyncOfferAddChannel(ctx, client, channel)
}

func (ingress *MemberIngress) asyncOfferAddChannel(
	ctx ClusterContext,
	client *aeron.Client,
	channel string,
) error {
	if ingress == nil {
		return errors.New("cluster client: nil member ingress")
	}
	if ingress.RegistrationID != conductor.NullCorrelationID {
		return nil
	}
	var (
		regID int64
		err   error
	)
	if ingressExclusive(ctx) {
		regID, err = client.OfferAddExclusivePublication(ctx.IngressStreamID, channel)
	} else {
		regID, err = client.OfferAddPublication(ctx.IngressStreamID, channel)
	}
	if err != nil {
		return err
	}
	ingress.RegistrationID = regID
	return nil
}

var memberIngressAsyncPollAddHook func(
	ingress *MemberIngress,
	ctx ClusterContext,
	client *aeron.Client,
) (bool, error)

func (ingress *MemberIngress) asyncPollAdd(
	ctx ClusterContext,
	client *aeron.Client,
) (bool, error) {
	if memberIngressAsyncPollAddHook != nil {
		return memberIngressAsyncPollAddHook(ingress, ctx, client)
	}
	if ingress == nil {
		return true, errors.New("cluster client: nil member ingress")
	}
	if ingress.Publication != nil || ingress.PublicationErr != nil {
		return true, ingress.PublicationErr
	}
	if ingress.RegistrationID == conductor.NullCorrelationID {
		return false, nil
	}
	var (
		pub  IngressPublication
		done bool
		err  error
	)
	if ingressExclusive(ctx) {
		var ep *aeron.ExclusivePublication
		ep, done, err = client.PollAddExclusivePublication(ingress.RegistrationID)
		pub = ep
	} else {
		var p *aeron.Publication
		p, done, err = client.PollAddPublication(ingress.RegistrationID)
		pub = p
	}
	if !done {
		return false, nil
	}
	ingress.RegistrationID = conductor.NullCorrelationID
	if err != nil {
		ingress.PublicationErr = err
		return true, err
	}
	ingress.Publication = pub
	return true, nil
}

func (ingress *MemberIngress) closePublication(client *aeron.Client) {
	if ingress == nil {
		return
	}
	if ingress.Publication != nil {
		_ = ingress.Publication.Close()
		ingress.Publication = nil
	}
	if client != nil && ingress.RegistrationID != conductor.NullCorrelationID {
		client.CancelPending(ingress.RegistrationID)
	}
	ingress.RegistrationID = conductor.NullCorrelationID
	ingress.PublicationErr = nil
}
