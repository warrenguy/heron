//go:build testhooks

package client

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

// Global testhook overrides; not safe for use with t.Parallel.
//
// pollAddEgressSubscriptionHook overrides egress PollAddSubscription in tests.
// Nil uses the default client method.
var pollAddEgressSubscriptionHook func(
	client *aeron.Client,
	registrationID int64,
) (*aeron.Subscription, bool, error)

// egressSubscriptionRetryNotReadyHook reports Java RESOURCE_TEMPORARILY_UNAVAILABLE
// for the current egress poll when true.
var egressSubscriptionRetryNotReadyHook func() bool

// ownedClientClosedOnMemberMapErrorHook is invoked after an owned transport client
// is closed due to member-map construction failure in NewAsyncConnect.
var ownedClientClosedOnMemberMapErrorHook func(client *aeron.Client)

// newOwnedConnectClientHook overrides owned-client creation in concludeClusterContext.
var newOwnedConnectClientHook func(ctx ClusterContext) (*aeron.Client, error)

func notifyOwnedClientClosedOnMemberMapError(client *aeron.Client) {
	if hook := ownedClientClosedOnMemberMapErrorHook; hook != nil {
		hook(client)
	}
}

func (ac *AsyncConnect) pollAddEgressSubscription(
	registrationID int64,
) (*aeron.Subscription, bool, error) {
	if pollAddEgressSubscriptionHook != nil {
		return pollAddEgressSubscriptionHook(ac.client, registrationID)
	}
	return ac.client.PollAddSubscription(registrationID)
}

func (ac *AsyncConnect) shouldRetryEgressSubscription() bool {
	if egressSubscriptionRetryNotReadyHook != nil {
		return egressSubscriptionRetryNotReadyHook()
	}
	return false
}

// CorrelationIDForTest exposes the active connect correlation id during connect tests.
func (ac *AsyncConnect) CorrelationIDForTest() int64 {
	if ac == nil {
		return conductor.NullCorrelationID
	}
	return ac.correlationID
}

// EgressSubscriptionForTest exposes the egress subscription during connect tests.
func (ac *AsyncConnect) EgressSubscriptionForTest() *aeron.Subscription {
	if ac == nil {
		return nil
	}
	return ac.egressSubscription
}

// ClusterContextForTest exposes the concluded cluster context during connect tests.
func (ac *AsyncConnect) ClusterContextForTest() ClusterContext {
	if ac == nil {
		return ClusterContext{}
	}
	return ac.ctx
}

func firstConnectedIngressPublicationForTest(
	memberMap MemberIngressMap,
) IngressPublication {
	ac := &AsyncConnect{memberMap: memberMap}
	return ac.firstConnectedIngressPublication()
}

// EgressRegIDForTest exposes the egress async add registration id during connect.
func (ac *AsyncConnect) EgressRegIDForTest() int64 {
	if ac == nil {
		return conductor.NullCorrelationID
	}
	return ac.egressRegID
}

func (ac *AsyncConnect) memberIngressForTest(memberID int32) (*MemberIngress, bool) {
	if ac == nil {
		return nil, false
	}
	return ac.memberMap.Lookup(memberID)
}

// MemberIngressRegIDForTest exposes a member async add registration id during connect.
func (ac *AsyncConnect) MemberIngressRegIDForTest(memberID int32) int64 {
	ingress, ok := ac.memberIngressForTest(memberID)
	if !ok {
		return conductor.NullCorrelationID
	}
	return ingress.RegistrationID
}

// SingleIngressRegIDForTest exposes the channel-only ingress async add registration id.
func (ac *AsyncConnect) SingleIngressRegIDForTest() int64 {
	return ac.MemberIngressRegIDForTest(channelOnlyMemberID)
}

// MemberIngressPublicationForTest exposes a resolved member ingress publication.
func (ac *AsyncConnect) MemberIngressPublicationForTest(memberID int32) IngressPublication {
	ingress, ok := ac.memberIngressForTest(memberID)
	if !ok {
		return nil
	}
	return ingress.Publication
}

// PrepareConnectRequestForTest exercises connect wire encoding during tests.
func (ac *AsyncConnect) PrepareConnectRequestForTest() error {
	if ac == nil {
		return errors.New("cluster client: nil async connect")
	}
	return ac.prepareConnectRequest()
}

// PrepareChallengeResponseForTest exercises challenge wire encoding during tests.
func (ac *AsyncConnect) PrepareChallengeResponseForTest() error {
	if ac == nil {
		return errors.New("cluster client: nil async connect")
	}
	return ac.prepareChallengeResponse()
}

// ConnectWireLengthForTest exposes the encoded connect wire length during tests.
func (ac *AsyncConnect) ConnectWireLengthForTest() int {
	if ac == nil {
		return 0
	}
	return ac.connectWireLength
}
