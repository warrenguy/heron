//go:build testhooks

package client

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

func (ac *AsyncConnect) pollAddEgressSubscription(
	registrationID int64,
) (*aeron.Subscription, bool, error) {
	hooks := asyncConnectEgressHooksFor(ac)
	if hooks != nil && hooks.pollAddEgressSubscription != nil {
		return hooks.pollAddEgressSubscription(ac.client, registrationID)
	}
	return ac.client.PollAddSubscription(registrationID)
}

func (ac *AsyncConnect) shouldRetryEgressSubscription() bool {
	hooks := asyncConnectEgressHooksFor(ac)
	if hooks != nil && hooks.egressSubscriptionRetryNotReady != nil {
		return hooks.egressSubscriptionRetryNotReady()
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

func firstConnectedIngressPublicationForTest(memberMap MemberIngressMap) IngressPublication {
	return (&AsyncConnect{memberMap: memberMap}).firstConnectedIngressPublication()
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

func (ac *AsyncConnect) runPrepareForTest(fn func(*AsyncConnect) error) error {
	if ac == nil {
		return errors.New("cluster client: nil async connect")
	}
	return fn(ac)
}

// PrepareConnectRequestForTest exercises connect wire encoding during tests.
func (ac *AsyncConnect) PrepareConnectRequestForTest() error {
	return ac.runPrepareForTest((*AsyncConnect).prepareConnectRequest)
}

// PrepareChallengeResponseForTest exercises challenge wire encoding during tests.
func (ac *AsyncConnect) PrepareChallengeResponseForTest() error {
	return ac.runPrepareForTest((*AsyncConnect).prepareChallengeResponse)
}

// ConnectWireLengthForTest exposes the encoded connect wire length during tests.
func (ac *AsyncConnect) ConnectWireLengthForTest() int {
	if ac == nil {
		return 0
	}
	return ac.connectWireLength
}

// ConnectWireBufferForTest exposes the connect wire buffer during tests.
func (ac *AsyncConnect) ConnectWireBufferForTest() []byte {
	if ac == nil {
		return nil
	}
	return ac.connectWireBuffer
}

// PollSendMessageForTest exercises pollSendMessage during connect tests.
func (ac *AsyncConnect) PollSendMessageForTest() (*ConnectResult, error) {
	return ac.pollSendMessage()
}
