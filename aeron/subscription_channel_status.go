package aeron

import (
	"errors"

	"github.com/warrenguy/heron/aeron/cnc"
)

var errSubscriptionCountersNil = errors.New("aeron: subscription counters view is nil")

// ChannelStatus reads the driver receive-channel-status counter.
//
// Subscriptions without a channel-status counter report ChannelEndpointStatusActive.
func (subscription *Subscription) ChannelStatus() (int64, error) {
	if subscription.IsClosed() {
		return 0, errors.New("aeron: subscription is closed")
	}
	if subscription.channelStatusCounterID == cnc.NullCounterID {
		return ChannelEndpointStatusActive, nil
	}
	if subscription.counters == nil {
		return 0, errSubscriptionCountersNil
	}
	return subscription.counters.CounterValue(subscription.channelStatusCounterID)
}

// TryResolveChannelEndpointPort returns the wire channel URI with an ephemeral
// endpoint=:0 port resolved from the local socket address counter.
//
// Channels without endpoint=:0 return the original URI. Returns ("", false) when
// wildcard resolution is required but the receive endpoint is not yet active.
// MDC control-mode=manual is not checked here (deferred; cluster egress does not
// use manual MDC).
func (subscription *Subscription) TryResolveChannelEndpointPort() (string, bool) {
	if subscription == nil || subscription.IsClosed() {
		return "", false
	}

	channel := subscription.Channel()
	if !ChannelEndpointNeedsWildcardPortResolution(channel) {
		return channel, true
	}

	if status, err := subscription.ChannelStatus(); err != nil ||
		status != cnc.ChannelEndpointStatusActive {
		return "", false
	}
	if subscription.counters == nil ||
		subscription.channelStatusCounterID == cnc.NullCounterID {
		return "", false
	}

	endpoint, ok := subscription.counters.FindLocalSocketAddress(
		cnc.ChannelEndpointStatusActive,
		subscription.channelStatusCounterID,
	)
	if !ok {
		return "", false
	}
	return ReplaceChannelEndpoint(channel, endpoint), true
}
