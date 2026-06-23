//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ac *AsyncConnect) pollAddEgressSubscription(
	registrationID int64,
) (*aeron.Subscription, bool, error) {
	return ac.client.PollAddSubscription(registrationID)
}

func (ac *AsyncConnect) shouldRetryEgressSubscription() bool {
	// Retry-not-ready is test-only until transport exposes Java
	// RESOURCE_TEMPORARILY_UNAVAILABLE on async subscription poll.
	return false
}
