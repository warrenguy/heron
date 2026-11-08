//go:build !testhooks

package archive

import "luas.fast/aeron"

func (ac *AsyncConnect) pollAddControlResponseSubscription(
	registrationID int64,
) (*aeron.Subscription, bool, error) {
	return ac.client.PollAddSubscription(registrationID)
}

func (ac *AsyncConnect) pollAddControlRequestPublication(
	registrationID int64,
) (ControlPublication, bool, error) {
	return ac.client.PollAddExclusivePublication(registrationID)
}

func (ac *AsyncConnect) shouldRetryControlResponseSubscription() bool {
	// Retry-not-ready is test-only until transport exposes Java
	// RESOURCE_TEMPORARILY_UNAVAILABLE on async subscription poll.
	return false
}
