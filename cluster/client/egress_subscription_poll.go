package client

import "github.com/warrenguy/heron/aeron"

// pollEgressSubscription polls subscription with handler up to fragmentLimit fragments.
// Returns 0 when subscription is nil.
func pollEgressSubscription(
	subscription *aeron.Subscription,
	handler aeron.FragmentHandler,
	fragmentLimit int,
) int {
	if subscription == nil {
		return 0
	}
	return subscription.Poll(handler, fragmentLimit)
}
