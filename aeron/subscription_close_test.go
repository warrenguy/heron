package aeron

import (
	"testing"

	"github.com/warrenguy/heron/aeron/conductor"
)

func TestSubscription_Close_withBoundConductor(t *testing.T) {
	fixture := newConductorResourceLifecycleFixture(t)
	sub := newTestSubscriptionBoundToConductor(t, fixture)

	requireCloseEnqueuesRemoveOnce(
		t,
		fixture,
		conductor.RemoveSubscription,
		sub.Close,
		sub.IsClosed,
	)
}
