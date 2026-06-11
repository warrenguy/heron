package aeron

import (
	"testing"

	"github.com/warrenguy/heron/aeron/conductor"
)

func TestPublication_Close_withBoundConductor(t *testing.T) {
	fixture := newConductorResourceLifecycleFixture(t)
	pub := newTestPublicationBoundToConductor(t, fixture)

	requireCloseEnqueuesRemoveOnce(
		t,
		fixture,
		conductor.RemovePublication,
		pub.Close,
		pub.IsClosed,
	)
}
