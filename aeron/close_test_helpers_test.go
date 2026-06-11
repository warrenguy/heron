package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func newConductorResourceLifecycleFixture(tb testing.TB) *testutil.ConductorFixture {
	tb.Helper()

	nanoNs := int64(0)
	config := testutil.DefaultConductorConfig(&nanoNs)
	config.ResourceLifecycle = ConductorResourceLifecycle
	return testutil.NewConductorFixture(tb, config)
}

func newTestPublicationBoundToConductor(
	tb testing.TB,
	fixture *testutil.ConductorFixture,
) *Publication {
	tb.Helper()

	pub := NewTestPublication(tb, DefaultTestPublicationLogConfig(), int64(1)<<40)
	pub.conductor = fixture.Conductor
	conductor.RegisterPublicationForTest(fixture.Conductor, pub.RegistrationID(), pub)
	return pub
}

func newTestSubscriptionBoundToConductor(
	tb testing.TB,
	fixture *testutil.ConductorFixture,
) *Subscription {
	tb.Helper()

	sub := NewSubscriptionForTest(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	sub.conductor = fixture.Conductor
	conductor.RegisterSubscriptionForTest(fixture.Conductor, sub.RegistrationID(), sub)
	return sub
}

func requireCloseEnqueuesRemoveOnce(
	tb testing.TB,
	fixture *testutil.ConductorFixture,
	removeTypeID int,
	close func() error,
	isClosed func() bool,
) {
	tb.Helper()

	require.NoError(tb, close())
	require.True(tb, isClosed())
	require.Equal(tb, []int{removeTypeID}, testutil.CommittedRecordTypeIDs(fixture.Ring))

	require.NoError(tb, close())
	require.Equal(tb, []int{removeTypeID}, testutil.CommittedRecordTypeIDs(fixture.Ring))
}
