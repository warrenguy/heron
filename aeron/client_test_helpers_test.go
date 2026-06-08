package aeron_test

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func newClientTestConductor(
	tb testutil.TB,
	nanoNs *int64,
	errorHandler func(error),
) *conductor.Conductor {
	tb.Helper()

	config := testutil.DefaultConductorConfig(nanoNs)
	config.ErrorHandler = errorHandler
	return testutil.NewConductorFixture(tb, config).Conductor
}

func newRapidTestClient(tb testutil.TB, clientID int64) *aeron.Client {
	tb.Helper()

	nanoNs := testutil.TestStartNanoNs
	fixture := testutil.NewConductorFixture(tb, testutil.DefaultConductorConfig(&nanoNs))
	return aeron.NewClientForTest(
		fixture.Conductor,
		func() int64 { return nanoNs },
		clientID,
	)
}
