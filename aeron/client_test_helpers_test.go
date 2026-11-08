package aeron_test

import (
	"luas.fast/aeron"
	"luas.fast/aeron/conductor"
	"luas.fast/aeron/internal/testutil"
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
