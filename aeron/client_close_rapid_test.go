package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestClient_firstDoWorkSeedsServiceTimestamps_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nanoNs := clientTestStartNanoNs
		interServiceTimeoutNs := clientTestInterServiceTimeout.Nanoseconds()
		advanceNs := int64(
			rapid.IntRange(0, int(interServiceTimeoutNs-1)).Draw(t, "advanceNs"),
		)

		conductorInstance := newClientTestConductor(t, &nanoNs, clientTestEpochMs, nil)
		nanoNs = clientTestStartNanoNs + advanceNs

		client := aeron.NewClientForTest(
			conductorInstance,
			func() int64 { return nanoNs },
		)
		client.DoWork()
		require.False(t, client.IsTerminating())
		require.False(t, conductorInstance.IsTerminating())
	})
}

func TestClient_conductorServiceTimeoutAfterInterval_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var gotErr error
		nanoNs := clientTestStartNanoNs
		interServiceTimeoutNs := clientTestInterServiceTimeout.Nanoseconds()
		advanceNs := int64(
			rapid.IntRange(
				int(interServiceTimeoutNs+1),
				int(3*interServiceTimeoutNs),
			).Draw(t, "advanceNs"),
		)

		conductorInstance := newClientTestConductor(
			t,
			&nanoNs,
			clientTestEpochMs,
			func(err error) { gotErr = err },
		)
		nanoNs = clientTestStartNanoNs + advanceNs

		require.Equal(t, 1, conductorInstance.DoWork())
		require.True(t, conductorInstance.IsTerminating())
		require.ErrorIs(t, gotErr, conductor.ErrConductorServiceTimeout)
		require.EqualError(t, gotErr, "conductor: service interval exceeded")
	})
}
