//go:build rapid

package conductor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
)

func drawUnpublishedDriverHeartbeat(t *rapid.T) int64 {
	if rapid.Bool().Draw(t, "useZeroHeartbeat") {
		return 0
	}
	return int64(rapid.IntRange(-10_000, -2).Draw(t, "negativeHeartbeat"))
}

func drawWorkNanoWithinKeepAliveGate(t *rapid.T) int64 {
	minNs := conductorTestIdleSleepDuration.Nanoseconds() + 1
	maxNs := conductorTestKeepAliveInterval.Nanoseconds() - 1
	return int64(rapid.Int64Range(minNs, maxNs).Draw(t, "workNanoNs"))
}

func TestConductor_driverTimeoutBoundaryAlive_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		driverHeartbeatMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "driverHeartbeatMs"))
		timeoutMs := conductorTestDriverTimeout.Milliseconds()
		epochMs := driverHeartbeatMs + timeoutMs

		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			epochMs,
			conductorTestWorkNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
	})
}

func TestConductor_driverTimeoutBoundaryStale_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		driverHeartbeatMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "driverHeartbeatMs"))
		timeoutMs := conductorTestDriverTimeout.Milliseconds()
		epochMs := driverHeartbeatMs + timeoutMs + 1

		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			epochMs,
			conductorTestWorkNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.True(t, conductorInstance.IsTerminating())
		require.Len(t, *gotErrors, 1)

		var staleErr *conductor.DriverKeepaliveStaleError
		require.ErrorAs(t, (*gotErrors)[0], &staleErr)
		require.Equal(t, timeoutMs+1, staleErr.AgeMs)
		require.Equal(t, timeoutMs, staleErr.TimeoutMs)
	})
}

func TestConductor_unpublishedDriverHeartbeatNotStale_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		heartbeatMs := drawUnpublishedDriverHeartbeat(rt)
		epochMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "epochMs"))

		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			epochMs,
			conductorTestWorkNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(heartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
	})
}

func TestConductor_keepAliveIntervalGating_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		driverHeartbeatMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "driverHeartbeatMs"))
		timeoutMs := conductorTestDriverTimeout.Milliseconds()
		extraAgeMs := int64(rapid.IntRange(1, 60_000).Draw(rt, "extraAgeMs"))
		epochMs := driverHeartbeatMs + timeoutMs + extraAgeMs
		workNanoNs := drawWorkNanoWithinKeepAliveGate(rt)

		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			epochMs,
			workNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		require.Zero(t, conductorInstance.DoWork())
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
		require.Zero(t, conductorInstance.TimeOfLastKeepAliveNsForTest())
	})
}

func TestConductor_serviceIntervalBoundaryAlive_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		epochMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "epochMs"))
		timeoutNs := conductorTestInterServiceTimeout.Nanoseconds()
		minNs := conductorTestIdleSleepDuration.Nanoseconds() + 1
		workNanoNs := int64(rapid.Int64Range(minNs, timeoutNs).Draw(rt, "workNanoNs"))

		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			epochMs,
			workNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(epochMs)

		conductorInstance.DoWork()
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
	})
}

func TestConductor_serviceIntervalBoundaryStale_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		epochMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "epochMs"))
		timeoutNs := conductorTestInterServiceTimeout.Nanoseconds()
		extraNs := int64(rapid.Int64Range(1, int64(time.Minute)).Draw(rt, "extraNs"))
		workNanoNs := timeoutNs + extraNs

		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			errorHandler,
			epochMs,
			workNanoNs,
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(epochMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.True(t, conductorInstance.IsTerminating())
		require.Len(t, *gotErrors, 1)
		require.ErrorIs(t, (*gotErrors)[0], conductor.ErrConductorServiceTimeout)
	})
}

func TestConductor_serviceIntervalSeedingAvoidsFalseTimeout_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		epochMs := int64(rapid.IntRange(1, 1_000_000_000).Draw(rt, "epochMs"))
		timeoutNs := conductorTestInterServiceTimeout.Nanoseconds()
		extraNs := int64(rapid.Int64Range(1, int64(time.Minute)).Draw(rt, "extraNs"))
		workNanoNs := timeoutNs + extraNs

		var nanoNs int64
		errorHandler, gotErrors := collectConductorErrors()
		conductorInstance := newConductorForTest(
			t,
			conductorTestConfigMutableNano(t, errorHandler, epochMs, &nanoNs),
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(epochMs)
		nanoNs = workNanoNs
		conductorInstance.SeedServiceTimestamps(nanoNs)

		conductorInstance.DoWork()
		require.False(t, conductorInstance.IsTerminating())
		require.Empty(t, *gotErrors)
	})
}
