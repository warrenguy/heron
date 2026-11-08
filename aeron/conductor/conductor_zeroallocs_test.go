package conductor_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron/conductor"
	"luas.fast/internal/testutil"
)

func TestConductor_DoWork_ZeroAllocs(t *testing.T) {
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, func(error) {}, fixedClocks{nanoNs: 0}),
	)

	testutil.AssertZeroAllocs(t, "DoWork", 1000, func() {
		conductorInstance.DoWork()
	})
}

func TestConductor_DoWork_ZeroAllocsAfterKeepaliveTick(t *testing.T) {
	var nanoNs int64
	epochMs := int64(1_700_000_000_123)

	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(t, func(error) {}, epochMs, &nanoNs),
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)
	nanoNs = (600 * time.Millisecond).Nanoseconds()

	require.Equal(t, 1, conductorInstance.DoWork())

	got, err := conductorInstance.CountersReaderForTest().CounterValue(0)
	require.NoError(t, err)
	require.Equal(t, epochMs, got)

	tickStep := conductorTestKeepAliveInterval.Nanoseconds() + 1

	testutil.AssertZeroAllocs(t, "DoWorkAfterKeepalive", 1000, func() {
		nanoNs += tickStep
		conductorInstance.DoWork()
	})
}

func TestConductor_DoWork_ZeroAllocsWithScheduledLinger(t *testing.T) {
	const startNs int64 = 14_000_000_000
	const lingerNs int64 = int64(time.Hour)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	*nanoNs = startNs

	dir := t.TempDir()
	logPath := filepath.Join(dir, "zero-alloc-linger.log")
	writeMinimalLogFileForRegistryTest(t, logPath)
	scheduleLingerForConductorLog(t, conductorInstance, logPath, lingerNs)

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.True(t, registry.HasScheduledLinger())

	testutil.AssertZeroAllocs(t, "DoWorkWithScheduledLinger", 1000, func() {
		conductorInstance.DoWork()
	})
}

func TestConductor_DoWork_ZeroAllocsWhenLingerExpires(t *testing.T) {
	const startNs int64 = 14_000_000_000
	const lingerNs int64 = int64(time.Millisecond)
	// The run count stays below the unmap queue length, so no unmap runs inline.
	const runs = 50

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)

	// AllocsPerRun calls the function once more than runs. Each log expires 1ns
	// after the previous log, so each DoWork sweeps one log.
	dir := t.TempDir()
	logPaths := make([]string, runs+1)
	for i := range logPaths {
		logPaths[i] = filepath.Join(dir, fmt.Sprintf("zero-alloc-expire-%d.log", i))
		writeMinimalLogFileForRegistryTest(t, logPaths[i])
		*nanoNs = startNs + int64(i)
		scheduleLingerForConductorLog(t, conductorInstance, logPaths[i], lingerNs)
	}
	*nanoNs = startNs + lingerNs - 1

	testutil.AssertZeroAllocs(t, "DoWorkWhenLingerExpires", runs, func() {
		*nanoNs++
		conductorInstance.DoWork()
	})

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.False(t, registry.HasScheduledLinger())
	for _, logPath := range logPaths {
		require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	}
}
