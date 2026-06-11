package conductor_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
)

const registryRapidBaseNowNs int64 = 1_000_000_000

type registryRapidOpKind int

const (
	registryRapidOpAcquire registryRapidOpKind = iota
	registryRapidOpRelease
	registryRapidOpSweep
)

var (
	registryRapidOpsNoRelease = []registryRapidOpKind{
		registryRapidOpAcquire,
		registryRapidOpSweep,
	}
	registryRapidOpsWithRelease = []registryRapidOpKind{
		registryRapidOpAcquire,
		registryRapidOpRelease,
		registryRapidOpSweep,
	}
)

type registryRapidModel struct {
	refCount         int32
	contains         bool
	lingerDeadlineNs int64
}

func drawRegistryRapidNowNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	offset := rapid.Int64Range(-500_000_000, 2_000_000_000).Draw(rt, label)
	return registryRapidBaseNowNs + offset
}

func drawRegistryRapidLingerNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	return rapid.Int64Range(-1_000_000_000, 1_000_000_000).Draw(rt, label)
}

func legalRegistryRapidOps(model registryRapidModel) []registryRapidOpKind {
	if model.refCount > 0 {
		return registryRapidOpsWithRelease
	}
	return registryRapidOpsNoRelease
}

func applyRegistryRapidRelease(
	model *registryRapidModel,
	nowNs int64,
	lingerNs int64,
) {
	if model.refCount > 1 {
		model.refCount--
		return
	}

	model.refCount = 0
	effectiveLingerNs := max(lingerNs, 0)
	if effectiveLingerNs == 0 {
		model.contains = false
		model.lingerDeadlineNs = 0
		return
	}

	model.contains = true
	model.lingerDeadlineNs = nowNs + effectiveLingerNs
}

func applyRegistryRapidAcquire(model *registryRapidModel) {
	if !model.contains {
		model.contains = true
		model.refCount = 1
		model.lingerDeadlineNs = 0
		return
	}

	model.refCount++
	model.lingerDeadlineNs = 0
}

func applyRegistryRapidSweep(model *registryRapidModel, nowNs int64) {
	if model.refCount != 0 || !model.contains || model.lingerDeadlineNs == 0 {
		return
	}
	if nowNs < model.lingerDeadlineNs {
		return
	}

	model.contains = false
	model.lingerDeadlineNs = 0
}

func observedRegistryRapidModel(
	registry *conductor.LogBuffersRegistry,
	logPath string,
) registryRapidModel {
	return registryRapidModel{
		refCount:         conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
		contains:         conductor.LogBuffersRegistryContainsForTest(registry, logPath),
		lingerDeadlineNs: conductor.LingerDeadlineNsForTest(registry, logPath),
	}
}

func assertRegistryRapidInvariants(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	model registryRapidModel,
) {
	rt.Helper()

	observed := observedRegistryRapidModel(registry, logPath)
	require.Equal(rt, model.refCount, observed.refCount)
	require.Equal(rt, model.contains, observed.contains)
	require.Equal(rt, model.lingerDeadlineNs, observed.lingerDeadlineNs)
}

type registryRapidStepTarget struct {
	registry   *conductor.LogBuffersRegistry
	logPath    string
	key        string
	model      *registryRapidModel
	liveMapped *any
}

func clearRegistryRapidLiveMappedIfAbsent(target registryRapidStepTarget) {
	if target.liveMapped != nil && !target.model.contains {
		*target.liveMapped = nil
	}
}

func runRegistryRapidStep(
	rt *rapid.T,
	target registryRapidStepTarget,
	op registryRapidOpKind,
	step int,
) {
	rt.Helper()

	switch op {
	case registryRapidOpAcquire:
		acquired, err := target.registry.Acquire(target.logPath)
		require.NoError(rt, err)
		require.NotNil(rt, acquired)

		if target.liveMapped != nil {
			if target.model.refCount > 0 && *target.liveMapped != nil {
				require.Same(rt, *target.liveMapped, acquired.MappedLogBuffer())
			} else {
				*target.liveMapped = acquired.MappedLogBuffer()
			}
		}

		applyRegistryRapidAcquire(target.model)

	case registryRapidOpRelease:
		require.Positive(rt, target.model.refCount)

		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("releaseNowOffset%d", step))
		lingerNs := drawRegistryRapidLingerNs(rt, fmt.Sprintf("lingerNs%d", step))
		conductor.ReleaseResolvedKeyAtForTest(target.registry, target.key, nowNs, lingerNs)
		applyRegistryRapidRelease(target.model, nowNs, lingerNs)
		clearRegistryRapidLiveMappedIfAbsent(target)

	case registryRapidOpSweep:
		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("sweepNowOffset%d", step))
		conductor.SweepLingeringLogBuffersForTest(target.registry, nowNs)
		applyRegistryRapidSweep(target.model, nowNs)
		clearRegistryRapidLiveMappedIfAbsent(target)
	}
}

func sweepAllRegistryRapidKeys(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	states []registryRapidKeyState,
	step int,
) {
	rt.Helper()

	nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("sweepNowOffset%d", step))
	conductor.SweepLingeringLogBuffersForTest(registry, nowNs)
	for i := range states {
		applyRegistryRapidSweep(&states[i].model, nowNs)
		assertRegistryRapidInvariants(rt, registry, states[i].logPath, states[i].model)
	}
}

func newRegistryRapidFixture(
	t *testing.T,
	rt *rapid.T,
) (*conductor.LogBuffersRegistry, string, string) {
	t.Helper()
	rt.Helper()

	registry, logPath := newRegistryWithLogFile(t, "rapid.log")
	key, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
	require.NoError(rt, err)
	return registry, logPath, key
}

func TestLogBuffersRegistry_acquireReleaseLinger_invariants_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		registry, logPath, key := newRegistryRapidFixture(t, rt)
		model := registryRapidModel{}

		var liveMapped any
		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			op := rapid.SampledFrom(legalRegistryRapidOps(model)).Draw(rt, fmt.Sprintf("op%d", step))
			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry:   registry,
				logPath:    logPath,
				key:        key,
				model:      &model,
				liveMapped: &liveMapped,
			}, op, step)
			assertRegistryRapidInvariants(rt, registry, logPath, model)
		}
	})
}

type registryRapidKeyState struct {
	logPath string
	key     string
	model   registryRapidModel
}

func assertOtherRegistryRapidKeysUnchanged(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	states []registryRapidKeyState,
	targetIndex int,
	before []registryRapidModel,
) {
	rt.Helper()

	for i, state := range states {
		if i == targetIndex {
			continue
		}
		assertRegistryRapidInvariants(rt, registry, state.logPath, before[i])
	}
}

func newMultiKeyRegistryRapidFixture(
	t *testing.T,
	rt *rapid.T,
	keyCount int,
) (*conductor.LogBuffersRegistry, []registryRapidKeyState) {
	t.Helper()
	rt.Helper()

	dir := t.TempDir()
	registry := conductor.NewLogBuffersRegistry(dir)
	states := make([]registryRapidKeyState, keyCount)
	for i := range keyCount {
		logPath := filepath.Join(dir, fmt.Sprintf("rapid-%d.log", i))
		writeMinimalLogFileForRegistryTest(t, logPath)
		key, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
		require.NoError(rt, err)
		states[i] = registryRapidKeyState{
			logPath: logPath,
			key:     key,
			model:   registryRapidModel{},
		}
	}
	return registry, states
}

func TestLogBuffersRegistry_multiKeyIsolation_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		keyCount := rapid.IntRange(2, 8).Draw(rt, "keyCount")
		registry, states := newMultiKeyRegistryRapidFixture(t, rt, keyCount)

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			targetIndex := rapid.IntRange(0, keyCount-1).Draw(rt, fmt.Sprintf("targetIndex%d", step))
			target := &states[targetIndex]
			op := rapid.SampledFrom(legalRegistryRapidOps(target.model)).Draw(rt, fmt.Sprintf("op%d", step))

			switch op {
			case registryRapidOpAcquire, registryRapidOpRelease:
				before := make([]registryRapidModel, len(states))
				for i, state := range states {
					before[i] = state.model
				}

				runRegistryRapidStep(rt, registryRapidStepTarget{
					registry: registry,
					logPath:  target.logPath,
					key:      target.key,
					model:    &target.model,
				}, op, step)

				assertOtherRegistryRapidKeysUnchanged(rt, registry, states, targetIndex, before)
				assertRegistryRapidInvariants(rt, registry, target.logPath, target.model)

			case registryRapidOpSweep:
				sweepAllRegistryRapidKeys(rt, registry, states, step)
			}
		}
	})
}
