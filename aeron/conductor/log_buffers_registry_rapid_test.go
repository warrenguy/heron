//go:build rapid

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
	registryRapidOpClosePublication
)

const registryRapidClosePublicationRegID int64 = 42

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

type registryRapidKeyState struct {
	logPath string
	key     string
	model   registryRapidModel
}

func drawRegistryRapidNowNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	offset := rapid.Int64Range(-500_000_000, 2_000_000_000).Draw(rt, label)
	return registryRapidBaseNowNs + offset
}

func drawRegistryRapidLingerNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	return rapid.Int64Range(0, 1_000_000_000).Draw(rt, label)
}

func drawRegistryRapidPositiveLingerNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	return rapid.Int64Range(1, 1_000_000_000).Draw(rt, label)
}

func legalRegistryRapidOps(model registryRapidModel) []registryRapidOpKind {
	if model.refCount > 0 {
		return registryRapidOpsWithRelease
	}
	return registryRapidOpsNoRelease
}

func legalRegistryRapidOpsWithClose(
	model registryRapidModel,
	publicationInMap bool,
) []registryRapidOpKind {
	base := legalRegistryRapidOps(model)
	ops := make([]registryRapidOpKind, len(base), len(base)+1)
	copy(ops, base)
	if model.refCount == 1 && publicationInMap {
		ops = append(ops, registryRapidOpClosePublication)
	}
	return ops
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
	if lingerNs == 0 {
		model.contains = false
		model.lingerDeadlineNs = 0
		return
	}

	model.contains = true
	model.lingerDeadlineNs = nowNs + lingerNs
}

func applyRegistryRapidAcquire(model *registryRapidModel) {
	if !model.contains {
		model.contains = true
		model.refCount = 1
	} else {
		model.refCount++
	}
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

func assertRegistryRapidInvariants(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	model registryRapidModel,
) {
	rt.Helper()

	require.Equal(
		rt,
		model.refCount,
		conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
	)
	require.Equal(
		rt,
		model.contains,
		conductor.LogBuffersRegistryContainsForTest(registry, logPath),
	)
	require.Equal(
		rt,
		model.lingerDeadlineNs,
		conductor.LingerDeadlineNsForTest(registry, logPath),
	)
	require.Equal(
		rt,
		conductor.LogBuffersRegistryCountScheduledLingersForTest(registry),
		conductor.LogBuffersRegistryLingerScheduledForTest(registry),
	)
	require.Equal(
		rt,
		conductor.LogBuffersRegistryLingerScheduledForTest(registry) > 0,
		registry.HasScheduledLinger(),
	)
}

func assertRegistryRapidLingerMmapRetained(
	rt *rapid.T,
	model registryRapidModel,
	liveMapped *any,
) {
	rt.Helper()

	if model.refCount != 0 || !model.contains || model.lingerDeadlineNs == 0 {
		return
	}
	require.NotNil(rt, liveMapped)
	require.NotNil(rt, *liveMapped)
}

type registryRapidStepTarget struct {
	registry         *conductor.LogBuffersRegistry
	logPath          string
	key              string
	model            *registryRapidModel
	liveMapped       *any
	conductor        *conductor.Conductor
	nanoNs           *int64
	closeLingerNs    int64
	publicationInMap *bool
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
		clearRegistryRapidLiveMappedIfEvicted(target)

	case registryRapidOpSweep:
		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("sweepNowOffset%d", step))
		conductor.SweepLingeringLogBuffersForTest(target.registry, nowNs)
		applyRegistryRapidSweep(target.model, nowNs)
		clearRegistryRapidLiveMappedIfEvicted(target)

	case registryRapidOpClosePublication:
		require.NotNil(rt, target.conductor)
		require.NotNil(rt, target.nanoNs)
		require.NotNil(rt, target.publicationInMap)
		require.True(rt, *target.publicationInMap)
		require.Equal(rt, int32(1), target.model.refCount)

		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("closeNowOffset%d", step))
		*target.nanoNs = nowNs

		require.NoError(rt, target.conductor.ClosePublication(
			stubPublication{registrationID: registryRapidClosePublicationRegID},
		))
		*target.publicationInMap = false
		require.Equal(rt, 0, conductor.PublicationCountForTest(target.conductor))

		applyRegistryRapidRelease(target.model, nowNs, target.closeLingerNs)
		clearRegistryRapidLiveMappedIfEvicted(target)
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

func registryRapidKeyStateForLog(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
) registryRapidKeyState {
	rt.Helper()

	key, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
	require.NoError(rt, err)
	return registryRapidKeyState{logPath: logPath, key: key}
}

type registryCloseRapidFixture struct {
	conductor     *conductor.Conductor
	registry      *conductor.LogBuffersRegistry
	state         registryRapidKeyState
	nanoNs        *int64
	closeLingerNs int64
	liveMapped    *any
}

func newRegistryCloseRapidFixture(
	t *testing.T,
	rt *rapid.T,
) registryCloseRapidFixture {
	t.Helper()
	rt.Helper()

	closeLingerNs := drawRegistryRapidPositiveLingerNs(rt, "closeLingerNs")

	dir := t.TempDir()
	logPath := filepath.Join(dir, "close-rapid.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	lifecycle := &lingerPublicationLifecycle{registryKey: logPath}
	conductorInstance, nanoNs := newConductorWithCloseLingerForTest(t, closeLingerNs, lifecycle)

	buffers, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)
	var liveMapped any = buffers.MappedLogBuffer()

	conductorInstance.InsertPublicationForTest(
		registryRapidClosePublicationRegID,
		stubPublication{registrationID: registryRapidClosePublicationRegID},
	)
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	state := registryRapidKeyStateForLog(rt, registry, logPath)
	return registryCloseRapidFixture{
		conductor:     conductorInstance,
		registry:      registry,
		state:         state,
		nanoNs:        nanoNs,
		closeLingerNs: closeLingerNs,
		liveMapped:    &liveMapped,
	}
}

func clearRegistryRapidLiveMappedIfEvicted(target registryRapidStepTarget) {
	if target.liveMapped != nil && !target.model.contains {
		*target.liveMapped = nil
	}
}

func newRegistryRapidFixture(
	t *testing.T,
	rt *rapid.T,
) (*conductor.LogBuffersRegistry, registryRapidKeyState) {
	t.Helper()
	rt.Helper()

	registry, logPath := newRegistryWithLogFile(t, "rapid.log")
	return registry, registryRapidKeyStateForLog(rt, registry, logPath)
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
		states[i] = registryRapidKeyStateForLog(rt, registry, logPath)
	}
	return registry, states
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

func TestLogBuffersRegistry_acquireReleaseLinger_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		registry, state := newRegistryRapidFixture(t, rt)
		model := registryRapidModel{}

		var liveMapped any
		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			op := rapid.SampledFrom(legalRegistryRapidOps(model)).Draw(rt, fmt.Sprintf("op%d", step))
			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry:   registry,
				logPath:    state.logPath,
				key:        state.key,
				model:      &model,
				liveMapped: &liveMapped,
			}, op, step)
			assertRegistryRapidInvariants(rt, registry, state.logPath, model)
		}
	})
}

func TestLogBuffersRegistry_closePublicationLinger_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		fixture := newRegistryCloseRapidFixture(t, rt)
		model := registryRapidModel{refCount: 1, contains: true}
		publicationInMap := true

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			op := rapid.SampledFrom(legalRegistryRapidOpsWithClose(model, publicationInMap)).
				Draw(rt, fmt.Sprintf("op%d", step))
			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry:         fixture.registry,
				logPath:          fixture.state.logPath,
				key:              fixture.state.key,
				model:            &model,
				liveMapped:       fixture.liveMapped,
				conductor:        fixture.conductor,
				nanoNs:           fixture.nanoNs,
				closeLingerNs:    fixture.closeLingerNs,
				publicationInMap: &publicationInMap,
			}, op, step)
			assertRegistryRapidInvariants(rt, fixture.registry, fixture.state.logPath, model)
			assertRegistryRapidLingerMmapRetained(rt, model, fixture.liveMapped)
		}
	})
}

func TestLogBuffersRegistry_multiKeyIsolation_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		keyCount := rapid.IntRange(2, 8).Draw(rt, "keyCount")
		registry, states := newMultiKeyRegistryRapidFixture(t, rt, keyCount)

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			targetIndex := rapid.IntRange(0, keyCount-1).Draw(rt, fmt.Sprintf("targetIndex%d", step))
			target := &states[targetIndex]
			op := rapid.SampledFrom(legalRegistryRapidOps(target.model)).Draw(rt, fmt.Sprintf("op%d", step))

			if op == registryRapidOpSweep {
				sweepAllRegistryRapidKeys(rt, registry, states, step)
				continue
			}

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
		}
	})
}
