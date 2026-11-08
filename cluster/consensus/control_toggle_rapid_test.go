//go:build rapid && testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// controlToggleModel models the toggle value, module state and log of one
// settled leader.
type controlToggleModel struct {
	value        int64
	state        ModuleState
	shutdownTake bool

	actions []modelClusterAction
}

type modelClusterAction struct {
	action   clusterwire.ClusterAction
	flags    int32
	previous ModuleState
}

// toggle applies the upstream ClusterControl.ToggleState.toggle rule.
func (m *controlToggleModel) toggle(state ControlToggleState) bool {
	if state == ControlToggleResume && m.value == int64(ControlToggleSuspend) {
		m.value = int64(state)
		return true
	}
	if m.value != int64(ControlToggleNeutral) {
		return false
	}
	m.value = int64(state)
	return true
}

func (m *controlToggleModel) appendTracked(action clusterwire.ClusterAction) {
	m.actions = append(m.actions, modelClusterAction{
		action:   action,
		previous: m.state,
	})
}

// slowTick applies one leader slow tick. Returns the expected work count.
func (m *controlToggleModel) slowTick() int {
	if m.state == ModuleStateSuspended && m.value == int64(ControlToggleResume) {
		m.appendTracked(clusterwire.ClusterAction_RESUME)
		m.state = ModuleStateActive
		m.value = int64(ControlToggleNeutral)
		return 1
	}
	if m.state != ModuleStateActive {
		return 0
	}
	switch m.value {
	case int64(ControlToggleSuspend):
		m.appendTracked(clusterwire.ClusterAction_SUSPEND)
		m.state = ModuleStateSuspended
		return 1

	case int64(ControlToggleSnapshot), int64(ControlToggleShutdown):
		m.actions = append(m.actions, modelClusterAction{action: clusterwire.ClusterAction_SNAPSHOT})
		m.state = ModuleStateSnapshot
		m.shutdownTake = m.value == int64(ControlToggleShutdown)
		return 1

	case int64(ControlToggleStandbySnapshot):
		m.actions = append(m.actions, modelClusterAction{
			action: clusterwire.ClusterAction_SNAPSHOT,
			flags:  clusterActionFlagsStandbySnapshot,
		})
		m.value = int64(ControlToggleNeutral)
		return 1

	case int64(ControlToggleAbort):
		m.state = ModuleStateTerminating
		return 1
	}
	return 0
}

func (m *controlToggleModel) snapshotEnd() {
	if m.shutdownTake {
		m.state = ModuleStateTerminating
		return
	}
	m.state = ModuleStateActive
	if m.value == int64(ControlToggleSnapshot) {
		m.value = int64(ControlToggleNeutral)
	}
}

func (m *controlToggleModel) requireLog(
	rt *rapid.T,
	a *Agent,
	pub *aeron.ExclusivePublication,
) {
	rt.Helper()
	logged := scanClusterActions(rt, pub)
	require.Len(rt, logged, len(m.actions))
	wantTracked := make([]uncommittedModuleState, 0)
	for i, want := range m.actions {
		require.Equal(rt, want.action, logged[i].Action)
		require.Equal(rt, want.flags, logged[i].Flags)
		if want.action != clusterwire.ClusterAction_SNAPSHOT {
			wantTracked = append(wantTracked, uncommittedModuleState{
				appendPosition: logged[i].LogPosition,
				previous:       want.previous,
			})
		}
	}
	require.Equal(rt, wantTracked, append([]uncommittedModuleState{}, a.uncommittedModuleStates...))
}

// TestControlToggle_model_rapid drives a settled leader with zero services
// through random toggle operations. After each step, the toggle value, module
// state, logged actions, and tracked module states match the model. The commit
// position stays at 0, so the agent keeps every tracked entry.
func TestControlToggle_model_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a, pub := settledLeaderWithLog(t, 0)
		counter := newFakeToggle(int64(ControlToggleNeutral))
		a.SetClusterControlToggle(counter)
		m := &controlToggleModel{
			value: int64(ControlToggleNeutral),
			state: ModuleStateActive,
		}
		nowNs := int64(1000)

		rt.Repeat(map[string]func(*rapid.T){
			"toolToggle": func(rt *rapid.T) {
				state := rapid.SampledFrom(allControlToggleStates).Draw(rt, "state")
				want := m.toggle(state)
				require.Equal(rt, want, state.Toggle(counter))
			},
			"rawWrite": func(rt *rapid.T) {
				value := rapid.SampledFrom([]int64{
					-1, 8, 9, 1 << 40, int64(ControlToggleNeutral),
				}).Draw(rt, "value")
				counter.Store(value)
				m.value = value
			},
			"slowTick": func(rt *rapid.T) {
				nowNs++
				want := m.slowTick()
				require.Equal(rt, want, a.clusterControlToggleWork(nowNs))
			},
			"snapshotDrive": func(rt *rapid.T) {
				nowNs++
				if m.state != ModuleStateSnapshot {
					require.Equal(rt, 0, a.serviceControlWork())
					require.Equal(rt, 0, a.snapshotTakeWork(nowNs))
					return
				}
				require.Equal(rt, 1, a.serviceControlWork())
				requireTakeNotConfigured(rt, a)
				m.snapshotEnd()
			},
			"electionComplete": func(rt *rapid.T) {
				nowNs++
				a.ElectionComplete(nowNs)
				m.value = int64(ControlToggleNeutral)
			},
			"": func(rt *rapid.T) {
				require.Equal(rt, m.value, counter.Load())
				require.Equal(rt, m.state, a.ModuleState())
				m.requireLog(rt, a, pub)
			},
		})
	})
}
