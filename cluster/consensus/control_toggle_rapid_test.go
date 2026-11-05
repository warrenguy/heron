//go:build rapid && testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// controlToggleModel models the toggle value, module state and log of one
// settled leader.
type controlToggleModel struct {
	value int64
	state ModuleState

	// actionFlags lists SNAPSHOT action flags in log order.
	actionFlags []int32
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

// slowTick applies one leader slow tick. Returns the expected work count.
func (m *controlToggleModel) slowTick() int {
	if m.state != ModuleStateActive {
		return 0
	}
	switch m.value {
	case int64(ControlToggleSnapshot):
		m.actionFlags = append(m.actionFlags, 0)
		m.state = ModuleStateSnapshot
		return 1
	case int64(ControlToggleStandbySnapshot):
		m.actionFlags = append(m.actionFlags, clusterActionFlagsStandbySnapshot)
		m.value = int64(ControlToggleNeutral)
		return 1
	}
	return 0
}

func (m *controlToggleModel) snapshotEnd() {
	m.state = ModuleStateActive
	if m.value == int64(ControlToggleSnapshot) {
		m.value = int64(ControlToggleNeutral)
	}
}

// scanClusterActionFlags returns the flags of each ClusterActionRequest in
// term 0, in log order. Each action must be a SNAPSHOT action.
func scanClusterActionFlags(t fragmentRequireT, pub *aeron.ExclusivePublication) []int32 {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	end := pub.Position()
	require.Less(t, end, int64(termBuf.Capacity()))
	var flags []int32
	for offset := 0; int64(offset) < end; {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
		require.Positive(t, frameLength)
		wire := termBuf.Bytes()[offset+logbuffer.OffsetFramePayload : offset+frameLength]
		offset += logbuffer.AlignFrameLength(frameLength)
		fields, ok := codecs.TryDecodeClusterActionRequestAt(wire, 0)
		require.True(t, ok)
		require.Equal(t, clusterwire.ClusterAction_SNAPSHOT, fields.Action)
		require.Equal(t, int64(offset), fields.LogPosition)
		flags = append(flags, fields.Flags)
	}
	return flags
}

// TestControlToggle_model_rapid drives a settled leader with zero services
// through random toggle operations. After each step, the toggle value, module
// state and logged SNAPSHOT action flags match the model.
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
				require.Equal(rt, m.actionFlags, scanClusterActionFlags(rt, pub))
			},
		})
	})
}
