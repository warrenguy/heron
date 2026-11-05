//go:build testhooks

package consensus

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

const (
	toggleTestTermID int64 = 9
	toggleTestNowNs  int64 = 1000
)

type toggleLeader struct {
	agent  *Agent
	pub    *aeron.ExclusivePublication
	toggle *fakeToggle
}

func newToggleLeader(t testing.TB, toggle int64) toggleLeader {
	t.Helper()
	a, _, pub := settledLeaderWithControl(t, 1)
	a.SetLeadershipTermID(toggleTestTermID)
	counter := newFakeToggle(toggle)
	a.SetClusterControlToggle(counter)
	return toggleLeader{agent: a, pub: pub, toggle: counter}
}

func TestClusterControlToggleWork(t *testing.T) {
	t.Parallel()

	type action struct {
		flags int32
	}
	type setupFunc = func(*testing.T, toggleLeader)
	type row struct {
		name       string
		toggle     int64
		setup      setupFunc
		wantWork   int
		wantToggle int64
		wantState  ModuleState
		wantAction *action
		wantWaits  int
	}
	unbindPublisher := func(_ *testing.T, l toggleLeader) {
		l.agent.SetLogPublisher(NewLogPublisher())
	}
	unchanged := func(name string, toggle int64, state ModuleState, setup setupFunc) row {
		return row{
			name:       name,
			toggle:     toggle,
			setup:      setup,
			wantToggle: toggle,
			wantState:  state,
		}
	}
	rows := []row{
		{
			name:       "SNAPSHOT in ACTIVE",
			toggle:     int64(ControlToggleSnapshot),
			wantWork:   1,
			wantToggle: int64(ControlToggleSnapshot),
			wantState:  ModuleStateSnapshot,
			wantAction: &action{flags: 0},
			wantWaits:  1,
		},
		{
			name:       "STANDBY_SNAPSHOT in ACTIVE",
			toggle:     int64(ControlToggleStandbySnapshot),
			wantWork:   1,
			wantToggle: int64(ControlToggleNeutral),
			wantState:  ModuleStateActive,
			wantAction: &action{flags: clusterActionFlagsStandbySnapshot},
		},
		{
			name:       "SNAPSHOT refused, coordination wait",
			toggle:     int64(ControlToggleSnapshot),
			wantToggle: int64(ControlToggleSnapshot),
			wantState:  ModuleStateActive,
			wantWaits:  1,
			setup: func(t *testing.T, l toggleLeader) {
				require.NoError(t, l.agent.RequestServiceAck(42))
			},
		},
		unchanged(
			"SNAPSHOT refused, ack failure",
			int64(ControlToggleSnapshot),
			ModuleStateActive,
			func(_ *testing.T, l toggleLeader) {
				l.agent.serviceAckFailure = ErrServiceAckOutOfSequence
			},
		),
		unchanged(
			"SNAPSHOT refused, unbound publisher",
			int64(ControlToggleSnapshot),
			ModuleStateActive,
			unbindPublisher,
		),
		unchanged(
			"STANDBY_SNAPSHOT append fails",
			int64(ControlToggleStandbySnapshot),
			ModuleStateActive,
			unbindPublisher,
		),
	}
	for _, toggle := range []int64{
		int64(ControlToggleSuspend),
		int64(ControlToggleShutdown),
		int64(ControlToggleAbort),
		int64(ControlToggleResume),
		int64(ControlToggleNeutral),
		int64(ControlToggleInactive),
		9,
		-1,
	} {
		rows = append(rows, unchanged(
			fmt.Sprintf("toggle %d in ACTIVE", toggle), toggle, ModuleStateActive, nil,
		))
	}
	for _, state := range heldModuleStates {
		for _, toggle := range allControlToggleStates {
			rows = append(rows, unchanged(
				toggle.String()+" in "+state.String(),
				int64(toggle),
				state,
				func(_ *testing.T, l toggleLeader) {
					l.agent.setModuleState(state, "test")
				},
			))
		}
	}
	for _, toggle := range []ControlToggleState{
		ControlToggleSnapshot,
		ControlToggleStandbySnapshot,
	} {
		rows = append(rows,
			unchanged(
				toggle.String()+" on a follower",
				int64(toggle),
				ModuleStateActive,
				func(_ *testing.T, l toggleLeader) {
					l.agent.RoleSet(RoleFollower)
				},
			),
			unchanged(
				toggle.String()+" during an election",
				int64(toggle),
				ModuleStateActive,
				func(_ *testing.T, l toggleLeader) {
					l.agent.election = &orderRunner{n: 1}
				},
			),
		)
	}
	rows = append(rows, unchanged(
		"nil seam",
		int64(ControlToggleSnapshot),
		ModuleStateActive,
		func(_ *testing.T, l toggleLeader) {
			l.agent.SetClusterControlToggle(nil)
		},
	))

	for _, tt := range rows {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newToggleLeader(t, tt.toggle)
			if tt.setup != nil {
				tt.setup(t, l)
			}
			prePosition := l.pub.Position()

			require.Equal(t, tt.wantWork, l.agent.slowTickWork(toggleTestNowNs))
			require.Equal(t, tt.wantToggle, l.toggle.Load())
			require.Equal(t, tt.wantState, l.agent.ModuleState())
			require.Equal(t, tt.wantWaits, l.agent.PendingServiceAckWaits())
			if tt.wantAction == nil {
				require.Equal(t, prePosition, l.pub.Position())
				return
			}
			require.Greater(t, l.pub.Position(), prePosition)
			fields := decodeClusterActionAtTermStart(t, l.pub)
			require.Equal(t, codecs.ClusterActionRequestFields{
				LeadershipTermID: toggleTestTermID,
				LogPosition:      l.pub.Position(),
				Timestamp:        toggleTestNowNs,
				Action:           clusterwire.ClusterAction_SNAPSHOT,
				Flags:            tt.wantAction.flags,
			}, fields)
		})
	}
}

func wireToggles(a *Agent, control, node int64) (*fakeToggle, *fakeToggle) {
	controlToggle := newFakeToggle(control)
	nodeToggle := newFakeToggle(node)
	a.SetClusterControlToggle(controlToggle)
	a.SetNodeControlToggle(nodeToggle)
	return controlToggle, nodeToggle
}

func TestClusterControlToggle_lifecycle(t *testing.T) {
	t.Parallel()

	const nowNs = toggleTestNowNs
	inactive := int64(ControlToggleInactive)
	neutral := int64(ControlToggleNeutral)
	snapshot := int64(ControlToggleSnapshot)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "election complete as leader",
			run: func(t *testing.T) {
				a := NewAgent(AgentConfig{Role: RoleLeader})
				control, node := wireToggles(a, inactive, inactive)
				a.ElectionComplete(nowNs)
				require.Equal(t, neutral, control.Load())
				require.Equal(t, neutral, node.Load())
			},
		},
		{
			name: "election complete as follower",
			run: func(t *testing.T) {
				a := NewAgent(AgentConfig{Role: RoleFollower})
				control, node := wireToggles(a, inactive, inactive)
				a.ElectionComplete(nowNs)
				require.Equal(t, inactive, control.Load())
				require.Equal(t, neutral, node.Load())
			},
		},
		{
			name: "prepare deactivates the control toggle",
			run: func(t *testing.T) {
				a, _, _ := settledLeaderWithControl(t, 1)
				control, node := wireToggles(a, snapshot, neutral)
				_, prepared, err := a.PrepareForNewLeadership(0, nowNs)
				require.NoError(t, err)
				require.True(t, prepared)
				require.Equal(t, inactive, control.Load())
				require.Equal(t, neutral, node.Load())
			},
		},
		{
			name: "resolved take resets SNAPSHOT",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 0)
				control, _ := wireToggles(f.agent, snapshot, neutral)
				require.Equal(t, 1, f.agent.slowTickWork(nowNs))
				f.commit.Store(f.agent.ExpectedAckPosition())
				require.Equal(t, snapshot, control.Load())

				result, _ := f.tickUntilResolved(t, nowNs+1, 1)
				require.NoError(t, result.Err)
				require.Equal(t, neutral, control.Load())
				require.Equal(t, ModuleStateActive, f.agent.ModuleState())
			},
		},
		{
			name: "failed take resets SNAPSHOT",
			run: func(t *testing.T) {
				a, _ := settledLeaderWithLog(t, 0)
				control, _ := wireToggles(a, snapshot, neutral)
				require.Equal(t, 1, a.slowTickWork(nowNs))
				require.Equal(t, 1, a.serviceControlWork())
				requireTakeNotConfigured(t, a)
				require.Equal(t, neutral, control.Load())
				require.Equal(t, ModuleStateActive, a.ModuleState())
			},
		},
		{
			name: "ack failure before the pop resets SNAPSHOT",
			run: func(t *testing.T) {
				a, q, _ := settledLeaderWithControl(t, 1)
				control, _ := wireToggles(a, snapshot, neutral)
				require.Equal(t, 1, a.slowTickWork(nowNs))
				enqueueServiceAck(t, q, a.ExpectedAckPosition(), 5, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
				require.Equal(t, neutral, control.Load())
				require.Equal(t, ModuleStateActive, a.ModuleState())
			},
		},
		{
			name: "follower snapshot end keeps the toggle",
			run: func(t *testing.T) {
				const consumePos int64 = 4096
				a := NewAgent(AgentConfig{Role: RoleFollower})
				control, _ := wireToggles(a, snapshot, neutral)
				a.handleClusterAction(snapshotActionFields(consumePos), consumePos)
				require.Equal(t, ModuleStateSnapshot, a.ModuleState())
				require.Equal(t, 1, a.serviceControlWork())
				requireTakeNotConfigured(t, a)
				require.Equal(t, ModuleStateActive, a.ModuleState())
				require.Equal(t, snapshot, control.Load())
			},
		},
		{
			name: "direct snapshot keeps a SUSPEND written meanwhile",
			run: func(t *testing.T) {
				a, _ := settledLeaderWithLog(t, 0)
				control, _ := wireToggles(a, neutral, neutral)
				require.NoError(t, a.RequestSnapshot(nowNs))
				require.True(t, ControlToggleSuspend.Toggle(control))
				require.Equal(t, 1, a.serviceControlWork())
				requireTakeNotConfigured(t, a)
				require.Equal(t, int64(ControlToggleSuspend), control.Load())
			},
		},
		{
			name: "direct snapshot covers a SNAPSHOT written mid-take",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 0)
				control, _ := wireToggles(f.agent, neutral, neutral)
				position := f.request(t, nowNs)
				f.agent.DoWork(nowNs)
				require.NotNil(t, f.agent.snapshotTake)
				require.True(t, ControlToggleSnapshot.Toggle(control))

				result, resolvedNs := f.tickUntilResolved(t, nowNs+1, 1)
				require.NoError(t, result.Err)
				require.Equal(t, position, result.LogPosition)
				require.Equal(t, neutral, control.Load())
				require.Equal(t, position, f.agent.logPublisher.Position())

				require.Equal(t, 0, f.agent.slowTickWork(resolvedNs+slowTickIntervalNs))
				require.Equal(t, position, f.agent.logPublisher.Position())
				require.Equal(t, ModuleStateActive, f.agent.ModuleState())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
