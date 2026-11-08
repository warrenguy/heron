//go:build testhooks

package consensus

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
)

const (
	toggleTestTermID int64 = 9
	toggleTestNowNs  int64 = 1000
)

type toggleAction struct {
	action clusterwire.ClusterAction
	flags  int32
}

type toggleLeader struct {
	agent      *Agent
	pub        *aeron.ExclusivePublication
	queue      *QueueConsensusIngress
	servicePub *aeron.Publication
	toggle     *fakeToggle
}

func newToggleLeader(t testing.TB, toggle int64, serviceCount int) toggleLeader {
	t.Helper()
	a, q, servicePub := agentWithServiceControl(t, serviceCount)
	pub := bindLeaderLog(t, a)
	a.SetLeadershipTermID(toggleTestTermID)
	counter := newFakeToggle(toggle)
	a.SetClusterControlToggle(counter)
	return toggleLeader{
		agent:      a,
		pub:        pub,
		queue:      q,
		servicePub: servicePub,
		toggle:     counter,
	}
}

// scanClusterActions decodes each ClusterActionRequest in term 0 in log
// order. It fails on any other frame.
func scanClusterActions(
	t fragmentRequireT,
	pub *aeron.ExclusivePublication,
) []codecs.ClusterActionRequestFields {
	t.Helper()
	require.Less(t, pub.Position(), int64(pub.TermBuffer(0).Capacity()))
	var actions []codecs.ClusterActionRequestFields
	for _, frame := range scanTermFrames(t, pub, codecs.TryDecodeClusterActionRequestAt) {
		require.True(t, frame.decoded)
		require.Equal(t, frame.end, frame.fields.LogPosition)
		actions = append(actions, frame.fields)
	}

	return actions
}

func TestClusterControlToggleWork(t *testing.T) {
	t.Parallel()

	type setupFunc = func(*testing.T, toggleLeader)
	type row struct {
		name   string
		toggle int64
		setup  setupFunc
		// direct calls RequestSnapshot in place of the slow tick.
		direct       bool
		zeroServices bool
		// retryAfterPop acks the pending wait, then expects the retry to
		// enter SNAPSHOT.
		retryAfterPop   bool
		wantTermination bool
		wantErr         error
		wantWork        int
		wantToggle      int64
		wantState       ModuleState
		wantAction      *toggleAction
		wantWaits       int
	}
	unbindPublisher := func(_ *testing.T, l toggleLeader) {
		l.agent.SetLogPublisher(NewLogPublisher())
	}
	closePublication := func(t *testing.T, l toggleLeader) {
		t.Helper()
		require.NotNil(t, l.agent.logPublisher.Publication())
		require.NoError(t, l.pub.Close())
		require.Equal(t, aeron.Closed, l.agent.logPublisher.Position())
		require.Same(t, l.pub, l.agent.logPublisher.Publication())
	}
	inState := func(state ModuleState) setupFunc {
		return func(_ *testing.T, l toggleLeader) {
			l.agent.setModuleState(state, "test")
		}
	}
	suspended := inState(ModuleStateSuspended)
	pendingAck := func(t *testing.T, l toggleLeader) {
		require.NoError(t, l.agent.RequestServiceAck(42))
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
			name:       "SUSPEND in ACTIVE",
			toggle:     int64(ControlToggleSuspend),
			wantWork:   1,
			wantToggle: int64(ControlToggleSuspend),
			wantState:  ModuleStateSuspended,
			wantAction: &toggleAction{action: clusterwire.ClusterAction_SUSPEND},
		},
		unchanged(
			"SUSPEND append fails",
			int64(ControlToggleSuspend),
			ModuleStateActive,
			unbindPublisher,
		),
		{
			name:       "RESUME in SUSPENDED",
			toggle:     int64(ControlToggleResume),
			setup:      suspended,
			wantWork:   1,
			wantToggle: int64(ControlToggleNeutral),
			wantState:  ModuleStateActive,
			wantAction: &toggleAction{action: clusterwire.ClusterAction_RESUME},
		},
		unchanged(
			"RESUME append fails",
			int64(ControlToggleResume),
			ModuleStateSuspended,
			func(t *testing.T, l toggleLeader) {
				suspended(t, l)
				unbindPublisher(t, l)
			},
		),
		{
			name:       "SNAPSHOT in ACTIVE",
			toggle:     int64(ControlToggleSnapshot),
			wantWork:   1,
			wantToggle: int64(ControlToggleSnapshot),
			wantState:  ModuleStateSnapshot,
			wantAction: &toggleAction{action: clusterwire.ClusterAction_SNAPSHOT},
			wantWaits:  1,
		},
		{
			name:       "direct RequestSnapshot in ACTIVE",
			toggle:     int64(ControlToggleNeutral),
			direct:     true,
			wantToggle: int64(ControlToggleNeutral),
			wantState:  ModuleStateSnapshot,
			wantAction: &toggleAction{action: clusterwire.ClusterAction_SNAPSHOT},
			wantWaits:  1,
		},
		{
			name:       "direct RequestSnapshot refused, coordination wait",
			toggle:     int64(ControlToggleNeutral),
			direct:     true,
			wantErr:    errCoordinationOutstanding,
			wantToggle: int64(ControlToggleNeutral),
			wantState:  ModuleStateActive,
			wantWaits:  1,
			setup:      pendingAck,
		},
		{
			name:       "STANDBY_SNAPSHOT in ACTIVE",
			toggle:     int64(ControlToggleStandbySnapshot),
			wantWork:   1,
			wantToggle: int64(ControlToggleNeutral),
			wantState:  ModuleStateActive,
			wantAction: &toggleAction{
				action: clusterwire.ClusterAction_SNAPSHOT,
				flags:  clusterActionFlagsStandbySnapshot,
			},
		},
		{
			name:       "SNAPSHOT refused, coordination wait",
			toggle:     int64(ControlToggleSnapshot),
			wantToggle: int64(ControlToggleSnapshot),
			wantState:  ModuleStateActive,
			wantWaits:  1,
			setup:      pendingAck,
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
		{
			name:       "STANDBY_SNAPSHOT keeps a SNAPSHOT written during the append",
			toggle:     int64(ControlToggleStandbySnapshot),
			wantWork:   1,
			wantToggle: int64(ControlToggleSnapshot),
			wantState:  ModuleStateActive,
			wantAction: &toggleAction{
				action: clusterwire.ClusterAction_SNAPSHOT,
				flags:  clusterActionFlagsStandbySnapshot,
			},
			setup: func(t *testing.T, l toggleLeader) {
				l.toggle.afterLoad = func() {
					l.toggle.Store(int64(ControlToggleNeutral))
					require.True(t, ControlToggleSnapshot.Toggle(l.toggle))
				}
			},
		},
		unchanged(
			"STANDBY_SNAPSHOT append fails",
			int64(ControlToggleStandbySnapshot),
			ModuleStateActive,
			unbindPublisher,
		),
		{
			name:            "SHUTDOWN in ACTIVE",
			toggle:          int64(ControlToggleShutdown),
			wantWork:        1,
			wantToggle:      int64(ControlToggleShutdown),
			wantState:       ModuleStateSnapshot,
			wantAction:      &toggleAction{action: clusterwire.ClusterAction_SNAPSHOT},
			wantWaits:       1,
			wantTermination: true,
		},
		{
			name:          "SHUTDOWN refused, coordination wait",
			toggle:        int64(ControlToggleShutdown),
			setup:         pendingAck,
			wantToggle:    int64(ControlToggleShutdown),
			wantState:     ModuleStateActive,
			wantWaits:     1,
			retryAfterPop: true,
		},
		{
			name:            "ABORT in ACTIVE, zero services",
			toggle:          int64(ControlToggleAbort),
			zeroServices:    true,
			wantWork:        1,
			wantToggle:      int64(ControlToggleAbort),
			wantState:       ModuleStateTerminating,
			wantTermination: true,
		},
		unchanged(
			"ABORT refused, unbound publisher",
			int64(ControlToggleAbort),
			ModuleStateActive,
			unbindPublisher,
		),
		unchanged(
			"ABORT refused, closed publication",
			int64(ControlToggleAbort),
			ModuleStateActive,
			closePublication,
		),
	}
	for _, toggle := range []int64{
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
			if state == ModuleStateSuspended && toggle == ControlToggleResume {
				continue
			}
			rows = append(rows, unchanged(
				toggle.String()+" in "+state.String(),
				int64(toggle),
				state,
				inState(state),
			))
		}
	}
	for _, toggle := range []ControlToggleState{
		ControlToggleSuspend,
		ControlToggleSnapshot,
		ControlToggleShutdown,
		ControlToggleAbort,
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

			services := 1
			if tt.zeroServices {
				services = 0
			}
			l := newToggleLeader(t, tt.toggle, services)

			if tt.setup != nil {
				tt.setup(t, l)
			}
			preState := l.agent.ModuleState()

			if tt.direct {
				err := l.agent.RequestSnapshot(toggleTestNowNs)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
				} else {
					require.NoError(t, err)
				}
			} else {
				require.Equal(t, tt.wantWork, l.agent.slowTickWork(toggleTestNowNs))
			}
			require.Equal(t, tt.wantToggle, l.toggle.Load())
			require.Equal(t, tt.wantState, l.agent.ModuleState())
			require.Equal(t, tt.wantWaits, l.agent.PendingServiceAckWaits())

			if tt.wantState == preState ||
				tt.wantState == ModuleStateSnapshot ||
				tt.wantState == ModuleStateTerminating {
				require.Empty(t, l.agent.uncommittedModuleStates)
			} else {
				require.Equal(t, []uncommittedModuleState{{
					appendPosition: l.pub.Position(),
					previous:       preState,
				}}, l.agent.uncommittedModuleStates)
			}

			requireToggleActions(t, l, tt.wantAction)

			if tt.wantTermination {
				require.Equal(t, l.pub.Position(), l.agent.TerminationPosition())
				require.Equal(t, toggleTestTermID, l.agent.terminationLeadershipTermID)
			} else {
				require.Equal(t, int64(nullValue), l.agent.TerminationPosition())
				require.Equal(t, int64(nullValue), l.agent.terminationLeadershipTermID)
			}

			if !tt.retryAfterPop {
				return
			}

			enqueueServiceAck(t, l.queue, 42, 0, 0)
			require.Equal(t, 1, l.agent.pollServiceControl())
			require.Equal(t, 1, l.agent.serviceControlWork())
			require.Equal(t, 0, l.agent.PendingServiceAckWaits())

			require.Equal(t, 1, l.agent.slowTickWork(toggleTestNowNs))
			require.Equal(t, int64(ControlToggleShutdown), l.toggle.Load())
			require.Equal(t, ModuleStateSnapshot, l.agent.ModuleState())
			require.Equal(t, 1, l.agent.PendingServiceAckWaits())
			requireToggleActions(t, l, &toggleAction{action: clusterwire.ClusterAction_SNAPSHOT})
			require.Equal(t, l.pub.Position(), l.agent.TerminationPosition())
			require.Equal(t, toggleTestTermID, l.agent.terminationLeadershipTermID)
		})
	}
}

func requireToggleActions(
	t *testing.T,
	l toggleLeader,
	want *toggleAction,
) {
	t.Helper()
	var wantActions []codecs.ClusterActionRequestFields
	if want != nil {
		wantActions = []codecs.ClusterActionRequestFields{{
			LeadershipTermID: toggleTestTermID,
			LogPosition:      l.pub.Position(),
			Timestamp:        toggleTestNowNs,
			Action:           want.action,
			Flags:            want.flags,
		}}
	}
	require.Equal(t, wantActions, scanClusterActions(t, l.pub))
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
			name: "failed take that ends the member keeps SNAPSHOT",
			run: func(t *testing.T) {
				a, _ := settledLeaderWithLog(t, 0)
				a.errorHandler = a.TerminateOnError
				control, _ := wireToggles(a, snapshot, neutral)
				require.Equal(t, 1, a.slowTickWork(nowNs))
				require.Equal(t, 1, a.serviceControlWork())
				requireTakeNotConfigured(t, a)
				require.Equal(t, snapshot, control.Load())
				requireTerminated(t, a, errSnapshotNotConfigured, false)
			},
		},
		{
			name: "ack failure before the pop keeps SNAPSHOT",
			run: func(t *testing.T) {
				a, q, _ := settledLeaderWithControl(t, 1)
				control, _ := wireToggles(a, snapshot, neutral)
				require.Equal(t, 1, a.slowTickWork(nowNs))
				enqueueServiceAck(t, q, a.ExpectedAckPosition(), 5, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
				require.Equal(t, snapshot, control.Load())
				requireTerminated(t, a, a.ServiceAckFailure(), false)
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

// TestClusterControlToggle_suspendedAcrossElection asserts that a SUSPENDED
// leader stays SUSPENDED through an election it wins. A RESUME toggle then
// resumes the leader in the new term.
func TestClusterControlToggle_suspendedAcrossElection(t *testing.T) {
	t.Parallel()

	const nextTermID = toggleTestTermID + 1
	nowNs := toggleTestNowNs
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64)})
	pub := bindLeaderLog(t, a)
	a.SetLeadershipTermID(toggleTestTermID)
	control, node := wireToggles(a, int64(ControlToggleNeutral), int64(ControlToggleNeutral))

	// suspend: the leader appends SUSPEND. The commit position reaches it.
	require.True(t, ControlToggleSuspend.Toggle(control))
	require.Equal(t, 1, a.slowTickWork(nowNs))
	require.Equal(t, ModuleStateSuspended, a.ModuleState())
	suspendEnd := pub.Position()
	require.Equal(t, 1, a.UpdateLeaderPosition(nowNs, suspendEnd))
	require.Equal(t, suspendEnd, a.CommitPosition())
	require.Empty(t, a.uncommittedModuleStates)

	// election: the member leads again in the next term.
	a.SetElectionFactory(func(ElectionSeed) ElectionRunner { return &orderRunner{} })
	require.Equal(t, 1, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false))
	_, prepared, err := a.PrepareForNewLeadership(a.CommitPosition(), nowNs)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, int64(ControlToggleInactive), control.Load())
	require.Equal(t, ModuleStateSuspended, a.ModuleState())
	require.NoError(t, a.JoinLogAsLeader(nextTermID, suspendEnd, 0, false))
	nowNs++
	a.ElectionComplete(nowNs)
	require.Equal(t, ModuleStateSuspended, a.ModuleState())
	require.Equal(t, int64(ControlToggleNeutral), control.Load())
	require.Equal(t, int64(NodeControlToggleNeutral), node.Load())

	// resume: a RESUME toggle appends RESUME in the next term.
	require.True(t, ControlToggleResume.Toggle(control))
	nowNs++
	require.Equal(t, 1, a.slowTickWork(nowNs))
	require.Equal(t, ModuleStateActive, a.ModuleState())
	require.Equal(t, int64(ControlToggleNeutral), control.Load())
	require.Equal(t, []codecs.ClusterActionRequestFields{
		{
			LeadershipTermID: toggleTestTermID,
			LogPosition:      suspendEnd,
			Timestamp:        toggleTestNowNs,
			Action:           clusterwire.ClusterAction_SUSPEND,
		},
		{
			LeadershipTermID: nextTermID,
			LogPosition:      pub.Position(),
			Timestamp:        nowNs,
			Action:           clusterwire.ClusterAction_RESUME,
		},
	}, scanClusterActions(t, pub))
	require.Equal(t, []uncommittedModuleState{{
		appendPosition: pub.Position(),
		previous:       ModuleStateSuspended,
	}}, a.uncommittedModuleStates)
}
