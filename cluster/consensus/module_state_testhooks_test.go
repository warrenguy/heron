//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/cluster/codecs/clusterwire"
)

// heldModuleStates lists every state that holds a leader.
var heldModuleStates = []ModuleState{
	ModuleStateInit,
	ModuleStateSuspended,
	ModuleStateSnapshot,
	ModuleStateQuitting,
	ModuleStateTerminating,
	ModuleStateClosed,
}

type recordingModuleStateCounter struct {
	states []ModuleState
}

func (c *recordingModuleStateCounter) StoreModuleState(state ModuleState) {
	c.states = append(c.states, state)
}

func (c *recordingModuleStateCounter) last() ModuleState {
	return c.states[len(c.states)-1]
}

type moduleStateChange struct {
	from   ModuleState
	to     ModuleState
	reason string
}

type moduleStateProbe struct {
	counter *recordingModuleStateCounter
	changes []moduleStateChange
}

func newModuleStateProbe(t *testing.T, a *Agent) *moduleStateProbe {
	t.Helper()
	p := &moduleStateProbe{counter: &recordingModuleStateCounter{}}
	SetModuleStateChangeRecorderForTest(t, func(from, to ModuleState, reason string) {
		p.changes = append(p.changes, moduleStateChange{from: from, to: to, reason: reason})
	})
	a.SetModuleStateCounter(p.counter)
	return p
}

func (p *moduleStateProbe) requireState(t *testing.T, a *Agent, state ModuleState) {
	t.Helper()
	require.Equal(t, state, a.ModuleState())
	require.Equal(t, state, p.counter.last())
}

func (p *moduleStateProbe) requireWiredActive(t *testing.T, a *Agent) {
	t.Helper()
	p.requireState(t, a, ModuleStateActive)
	require.Equal(t, []ModuleState{ModuleStateActive}, p.counter.states)
	require.Empty(t, p.changes)
}

// mark returns the store count for requireUnchangedSince.
func (p *moduleStateProbe) mark() int { return len(p.counter.states) }

// requireUnchangedSince checks that no store ran after mark. Each change
// stores once, so the change log is unchanged too.
func (p *moduleStateProbe) requireUnchangedSince(
	t *testing.T,
	a *Agent,
	state ModuleState,
	mark int,
) {
	t.Helper()
	p.requireState(t, a, state)
	require.Len(t, p.counter.states, mark)
}

func (p *moduleStateProbe) requireTransition(
	t *testing.T,
	a *Agent,
	from, to ModuleState,
	reason string,
) {
	t.Helper()
	p.requireState(t, a, to)
	require.NotEmpty(t, p.changes)
	got := p.changes[len(p.changes)-1]
	require.Equal(t, moduleStateChange{from: from, to: to, reason: reason}, got)
}

func probeAckedSnapshot(
	t *testing.T,
	services int,
) (*takeFixture, *moduleStateProbe, int64) {
	t.Helper()
	f := newTakeLeader(t, services)
	p := newModuleStateProbe(t, f.agent)
	position := f.request(t, 100)
	f.ack(t, position, 0, 77, 0)
	return f, p, position
}

func probePoppedTake(t *testing.T) (*takeFixture, *moduleStateProbe, int64) {
	t.Helper()
	f, p, position := probeAckedSnapshot(t, 1)
	f.archive.stopNever = true
	f.agent.DoWork(101)
	require.NotNil(t, f.agent.snapshotTake)
	return f, p, position
}

func TestAgent_moduleStateTransitions(t *testing.T) {
	t.Run("new agent", func(t *testing.T) {
		a := NewAgent(AgentConfig{})
		require.Equal(t, ModuleStateActive, a.ModuleState())
		p := newModuleStateProbe(t, a)
		p.requireWiredActive(t, a)
	})

	t.Run("request snapshot", func(t *testing.T) {
		f := newTakeLeader(t, 1)
		p := newModuleStateProbe(t, f.agent)
		p.requireWiredActive(t, f.agent)

		f.request(t, 100)
		p.requireTransition(t, f.agent, ModuleStateActive, ModuleStateSnapshot, "snapshot action")
		require.Equal(t, []ModuleState{ModuleStateActive, ModuleStateSnapshot}, p.counter.states)
	})

	t.Run("follower flags 0", func(t *testing.T) {
		a := NewAgent(AgentConfig{Role: RoleFollower, ServiceCount: 1})
		p := newModuleStateProbe(t, a)
		a.handleClusterAction(snapshotActionFields(4096), 4096)
		p.requireTransition(t, a, ModuleStateActive, ModuleStateSnapshot, "snapshot action")
		require.Len(t, p.changes, 1)
	})

	t.Run("skip", func(t *testing.T) {
		const consumePos int64 = 4096
		cases := []struct {
			name  string
			setup func(a *Agent)
			call  func(a *Agent)
		}{
			{
				name: "flags not 0",
				call: func(a *Agent) {
					fields := snapshotActionFields(consumePos)
					fields.Flags = 1
					a.handleClusterAction(fields, consumePos)
				},
			},
			{
				name: "wrong position",
				call: func(a *Agent) {
					a.handleClusterAction(snapshotActionFields(consumePos+32), consumePos)
				},
			},
			{
				name: "wait pending",
				setup: func(a *Agent) {
					a.snapshotWaitPosition = consumePos
				},
				call: func(a *Agent) {
					a.handleClusterAction(snapshotActionFields(consumePos+32), consumePos+32)
				},
			},
			{
				name: "retired position",
				setup: func(a *Agent) {
					a.abandonSnapshotPosition(consumePos)
				},
				call: func(a *Agent) {
					a.handleClusterAction(snapshotActionFields(consumePos), consumePos)
				},
			},
			{
				name: "ack failure",
				setup: func(a *Agent) {
					a.serviceAckFailure = ErrServiceAckOutOfSequence
				},
				call: func(a *Agent) {
					a.handleClusterAction(snapshotActionFields(consumePos), consumePos)
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				a := NewAgent(AgentConfig{Role: RoleFollower, ServiceCount: 1})
				p := newModuleStateProbe(t, a)
				if tc.setup != nil {
					tc.setup(a)
				}
				tc.call(a)
				p.requireWiredActive(t, a)
			})
		}
	})

	t.Run("pop keeps snapshot", func(t *testing.T) {
		f, p, _ := probeAckedSnapshot(t, 1)
		f.archive.stopNever = true
		mark := p.mark()

		f.agent.DoWork(101)
		require.NotNil(t, f.agent.snapshotTake)
		p.requireUnchangedSince(t, f.agent, ModuleStateSnapshot, mark)
	})

	t.Run("take resolves", func(t *testing.T) {
		f, p, _ := probeAckedSnapshot(t, 1)
		result, _ := f.tickUntilResolved(t, 101, 1)
		require.NoError(t, result.Err)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateActive, "snapshot complete")
	})

	t.Run("null service ack", func(t *testing.T) {
		f, p, position := probeAckedSnapshot(t, 2)
		f.ack(t, position, 0, nullValue, 1)
		result, _ := f.tickUntilResolved(t, 101, 1)
		require.ErrorIs(t, result.Err, errSnapshotServiceFailed)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateActive, "snapshot complete")
	})

	t.Run("prepare pending wait", func(t *testing.T) {
		f := newTakeLeader(t, 1)
		p := newModuleStateProbe(t, f.agent)
		f.request(t, 100)
		_, prepared, err := f.agent.PrepareForNewLeadership(0, 101)
		require.NoError(t, err)
		require.True(t, prepared)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateActive, "snapshot wait retired")
	})

	t.Run("prepare take", func(t *testing.T) {
		f, p, _ := probePoppedTake(t)
		_, prepared, err := f.agent.PrepareForNewLeadership(0, 102)
		require.NoError(t, err)
		require.True(t, prepared)
		require.Nil(t, f.agent.snapshotTake)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateActive, "snapshot cancelled")
	})

	t.Run("join with take", func(t *testing.T) {
		f, p, _ := probePoppedTake(t)
		joined, err := f.agent.JoinLogAsFollower(FollowerLogJoinParams{}, 102)
		require.NoError(t, err)
		require.False(t, joined)
		require.Nil(t, f.agent.snapshotTake)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateActive, "snapshot cancelled")
	})

	t.Run("join with wait", func(t *testing.T) {
		f := newTakeLeader(t, 1)
		p := newModuleStateProbe(t, f.agent)
		f.request(t, 100)
		mark := p.mark()

		joined, err := f.agent.JoinLogAsFollower(FollowerLogJoinParams{}, 101)
		require.NoError(t, err)
		require.False(t, joined)
		p.requireUnchangedSince(t, f.agent, ModuleStateSnapshot, mark)
	})

	t.Run("close snapshot take", func(t *testing.T) {
		f, p, _ := probePoppedTake(t)
		f.agent.CloseSnapshotTake()
		require.Nil(t, f.agent.snapshotTake)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateActive, "snapshot cancelled")
	})

	t.Run("mark closed before take close", func(t *testing.T) {
		f, p, _ := probePoppedTake(t)
		f.agent.MarkClosed()
		f.agent.CloseSnapshotTake()
		require.Nil(t, f.agent.snapshotTake)
		p.requireTransition(t, f.agent, ModuleStateSnapshot, ModuleStateClosed, "closed")
		wantStates := []ModuleState{ModuleStateActive, ModuleStateSnapshot, ModuleStateClosed}
		require.Equal(t, wantStates, p.counter.states)
	})

	t.Run("build replaces take without wait", func(t *testing.T) {
		f := newTakeLeader(t, 0)
		p := newModuleStateProbe(t, f.agent)
		position := f.request(t, 100)
		f.agent.snapshotTake = &snapshotTake{logPosition: position}
		f.agent.snapshotWaitPosition = 0
		mark := p.mark()

		f.agent.buildSnapshotTake(position)
		require.NotNil(t, f.agent.snapshotTake)
		p.requireUnchangedSince(t, f.agent, ModuleStateSnapshot, mark)
	})

	t.Run("ack failure before pop", func(t *testing.T) {
		f := newTakeLeader(t, 1)
		p := newModuleStateProbe(t, f.agent)
		position := f.request(t, 100)
		f.ack(t, position, 5, 77, 0)
		f.agent.DoWork(101)
		require.ErrorIs(t, f.agent.ServiceAckFailure(), ErrServiceAckOutOfSequence)
		require.Nil(t, f.agent.snapshotTake)
		p.requireTransition(
			t, f.agent, ModuleStateSnapshot, ModuleStateClosed, "unexpected termination",
		)
	})

	t.Run("ack failure after pop", func(t *testing.T) {
		f, p, position := probePoppedTake(t)

		f.ack(t, position, 5, 77, 0)
		f.agent.DoWork(102)
		require.Error(t, f.agent.ServiceAckFailure())
		require.NotNil(t, f.agent.snapshotTake)
		p.requireTransition(
			t, f.agent, ModuleStateSnapshot, ModuleStateClosed, "unexpected termination",
		)

		require.Equal(t, 0, f.agent.DoWork(103))
		_, resolved := f.agent.LastSnapshotTake()
		require.False(t, resolved)
	})

	t.Run("take failure that ends the member", func(t *testing.T) {
		f, p, _ := probeAckedSnapshot(t, 1)
		f.agent.errorHandler = f.agent.TerminateOnError
		f.archive.startScript = func(s *fakeSnapshotStart) { s.offerErr = errTerminationFirst }

		f.agent.DoWork(101)
		p.requireTransition(
			t, f.agent, ModuleStateSnapshot, ModuleStateClosed, "unexpected termination",
		)
		wantStates := []ModuleState{ModuleStateActive, ModuleStateSnapshot, ModuleStateClosed}
		require.Equal(t, wantStates, p.counter.states)
		require.Len(t, p.changes, 2)
	})

	t.Run("terminate stores the caller reason", func(t *testing.T) {
		a := NewAgent(AgentConfig{})
		p := newModuleStateProbe(t, a)
		a.terminate(nil, true, "planned reason")
		p.requireTransition(t, a, ModuleStateActive, ModuleStateClosed, "planned reason")
		require.Equal(t, []ModuleState{ModuleStateActive, ModuleStateClosed}, p.counter.states)
	})

	t.Run("fatal mark from active", func(t *testing.T) {
		cases := []struct {
			name string
			mark func(t *testing.T, a *Agent)
		}{
			{
				name: "append rank failure",
				mark: func(t *testing.T, a *Agent) {
					armAppendRankTimeout(a)
					a.resolveAppendPosition(0)
					a.resolveAppendPosition(100)
					require.Error(t, a.AppendRankFailure())
				},
			},
			{
				name: "service ack failure",
				mark: func(t *testing.T, a *Agent) {
					q := attachQueueServiceIngress(a)
					enqueueServiceAck(t, q, 10, 1, 0)
					require.Equal(t, 1, a.pollServiceControl())
					require.Error(t, a.ServiceAckFailure())
				},
			},
			{
				name: "service publish failure",
				mark: func(t *testing.T, a *Agent) {
					proxy, pub := boundServiceProxy(t)
					require.NoError(t, pub.Close())
					a.SetServiceProxy(proxy)
					a.SetLocalLogStream("aeron:ipc", 100)
					require.NoError(t, a.JoinLogAsLeader(1, 10, 1, false))
					require.Error(t, a.ServicePublishFailure())
				},
			},
			{
				name: "timer failure",
				mark: func(t *testing.T, a *Agent) {
					a.timers = newSmallTimerService(testWheelCapacity)
					for id := int64(1); id <= 3; id++ {
						a.scheduleTimerOrMarkFailure(id, 0)
					}
					require.Error(t, a.TimerFailure())
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				a := NewAgent(AgentConfig{Role: RoleFollower, ServiceCount: 1})
				p := newModuleStateProbe(t, a)
				tc.mark(t, a)
				p.requireTransition(
					t, a, ModuleStateActive, ModuleStateClosed, "unexpected termination",
				)
				require.Equal(t, []ModuleState{ModuleStateActive, ModuleStateClosed}, p.counter.states)
				require.Len(t, p.changes, 1)
				require.ErrorIs(t, a.TerminationError(), ErrTerminated)

				mark := p.mark()
				a.MarkClosed()
				p.requireUnchangedSince(t, a, ModuleStateClosed, mark)
			})
		}
	})

	t.Run("mark closed", func(t *testing.T) {
		f := newTakeLeader(t, 0)
		p := newModuleStateProbe(t, f.agent)
		wantStates := []ModuleState{ModuleStateActive, ModuleStateClosed}
		f.agent.MarkClosed()
		p.requireTransition(t, f.agent, ModuleStateActive, ModuleStateClosed, "closed")
		require.Equal(t, wantStates, p.counter.states)

		f.agent.MarkClosed()
		require.Equal(t, wantStates, p.counter.states)
		require.Len(t, p.changes, 1)

		pre := f.agent.logPublisher.Position()
		err := f.agent.RequestSnapshot(200)
		require.ErrorIs(t, err, errSnapshotModuleNotActive)
		require.Equal(t, pre, f.agent.logPublisher.Position())
		require.False(t, f.agent.HasPendingSnapshotAction())
		require.Equal(t, 0, f.agent.PendingServiceAckWaits())
		p.requireState(t, f.agent, ModuleStateClosed)
		require.Equal(t, wantStates, p.counter.states)
		require.Len(t, p.changes, 1)
	})

	t.Run("control toggle suspend and resume", func(t *testing.T) {
		a, _ := settledLeaderWithLog(t, 0)
		p := newModuleStateProbe(t, a)
		control := newFakeToggle(int64(ControlToggleSuspend))
		a.SetClusterControlToggle(control)

		require.Equal(t, 1, a.slowTickWork(100))
		p.requireTransition(t, a, ModuleStateActive, ModuleStateSuspended, "ClusterControl.SUSPEND")

		require.True(t, ControlToggleResume.Toggle(control))
		require.Equal(t, 1, a.slowTickWork(101))
		p.requireTransition(t, a, ModuleStateSuspended, ModuleStateActive, "ClusterControl.RESUME")
		require.Len(t, p.changes, 2)
	})

	t.Run("replayed suspend and resume", func(t *testing.T) {
		const consumePos int64 = 4096
		a := NewAgent(AgentConfig{Role: RoleFollower, ServiceCount: 1})
		p := newModuleStateProbe(t, a)
		fields := snapshotActionFields(consumePos)

		fields.Action = clusterwire.ClusterAction_SUSPEND
		a.handleClusterAction(fields, consumePos)
		p.requireTransition(
			t, a, ModuleStateActive, ModuleStateSuspended, "ReplayClusterAction.SUSPENDED",
		)

		fields.Action = clusterwire.ClusterAction_RESUME
		a.handleClusterAction(fields, consumePos)
		p.requireTransition(t, a, ModuleStateSuspended, ModuleStateActive, "ReplayClusterAction.ACTIVE")
		require.Len(t, p.changes, 2)
	})

	t.Run("rollback", func(t *testing.T) {
		a, _ := settledLeaderWithLog(t, 0)
		p := newModuleStateProbe(t, a)
		a.SetClusterControlToggle(newFakeToggle(int64(ControlToggleSuspend)))
		require.Equal(t, 1, a.slowTickWork(100))
		p.requireState(t, a, ModuleStateSuspended)

		_, prepared, err := a.PrepareForNewLeadership(0, 101)
		require.NoError(t, err)
		require.True(t, prepared)
		p.requireTransition(t, a, ModuleStateSuspended, ModuleStateActive, "rollback")
		require.Len(t, p.changes, 2)
	})

	recordingElection := func(ElectionSeed) ElectionRunner {
		return &recordingRunner{}
	}

	t.Run("election and role", func(t *testing.T) {
		a := NewAgent(AgentConfig{Role: RoleLeader})
		p := newModuleStateProbe(t, a)
		a.SetElectionFactory(recordingElection)
		require.Equal(t, 1, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false))
		a.ElectionComplete(10)
		a.RoleSet(RoleLeader)
		a.RoleSet(RoleFollower)
		p.requireWiredActive(t, a)
	})

	t.Run("election keeps snapshot", func(t *testing.T) {
		f := newTakeLeader(t, 1)
		p := newModuleStateProbe(t, f.agent)
		f.request(t, 100)
		p.requireState(t, f.agent, ModuleStateSnapshot)
		mark := p.mark()
		f.agent.SetElectionFactory(recordingElection)
		require.Equal(t, 1, f.agent.enterElection(ElectionTriggerHigherTerm, false))
		f.agent.ElectionComplete(20)
		f.agent.RoleSet(RoleLeader)
		p.requireUnchangedSince(t, f.agent, ModuleStateSnapshot, mark)
	})
}
