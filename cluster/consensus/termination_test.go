//go:build testhooks

package consensus

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	errTerminationFirst  = errors.New("first cause")
	errTerminationSecond = errors.New("second cause")
)

func TestTerminationError(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("%w: code=7", errTerminationFirst)
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "error text with a cause",
			run: func(t *testing.T) {
				err := &TerminationError{Cause: wrapped}
				require.EqualError(t, err, "consensus: member terminated: first cause: code=7")
			},
		},
		{
			name: "error text without a cause",
			run: func(t *testing.T) {
				err := &TerminationError{Expected: true}
				require.EqualError(t, err, "consensus: member terminated")
			},
		},
		{
			name: "matches ErrTerminated",
			run: func(t *testing.T) {
				require.ErrorIs(t, &TerminationError{}, ErrTerminated)
				require.ErrorIs(t, &TerminationError{Cause: wrapped}, ErrTerminated)
			},
		},
		{
			name: "matches a wrapped cause sentinel",
			run: func(t *testing.T) {
				err := &TerminationError{Cause: wrapped}
				require.ErrorIs(t, err, errTerminationFirst)
				require.NotErrorIs(t, err, errTerminationSecond)
			},
		},
		{
			name: "errors.As reaches the value through a wrap",
			run: func(t *testing.T) {
				want := &TerminationError{Cause: wrapped, Expected: true}
				var got *TerminationError
				require.ErrorAs(t, fmt.Errorf("run: %w", want), &got)
				require.Same(t, want, got)
				require.True(t, got.Expected)
			},
		},
		{
			name: "Unwrap returns the cause",
			run: func(t *testing.T) {
				require.Same(t, wrapped, (&TerminationError{Cause: wrapped}).Unwrap())
				require.NoError(t, (&TerminationError{}).Unwrap())
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

func agentWithReports(cfg AgentConfig) (*Agent, *[]error) {
	reported := new([]error)
	cfg.Commit = new(atomic.Int64)
	cfg.ErrorHandler = func(err error) { *reported = append(*reported, err) }
	return NewAgent(cfg), reported
}

// armAppendRankTimeout scripts two not-ready reads with a 100ns timeout. Reads
// at 0 and 100 mark the append-rank failure.
func armAppendRankTimeout(a *Agent) *scriptedAppendSource {
	source := &scriptedAppendSource{
		results: []scriptedAppendResult{{0, false}, {0, false}},
	}
	a.appendSource = source
	a.appendNotReadyTimeoutNs = 100
	a.SetLogRecordingControl(&fakeLogRecordingControl{active: true})
	return source
}

func requireTerminated(t fragmentRequireT, a *Agent, cause error, expected bool) {
	t.Helper()
	var termination *TerminationError
	require.ErrorAs(t, a.TerminationError(), &termination)
	if cause == nil {
		require.NoError(t, termination.Cause)
	} else {
		require.Same(t, cause, termination.Cause)
	}
	require.Equal(t, expected, termination.Expected)
	require.ErrorIs(t, a.TerminationError(), ErrTerminated)
	require.Equal(t, ModuleStateClosed, a.ModuleState())
}

func TestAgent_terminate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, a *Agent, reported *[]error)
	}{
		{
			name: "returns untyped nil before the end",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				err := a.TerminationError()
				require.NoError(t, err, "a typed nil pointer would read non-nil")
				require.Equal(t, ModuleStateActive, a.ModuleState())
				require.Empty(t, *reported)
			},
		},
		{
			name: "failMember reports once and ends the member",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.failMember(errTerminationFirst)
				require.Equal(t, []error{errTerminationFirst}, *reported)
				requireTerminated(t, a, errTerminationFirst, false)
			},
		},
		{
			name: "first cause wins and a second cause does not report",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.failMember(errTerminationFirst)
				a.failMember(errTerminationSecond)
				a.terminate(errTerminationSecond, true, "second")
				require.Equal(t, []error{errTerminationFirst}, *reported)
				requireTerminated(t, a, errTerminationFirst, false)
				require.NotErrorIs(t, a.TerminationError(), errTerminationSecond)
			},
		},
		{
			name: "expected terminate does not report",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.terminate(errTerminationFirst, true, "planned")
				require.Empty(t, *reported)
				requireTerminated(t, a, errTerminationFirst, true)

				a.failMember(errTerminationSecond)
				require.Empty(t, *reported)
				requireTerminated(t, a, errTerminationFirst, true)
			},
		},
		{
			name: "expected terminate may leave the cause nil",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.terminate(nil, true, "planned")
				require.Empty(t, *reported)
				requireTerminated(t, a, nil, true)
				require.EqualError(t, a.TerminationError(), "consensus: member terminated")
			},
		},
		{
			name: "DoWork returns 0 after the end",
			run: func(t *testing.T, a *Agent, _ *[]error) {
				election := &orderRunner{n: 3}
				a.SetElection(election)
				require.Equal(t, 3, a.DoWork(1))
				a.failMember(errTerminationFirst)
				require.Equal(t, 0, a.DoWork(2))
				require.Equal(t, 1, election.calls)
			},
		},
		{
			name: "MarkClosed after the end keeps the value",
			run: func(t *testing.T, a *Agent, _ *[]error) {
				a.failMember(errTerminationFirst)
				a.MarkClosed()
				requireTerminated(t, a, errTerminationFirst, false)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a, reported := agentWithReports(AgentConfig{})
			tt.run(t, a, reported)
		})
	}
}

type terminatingRunner struct {
	orderRunner
	agent *Agent
	cause error
}

func (r *terminatingRunner) DoWork(nowNs int64) int {
	work := r.orderRunner.DoWork(nowNs)
	r.agent.failMember(r.cause)
	return work
}

// queueRetriableJoinLog leaves a pending JoinLog that the next retry publishes.
func queueRetriableJoinLog(t *testing.T, a *Agent) {
	t.Helper()
	a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
	require.Equal(t, 0, a.publishOrQueueJoinLog(sampleJoinLogFields()))
	require.True(t, a.HasPendingJoinLog())
	connected, _ := boundServiceProxy(t)
	a.SetServiceProxy(connected)
}

func TestAgent_doWorkStopsAtTermination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "service control work skips the election step",
			run: func(t *testing.T) {
				const startPos int64 = 64
				a, reported := agentWithReports(AgentConfig{ServiceCount: 1})
				q := attachQueueServiceIngress(a)
				election := &orderRunner{n: 5}
				a.SetElection(election)
				starts := 0
				require.NoError(t, a.AwaitServiceStart(startPos, func() { starts++ }))

				// Phase 1: a tick before the ack runs the election.
				require.Equal(t, 5, a.DoWork(1))
				require.Equal(t, 1, election.calls)

				// Phase 2: the NULL_VALUE start ack ends the member in the same tick.
				enqueueServiceAckRelevant(t, q, startPos, 0, nullValue, 0)
				require.Equal(t, 2, a.DoWork(2), "service control poll and work")
				require.Equal(t, 1, starts)
				require.Equal(t, 1, election.calls, "the election step does not run")
				require.Len(t, *reported, 1)
				require.ErrorIs(t, (*reported)[0], ErrServiceStartFailed)
				requireTerminated(t, a, (*reported)[0], false)

				// Phase 3: DoWork returns 0 after the end.
				require.Equal(t, 0, a.DoWork(3))
				require.Equal(t, 1, election.calls)
				require.Len(t, *reported, 1)
			},
		},
		{
			name: "election step skips the JoinLog retry",
			run: func(t *testing.T) {
				a, reported := agentWithReports(AgentConfig{})
				queueRetriableJoinLog(t, a)
				election := &terminatingRunner{
					orderRunner: orderRunner{n: 5},
					agent:       a,
					cause:       errTerminationFirst,
				}
				a.SetElection(election)

				// Phase 1: the election ends the member. The retry does not run.
				require.Equal(t, 5, a.DoWork(1), "election work only")
				require.True(t, a.HasPendingJoinLog())
				require.Equal(t, []error{errTerminationFirst}, *reported)
				requireTerminated(t, a, errTerminationFirst, false)

				// Phase 2: DoWork returns 0 after the end.
				require.Equal(t, 0, a.DoWork(2))
				require.Equal(t, 1, election.calls)
				require.True(t, a.HasPendingJoinLog())
			},
		},
		{
			name: "service stop skips the JoinLog retry",
			run: func(t *testing.T) {
				a, reported := agentWithReports(AgentConfig{ServiceCount: 1})
				queueRetriableJoinLog(t, a)
				failure := errors.New("service: boom")
				require.NoError(t, a.SetServiceLiveness(0, &fakeServiceLiveness{failure: failure}))

				// Phase 1: the due slow tick ends the member. The retry does not run.
				require.Equal(t, 1, a.DoWork(0))
				require.True(t, a.HasPendingJoinLog())
				requireServiceStopped(t, a, *reported, failure)

				// Phase 2: DoWork returns 0 after the end.
				require.Equal(t, 0, a.DoWork(slowTickIntervalNs))
				require.True(t, a.HasPendingJoinLog())
				require.Len(t, *reported, 1)
			},
		},
		{
			name: "consensus work skips the JoinLog retry",
			run: func(t *testing.T) {
				a, reported := agentWithReports(AgentConfig{})
				source := armAppendRankTimeout(a)

				// Phase 1: a not-ready read before the timeout does not end the member.
				require.Equal(t, 0, a.DoWork(0))
				require.NoError(t, a.TerminationError())

				// Phase 2: the timeout ends the member. The retry does not run.
				queueRetriableJoinLog(t, a)
				require.Equal(t, 0, a.DoWork(100))
				require.True(t, a.HasPendingJoinLog())
				require.ErrorIs(t, a.AppendRankFailure(), ErrAppendPositionUnavailable)
				require.Len(t, *reported, 1)
				requireTerminated(t, a, a.AppendRankFailure(), false)

				// Phase 3: DoWork returns 0 after the end.
				require.Equal(t, 0, a.DoWork(200))
				require.Equal(t, 2, source.reads)
				require.True(t, a.HasPendingJoinLog())
			},
		},
		{
			name: "take failure ends the member",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 1)
				record := f.agent.errorHandler
				f.agent.errorHandler = func(err error) {
					record(err)
					f.agent.TerminateOnError(err)
				}
				f.archive.startScript = func(s *fakeSnapshotStart) { s.offerErr = errTerminationFirst }
				position := f.request(t, 100)
				f.ack(t, position, 0, 77, 0)
				election := &orderRunner{n: 5}
				f.agent.SetElection(election)

				// Phase 1: the take step ends the member.
				f.agent.DoWork(101)
				require.Zero(t, election.calls)
				require.Len(t, f.reported, 1)
				report := f.reported[0]
				require.ErrorIs(t, report.err, errSnapshotRecordingStart)
				require.Equal(t, ModuleStateSnapshot, report.moduleState)
				requireTerminated(t, f.agent, report.err, false)

				// Phase 2: DoWork returns 0 after the end.
				require.Equal(t, 0, f.agent.DoWork(102))
				require.Zero(t, election.calls)
				require.Len(t, f.reported, 1)
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

type fakeArchiveConnection struct {
	connected bool
	reads     int
}

func (c *fakeArchiveConnection) IsConnected() bool {
	c.reads++
	return c.connected
}

// armLeaderQuietElection makes the next slowTickWork install an election.
func armLeaderQuietElection(a *Agent) {
	a.leaderHeartbeatTimeoutNs = 1
	a.SetElectionFactory(func(ElectionSeed) ElectionRunner { return &orderRunner{} })
}

func TestAgent_lostArchive(t *testing.T) {
	t.Parallel()

	const deadlineNs int64 = 100
	tests := []struct {
		name string
		run  func(t *testing.T, a *Agent, reported *[]error)
	}{
		{
			name: "false at a due slow tick ends the member",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				armLeaderQuietElection(a)
				a.SetArchiveConnection(&fakeArchiveConnection{})
				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				require.Equal(t, []error{ErrLocalArchiveDisconnected}, *reported)
				requireTerminated(t, a, ErrLocalArchiveDisconnected, false)
				require.Equal(t, deadlineNs, a.slowTickDeadlineNs)
				require.Zero(t, a.ElectionCount(), "the tick skips slowTickWork")
				require.Equal(t, ElectionTriggerNone, a.LastElectionTrigger())
			},
		},
		{
			name: "true at a due slow tick runs slowTickWork",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				armLeaderQuietElection(a)
				connection := &fakeArchiveConnection{connected: true}
				a.SetArchiveConnection(connection)
				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				require.Equal(t, 1, connection.reads)
				require.Equal(t, int64(1), a.ElectionCount())
				require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
				require.Empty(t, *reported)
				require.NoError(t, a.TerminationError())
			},
		},
		{
			name: "false before the deadline does nothing",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				connection := &fakeArchiveConnection{}
				a.SetArchiveConnection(connection)
				require.Equal(t, 0, a.slowTickIfDue(deadlineNs-1))
				require.Zero(t, connection.reads)
				require.Empty(t, *reported)
				require.NoError(t, a.TerminationError())
			},
		},
		{
			name: "nil connection does nothing",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.SetArchiveConnection(&fakeArchiveConnection{})
				a.SetArchiveConnection(nil)
				a.slowTickIfDue(deadlineNs)
				require.Empty(t, *reported)
				require.NoError(t, a.TerminationError())
			},
		},
		{
			name: "false during a live election ends the member through DoWork",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				election := &orderRunner{n: 5}
				a.SetElection(election)
				ingress := &countingConsensusIngress{}
				a.SetIngress(ingress)
				connection := &fakeArchiveConnection{connected: true}
				a.SetArchiveConnection(connection)

				// Phase 1: a connected tick runs the ingress poll and the election.
				require.Equal(t, 5, a.DoWork(deadlineNs))
				require.Equal(t, 1, ingress.polls)
				require.Equal(t, 1, election.calls)

				// Phase 2: the lost archive skips every later step.
				connection.connected = false
				require.Equal(t, 1, a.DoWork(a.slowTickDeadlineNs))
				require.Equal(t, 1, ingress.polls)
				require.Equal(t, 1, election.calls)
				require.Equal(t, []error{ErrLocalArchiveDisconnected}, *reported)
				requireTerminated(t, a, ErrLocalArchiveDisconnected, false)

				// Phase 3: DoWork returns 0 after the end.
				require.Equal(t, 0, a.DoWork(a.slowTickDeadlineNs))
				require.Equal(t, 1, ingress.polls)
				require.Equal(t, 1, election.calls)
				require.Len(t, *reported, 1)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a, reported := agentWithReports(AgentConfig{})
			a.slowTickDeadlineNs = deadlineNs
			tt.run(t, a, reported)
		})
	}
}

func TestAgent_TerminateOnError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, a *Agent, reported *[]error)
	}{
		{
			name: "the call does not report",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.TerminateOnError(errTerminationFirst)
				require.Empty(t, *reported)
				requireTerminated(t, a, errTerminationFirst, false)
			},
		},
		{
			name: "the first cause wins over a later mark",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.TerminateOnError(errTerminationFirst)
				a.TerminateOnError(errTerminationSecond)
				a.failMember(errTerminationSecond)
				require.Empty(t, *reported)
				requireTerminated(t, a, errTerminationFirst, false)
			},
		},
		{
			name: "a nil cause is a silent no-op",
			run: func(t *testing.T, a *Agent, reported *[]error) {
				a.TerminateOnError(nil)
				require.Empty(t, *reported)
				require.NoError(t, a.TerminationError())
				require.Equal(t, ModuleStateActive, a.ModuleState())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a, reported := agentWithReports(AgentConfig{})
			tt.run(t, a, reported)
		})
	}
}

// fakeServiceLiveness stubs one service for the slow tick. reads counts
// ServiceFailure calls.
type fakeServiceLiveness struct {
	failure    error
	terminated bool
	closed     bool
	reads      int
}

func (f *fakeServiceLiveness) ServiceFailure() error {
	f.reads++
	return f.failure
}

func (f *fakeServiceLiveness) IsTerminated() bool { return f.terminated }

func (f *fakeServiceLiveness) IsClosed() bool { return f.closed }

// requireServiceStopped requires one ErrServiceStopped report. The report must
// end the member unexpectedly. A non-nil cause must sit in its chain.
func requireServiceStopped(t testing.TB, a *Agent, reported []error, cause error) {
	t.Helper()
	require.Len(t, reported, 1)
	require.ErrorIs(t, reported[0], ErrServiceStopped)

	if cause != nil {
		require.ErrorIs(t, reported[0], cause)
	}

	requireTerminated(t, a, reported[0], false)
}

// requireServiceUnread requires that the slow tick did not read source. The
// member must not end.
func requireServiceUnread(t testing.TB, a *Agent, reported []error, source *fakeServiceLiveness) {
	t.Helper()
	require.Zero(t, source.reads)
	require.Empty(t, reported)
	require.NoError(t, a.TerminationError())
}

func TestAgent_SetServiceLiveness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "negative id",
			run: func(t *testing.T) {
				a, _ := agentWithReports(AgentConfig{ServiceCount: 1})
				err := a.SetServiceLiveness(-1, &fakeServiceLiveness{})
				require.EqualError(t, err, "consensus: service id -1 out of range")
			},
		},
		{
			name: "id at ServiceCount",
			run: func(t *testing.T) {
				a, _ := agentWithReports(AgentConfig{ServiceCount: 1})
				err := a.SetServiceLiveness(1, &fakeServiceLiveness{})
				require.EqualError(t, err, "consensus: service id 1 out of range")
			},
		},
		{
			name: "any id when ServiceCount is 0",
			run: func(t *testing.T) {
				a, _ := agentWithReports(AgentConfig{})
				err := a.SetServiceLiveness(0, &fakeServiceLiveness{})
				require.EqualError(t, err, "consensus: service id 0 out of range")
			},
		},
		{
			name: "valid id",
			run: func(t *testing.T) {
				a, _ := agentWithReports(AgentConfig{ServiceCount: 1})
				source := &fakeServiceLiveness{}
				require.NoError(t, a.SetServiceLiveness(0, source))
				require.Same(t, source, a.serviceLiveness[0])
			},
		},
		{
			name: "nil clears the slot",
			run: func(t *testing.T) {
				a, _ := agentWithReports(AgentConfig{ServiceCount: 1})
				source := &fakeServiceLiveness{}
				require.NoError(t, a.SetServiceLiveness(0, source))
				require.NoError(t, a.SetServiceLiveness(0, nil))
				require.Nil(t, a.serviceLiveness[0])
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

func TestAgent_serviceStop(t *testing.T) {
	t.Parallel()

	const deadlineNs int64 = 100
	failure := errors.New("service: boom")

	newAgent := func(t testing.TB, serviceCount int) (*Agent, *[]error, *fakeServiceLiveness) {
		t.Helper()
		a, reported := agentWithReports(AgentConfig{ServiceCount: serviceCount})
		a.slowTickDeadlineNs = deadlineNs
		source := &fakeServiceLiveness{failure: failure}
		return a, reported, source
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "failure at a due slow tick ends the member",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				armLeaderQuietElection(a)
				require.NoError(t, a.SetServiceLiveness(0, source))

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				requireServiceStopped(t, a, *reported, failure)
				require.Zero(t, a.ElectionCount(), "the tick skips slowTickWork")
				require.Equal(t, ElectionTriggerNone, a.LastElectionTrigger())
			},
		},
		{
			name: "termination stop outside TERMINATING ends the member",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				source.failure = nil
				source.terminated = true
				require.NoError(t, a.SetServiceLiveness(0, source))

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				requireServiceStopped(t, a, *reported, nil)
				require.ErrorContains(t, (*reported)[0], "service 0 stopped at its termination position")
			},
		},
		{
			name: "close ends the member",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				source.failure = nil
				source.closed = true
				require.NoError(t, a.SetServiceLiveness(0, source))

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				requireServiceStopped(t, a, *reported, nil)
				require.ErrorContains(t, (*reported)[0], "service 0 closed")
			},
		},
		{
			name: "failure wins over a close",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				source.closed = true
				require.NoError(t, a.SetServiceLiveness(0, source))

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				requireServiceStopped(t, a, *reported, failure)
			},
		},
		{
			name: "running source does nothing",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				source.failure = nil
				armLeaderQuietElection(a)
				require.NoError(t, a.SetServiceLiveness(0, source))

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				require.Empty(t, *reported)
				require.NoError(t, a.TerminationError())
				require.Equal(t, 1, source.reads)
				require.Equal(t, int64(1), a.ElectionCount())
				require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
			},
		},
		{
			name: "TERMINATING skips the read",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				require.NoError(t, a.SetServiceLiveness(0, source))
				a.setModuleState(ModuleStateTerminating, "test")

				require.Equal(t, 0, a.slowTickIfDue(deadlineNs))
				requireServiceUnread(t, a, *reported, source)
				require.Equal(t, ModuleStateTerminating, a.ModuleState())
			},
		},
		{
			name: "CLOSED skips the read",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				require.NoError(t, a.SetServiceLiveness(0, source))
				a.MarkClosed()

				require.Equal(t, 0, a.slowTickIfDue(deadlineNs))
				requireServiceUnread(t, a, *reported, source)
				require.Equal(t, ModuleStateClosed, a.ModuleState())
			},
		},
		{
			name: "start wait queued skips the read",
			run: func(t *testing.T) {
				const startPos int64 = 64
				a, reported, source := newAgent(t, 1)
				require.NoError(t, a.SetServiceLiveness(0, source))
				require.NoError(t, a.AwaitServiceStart(startPos, func() {}))

				// Phase 1: a due tick skips the read while the start wait leads.
				require.Equal(t, 0, a.slowTickIfDue(deadlineNs))
				requireServiceUnread(t, a, *reported, source)

				// Phase 2: the ack arrives on a due tick. The slow tick runs before the
				// ack clears the start wait.
				a.slowTickDeadlineNs = deadlineNs
				q := attachQueueServiceIngress(a)
				enqueueServiceAck(t, q, startPos, 0, 0)
				a.DoWork(deadlineNs)
				requireServiceUnread(t, a, *reported, source)
				require.Nil(t, a.serviceStartHandler)

				// Phase 3: the next due slow tick ends the member.
				require.Equal(t, 1, a.DoWork(deadlineNs+slowTickIntervalNs))
				requireServiceStopped(t, a, *reported, failure)
				require.NotErrorIs(t, a.TerminationError(), ErrServiceStartFailed)
			},
		},
		{
			name: "before the deadline does nothing",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				require.NoError(t, a.SetServiceLiveness(0, source))

				require.Equal(t, 0, a.slowTickIfDue(deadlineNs-1))
				requireServiceUnread(t, a, *reported, source)
			},
		},
		{
			name: "nil slot does nothing",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				require.NoError(t, a.SetServiceLiveness(0, source))
				require.NoError(t, a.SetServiceLiveness(0, nil))

				require.Equal(t, 0, a.slowTickIfDue(deadlineNs))
				requireServiceUnread(t, a, *reported, source)
			},
		},
		{
			name: "second slot stops first",
			run: func(t *testing.T) {
				a, reported, stopped := newAgent(t, 2)
				running := &fakeServiceLiveness{}
				require.NoError(t, a.SetServiceLiveness(0, running))
				require.NoError(t, a.SetServiceLiveness(1, stopped))

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				requireServiceStopped(t, a, *reported, failure)
				require.ErrorContains(t, (*reported)[0], "service 1")
				require.Equal(t, 1, running.reads)
				require.Equal(t, 1, stopped.reads)
			},
		},
		{
			name: "lost archive wins in the same tick",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				require.NoError(t, a.SetServiceLiveness(0, source))
				a.SetArchiveConnection(&fakeArchiveConnection{})

				require.Equal(t, 1, a.slowTickIfDue(deadlineNs))
				require.Equal(t, []error{ErrLocalArchiveDisconnected}, *reported)
				requireTerminated(t, a, ErrLocalArchiveDisconnected, false)
				require.Zero(t, source.reads)
			},
		},
		{
			name: "during a live election ends the member through DoWork",
			run: func(t *testing.T) {
				a, reported, source := newAgent(t, 1)
				source.failure = nil
				election := &orderRunner{n: 5}
				a.SetElection(election)
				ingress := &countingConsensusIngress{}
				a.SetIngress(ingress)
				require.NoError(t, a.SetServiceLiveness(0, source))

				// Phase 1: a running service lets the ingress poll and the election run.
				require.Equal(t, 5, a.DoWork(deadlineNs))
				require.Equal(t, 1, ingress.polls)
				require.Equal(t, 1, election.calls)
				require.Equal(t, 1, source.reads)

				// Phase 2: the slow tick ends the member. DoWork skips every later step.
				source.failure = failure
				require.Equal(t, 1, a.DoWork(a.slowTickDeadlineNs))
				require.Equal(t, 1, ingress.polls)
				require.Equal(t, 1, election.calls)
				requireServiceStopped(t, a, *reported, failure)

				// Phase 3: DoWork returns 0 after the end.
				require.Equal(t, 0, a.DoWork(a.slowTickDeadlineNs))
				require.Equal(t, 1, ingress.polls)
				require.Equal(t, 1, election.calls)
				require.Len(t, *reported, 1)
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
