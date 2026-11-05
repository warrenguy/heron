package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

func TestHandleInit_singleNodeUpdateErrorStaysInit(t *testing.T) {
	t.Parallel()

	cfg, deps := singleNodeElectionDeps(t)
	deps.NodeState = &failingNodeState{store: deps.NodeState, err: errors.New("sync failed")}
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(1_000_000_000))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Nil(t, e.Leader())
	require.Equal(t, cfg.LeadershipTermID, e.LeadershipTermID())
}

func TestHandleInit_resetsReplay(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)

	// Keep prepare from collapsing the log-to-append gap on first init.
	cfg.IsNodeStartup = true
	spy := &spyLogReplay{position: testLogPosition, done: false}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	deps.Host = host
	deps.Handler = &recordingAgentHandler{
		appendPositionReturn: election.NullPosition,
	}
	e := election.NewElection(cfg, deps)

	// First init sets isFirstInit false.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Install a live helper without finishing it.
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)
	require.Positive(t, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	require.Equal(t, 0, spy.closeCalls)

	// Force Init. Non-first branch must Close the helper.
	election.SetStateForTest(e, election.ElectionStateInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs+2))
	require.Equal(t, 1, spy.closeCalls)

	// Pointer cleared: a second Close is a no-op.
	e.Close()
	require.Equal(t, 1, spy.closeCalls)
}

// Asserts re-entering Init clears the catchup join bound so the next install
// derives the bound from its own leadership term. Agent-side catchup teardown
// runs inside PrepareForNewLeadership and is covered by consensus tests.
func TestHandleInit_resetsCatchupJoinPosition(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	// First init sets isFirstInit false.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Install with the leader log ahead so the join bound is recorded.
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.LogPosition = cfg.AppendPosition + 100
	e.OnNewLeadershipTerm(fields)
	require.Equal(
		t,
		cfg.AppendPosition+100,
		election.CatchupJoinPositionForTest(e),
	)

	// Force Init. Non-first branch must clear the bound.
	election.SetStateForTest(e, election.ElectionStateInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.NullPosition, election.CatchupJoinPositionForTest(e))
}

func TestHandleInit_resetsReplication(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	// First init sets isFirstInit false.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	lr := &countingLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	// Installed helper still receives signals before the non-first init.
	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.Equal(t, 1, lr.signalCalls)

	election.SetStateForTest(e, election.ElectionStateInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, 1, lr.closeCalls)

	// Cleared helper receives no further signals.
	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.Equal(t, 1, lr.signalCalls)
}

func TestHandleInit_stopsLogReplicationBeforePrepare(t *testing.T) {
	t.Parallel()

	// newStoppingElection runs the first Init pass, then installs lr and
	// re-enters Init.
	newStoppingElection := func(
		t *testing.T,
		lr election.LogReplication,
	) (*election.Election, *recordingAgentHandler, int64) {
		t.Helper()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		handler := &recordingAgentHandler{appendPositionReturn: cfg.AppendPosition}
		deps.Handler = handler
		e := election.NewElection(cfg, deps)
		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, 1, handler.prepareCalls)
		election.SetLogReplicationForTest(e, lr)
		election.SetStateForTest(e, election.ElectionStateInit)
		return e, handler, clock.nowNs
	}

	t.Run("open helper holds init without prepare", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{work: 1, holdOpen: true}
		e, handler, t0 := newStoppingElection(t, lr)

		for i := range 2 {
			require.Equal(t, 1, e.DoWork(t0+1+int64(i)))
			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, 1, handler.prepareCalls)
			require.Equal(t, i+1, lr.closeCalls)
			require.Equal(t, i+1, lr.doWorkCalls)
		}

		// The stop resolves: prepare runs on the same pass.
		lr.holdOpen = false
		require.Equal(t, 1, e.DoWork(t0+3))
		require.Equal(t, 2, handler.prepareCalls)
		require.Equal(t, election.ElectionStateCanvass, e.State())
	})

	t.Run("closed helper lets prepare run on the same pass", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{}
		e, handler, t0 := newStoppingElection(t, lr)

		require.Equal(t, 1, e.DoWork(t0+1))
		require.Equal(t, 1, lr.closeCalls)
		require.Equal(t, 2, handler.prepareCalls)
		require.Equal(t, election.ElectionStateCanvass, e.State())
	})

	t.Run("stop error clears the helper and prepare runs", func(t *testing.T) {
		t.Parallel()

		errStop := errors.New("stop failed")
		lr := &countingLogReplicationForTest{err: errStop}
		e, handler, t0 := newStoppingElection(t, lr)

		require.Equal(t, 1, e.DoWork(t0+1))
		require.ErrorIs(t, e.LastError(), errStop)
		require.Equal(t, 2, handler.prepareCalls)
		require.Equal(t, election.ElectionStateCanvass, e.State())

		// The error cleared the helper. The next Init pass does not close it again.
		election.SetStateForTest(e, election.ElectionStateInit)
		require.Equal(t, 1, e.DoWork(t0+2))
		require.Equal(t, 1, lr.closeCalls)
		require.Equal(t, 3, handler.prepareCalls)
	})
}

func TestHandleInit_multiNodeEntersCanvass(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	observer := &recordingStateObserver{}
	deps.StateObserver = observer
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
	}, observer.states)
}

func TestHandleInit_singleNodeShortcut(t *testing.T) {
	t.Parallel()

	cfg, deps := singleNodeElectionDeps(t)
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(1_000_000_000))
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, e.ThisMemberID(), e.Leader().ID)
	require.Equal(t, int64(1), e.LeadershipTermID())
}

func TestHandleInit_nonStartupPreparesForNewLeadershipOnce(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	preparedAppend := cfg.AppendPosition + 1
	handler := &recordingAgentHandler{appendPositionReturn: preparedAppend}
	host := &recordingElectionHost{clock: clock, role: consensus.RoleLeader}
	deps.Handler = handler
	deps.Host = host
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, 1, handler.prepareCalls)
	require.Equal(t, cfg.LogPosition, handler.prepareLogPosition)
	require.Equal(t, clock.nowNs, handler.prepareNowNs)
	require.Equal(t, consensus.RoleFollower, host.role)
	require.Equal(t, preparedAppend, e.AppendPosition())
}

func TestHandleInit_prepareErrorFailsClosed(t *testing.T) {
	t.Parallel()

	const hostCommit int64 = 90
	prepareErr := errors.New("prepare stop failed")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	host := &recordingElectionHost{commitPosition: hostCommit}
	deps.Host = host
	handler := &recordingAgentHandler{
		appendPositionReturn: cfg.AppendPosition + 1,
		prepareErr:           prepareErr,
	}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	seedAppend := e.AppendPosition()
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, seedAppend, e.AppendPosition())
	require.Equal(t, prepareErr, e.LastError())
	require.Equal(t, hostCommit, e.LogPosition())
	require.Equal(t, 1, handler.prepareCalls)

	// Bounded attempt is retried rather than abandoned.
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, 2, handler.prepareCalls)
	require.Equal(t, seedAppend, e.AppendPosition())
	require.Equal(t, prepareErr, e.LastError())
	require.Equal(t, hostCommit, e.LogPosition())
}

func TestHandleInit_prepareSuccessAssignsAppend(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	preparedAppend := cfg.AppendPosition + 50
	handler := &recordingAgentHandler{appendPositionReturn: preparedAppend}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, preparedAppend, e.AppendPosition())
	require.Equal(t, 1, handler.prepareCalls)
}

func TestHandleInit_prepareNullPositionLeavesAppendUnchanged(t *testing.T) {
	t.Parallel()

	// Sentinel guard: NullPosition from prepare must not overwrite appendPosition.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	handler := &recordingAgentHandler{appendPositionReturn: election.NullPosition}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	seedAppend := e.AppendPosition()
	require.NotEqual(t, election.NullPosition, seedAppend)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, seedAppend, e.AppendPosition())
	require.Equal(t, 1, handler.prepareCalls)
}

func TestHandleInit_holdsWhilePreparePending(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = true
	spy := &spyLogReplay{position: testLogPosition, done: false}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	handler := &recordingAgentHandler{scriptPreparePending: true}
	deps.Host = host
	deps.Handler = handler
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, 0, handler.prepareCalls)

	election.SetStateForTest(e, election.ElectionStateLeaderReplay)
	require.Positive(t, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	require.Equal(t, 0, spy.closeCalls)

	election.SetStateForTest(e, election.ElectionStateInit)
	pub.Reset()
	require.Equal(t, 1, e.DoWork(clock.nowNs+2))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.True(t, handler.IsPrepareInProgress())
	require.Equal(t, 1, handler.prepareCalls)
	require.Equal(t, 1, spy.closeCalls)
	require.Empty(t, pub.Sent)

	// Re-poll of a pending stop reports zero work.
	require.Equal(t, 0, e.DoWork(clock.nowNs+3))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.True(t, handler.IsPrepareInProgress())
	require.Equal(t, 2, handler.prepareCalls)
	require.Equal(t, 1, spy.closeCalls)
	require.Empty(t, pub.Sent)
}

// electionPendingPrepare starts e with a handler that scripts prepare as
// pending. The first Init tick must stay in Init.
func electionPendingPrepare(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	nowNs int64,
) (*election.Election, *recordingAgentHandler) {
	t.Helper()

	handler := &recordingAgentHandler{scriptPreparePending: true}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.True(t, handler.IsPrepareInProgress())
	return e, handler
}

func TestHandleInit_exitsAfterPrepared(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	e, handler := electionPendingPrepare(t, cfg, deps, clock.nowNs)
	preparedAppend := cfg.AppendPosition + 50
	handler.appendPositionReturn = preparedAppend
	require.Equal(t, cfg.AppendPosition, e.AppendPosition())

	handler.scriptPreparePending = false
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, preparedAppend, e.AppendPosition())
	require.False(t, handler.IsPrepareInProgress())
	require.Equal(t, 2, handler.prepareCalls)
}

func TestHandleInit_singleNodeBecomesLeaderAfterPrepared(t *testing.T) {
	t.Parallel()

	cfg, deps := singleNodeElectionDeps(t)
	cfg.IsNodeStartup = false
	e, handler := electionPendingPrepare(t, cfg, deps, 1_000_000_000)
	require.Nil(t, e.Leader())

	handler.scriptPreparePending = false
	require.Equal(t, 1, e.DoWork(1_000_000_001))
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, e.ThisMemberID(), e.Leader().ID)
	require.False(t, handler.IsPrepareInProgress())
}

func TestHandleInit_pendingThenErrorRecallsPrepare(t *testing.T) {
	t.Parallel()

	prepareErr := errors.New("prepare stop failed")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	e, handler := electionPendingPrepare(t, cfg, deps, clock.nowNs)

	handler.scriptPreparePending = false
	handler.prepareErr = prepareErr
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.False(t, handler.IsPrepareInProgress())
	require.Equal(t, prepareErr, e.LastError())
	require.Equal(t, 2, handler.prepareCalls)

	handler.prepareErr = nil
	handler.appendPositionReturn = cfg.AppendPosition + 1
	require.Equal(t, 1, e.DoWork(clock.nowNs+2))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, 3, handler.prepareCalls)
	require.False(t, handler.IsPrepareInProgress())
}

func TestHandleInit_startupSkipsPrepare(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = true
	handler := &recordingAgentHandler{}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, 0, handler.prepareCalls)
	require.False(t, handler.IsPrepareInProgress())
}

// TestHandleError_pendingPrepareResumes asserts a direct HandleError keeps the
// prepare episode open on the handler. The next Init tick polls
// PrepareForNewLeadership.
func TestHandleError_pendingPrepareResumes(t *testing.T) {
	t.Parallel()

	prepareErr := errors.New("direct handle error")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	e, handler := electionPendingPrepare(t, cfg, deps, clock.nowNs)
	require.Equal(t, 1, handler.prepareCalls)

	e.HandleError(clock.nowNs+1, prepareErr)
	require.True(t, handler.IsPrepareInProgress())
	require.Equal(t, prepareErr, e.LastError())
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, 1, handler.prepareCalls)

	handler.scriptPreparePending = false
	handler.appendPositionReturn = cfg.AppendPosition + 1
	require.Equal(t, 1, e.DoWork(clock.nowNs+2))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, 2, handler.prepareCalls)
	require.False(t, handler.IsPrepareInProgress())
}
