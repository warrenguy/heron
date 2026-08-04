//go:build testhooks

package consensus

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
)

func TestCatchupPoll_pollsWhenLogAdapterImagePresent(t *testing.T) {
	adapter, image := boundLogAdapter(t)
	span := writeSessionMessageAtJoin(t, image, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	})

	commit := new(atomic.Int64)
	a := NewAgent(AgentConfig{
		Commit:     commit,
		LogAdapter: adapter,
	})

	work := a.CatchupPoll(span+32, 100)
	require.Positive(t, work)
	require.Equal(t, span, adapter.Position())
	require.Equal(t, adapter.Position(), a.CommitPosition())

	// work == 0 still ProposeMaxCommit from adapter position (prior consume).
	commit.Store(0)
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, 0, a.CatchupPoll(span+32, 101))
	require.Equal(t, adapter.Position(), a.CommitPosition())
}

func TestCatchupPoll_stubWhenNoImage(t *testing.T) {
	t.Run("nil adapter", func(t *testing.T) {
		a := NewAgent(AgentConfig{Commit: new(atomic.Int64)})
		require.Equal(t, 0, a.CatchupPoll(1<<20, 1))
		require.Equal(t, int64(0), a.CommitPosition())
	})
	t.Run("adapter without image", func(t *testing.T) {
		a := NewAgent(AgentConfig{
			Commit:     new(atomic.Int64),
			LogAdapter: NewLogAdapter(0),
		})
		require.Nil(t, a.logAdapter.Image())
		require.Equal(t, 0, a.CatchupPoll(1<<20, 1))
		require.Equal(t, int64(0), a.CommitPosition())
	})
}

// Bound 0: frame start is not strictly before bound; no work, commit stays.
func TestCatchupPoll_boundZeroYieldsNoWork(t *testing.T) {
	adapter, image := boundLogAdapter(t)
	_ = writeSessionMessageAtJoin(t, image, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	})
	a := NewAgent(AgentConfig{
		Commit:     new(atomic.Int64),
		LogAdapter: adapter,
	})
	require.Equal(t, 0, a.CatchupPoll(0, 1))
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(0), adapter.Position())
}

func TestNewLogReplication_defaultNil(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.NewLogReplication("leader", "response", 1, 2, 3))
}

func TestNewLogReplication_syntheticHelperInject(t *testing.T) {
	a := NewAgent(AgentConfig{})
	a.SetLogReplicationFactory(func(
		leaderArchiveEndpoint, responseArchiveEndpoint string,
		leaderRecordingID, stopPosition, nowNs int64,
	) LogReplication {
		require.Equal(t, "leader-ep", leaderArchiveEndpoint)
		require.Equal(t, "response-ep", responseArchiveEndpoint)
		require.Equal(t, int64(11), leaderRecordingID)
		require.Equal(t, int64(22), stopPosition)
		require.Equal(t, int64(33), nowNs)
		return newSyntheticLogReplication(syntheticLogReplicationConfig{
			RecordingID:     99,
			DoneAfterDoWork: 2,
		})
	})

	rep := a.NewLogReplication("leader-ep", "response-ep", 11, 22, 33)
	require.NotNil(t, rep)
	require.Equal(t, int64(99), rep.RecordingID())
	require.False(t, rep.IsDone())

	require.Equal(t, 1, rep.DoWork(1))
	require.False(t, rep.IsDone())
	require.Equal(t, 1, rep.DoWork(2))
	require.True(t, rep.IsDone())
	require.Equal(t, 0, rep.DoWork(3))
	require.True(t, rep.IsDone())

	a.SetLogReplicationFactory(nil)
	require.Nil(t, a.NewLogReplication("leader-ep", "response-ep", 11, 22, 33))
}

func TestNewLogReplay_defaultDoneStub(t *testing.T) {
	a := NewAgent(AgentConfig{})
	replay := a.NewLogReplay(10, 200)
	require.NotNil(t, replay)
	require.True(t, replay.IsDone())
	require.Equal(t, int64(200), replay.Position())
	require.Equal(t, 0, replay.DoWork(1))
}

func TestNewLogReplay_doneLogReplayCloseIsIdempotent(t *testing.T) {
	replay := &doneLogReplay{position: 55}
	require.True(t, replay.IsDone())
	require.Equal(t, int64(55), replay.Position())

	replay.Close()
	replay.Close()

	require.True(t, replay.IsDone())
	require.Equal(t, int64(55), replay.Position())
	require.Equal(t, 0, replay.DoWork(1))
}

func TestNewLogReplay_factoryOverride(t *testing.T) {
	a := NewAgent(AgentConfig{})
	stub := &doneLogReplay{position: 42}
	a.SetLogReplayFactory(func(logPosition, appendPosition int64) LogReplay {
		require.Equal(t, int64(5), logPosition)
		require.Equal(t, int64(9), appendPosition)
		return stub
	})

	replay := a.NewLogReplay(5, 9)
	require.Same(t, stub, replay)
	require.Equal(t, int64(42), replay.Position())

	// Factory returning nil falls through to a done-stub at appendPosition.
	// Production factories must not use this fallthrough for a real catchup. The
	// done stub only prevents a soft livelock.
	a.SetLogReplayFactory(func(_, _ int64) LogReplay {
		return nil
	})
	nilFallback := a.NewLogReplay(3, 88)
	require.NotNil(t, nilFallback)
	require.True(t, nilFallback.IsDone())
	require.Equal(t, int64(88), nilFallback.Position())
	require.Equal(t, 0, nilFallback.DoWork(1))

	a.SetLogReplayFactory(nil)
	fallback := a.NewLogReplay(1, 77)
	require.True(t, fallback.IsDone())
	require.Equal(t, int64(77), fallback.Position())
}

func TestSyntheticLogReplication_doneImmediately(t *testing.T) {
	for _, tt := range []struct {
		name            string
		doneAfterDoWork int
	}{
		{name: "zero", doneAfterDoWork: 0},
		{name: "negative", doneAfterDoWork: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rep := newSyntheticLogReplication(syntheticLogReplicationConfig{
				RecordingID:     7,
				DoneAfterDoWork: tt.doneAfterDoWork,
			})
			require.True(t, rep.IsDone())
			require.Equal(t, 0, rep.DoWork(1))
			require.Equal(t, int64(7), rep.RecordingID())

			// OnSignal is a no-op for the inject double.
			rep.OnSignal(1, 2, 3, 0)
		})
	}
}

// syntheticLogReplicationConfig configures a duty-cycle inject double for
// [LogReplication].
//
// DoneAfterDoWork is the number of [LogReplication.DoWork] calls that return
// non-zero work before [LogReplication.IsDone] becomes true. Zero or negative
// means IsDone is true immediately and DoWork returns 0.
type syntheticLogReplicationConfig struct {
	RecordingID     int64
	DoneAfterDoWork int
}

// syntheticLogReplication is a duty-cycle inject double for [LogReplication].
// Pre-seeded; DoWork never blocks on archive control.
type syntheticLogReplication struct {
	recordingID int64
	remaining   int
}

func newSyntheticLogReplication(cfg syntheticLogReplicationConfig) LogReplication {
	return &syntheticLogReplication{
		recordingID: cfg.RecordingID,
		remaining:   cfg.DoneAfterDoWork,
	}
}

func (r *syntheticLogReplication) DoWork(int64) int {
	if r.remaining <= 0 {
		return 0
	}
	r.remaining--
	return 1
}

func (r *syntheticLogReplication) RecordingID() int64 { return r.recordingID }

func (r *syntheticLogReplication) IsDone() bool { return r.remaining <= 0 }

func (r *syntheticLogReplication) OnSignal(int64, int64, int64, archivewire.RecordingSignal) {
}

var _ LogReplication = (*syntheticLogReplication)(nil)

// catchupReplayStart records one StartCatchupReplay argument set.
type catchupReplayStart struct {
	logPosition     int64
	catchupEndpoint string
	logSessionID    int32
}

// fakeCatchupControl is a [CatchupControl] double that records the calls the
// Agent makes. Replay session ids are derived from the start count; the Agent
// discards them.
type fakeCatchupControl struct {
	destination string

	ensureCalls int
	removeCalls int
	stopCalls   int

	starts   []catchupReplayStart
	startErr error
}

var _ CatchupControl = (*fakeCatchupControl)(nil)

func (c *fakeCatchupControl) EnsureCatchupLogDestination(string) string {
	c.ensureCalls++
	return c.destination
}

func (c *fakeCatchupControl) RemoveCatchupLogDestination() error {
	c.removeCalls++
	return nil
}

func (c *fakeCatchupControl) StartCatchupReplay(
	logPosition int64,
	catchupEndpoint string,
	logSessionID int32,
) (int64, error) {
	c.starts = append(c.starts, catchupReplayStart{
		logPosition:     logPosition,
		catchupEndpoint: catchupEndpoint,
		logSessionID:    logSessionID,
	})
	if c.startErr != nil {
		return 0, c.startErr
	}
	return int64(len(c.starts)), nil
}

func (c *fakeCatchupControl) StopCatchupReplays() error {
	c.stopCalls++
	return nil
}

// Asserts a leader serves one follower catchup at a time.
//
// Phases: start; refuse another follower; the served follower retries by
// replacing its own replay; stop reopens the gate.
func TestOnCatchupPosition_oneFollowerAtATime(t *testing.T) {
	const catchupEndpoint = "127.0.0.1:9100"

	publisher, _ := boundLogPublisher(t)
	require.NotEqual(t, int32(nullValue), publisher.SessionID())

	control := &fakeCatchupControl{destination: "aeron:udp?endpoint=" + catchupEndpoint}
	a := NewAgent(AgentConfig{Role: RoleLeader})
	a.SetLogPublisher(publisher)
	a.SetLogRecordingID(42)
	a.SetCatchupControl(control)

	served := codecs.CatchupPositionFields{
		LogPosition:      100,
		FollowerMemberID: 1,
		CatchupEndpoint:  catchupEndpoint,
	}
	other := served
	other.FollowerMemberID = 2

	a.OnCatchupPosition(served)
	require.Len(t, control.starts, 1)
	require.Equal(t, int64(100), control.starts[0].logPosition)
	require.Equal(t, catchupEndpoint, control.starts[0].catchupEndpoint)
	require.Equal(t, publisher.SessionID(), control.starts[0].logSessionID)

	// Another follower is refused while the first replay runs.
	a.OnCatchupPosition(other)
	require.Len(t, control.starts, 1)
	require.Equal(t, 0, control.stopCalls)

	// The served follower asks again: drop the stale replay, start a fresh one.
	a.OnCatchupPosition(served)
	require.Len(t, control.starts, 2)
	require.Equal(t, 1, control.stopCalls)

	a.StopAllCatchups()
	require.Equal(t, 2, control.stopCalls)

	a.OnCatchupPosition(other)
	require.Len(t, control.starts, 3)
}

// Asserts a failed start leaves the gate open. Nothing is replaying.
func TestOnCatchupPosition_startFailureLeavesGateOpen(t *testing.T) {
	publisher, _ := boundLogPublisher(t)
	control := &fakeCatchupControl{startErr: errors.New("archive refused")}
	a := NewAgent(AgentConfig{Role: RoleLeader})
	a.SetLogPublisher(publisher)
	a.SetLogRecordingID(42)
	a.SetCatchupControl(control)

	fields := codecs.CatchupPositionFields{
		LogPosition:     100,
		CatchupEndpoint: "127.0.0.1:9100",
	}

	a.OnCatchupPosition(fields)
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 2)
}

// Asserts Ensure delegates every call and Remove reaches the control. The
// control owns the open state; the agent keeps no destination of its own.
func TestEnsureCatchupLogDestination_delegatesToControl(t *testing.T) {
	const (
		catchupEndpoint = "127.0.0.1:9101"
		destination     = "aeron:udp?endpoint=" + catchupEndpoint
	)

	control := &fakeCatchupControl{destination: destination}
	a := NewAgent(AgentConfig{})
	a.SetCatchupControl(control)

	require.Equal(t, destination, a.EnsureCatchupLogDestination(catchupEndpoint))
	require.Equal(t, destination, a.EnsureCatchupLogDestination(catchupEndpoint))
	require.Equal(t, 2, control.ensureCalls)

	a.RemoveCatchupLogDestination()
	require.Equal(t, 1, control.removeCalls)

	require.Equal(t, destination, a.EnsureCatchupLogDestination(catchupEndpoint))
	require.Equal(t, 3, control.ensureCalls)
}

// Asserts PrepareForNewLeadership tears down both catchup halves.
// Leader-side replays stop and the gate reopens. The follower receive path
// closes. Both destinations clear.
func TestPrepareForNewLeadership_tearsDownCatchup(t *testing.T) {
	const catchupEndpoint = "127.0.0.1:9102"

	publisher, _ := boundLogPublisher(t)
	control := &fakeCatchupControl{destination: "aeron:udp?endpoint=" + catchupEndpoint}
	a := NewAgent(AgentConfig{Role: RoleLeader})
	a.SetLogPublisher(publisher)
	a.SetLogRecordingID(42)
	a.SetCatchupControl(control)

	fields := codecs.CatchupPositionFields{
		LogPosition:      100,
		FollowerMemberID: 1,
		CatchupEndpoint:  catchupEndpoint,
	}
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 1)

	require.NotEmpty(t, a.EnsureCatchupLogDestination(catchupEndpoint))

	_, err := a.PrepareForNewLeadership(100, 0)
	require.NoError(t, err)
	require.Equal(t, 1, control.stopCalls)
	require.Equal(t, 1, control.removeCalls)

	// Gate reopened: the next leadership term may serve a new catchup.
	a.RoleSet(RoleLeader)
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 2)
}
