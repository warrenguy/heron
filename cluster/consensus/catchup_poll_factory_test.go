//go:build testhooks

package consensus

import (
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
