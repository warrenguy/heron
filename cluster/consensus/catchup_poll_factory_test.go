//go:build testhooks

package consensus

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// catchupPollSessionFields is a fixed SessionMessageHeader payload for
// CatchupPoll frame fixtures.
var catchupPollSessionFields = codecs.SessionMessageHeaderFields{
	LeadershipTermID: 1,
	ClusterSessionID: 2,
	Timestamp:        3,
}

func TestCatchupPoll_pollsWhenLogAdapterImagePresent(t *testing.T) {
	adapter, image := boundLogAdapter(t)
	span := writeSessionMessageAtJoin(t, image, catchupPollSessionFields)

	commit := new(atomic.Int64)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Commit:                    commit,
		LogAdapter:                adapter,
		Egress:                    eg,
		LeadershipTermID:          5,
		LeaderHeartbeatIntervalNs: 1000,
		LeaderMemberID:            1,
	})

	// Seed cadence so the first publish is not delayed by interval.
	a.CatchupInitiated(0)

	work := a.CatchupPoll(span+32, 100)
	require.Positive(t, work)
	require.Equal(t, span, adapter.Position())
	require.Equal(t, adapter.Position(), a.CommitPosition())

	// Publishes adapter position under the catchup flag.
	require.Equal(t, []appendPublishCall{{
		term:  5,
		pos:   adapter.Position(),
		flags: AppendPositionFlagCatchup,
	}}, eg.appends)

	// Catchup must not raise the steady unflagged floor.
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)

	// work == 0 still ProposeMaxCommit from adapter position (prior consume).
	// Interval not due and position flat: no second publish.
	commit.Store(0)
	require.Equal(t, int64(0), a.CommitPosition())
	nBefore := len(eg.appends)
	require.Equal(t, 0, a.CatchupPoll(span+32, 101))
	require.Equal(t, adapter.Position(), a.CommitPosition())
	require.Len(t, eg.appends, nBefore)
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
}

// Asserts CatchupPoll does not raise lastPublishedAppendPosition. After
// catchup, steady unflagged publish floors on the recorded high-water only,
// not the consumed adapter position.
func TestCatchupPoll_doesNotRaiseLastPublishedForSteady(t *testing.T) {
	adapter, image := boundLogAdapter(t)
	span := writeSessionMessageAtJoin(t, image, catchupPollSessionFields)
	require.Positive(t, span)

	const recorded int64 = 40
	appendV := new(atomic.Int64)
	appendV.Store(recorded)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Role:                      RoleFollower,
		Commit:                    new(atomic.Int64),
		LogAdapter:                adapter,
		Egress:                    eg,
		AppendSource:              &AtomicAppendPosition{V: appendV},
		LeadershipTermID:          3,
		LeaderHeartbeatIntervalNs: 1000,
		LeaderMemberID:            1,
	})

	// Seed a prior steady floor below the adapter high-water.
	a.lastPublishedAppendPosition = recorded
	a.CatchupInitiated(0)

	require.Positive(t, a.CatchupPoll(span+32, 10))
	require.Equal(t, span, adapter.Position())
	require.Greater(t, adapter.Position(), recorded)
	require.Equal(t, []appendPublishCall{{
		term:  3,
		pos:   adapter.Position(),
		flags: AppendPositionFlagCatchup,
	}}, eg.appends)

	// Steady floor stays at the recorded high-water.
	require.Equal(t, recorded, a.lastPublishedAppendPosition)

	// Steady path after catchup: unflagged publish must not leak adapter pos.
	eg.appends = nil

	// Interval due so a flat recorded position still publishes.
	require.Equal(t, 1, a.updateFollowerPosition(10+1000, recorded, AppendPositionFlagNone))
	require.Equal(t, []appendPublishCall{{
		term:  3,
		pos:   recorded,
		flags: AppendPositionFlagNone,
	}}, eg.appends)
	require.Equal(t, recorded, a.lastPublishedAppendPosition)
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
	_ = writeSessionMessageAtJoin(t, image, catchupPollSessionFields)
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
// Agent makes. Replay session ids are derived from the start count. Retained
// sessions mirror the production ledger until single-session or bulk stop.
type fakeCatchupControl struct {
	destination string

	ensureCalls int
	removeCalls int
	stopCalls   int
	reseedCalls int
	reseedErr   error

	starts      []catchupReplayStart
	singleStops []int64
	retained    []int64
	startErr    error
	stopOneErr  error
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

func (c *fakeCatchupControl) AddLiveLogDestination() error {
	return nil
}

func (c *fakeCatchupControl) ReseedLogSubscription() error {
	c.reseedCalls++
	return c.reseedErr
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
	id := int64(len(c.starts))
	c.retained = append(c.retained, id)
	return id, nil
}

func (c *fakeCatchupControl) StopCatchupReplay(replaySessionID int64) error {
	c.singleStops = append(c.singleStops, replaySessionID)
	for i, id := range c.retained {
		if id != replaySessionID {
			continue
		}
		if c.stopOneErr != nil {
			return c.stopOneErr
		}
		c.retained = append(c.retained[:i], c.retained[i+1:]...)
		return nil
	}
	return nil
}

func (c *fakeCatchupControl) StopCatchupReplays() error {
	c.stopCalls++
	c.retained = nil
	return nil
}

var errTestCatchupStopRefused = errors.New("test: catchup stop refused")

// catchupUDPDestination builds a control destination URI for endpoint.
func catchupUDPDestination(endpoint string) string {
	return "aeron:udp?endpoint=" + endpoint
}

// newLeaderCatchupAgent builds a RoleLeader agent with a bound log publisher,
// recording id 42, and catchup control. Destination seeds the control double.
func newLeaderCatchupAgent(
	t testing.TB,
	cfg AgentConfig,
	destination string,
) (*Agent, *fakeCatchupControl, *LogPublisher) {
	t.Helper()
	cfg.Role = RoleLeader
	publisher, _ := boundLogPublisher(t)
	control := &fakeCatchupControl{destination: destination}
	a := NewAgent(cfg)
	a.SetLogPublisher(publisher)
	a.SetLogRecordingID(42)
	a.SetCatchupControl(control)
	return a, control, publisher
}

// startFollowerCatchup requests a catchup replay at log position 0.
func startFollowerCatchup(a *Agent, memberID int32, endpoint string) {
	a.OnCatchupPosition(codecs.CatchupPositionFields{
		LogPosition:      0,
		FollowerMemberID: memberID,
		CatchupEndpoint:  endpoint,
	})
}

// Asserts TrackCatchupCompletion stops the archive session, publishes
// StopCatchup, and deletes the map entry only after both halves succeed.
//
// Phases:
//   - below leader position: no-op
//   - stop failure: entry stays, stopped false, no publish
//   - stop success and publish refuse: stopped true, entry stays
//   - leader advances past follower logPosition: publish still retries
//   - publish success: entry deleted
//   - unflagged AppendPosition with a live entry still drives cleanup
func TestTrackCatchupCompletion_stopThenPublish(t *testing.T) {
	const catchupEndpoint = "127.0.0.1:9101"
	const memberID int32 = 3

	eg := &recordingEgress{}
	a, control, publisher := newLeaderCatchupAgent(t, AgentConfig{
		Egress:           eg,
		LeadershipTermID: 7,
	}, catchupUDPDestination(catchupEndpoint))

	startFollowerCatchup(a, memberID, catchupEndpoint)
	require.Len(t, a.catchupReplays, 1)
	sessionID := a.catchupReplays[memberID].replaySessionID
	leaderPos := publisher.Position()

	// Below leader position: no stop, no publish.
	a.TrackCatchupCompletion(memberID, leaderPos-1, AppendPositionFlagCatchup)
	require.Empty(t, control.singleStops)
	require.Empty(t, eg.stopCatchups)
	require.Len(t, a.catchupReplays, 1)

	// Stop refuses: entry stays active and unstopped.
	control.stopOneErr = errTestCatchupStopRefused
	a.TrackCatchupCompletion(memberID, leaderPos, AppendPositionFlagCatchup)
	require.Equal(t, []int64{sessionID}, control.singleStops)
	require.Empty(t, eg.stopCatchups)
	require.False(t, a.catchupReplays[memberID].stopped)

	// Stop succeeds; publish refuses: stopped stays, entry remains for retry.
	control.stopOneErr = nil
	control.singleStops = nil
	eg.refuseStopCatchup = true
	a.TrackCatchupCompletion(memberID, leaderPos, AppendPositionFlagCatchup)
	require.Equal(t, []int64{sessionID}, control.singleStops)
	require.True(t, a.catchupReplays[memberID].stopped)
	require.Empty(t, eg.stopCatchups)

	// Leader advances past the follower-reported position. Stop is already
	// latched. Publish retry must not re-check the position gate.
	advanced := publisher.AppendMessage(7, 0, 0, []byte("advance"))
	require.Greater(t, advanced, leaderPos)
	eg.refuseStopCatchup = false
	a.TrackCatchupCompletion(memberID, leaderPos, AppendPositionFlagNone)
	require.Equal(t, []int64{sessionID}, control.singleStops) // no second stop
	require.Equal(t, []stopCatchupPublishCall{{
		term: 7, followerID: memberID,
	}}, eg.stopCatchups)
	require.Empty(t, a.catchupReplays)
}

// Asserts same-member OnCatchupPosition keeps the map entry when archive stop
// fails. The ledger session id stays tracked. No second start runs.
func TestOnCatchupPosition_restartStopFailureKeepsMember(t *testing.T) {
	a, control, _ := newLeaderCatchupAgent(t, AgentConfig{}, "")

	fields := codecs.CatchupPositionFields{
		LogPosition:      100,
		FollowerMemberID: 1,
		CatchupEndpoint:  "127.0.0.1:9100",
	}

	a.OnCatchupPosition(fields)
	require.Len(t, a.catchupReplays, 1)
	sessionID := a.catchupReplays[1].replaySessionID
	require.Equal(t, []int64{sessionID}, control.retained)

	control.stopOneErr = errTestCatchupStopRefused
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 1)
	require.Equal(t, []int64{sessionID}, control.singleStops)
	require.Len(t, a.catchupReplays, 1)
	require.Equal(t, sessionID, a.catchupReplays[1].replaySessionID)
	require.Equal(t, []int64{sessionID}, control.retained)

	control.stopOneErr = nil
	control.singleStops = nil
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 2)
	require.Equal(t, []int64{sessionID}, control.singleStops)
	require.Len(t, a.catchupReplays, 1)
	require.NotEqual(t, sessionID, a.catchupReplays[1].replaySessionID)
	require.Equal(t, []int64{a.catchupReplays[1].replaySessionID}, control.retained)
}

// Asserts TrackCatchupCompletion is a no-op with no map entry and no catchup
// flag.
func TestTrackCatchupCompletion_idleWithoutEntryOrFlag(t *testing.T) {
	publisher, _ := boundLogPublisher(t)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{Role: RoleLeader, Egress: eg})
	a.SetLogPublisher(publisher)

	a.TrackCatchupCompletion(9, publisher.Position(), AppendPositionFlagNone)
	require.Empty(t, eg.stopCatchups)
	require.Empty(t, a.catchupReplays)
}

// Asserts catchup-flagged steady AppendPosition refreshes liveness only and
// drives completion. Does not update the position table.
func TestSteadyAppendPosition_catchupFlag_skipsPositionUpdate(t *testing.T) {
	const memberID int32 = 4
	const catchupEndpoint = "127.0.0.1:9102"

	pt := &spyPositionTable{}
	eg := &recordingEgress{}
	a, _, publisher := newLeaderCatchupAgent(t, AgentConfig{
		Positions:        pt,
		Egress:           eg,
		LeadershipTermID: 3,
	}, catchupUDPDestination(catchupEndpoint))

	startFollowerCatchup(a, memberID, catchupEndpoint)

	leaderPos := publisher.Position()
	a.handleSteadyAppendPosition(codecs.AppendPositionFields{
		LeadershipTermID: 3,
		LogPosition:      leaderPos,
		FollowerMemberID: memberID,
		Flags:            AppendPositionFlagCatchup,
	}, 1)

	require.Empty(t, pt.updates)
	require.Equal(t, []int32{memberID}, pt.liveness)
	require.Equal(t, []stopCatchupPublishCall{{
		term: 3, followerID: memberID,
	}}, eg.stopCatchups)
	require.Empty(t, a.catchupReplays)
}

// Asserts unflagged steady AppendPosition updates the position table and still
// drives completion when a map entry is live.
func TestSteadyAppendPosition_unflagged_updatesAndCompletes(t *testing.T) {
	const memberID int32 = 5
	const catchupEndpoint = "127.0.0.1:9103"

	pt := &spyPositionTable{}
	eg := &recordingEgress{}
	a, _, publisher := newLeaderCatchupAgent(t, AgentConfig{
		Positions:        pt,
		Egress:           eg,
		LeadershipTermID: 2,
	}, catchupUDPDestination(catchupEndpoint))

	startFollowerCatchup(a, memberID, catchupEndpoint)

	leaderPos := publisher.Position()
	a.handleSteadyAppendPosition(codecs.AppendPositionFields{
		LeadershipTermID: 2,
		LogPosition:      leaderPos,
		FollowerMemberID: memberID,
		Flags:            AppendPositionFlagNone,
	}, 1)

	require.Equal(t, []memberPositionUpdate{{
		memberID:         memberID,
		leadershipTermID: 2,
		logPosition:      leaderPos,
	}}, pt.updates)
	require.Empty(t, pt.liveness)
	require.Equal(t, []stopCatchupPublishCall{{
		term: 2, followerID: memberID,
	}}, eg.stopCatchups)
	require.Empty(t, a.catchupReplays)
}

// Asserts steady StopCatchup with matching term and member removes the catchup
// destination.
func TestSteadyStopCatchup_removesDestination(t *testing.T) {
	control := &fakeCatchupControl{destination: catchupUDPDestination("catchup")}
	a := NewAgent(AgentConfig{
		Role:             RoleFollower,
		ThisMemberID:     1,
		LeadershipTermID: 4,
	})
	a.SetCatchupControl(control)
	a.CatchupLogDestinationSet(control.destination)

	a.handleSteadyStopCatchup(codecs.StopCatchupFields{
		LeadershipTermID: 4,
		FollowerMemberID: 1,
	})
	require.Equal(t, 1, control.removeCalls)

	// Mismatched member: no second remove.
	a.handleSteadyStopCatchup(codecs.StopCatchupFields{
		LeadershipTermID: 4,
		FollowerMemberID: 9,
	})
	require.Equal(t, 1, control.removeCalls)

	// Mismatched term: no remove.
	a.handleSteadyStopCatchup(codecs.StopCatchupFields{
		LeadershipTermID: 5,
		FollowerMemberID: 1,
	})
	require.Equal(t, 1, control.removeCalls)
}

// Asserts steady ingress dispatches StopCatchup wire to handleSteadyStopCatchup.
func TestSteadyStopCatchup_wireDispatch(t *testing.T) {
	control := &fakeCatchupControl{destination: catchupUDPDestination("catchup")}
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{
		Role:             RoleFollower,
		ThisMemberID:     1,
		LeadershipTermID: 4,
		Ingress:          q,
	})
	a.SetCatchupControl(control)
	a.CatchupLogDestinationSet(control.destination)

	wireLen, ok := codecs.ClusterWireMessageLen(clusterwire.StopCatchupSbeBlockLength)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeStopCatchup(
		buf, 0, clusterwire.StopCatchupSbeSchemaVersion, codecs.StopCatchupFields{
			LeadershipTermID: 4,
			FollowerMemberID: 1,
		},
	)
	require.True(t, ok)
	q.Enqueue(buf[:end])

	// No election runner: steady fragment path handles StopCatchup.
	require.Nil(t, a.Election())
	require.Equal(t, 1, a.pollConsensusIngress(1))
	require.Equal(t, 1, control.removeCalls)

	// Mismatched member on wire: no remove.
	end, ok = codecs.EncodeStopCatchup(
		buf, 0, clusterwire.StopCatchupSbeSchemaVersion, codecs.StopCatchupFields{
			LeadershipTermID: 4,
			FollowerMemberID: 9,
		},
	)
	require.True(t, ok)
	q.Enqueue(buf[:end])
	require.Equal(t, 1, a.pollConsensusIngress(2))
	require.Equal(t, 1, control.removeCalls)
}

// Asserts nil positions still drives TrackCatchupCompletion for unflagged
// AppendPosition.
func TestSteadyAppendPosition_nilPositions_stillCompletes(t *testing.T) {
	const memberID int32 = 6
	const catchupEndpoint = "127.0.0.1:9104"

	eg := &recordingEgress{}
	a, _, publisher := newLeaderCatchupAgent(t, AgentConfig{
		Egress:           eg,
		LeadershipTermID: 2,
		// Positions intentionally nil.
	}, catchupUDPDestination(catchupEndpoint))

	startFollowerCatchup(a, memberID, catchupEndpoint)
	require.Len(t, a.catchupReplays, 1)

	leaderPos := publisher.Position()
	a.handleSteadyAppendPosition(codecs.AppendPositionFields{
		LeadershipTermID: 2,
		LogPosition:      leaderPos,
		FollowerMemberID: memberID,
		Flags:            AppendPositionFlagNone,
	}, 1)

	require.Equal(t, []stopCatchupPublishCall{{
		term: 2, followerID: memberID,
	}}, eg.stopCatchups)
	require.Empty(t, a.catchupReplays)
}

// Asserts a leader tracks one catchup replay per follower.
//
// Phases:
//   - start for one follower
//   - second follower gets its own replay
//   - same-member restart stops only that session
//   - StopAllCatchups drains the map
func TestOnCatchupPosition_oneReplayPerFollower(t *testing.T) {
	const catchupEndpoint = "127.0.0.1:9100"

	a, control, publisher := newLeaderCatchupAgent(t, AgentConfig{}, "")
	require.NotEqual(t, int32(nullValue), publisher.SessionID())

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
	require.Len(t, a.catchupReplays, 1)
	require.Equal(t, int64(1), a.catchupReplays[1].replaySessionID)
	require.False(t, a.catchupReplays[1].stopped)

	// Distinct followers get concurrent replays.
	a.OnCatchupPosition(other)
	require.Len(t, control.starts, 2)
	require.Empty(t, control.singleStops)
	require.Equal(t, 0, control.stopCalls)
	require.Len(t, a.catchupReplays, 2)
	require.Equal(t, int64(2), a.catchupReplays[2].replaySessionID)
	require.Equal(t, []int64{1, 2}, control.retained)

	// The first follower asks again: stop only its session, start a fresh one.
	a.OnCatchupPosition(served)
	require.Len(t, control.starts, 3)
	require.Equal(t, []int64{1}, control.singleStops)
	require.Equal(t, 0, control.stopCalls)
	require.Len(t, a.catchupReplays, 2)
	require.Equal(t, int64(3), a.catchupReplays[1].replaySessionID)
	require.Equal(t, int64(2), a.catchupReplays[2].replaySessionID)
	require.Equal(t, []int64{2, 3}, control.retained)

	a.StopAllCatchups()
	require.Equal(t, 1, control.stopCalls)
	require.Empty(t, a.catchupReplays)
	require.Empty(t, control.retained)

	a.OnCatchupPosition(other)
	require.Len(t, control.starts, 4)
	require.Len(t, a.catchupReplays, 1)
	require.Equal(t, int64(4), a.catchupReplays[2].replaySessionID)
}

// Asserts a failed start leaves no map entry. Nothing is replaying.
func TestOnCatchupPosition_startFailureLeavesMapEmpty(t *testing.T) {
	a, control, _ := newLeaderCatchupAgent(t, AgentConfig{}, "")
	control.startErr = errors.New("archive refused")

	fields := codecs.CatchupPositionFields{
		LogPosition:      100,
		FollowerMemberID: 1,
		CatchupEndpoint:  "127.0.0.1:9100",
	}

	a.OnCatchupPosition(fields)
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 2)
	require.Empty(t, a.catchupReplays)
	require.Empty(t, control.retained)
}

// Asserts a same-member restart that fails on the new start leaves no map
// entry for that member. The prior session is stopped first.
func TestOnCatchupPosition_restartStartFailureClearsMember(t *testing.T) {
	a, control, _ := newLeaderCatchupAgent(t, AgentConfig{}, "")

	fields := codecs.CatchupPositionFields{
		LogPosition:      100,
		FollowerMemberID: 1,
		CatchupEndpoint:  "127.0.0.1:9100",
	}

	a.OnCatchupPosition(fields)
	require.Len(t, a.catchupReplays, 1)
	require.Equal(t, []int64{1}, control.retained)

	control.startErr = errors.New("archive refused")
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 2)
	require.Equal(t, []int64{1}, control.singleStops)
	require.Empty(t, a.catchupReplays)
	require.Empty(t, control.retained)
}

// Asserts Ensure delegates every call and Remove reaches the control. The
// control owns the open state; the agent keeps no destination of its own.
func TestEnsureCatchupLogDestination_delegatesToControl(t *testing.T) {
	const catchupEndpoint = "127.0.0.1:9101"
	destination := catchupUDPDestination(catchupEndpoint)

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
// Leader-side replays stop and the map drains. The follower receive path
// closes. Both destinations clear.
func TestPrepareForNewLeadership_tearsDownCatchup(t *testing.T) {
	const catchupEndpoint = "127.0.0.1:9102"

	a, control, _ := newLeaderCatchupAgent(t, AgentConfig{}, catchupUDPDestination(catchupEndpoint))

	fields := codecs.CatchupPositionFields{
		LogPosition:      100,
		FollowerMemberID: 1,
		CatchupEndpoint:  catchupEndpoint,
	}
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 1)
	require.Len(t, a.catchupReplays, 1)

	require.NotEmpty(t, a.EnsureCatchupLogDestination(catchupEndpoint))

	_, prepared, err := a.PrepareForNewLeadership(100, 0)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, 1, control.stopCalls)
	require.Equal(t, 1, control.removeCalls)
	require.Empty(t, a.catchupReplays)

	// Map drained: the next leadership term may serve a new catchup.
	a.RoleSet(RoleLeader)
	a.OnCatchupPosition(fields)
	require.Len(t, control.starts, 2)
	require.Len(t, a.catchupReplays, 1)
}

// newReseedAgent builds a follower with adapter, a stub join source and
// control. A nil control leaves the agent without one.
func newReseedAgent(
	adapter *LogAdapter,
	control *fakeCatchupControl,
) (*Agent, *stubLogJoinSource) {
	a := NewAgent(AgentConfig{
		Commit:     new(atomic.Int64),
		LogAdapter: adapter,
	})
	if control != nil {
		a.SetCatchupControl(control)
	}
	source := &stubLogJoinSource{}
	a.SetLogJoinSource(source)
	return a, source
}

// requirePrepared runs one PrepareForNewLeadership call and requires it to
// report prepared.
func requirePrepared(t *testing.T, a *Agent) {
	t.Helper()
	_, prepared, err := a.PrepareForNewLeadership(0, 0)
	require.NoError(t, err)
	require.True(t, prepared)
}

// Asserts prepare replaces the log subscription when the adapter holds no open
// image or the last follower join query missed. An open image with no miss
// keeps the subscription and the image.
func TestPrepareForNewLeadership_reseedsLogSubscriptionForNewTerm(t *testing.T) {
	t.Parallel()

	const logSessionID int32 = 7

	openImage := openLogImage(math.MaxInt64)
	closedImage := closedLogImage(math.MaxInt64)

	for _, tt := range []struct {
		name    string
		adapter logStopAdapter
		// queries are the join source results, one per join query before prepare.
		queries []bool
		// nilSource clears the join source before the queries.
		nilSource   bool
		wantReseeds int
	}{
		{name: "no image", adapter: unboundLogImage, wantReseeds: 1},
		{name: "closed image without end of stream", adapter: closedImage, wantReseeds: 1},
		{
			name:        "closed image at end of stream",
			adapter:     closedLogImage(logStopEndOfStream),
			wantReseeds: 1,
		},
		{name: "open image", adapter: openImage},
		{name: "open image, missed query", adapter: openImage, queries: []bool{false}, wantReseeds: 1},
		{name: "open image, missed then ready query", adapter: openImage, queries: []bool{false, true}},
		{
			name:        "open image, nil join source query",
			adapter:     openImage,
			queries:     []bool{false},
			nilSource:   true,
			wantReseeds: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			adapter := tt.adapter(t)
			image := adapter.Image()
			control := &fakeCatchupControl{}
			a, source := newReseedAgent(adapter, control)
			if tt.nilSource {
				a.SetLogJoinSource(nil)
			}
			for _, ready := range tt.queries {
				source.readyOK = ready
				_, ok := a.FollowerLogJoinReady(logSessionID)
				require.Equal(t, ready, ok)
			}

			requirePrepared(t, a)
			require.Equal(t, tt.wantReseeds, control.reseedCalls)
			if tt.wantReseeds == 0 {
				require.Same(t, image, adapter.Image())
				return
			}
			require.Nil(t, adapter.Image())
		})
	}

	t.Run("miss consumed", func(t *testing.T) {
		t.Parallel()

		adapter := openImage(t)
		control := &fakeCatchupControl{}
		a, source := newReseedAgent(adapter, control)
		source.readyOK = false
		_, ok := a.FollowerLogJoinReady(logSessionID)
		require.False(t, ok)
		requirePrepared(t, a)
		require.Equal(t, 1, control.reseedCalls)

		_, rebound := boundLogAdapter(t)
		adapter.SetImage(rebound)
		requirePrepared(t, a)
		require.Equal(t, 1, control.reseedCalls)
		require.Same(t, rebound, adapter.Image())
	})

	t.Run("nil adapter", func(t *testing.T) {
		t.Parallel()

		control := &fakeCatchupControl{}
		a, _ := newReseedAgent(nil, control)
		requirePrepared(t, a)
		require.Zero(t, control.reseedCalls)
	})

	t.Run("nil control", func(t *testing.T) {
		t.Parallel()

		adapter := closedImage(t)
		image := adapter.Image()
		a, _ := newReseedAgent(adapter, nil)
		requirePrepared(t, a)
		require.Same(t, image, adapter.Image())
	})

	t.Run("reseed error", func(t *testing.T) {
		t.Parallel()

		adapter := closedImage(t)
		control := &fakeCatchupControl{reseedErr: errors.New("driver refused")}
		a, _ := newReseedAgent(adapter, control)
		requirePrepared(t, a)
		require.Equal(t, 1, control.reseedCalls)
		require.Nil(t, adapter.Image())
	})

	t.Run("recording stop pending", func(t *testing.T) {
		t.Parallel()

		adapter := closedImage(t)
		control := &fakeCatchupControl{}
		a, _ := newReseedAgent(adapter, control)
		recording := &fakeLogRecordingControl{pollStopPending: true}
		a.SetLogRecordingControl(recording)

		// Phase: first call reseeds. The stop stays pending.
		_, prepared, err := a.PrepareForNewLeadership(0, 0)
		require.NoError(t, err)
		require.False(t, prepared)
		require.Equal(t, 1, control.reseedCalls)

		// Phase: re-poll. No second reseed in the same episode.
		recording.pollStopPending = false
		requirePrepared(t, a)
		require.Equal(t, 1, control.reseedCalls)

		// Phase: next episode. The adapter still holds no image.
		requirePrepared(t, a)
		require.Equal(t, 2, control.reseedCalls)
	})

	t.Run("live election", func(t *testing.T) {
		t.Parallel()

		adapter := closedImage(t)
		control := &fakeCatchupControl{}
		a, _ := newReseedAgent(adapter, control)
		a.SetElection(&orderRunner{})
		elections := a.ElectionCount()

		requirePrepared(t, a)
		require.Equal(t, 1, control.reseedCalls)
		require.Zero(t, a.logStopTick())
		require.Equal(t, elections, a.ElectionCount())
	})
}
