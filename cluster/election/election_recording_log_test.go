package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const testRecordingID int64 = 42

func openElectionRecordingLog(t *testing.T) (*recordinglog.Log, string) {
	t.Helper()
	dir := t.TempDir()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	return log, dir
}

func openPriorTermLog(t *testing.T, priorTerm int64) (*recordinglog.Log, string) {
	t.Helper()
	log, dir := openElectionRecordingLog(t)
	require.NoError(t, log.AppendTerm(testRecordingID, priorTerm, 0, 1))
	return log, dir
}

func electionWithRealLog(
	t *testing.T,
	recordingID int64,
	log *recordinglog.Log,
	fileSyncLevel recordinglog.FileSyncLevel,
) (*election.Election, election.ElectionConfig, *FakeClock) {
	t.Helper()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	deps.Host = &recordingElectionHost{clock: clock, localLogRecordingID: recordingID}
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	deps.RecordingLogFileSyncLevel = fileSyncLevel
	return election.NewElection(cfg, deps), cfg, clock
}

func electionAtStateWithLog(
	t *testing.T,
	recordingID, leadershipTermID int64,
	log election.RecordingLogCoherency,
	lookup election.RecordingLogLookup,
	state election.ElectionState,
) (*election.Election, *FakeEpochClock) {
	t.Helper()

	cfg, deps, clock, epoch, _ := testElectionDeps(t)
	cfg.LeadershipTermID = leadershipTermID
	deps.Host = &recordingElectionHost{clock: clock, localLogRecordingID: recordingID}
	deps.RecordingLog = log
	deps.RecordingLogLookup = lookup
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, state)
	return e, epoch
}

// mapArchiveLister is a map-backed ArchiveLister for PlanRecovery in election tests.
type mapArchiveLister map[int64]recordinglog.LogRecordingExtents

func (m mapArchiveLister) ListRecording(id int64) (recordinglog.LogRecordingExtents, error) {
	ext, ok := m[id]
	if !ok {
		return recordinglog.LogRecordingExtents{}, recordinglog.ErrUnknownRecordingID
	}
	return ext, nil
}

func termExtents(start, stop int64) recordinglog.LogRecordingExtents {
	return recordinglog.LogRecordingExtents{
		StartPosition:    start,
		StopPosition:     stop,
		InitialTermID:    0,
		TermBufferLength: 64 * 1024,
		MtuLength:        1408,
		SessionID:        1,
	}
}

func reopenRecordingLog(t *testing.T, dir string) *recordinglog.Log {
	t.Helper()
	reopened, err := recordinglog.Open(dir, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func requireTermIdentity(
	t *testing.T,
	log *recordinglog.Log,
	termID, recordingID, logPosition int64,
) *recordinglog.Entry {
	t.Helper()
	entry := log.FindTermEntry(termID)
	require.NotNil(t, entry)
	require.Equal(t, recordingID, entry.RecordingID)
	require.Equal(t, logPosition, entry.LogPosition)
	return entry
}

func requireTermBasePair(
	t *testing.T,
	log *recordinglog.Log,
	openTermID, priorTermID, termBase int64,
) *recordinglog.Entry {
	t.Helper()
	got := requireTermIdentity(t, log, openTermID, testRecordingID, recordinglog.NullPosition)
	require.Equal(t, termBase, got.TermBaseLogPosition)
	requireTermIdentity(t, log, priorTermID, testRecordingID, termBase)
	return got
}

func requireDurableTermIdentity(
	t *testing.T,
	log *recordinglog.Log,
	dir string,
	termID, recordingID, logPosition int64,
) *recordinglog.Entry {
	t.Helper()
	require.NoError(t, log.Close())
	return requireTermIdentity(t, reopenRecordingLog(t, dir), termID, recordingID, logPosition)
}

func TestRecordingLog_leaderInitEnsureCoherent(t *testing.T) {
	t.Parallel()

	// FileSyncData exercises Force on the election commit path. Reopen only
	// proves term rows survived the write+close cycle (page cache); it does
	// not distinguish sync levels.
	for _, tc := range []struct {
		name string
		sync recordinglog.FileSyncLevel
	}{
		{name: "FileSyncNone", sync: recordinglog.FileSyncNone},
		{name: "FileSyncData", sync: recordinglog.FileSyncData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			log, dir := openElectionRecordingLog(t)
			e, cfg, clock := electionWithRealLog(t, testRecordingID, log, tc.sync)

			election.SetStateForTest(e, election.ElectionStateLeaderInit)
			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, election.ElectionStateLeaderReady, e.State())
			require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))

			requireDurableTermIdentity(
				t,
				log,
				dir,
				cfg.LeadershipTermID,
				testRecordingID,
				recordinglog.NullPosition,
			)
		})
	}
}

func TestRecordingLog_followerLogJoinEnsureCoherent(t *testing.T) {
	t.Parallel()

	log, dir := openElectionRecordingLog(t)
	cfg, deps, clock, _, _ := testElectionDeps(t)
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	adoptedTerm := driveFollowerLogJoinToReady(t, cfg, deps, clock)

	requireDurableTermIdentity(t, log, dir, adoptedTerm, testRecordingID, recordinglog.NullPosition)
}

func TestRecordingLog_followerReplicationCommit(t *testing.T) {
	t.Parallel()

	log, dir := openElectionRecordingLog(t)
	e, cfg, clock := electionWithRealLog(t, testRecordingID, log, recordinglog.FileSyncNone)

	// Successor term already at append stop; commit closed term and recanvass.
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)

	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	wantTerm := cfg.LeadershipTermID + 1
	require.Equal(t, wantTerm, election.LogLeadershipTermIDForTest(e))

	entry := requireDurableTermIdentity(t, log, dir, wantTerm, testRecordingID, cfg.AppendPosition)
	require.Equal(t, cfg.AppendPosition, entry.TermBaseLogPosition)
}

func TestRecordingLog_nullRecordingIDSkipsWrite(t *testing.T) {
	t.Parallel()

	log, _ := openElectionRecordingLog(t)
	e, _, clock := electionWithRealLog(t, election.NullValue, log, recordinglog.FileSyncNone)

	election.SetStateForTest(e, election.ElectionStateLeaderInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	require.Empty(t, log.Entries())
}

func TestRecordingLog_nilLogNoOp(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, localLogRecordingID: testRecordingID}
	deps.Host = host
	// RecordingLog left nil deliberately.
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionStateLeaderInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))
}

func TestRecordingLog_planRecoverySeedsElection(t *testing.T) {
	t.Parallel()

	log, _ := openElectionRecordingLog(t)
	const (
		termID      int64 = 3
		termBase    int64 = 0
		termStop    int64 = 500
		recordingID int64 = 55
	)
	require.NoError(t, log.AppendTerm(recordingID, termID, termBase, 1_000))
	require.NoError(t, log.CommitLogPosition(termID, termStop))

	plan, err := log.PlanRecovery(
		mapArchiveLister{recordingID: termExtents(termBase, termStop)},
		recordinglog.NewRecoveryPlanConfig(0),
	)
	require.NoError(t, err)

	cfg, deps, clock, _, _ := testElectionDeps(t)
	// Start from non-matching positions so seed must overwrite.
	cfg.LeadershipTermID = 99
	cfg.TermBaseLogPosition = 99
	cfg.LogPosition = 99
	cfg.AppendPosition = 99
	election.ApplyRecoveryPlan(&cfg, plan)
	require.Equal(t, termID, cfg.LeadershipTermID)
	require.Equal(t, int64(0), cfg.LogPosition)
	require.Equal(t, termStop, cfg.AppendPosition)

	host := &recordingElectionHost{clock: clock, localLogRecordingID: plan.TermRecordingID()}
	deps.Host = host
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	e := election.NewElection(cfg, deps)
	require.Equal(t, termID, election.LogLeadershipTermIDForTest(e))

	// One more open-term adoption after seed.
	election.SetStateForTest(e, election.ElectionStateLeaderInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	requireTermIdentity(t, log, termID, recordingID, termStop)
}

func termIdentities(entries []recordinglog.Entry) []recordinglog.Entry {
	out := make([]recordinglog.Entry, 0, len(entries))
	for _, entry := range entries {
		if !entry.Valid || entry.Type != recordinglog.EntryTypeTerm {
			continue
		}
		out = append(out, recordinglog.Entry{
			RecordingID:         entry.RecordingID,
			LeadershipTermID:    entry.LeadershipTermID,
			TermBaseLogPosition: entry.TermBaseLogPosition,
			LogPosition:         entry.LogPosition,
			Timestamp:           entry.Timestamp,
			Type:                entry.Type,
			Valid:               entry.Valid,
		})
	}
	return out
}

func TestRecordingLog_replayNewLeadershipTermEvent(t *testing.T) {
	t.Parallel()

	const (
		priorTerm int64 = 4
		termID    int64 = 5
		baseB     int64 = 1_000
		eventPos  int64 = 1_800
	)
	event := codecs.NewLeadershipTermEventFields{
		LeadershipTermID:    termID,
		TermBaseLogPosition: baseB,
		LogPosition:         eventPos,
		Timestamp:           9,
	}

	for code := range election.ElectionStateNameCount() {
		state := election.ElectionState(code)
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()

			log, dir := openPriorTermLog(t, priorTerm)
			e, epoch := electionAtStateWithLog(
				t, testRecordingID, priorTerm, log, log, state,
			)

			before := termIdentities(log.Entries())
			beforeTerm := election.LogLeadershipTermIDForTest(e)
			beforePos := e.LogPosition()
			require.NoError(t, e.HandleReplayNewLeadershipTermEvent(event))
			require.Equal(t, state, e.State())

			writesTerm := state == election.ElectionStateFollowerCatchup ||
				state == election.ElectionStateFollowerReplay
			if !writesTerm {
				require.Equal(t, before, termIdentities(log.Entries()))
				require.Equal(t, beforeTerm, election.LogLeadershipTermIDForTest(e))
				require.Equal(t, beforePos, e.LogPosition())
				return
			}

			require.Equal(t, termID, election.LogLeadershipTermIDForTest(e))
			require.Equal(t, eventPos, e.LogPosition())
			got := requireTermBasePair(t, log, termID, priorTerm, baseB)
			require.Equal(t, epoch.nowMs, got.Timestamp)

			require.NoError(t, e.HandleReplayNewLeadershipTermEvent(event))
			require.Len(t, termIdentities(log.Entries()), 2)

			want := termIdentities(log.Entries())
			require.NoError(t, log.Close())
			require.Equal(t, want, termIdentities(reopenRecordingLog(t, dir).Entries()))
		})
	}

	t.Run("null_recording_id", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		e, _ := electionAtStateWithLog(
			t, election.NullValue, priorTerm, log, log, election.ElectionStateFollowerCatchup,
		)
		before := termIdentities(log.Entries())

		require.NoError(t, e.HandleReplayNewLeadershipTermEvent(event))
		require.Equal(t, before, termIdentities(log.Entries()))
		require.Equal(t, termID, election.LogLeadershipTermIDForTest(e))
		require.Equal(t, eventPos, e.LogPosition())
	})

	t.Run("nil_log", func(t *testing.T) {
		t.Parallel()

		e, _ := electionAtStateWithLog(
			t, testRecordingID, priorTerm, nil, nil, election.ElectionStateFollowerReplay,
		)

		require.NotPanics(t, func() {
			require.NoError(t, e.HandleReplayNewLeadershipTermEvent(event))
		})
		require.Equal(t, termID, election.LogLeadershipTermIDForTest(e))
		require.Equal(t, eventPos, e.LogPosition())
	})
}

func driveCatchupToReady(
	t *testing.T,
	log *recordinglog.Log,
	onPoll func(e *election.Election),
) int64 {
	t.Helper()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.LeadershipTermID = testLeadershipTermID
	joinPos := cfg.AppendPosition + 100
	host := &recordingElectionHost{
		clock:               clock,
		localLogRecordingID: testRecordingID,
		catchupPollResult:   3,
		commitPosition:      joinPos,
		logJoined:           true,
		liveLogDestination:  "aeron:udp?endpoint=live-log",
		followerLogJoinOK:   true,
	}
	deps.Host = host
	deps.Handler = &recordingAgentHandler{joinFollowerReturn: true}
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	deps.RecordingLogFileSyncLevel = recordinglog.FileSyncNone

	var e *election.Election
	if onPoll != nil {
		host.catchupPollHook = func(int64, int64) { onPoll(e) }
	}
	e, _ = forceFollowerCatchup(t, cfg, deps, joinPos, 100)

	require.Equal(t, 4, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerLogInit, e.State())
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateFollowerReady, e.State())
	return joinPos
}

func TestRecordingLog_followerCatchupJoinKeepsEventTermBase(t *testing.T) {
	t.Parallel()

	const (
		priorTerm int64 = testLeadershipTermID - 1
		baseB     int64 = 100
	)
	event := codecs.NewLeadershipTermEventFields{
		LeadershipTermID:    testLeadershipTermID,
		TermBaseLogPosition: baseB,
		LogPosition:         baseB + 10,
		Timestamp:           9,
	}

	t.Run("event_keeps_base", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		joinPos := driveCatchupToReady(t, log, func(e *election.Election) {
			require.NoError(t, e.HandleReplayNewLeadershipTermEvent(event))
		})
		require.Greater(t, joinPos, baseB)

		requireTermBasePair(t, log, testLeadershipTermID, priorTerm, baseB)
	})

	t.Run("join_without_event_writes_join_base", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		joinPos := driveCatchupToReady(t, log, nil)

		requireTermBasePair(t, log, testLeadershipTermID, priorTerm, joinPos)
	})
}

type failFirstEnsure struct {
	election.RecordingLogCoherency
	err   error
	calls int
}

func (f *failFirstEnsure) EnsureCoherent(p recordinglog.EnsureCoherentParams) error {
	f.calls++
	if f.calls == 1 {
		return f.err
	}
	return f.RecordingLogCoherency.EnsureCoherent(p)
}

func TestReplayNewLeadershipTermEvent_appendErrorRestartsElection(t *testing.T) {
	t.Parallel()

	const (
		priorTerm int64 = 5
		termID    int64 = 6
		baseB     int64 = 1_000
		stopS     int64 = 2_000
	)
	refuse := errors.New("recording log refuse")
	event := codecs.NewLeadershipTermEventFields{
		LeadershipTermID:    termID,
		TermBaseLogPosition: baseB,
		LogPosition:         baseB + 64,
		Timestamp:           9,
	}

	t.Run("catchup", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		fake := &failFirstEnsure{RecordingLogCoherency: log, err: refuse}
		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.LeadershipTermID = priorTerm
		pollErr := errors.New("test: catchup image closed")
		host := &recordingElectionHost{
			clock:               clock,
			localLogRecordingID: testRecordingID,
			commitPosition:      baseB,
			catchupPollResult:   3,
			catchupPollErr:      pollErr,
			liveLogDestination:  "aeron:udp?endpoint=live-log",
		}
		deps.Host = host
		deps.Handler = &recordingAgentHandler{appendPositionReturn: stopS}
		deps.RecordingLog = fake
		deps.RecordingLogLookup = log
		var e *election.Election
		host.catchupPollHook = func(int64, int64) {
			require.Error(t, e.HandleReplayNewLeadershipTermEvent(event))
		}
		e = election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		fields := installNewLeadershipTermFields(cfg, 1, termID)
		fields.LogPosition = 500
		fields.CommitPosition = baseB
		e.OnNewLeadershipTerm(fields)
		election.SetStateForTest(e, election.ElectionStateFollowerCatchup)
		appendBefore := e.AppendPosition()

		e.DoWork(clock.nowNs)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.ErrorContains(t, e.LastError(), "election: append log term entry")
		require.ErrorIs(t, e.LastError(), refuse)
		require.ErrorIs(t, e.LastError(), pollErr)
		require.Equal(t, baseB, e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())
		require.Equal(t, priorTerm, election.LogLeadershipTermIDForTest(e))
		require.Equal(t, 1, fake.calls)
	})

	t.Run("replay", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		fake := &failFirstEnsure{RecordingLogCoherency: log, err: refuse}
		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.LeadershipTermID = priorTerm
		cfg.LogPosition = 0
		cfg.AppendPosition = stopS
		host := &recordingElectionHost{
			clock:               clock,
			localLogRecordingID: testRecordingID,
			commitPosition:      baseB,
		}
		deps.Host = host
		deps.RecordingLog = fake
		deps.RecordingLogLookup = log
		var e *election.Election
		host.newLogReplay = func(int64, int64) election.LogReplay {
			return &spyLogReplay{doWork: func(int64) int {
				require.Error(t, e.HandleReplayNewLeadershipTermEvent(event))
				return 1
			}}
		}
		e = election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		fields := installNewLeadershipTermFields(cfg, 1, termID)
		fields.TermBaseLogPosition = 0
		fields.LogPosition = stopS + 500
		fields.CommitPosition = baseB + 500
		e.OnNewLeadershipTerm(fields)
		require.Equal(t, election.ElectionStateFollowerReplay, e.State())

		// The first tick installs the replay. The delivering tick reads the
		// stored error.
		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateFollowerReplay, e.State())
		require.Equal(t, 0, fake.calls)

		e.DoWork(clock.nowNs)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.ErrorContains(t, e.LastError(), "election: append log term entry")
		require.ErrorIs(t, e.LastError(), refuse)
		require.Equal(t, baseB, e.LogPosition())
		require.Equal(t, 1, fake.calls)
	})

	t.Run("stored_error", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		fake := &failFirstEnsure{RecordingLogCoherency: log, err: refuse}
		e, _ := electionAtStateWithLog(
			t, testRecordingID, priorTerm, fake, log, election.ElectionStateFollowerCatchup,
		)

		err1 := e.HandleReplayNewLeadershipTermEvent(event)
		err2 := e.HandleReplayNewLeadershipTermEvent(event)
		require.ErrorContains(t, err1, "election: append log term entry")
		require.ErrorIs(t, err1, refuse)
		require.Equal(t, err1, err2)
		require.Equal(t, 1, fake.calls)
		require.Equal(t, election.ElectionStateFollowerCatchup, e.State())
		require.Nil(t, log.FindTermEntry(termID))
	})

	t.Run("retry", func(t *testing.T) {
		t.Parallel()

		log, _ := openPriorTermLog(t, priorTerm)
		fake := &failFirstEnsure{RecordingLogCoherency: log, err: refuse}
		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.LeadershipTermID = priorTerm
		host := &recordingElectionHost{
			clock:               clock,
			localLogRecordingID: testRecordingID,
			commitPosition:      baseB,
			catchupPollResult:   1,
			liveLogDestination:  "aeron:udp?endpoint=live-log",
		}
		handler := &recordingAgentHandler{appendPositionReturn: stopS}
		deps.Host = host
		deps.Handler = handler
		deps.RecordingLog = fake
		deps.RecordingLogLookup = log
		deps.RecordingLogFileSyncLevel = recordinglog.FileSyncNone
		var e *election.Election
		host.catchupPollHook = func(int64, int64) {
			_ = e.HandleReplayNewLeadershipTermEvent(event)
		}
		e = election.NewElection(cfg, deps)

		election.SetStateForTest(e, election.ElectionStateCanvass)
		failed := installNewLeadershipTermFields(cfg, 1, termID)
		failed.LogPosition = 500
		failed.CommitPosition = baseB
		e.OnNewLeadershipTerm(failed)
		election.SetStateForTest(e, election.ElectionStateFollowerCatchup)
		appendBefore := e.AppendPosition()
		e.DoWork(clock.nowNs)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.ErrorContains(t, e.LastError(), "election: append log term entry")
		require.ErrorIs(t, e.LastError(), refuse)
		require.Equal(t, baseB, e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())
		require.Equal(t, priorTerm, election.LogLeadershipTermIDForTest(e))

		e.DoWork(clock.nowNs)
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, stopS, e.AppendPosition())

		truncated := installNewLeadershipTermFields(cfg, 1, termID)
		truncated.NextLeadershipTermID = termID
		truncated.NextTermBaseLogPosition = baseB
		truncated.TermBaseLogPosition = baseB
		truncated.LogPosition = stopS
		truncated.CommitPosition = baseB
		e.OnNewLeadershipTerm(truncated)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, []truncateCall{{
			leadershipTermID: priorTerm,
			logPosition:      baseB,
		}}, host.truncateCalls)
		requireTermIdentity(t, log, priorTerm, testRecordingID, baseB)
		require.Equal(t, baseB, e.LogPosition())
		require.EqualError(t, e.LastError(), "election: truncating cluster log")

		handler.appendPositionReturn = baseB
		e.DoWork(clock.nowNs)
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, baseB, e.AppendPosition())

		e.OnNewLeadershipTerm(truncated)
		require.Equal(t, election.ElectionStateFollowerReplay, e.State())
		require.Len(t, host.truncateCalls, 1)

		e.DoWork(clock.nowNs)
		require.Equal(t, election.ElectionStateFollowerCatchupInit, e.State())
		require.Len(t, host.truncateCalls, 1)
		require.Equal(t, baseB, e.LogPosition())

		election.SetStateForTest(e, election.ElectionStateFollowerCatchup)
		e.DoWork(clock.nowNs)
		requireTermBasePair(t, log, termID, priorTerm, baseB)
		require.Equal(t, 2, fake.calls)
	})
}
