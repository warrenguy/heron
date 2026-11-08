//go:build rapid

package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/cluster/recordinglog"
	"github.com/warrenguy/luas/internal/testutil"
)

type replayTermModel struct {
	rows    []recordinglog.Entry
	logTerm int64
	logPos  int64
}

func (m *replayTermModel) contains(id int64) bool {
	for i := range m.rows {
		if m.rows[i].LeadershipTermID == id {
			return true
		}
	}
	return false
}

// appendNew commits term id-1 to base. Callers append ids in order, so term
// id-1 is the last row.
func (m *replayTermModel) appendNew(id, base, timestamp int64) {
	if n := len(m.rows); n > 0 && m.rows[n-1].LeadershipTermID == id-1 {
		m.rows[n-1].LogPosition = base
	}
	m.rows = append(m.rows, recordinglog.Entry{
		RecordingID:         testRecordingID,
		LeadershipTermID:    id,
		TermBaseLogPosition: base,
		LogPosition:         recordinglog.NullPosition,
		Timestamp:           timestamp,
		Type:                recordinglog.EntryTypeTerm,
		Valid:               true,
	})
}

func (m *replayTermModel) appendThrough(
	from, gapBase, targetBase int64,
	event codecs.NewLeadershipTermEventFields,
) {
	for id := from; id < event.LeadershipTermID; id++ {
		m.appendNew(id, gapBase, event.Timestamp)
	}
	m.appendNew(event.LeadershipTermID, targetBase, event.Timestamp)
}

func (m *replayTermModel) apply(
	state election.ElectionState,
	event codecs.NewLeadershipTermEventFields,
) {
	if state != election.ElectionStateFollowerCatchup &&
		state != election.ElectionStateFollowerReplay {
		return
	}
	m.logTerm = event.LeadershipTermID
	m.logPos = event.LogPosition
	if m.contains(event.LeadershipTermID) {
		return
	}
	if len(m.rows) == 0 {
		m.appendThrough(0, 0, event.TermBaseLogPosition, event)
		return
	}
	last := len(m.rows) - 1
	if m.rows[last].LogPosition == recordinglog.NullPosition {
		m.rows[last].LogPosition = event.TermBaseLogPosition
	}
	fillBase := m.rows[last].LogPosition
	m.appendThrough(m.rows[last].LeadershipTermID+1, fillBase, fillBase, event)
}

func requireReplayTermModel(
	rt *rapid.T,
	log *recordinglog.Log,
	e *election.Election,
	m replayTermModel,
) {
	rt.Helper()
	require.Equal(rt, m.rows, termIdentities(log.Entries()))
	require.Equal(rt, m.logTerm, election.LogLeadershipTermIDForTest(e))
	require.Equal(rt, m.logPos, e.LogPosition())
}

func TestElection_replayNewLeadershipTermEvent_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		log, dir := openElectionRecordingLog(t)

		seed := rapid.IntRange(0, 4).Draw(rt, "seedRows")
		var floor int64
		for i := range seed {
			base := rapid.Int64Range(floor, floor+500).Draw(rt, "seedBase")
			require.NoError(rt, log.AppendTerm(testRecordingID, int64(i), base, 1))
			floor = base
		}

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.LeadershipTermID = election.NullValue
		cfg.TermBaseLogPosition = 0
		cfg.LogPosition = 0
		cfg.AppendPosition = 0
		deps.Host = &recordingElectionHost{clock: clock, localLogRecordingID: testRecordingID}
		deps.RecordingLog = log
		deps.RecordingLogLookup = log
		deps.RecordingLogFileSyncLevel = recordinglog.FileSyncNone
		e := election.NewElection(cfg, deps)
		model := replayTermModel{
			rows:    termIdentities(log.Entries()),
			logTerm: election.LogLeadershipTermIDForTest(e),
			logPos:  e.LogPosition(),
		}

		steps := rapid.IntRange(1, 6).Draw(rt, "steps")
		for range steps {
			// Bias toward the two states that write.
			state := rapid.SampledFrom([]election.ElectionState{
				election.ElectionStateFollowerCatchup,
				election.ElectionStateFollowerReplay,
			}).Draw(rt, "consumeState")
			if rapid.Bool().Draw(rt, "anyState") {
				state = election.ElectionState(
					rapid.IntRange(0, election.ElectionStateNameCount()-1).Draw(rt, "state"),
				)
			}
			next := int64(0)
			baseFloor := int64(0)
			if n := len(model.rows); n > 0 {
				next = model.rows[n-1].LeadershipTermID + 1
				baseFloor = model.rows[n-1].TermBaseLogPosition
			}
			var termID int64
			switch rapid.IntRange(0, 2).Draw(rt, "termKind") {
			case 0:
				if len(model.rows) == 0 {
					termID = next
				} else {
					idx := rapid.IntRange(0, len(model.rows)-1).Draw(rt, "existing")
					termID = model.rows[idx].LeadershipTermID
				}

			case 1:
				termID = next

			default:
				termID = next + 1
			}
			base := rapid.Int64Range(baseFloor, baseFloor+1_000).Draw(rt, "base")
			logPos := rapid.Int64Range(base, base+1_000).Draw(rt, "logPos")
			event := codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    termID,
				TermBaseLogPosition: base,
				LogPosition:         logPos,
				Timestamp:           rapid.Int64Range(0, 1<<62).Draw(rt, "timestamp"),
			}

			election.SetStateForTest(e, state)
			require.NoError(rt, e.HandleReplayNewLeadershipTermEvent(event))
			require.Equal(rt, state, e.State())
			model.apply(state, event)
			requireReplayTermModel(rt, log, e, model)
		}

		want := termIdentities(log.Entries())
		require.NoError(rt, log.Close())
		reopened := reopenRecordingLog(t, dir)
		require.Equal(rt, want, termIdentities(reopened.Entries()))
		require.NoError(rt, reopened.Close())
	})
}
