//go:build rapid && testhooks

package consensus

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

// Asserts TrackCatchupCompletion against a reference model over random
// AppendPosition sequences (flag, position, stop failure, publish failure).
func TestTrackCatchupCompletion_sequences_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		eg := &recordingEgress{}
		a, control, publisher := newLeaderCatchupAgent(
			t,
			AgentConfig{Egress: eg, LeadershipTermID: 1},
			"",
		)

		// Reference map mirrors a.catchupReplays.
		ref := make(map[int32]catchupReplay)
		leaderPos := publisher.Position()

		n := rapid.IntRange(1, 80).Draw(rt, "n")
		for range n {
			op := rapid.IntRange(0, 2).Draw(rt, "op")
			switch op {
			case 0:
				// Start or restart catchup for a member.
				member := int32(rapid.IntRange(1, 4).Draw(rt, "member"))
				startFollowerCatchup(a, member, "127.0.0.1:9300")

				// Sync reference from agent after the start/restart.
				got, ok := a.catchupReplays[member]
				require.True(rt, ok)
				ref[member] = got
			case 1:
				// AppendPosition tick: random flag, position, failures.
				member := int32(rapid.IntRange(1, 4).Draw(rt, "tickMember"))
				flags := AppendPositionFlagNone
				if rapid.Bool().Draw(rt, "flagged") {
					flags = AppendPositionFlagCatchup
				}

				// Position relative to leader: below or at/above.
				delta := int64(rapid.IntRange(0, 10).Draw(rt, "delta"))
				logPos := leaderPos + delta
				if !rapid.Bool().Draw(rt, "atOrAbove") {
					logPos = leaderPos - 1 - delta
				}
				control.stopOneErr = nil
				if rapid.Bool().Draw(rt, "stopFail") {
					control.stopOneErr = errTestCatchupStopRefused
				}
				eg.refuseStopCatchup = rapid.Bool().Draw(rt, "pubFail")

				a.TrackCatchupCompletion(member, logPos, flags)

				// Reference model mirrors TrackCatchupCompletion. A flagged position for an
				// absent member keeps the map untouched. Position gates the stop half only.
				// Once stopped, publish retry ignores leader advance.
				entry, active := ref[member]
				if active {
					if !entry.stopped && logPos >= leaderPos && control.stopOneErr == nil {
						entry.stopped = true
						ref[member] = entry
					}
					if entry.stopped && !eg.refuseStopCatchup {
						delete(ref, member)
					}
				}
			case 2:
				a.StopAllCatchups()
				clear(ref)
			}

			// Map matches reference.
			require.Len(rt, a.catchupReplays, len(ref))
			for member, want := range ref {
				got, ok := a.catchupReplays[member]
				require.True(rt, ok, "member %d missing from map", member)
				require.Equal(rt, want, got)
			}
		}
	})
}

// Asserts one replay per member and that the control ledger matches the agent
// map under random request, complete, and teardown sequences.
func TestCatchupReplays_mapLifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		a, control, _ := newLeaderCatchupAgent(t, AgentConfig{}, "")

		const endpoint = "127.0.0.1:9200"
		n := rapid.IntRange(1, 50).Draw(rt, "n")
		for range n {
			op := rapid.IntRange(0, 2).Draw(rt, "op")
			switch op {
			case 0:
				// Start or restart a follower catchup.
				member := int32(rapid.IntRange(1, 5).Draw(rt, "member"))
				a.OnCatchupPosition(codecs.CatchupPositionFields{
					LogPosition:      rapid.Int64Range(0, 1000).Draw(rt, "pos"),
					FollowerMemberID: member,
					CatchupEndpoint:  endpoint,
				})
			case 1:
				// Full teardown: drain map and ledger.
				a.StopAllCatchups()
			case 2:
				// Successful complete for one member: stop session, drop entry.
				// Archive stop half only. Publish half is not exercised here.
				for member, replay := range a.catchupReplays {
					_ = control.StopCatchupReplay(replay.replaySessionID)
					delete(a.catchupReplays, member)
					break
				}
			}

			requireCatchupLedgerMatchesMap(rt, a, control)
		}
	})
}

// Asserts one session per member and that the control retained set equals the
// map session set.
func requireCatchupLedgerMatchesMap(
	rt *rapid.T,
	a *Agent,
	control *fakeCatchupControl,
) {
	rt.Helper()

	require.Len(rt, control.retained, len(a.catchupReplays))

	mapSessions := make(map[int64]struct{}, len(a.catchupReplays))
	for _, replay := range a.catchupReplays {
		require.False(rt, replay.stopped)
		_, dup := mapSessions[replay.replaySessionID]
		require.False(rt, dup, "session %d claimed by two members", replay.replaySessionID)
		mapSessions[replay.replaySessionID] = struct{}{}
	}
	for _, id := range control.retained {
		_, ok := mapSessions[id]
		require.True(rt, ok, "ledger session %d missing from map", id)
	}
}
