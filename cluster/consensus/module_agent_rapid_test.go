//go:build rapid

package consensus

import (
	"sync/atomic"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestProposeMaxCommit_monotonic_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a := NewAgent(AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: 10,
		})
		n := rapid.IntRange(1, 40).Draw(rt, "n")
		now := int64(0)
		oracle := int64(0)
		for range n {
			q := rapid.Int64Range(0, 1000).Draw(rt, "q")
			// Force gate open each step via interval advance.
			now += a.leaderHeartbeatIntervalNs
			require.Equal(rt, 1, a.UpdateLeaderPosition(now, q))
			oracle = max(oracle, q)
			require.Equal(rt, oracle, a.CommitPosition())
		}
	})
}

func TestUpdateLeaderPosition_commitBoundedByAppend_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		// Option B pure oracle: apply host min before the gate, then
		// UpdateLeaderPosition. No AppendSource - this path does not call
		// consensusWork or rank tables. Append is non-decreasing (log never
		// shrinks). Invariant: CommitPosition never exceeds current append.
		a := NewAgent(AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: 10,
		})
		n := rapid.IntRange(1, 40).Draw(rt, "n")
		now := int64(0)
		appendPos := int64(0)
		for range n {
			// Non-decreasing append: draw delta, not absolute that can regress.
			delta := rapid.Int64Range(0, 50).Draw(rt, "appendDelta")
			appendPos += delta
			rankedQuorum := rapid.Int64Range(0, appendPos+50).Draw(rt, "rankedQuorum")
			// Host min before gate - do not pass unconstrained ranked quorum.
			q := min(rankedQuorum, appendPos)
			now += a.leaderHeartbeatIntervalNs
			_ = a.UpdateLeaderPosition(now, q)
			require.LessOrEqual(rt, a.CommitPosition(), appendPos)
		}
	})
}

func TestUpdateLeaderPosition_timeOfLastLogUpdate_monotonicOnPublish_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a := NewAgent(AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: 50,
		})
		n := rapid.IntRange(1, 50).Draw(rt, "n")
		now := int64(0)
		prevLogUpdate := int64(0)
		for range n {
			// Mix deltas and quorums so some steps fail the gate.
			delta := rapid.Int64Range(1, 80).Draw(rt, "delta")
			now += delta
			q := rapid.Int64Range(0, 200).Draw(rt, "q")
			commitBefore := a.CommitPosition()

			// Independent oracle for gate open.
			intervalDue := now >= prevLogUpdate+a.leaderHeartbeatIntervalNs
			expectWork := 0
			if q > commitBefore || intervalDue {
				expectWork = 1
			}

			work := a.UpdateLeaderPosition(now, q)
			require.Equal(rt, expectWork, work)

			if work == 1 {
				require.Equal(rt, now, a.timeOfLastLogUpdateNs)
				prevLogUpdate = a.timeOfLastLogUpdateNs
			} else {
				require.Equal(rt, prevLogUpdate, a.timeOfLastLogUpdateNs)
			}
		}
	})
}

// TestOnSteadyCanvassPosition_guard_rapid asserts settled canvass publish and
// position-update gates over random role, term, and collaborator wiring.
func TestOnSteadyCanvassPosition_guard_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		roles := []Role{RoleFollower, RoleCandidate, RoleLeader}
		role := roles[rapid.IntRange(0, len(roles)-1).Draw(rt, "role")]
		leaderTerm := rapid.Int64Range(0, 20).Draw(rt, "leaderTerm")
		canvassTerm := rapid.Int64Range(0, 25).Draw(rt, "canvassTerm")
		followerID := rapid.Int32Range(0, 10).Draw(rt, "followerID")
		wirePositions := rapid.Bool().Draw(rt, "wirePositions")
		wireEgress := rapid.Bool().Draw(rt, "wireEgress")

		pt := &spyPositionTable{}
		eg := &recordingEgress{}
		cfg := AgentConfig{
			Role:             role,
			Commit:           new(atomic.Int64),
			ThisMemberID:     0,
			LeadershipTermID: leaderTerm,
		}
		if wirePositions {
			cfg.Positions = pt
		}
		if wireEgress {
			cfg.Egress = eg
		}
		a := NewAgent(cfg)

		fields := codecs.CanvassPositionFields{
			LogLeadershipTermID: canvassTerm,
			LogPosition:         rapid.Int64Range(0, 1000).Draw(rt, "logPos"),
			LeadershipTermID:    canvassTerm,
			FollowerMemberID:    followerID,
		}
		a.onSteadyCanvassPosition(fields, 1)

		expectSideEffects := role == RoleLeader &&
			canvassTerm <= leaderTerm &&
			wirePositions &&
			wireEgress
		if expectSideEffects {
			require.Len(rt, eg.answers, 1)
			require.Len(rt, pt.updates, 1)
		} else {
			require.Empty(rt, eg.answers)
			require.Empty(rt, pt.updates)
		}
	})
}
