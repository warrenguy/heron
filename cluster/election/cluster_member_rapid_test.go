//go:build rapid

package election_test

import (
	"cmp"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestCompareLog_antisymmetry_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		lhsTerm := rapid.Int64().Draw(rt, "lhsTerm")
		lhsPos := rapid.Int64().Draw(rt, "lhsPos")
		rhsTerm := rapid.Int64().Draw(rt, "rhsTerm")
		rhsPos := rapid.Int64().Draw(rt, "rhsPos")

		forward := election.CompareLog(lhsTerm, lhsPos, rhsTerm, rhsPos)
		reverse := election.CompareLog(rhsTerm, rhsPos, lhsTerm, lhsPos)
		require.Equal(rt, -forward, reverse)
	})
}

func TestWillVoteFor_matchesCompareLog_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		member := newTestMemberWithLog(
			1,
			rapid.Int64().Draw(rt, "memberTerm"),
			drawLogPosition(rt, "memberPos"),
			0,
		)
		candidate := newTestMemberWithLog(
			2,
			rapid.Int64().Draw(rt, "candidateTerm"),
			drawLogPosition(rt, "candidatePos"),
			0,
		)

		got := election.WillVoteFor(member, candidate)
		want := member.LogPosition != election.NullPosition &&
			election.CompareLogMembers(member, candidate) <= 0
		require.Equal(rt, want, got)
		require.Equal(rt, got, member.WillVoteFor(candidate))
	})
}

func drawLogPosition(rt *rapid.T, label string) int64 {
	rt.Helper()
	return rapid.OneOf(rapid.Just(election.NullPosition), rapid.Int64()).Draw(rt, label)
}

func TestQuorumThreshold_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		memberCount := rapid.IntRange(1, 1024).Draw(rt, "memberCount")
		threshold := election.QuorumThreshold(memberCount)
		require.Equal(rt, (memberCount>>1)+1, threshold)
		require.Greater(rt, 2*threshold, memberCount)
		require.LessOrEqual(rt, 2*(threshold-1), memberCount)
	})
}

// quorumPositionOracle is an independent model of QuorumPosition.
//
// Params:
//   - members: membership under test
//   - k: ranking depth (caller passes QuorumThreshold(n) from the property)
//   - nowNs, timeoutNs: same IsActive window as QuorumPosition
//
// Algorithm:
//  1. collect LogPosition of members with IsActive(nowNs, timeoutNs)
//  2. sort descending
//  3. if fewer than k values, return 0 (zero-fill)
//  4. else return max(0, active[k-1])  // k-th highest; clamp for NullPosition
//
// k-th means the value at 0-based index k-1 after that descending sort.
func quorumPositionOracle(members []*election.Member, k int, nowNs, timeoutNs int64) int64 {
	active := make([]int64, 0, len(members))
	for _, m := range members {
		if m.IsActive(nowNs, timeoutNs) {
			active = append(active, m.LogPosition)
		}
	}
	slices.SortFunc(active, func(a, b int64) int {
		return cmp.Compare(b, a)
	})
	if len(active) < k {
		return 0
	}
	return max(active[k-1], 0)
}

// drawQuorumLogPosition draws LogPosition values biased toward NullPosition,
// non-negative positions, and full int64 range (negatives other than Null).
func drawQuorumLogPosition(rt *rapid.T, label string) int64 {
	rt.Helper()
	return rapid.OneOf(
		rapid.Just(election.NullPosition),
		rapid.Int64Min(0),
		rapid.Int64(),
	).Draw(rt, label)
}

// TestQuorumPosition_rapid checks QuorumPosition against quorumPositionOracle for
// random clusters, activity windows, and dirty ranked buffers.
//
// Ranking for this test:
//   - k = QuorumThreshold(n) (len of ranked scratch)
//   - k-th = k-th highest active LogPosition after sort descending (index k-1);
//     missing ranks stay 0; only strict > displaces a ranked slot
//   - oracle documents the same model; see quorumPositionOracle
//
// Drawn inputs:
//   - n: member count 1..9
//   - nowNs, timeoutNs: IsActive window (active when ts+timeoutNs > nowNs)
//   - term: LeadershipTermID (must not affect ranking)
//   - pos: LogPosition via drawQuorumLogPosition
//   - ts: TimeOfLastAppendPositionNs
//   - dirtyRanked: when true, prefill ranked with garbage before the call
//
// Also asserts:
//   - result >= 0
//   - second call is idempotent
//   - members are not mutated
func TestQuorumPosition_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		n := rapid.IntRange(1, 9).Draw(rt, "n")
		nowNs := rapid.Int64Range(0, 100).Draw(rt, "nowNs")
		timeoutNs := rapid.Int64Range(0, 50).Draw(rt, "timeoutNs")

		members := make([]*election.Member, n)
		for i := range n {
			members[i] = newTestMemberWithLog(
				int32(i),
				rapid.Int64().Draw(rt, "term"),
				drawQuorumLogPosition(rt, "pos"),
				rapid.Int64Range(-10, 100).Draw(rt, "ts"),
			)
		}

		// Snapshot fields QuorumPosition must not mutate.
		type snap struct {
			logPos int64
			ts     int64
			term   int64
		}
		before := make([]snap, n)
		for i, m := range members {
			before[i] = snap{
				logPos: m.LogPosition,
				ts:     m.TimeOfLastAppendPositionNs,
				term:   m.LeadershipTermID,
			}
		}

		k := election.QuorumThreshold(n)
		ranked := make([]int64, k)

		// Optionally dirty-fill ranked so zero-on-entry is exercised.
		if rapid.Bool().Draw(rt, "dirtyRanked") {
			for i := range ranked {
				ranked[i] = rapid.Int64().Draw(rt, "dirty")
			}
		}

		want := quorumPositionOracle(members, k, nowNs, timeoutNs)
		got := election.QuorumPosition(members, ranked, nowNs, timeoutNs)
		require.Equal(rt, want, got)
		require.GreaterOrEqual(rt, got, int64(0))

		got2 := election.QuorumPosition(members, ranked, nowNs, timeoutNs)
		require.Equal(rt, got, got2)

		for i, m := range members {
			require.Equal(rt, before[i].logPos, m.LogPosition)
			require.Equal(rt, before[i].ts, m.TimeOfLastAppendPositionNs)
			require.Equal(rt, before[i].term, m.LeadershipTermID)
		}
	})
}

// TestHasActiveQuorum_rapid asserts HasActiveQuorum against an IsActive count
// oracle for random clusters and activity windows.
//
// Drawn inputs:
//   - n: member count 1..9
//   - nowNs, timeoutNs: IsActive window (active when ts+timeoutNs > nowNs)
//   - ts: TimeOfLastAppendPositionNs per member
//   - term: LeadershipTermID over full int64 (must not affect result)
//   - pos: LogPosition via drawQuorumLogPosition (must not affect result)
//
// Oracle: count members with IsActive. True when count >= QuorumThreshold(n).
// After the oracle check, scramble term and position on every member. The
// result must not move. A rewrite that reads either field fails the scramble.
func TestHasActiveQuorum_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		n := rapid.IntRange(1, 9).Draw(rt, "n")
		nowNs := rapid.Int64Range(0, 100).Draw(rt, "nowNs")
		timeoutNs := rapid.Int64Range(0, 50).Draw(rt, "timeoutNs")

		members := make([]*election.Member, n)
		active := 0
		for i := range n {
			members[i] = newTestMemberWithLog(
				int32(i),
				rapid.Int64().Draw(rt, "term"),
				drawQuorumLogPosition(rt, "pos"),
				rapid.Int64Range(-10, 100).Draw(rt, "ts"),
			)
			if members[i].IsActive(nowNs, timeoutNs) {
				active++
			}
		}

		want := active >= election.QuorumThreshold(n)
		got := election.HasActiveQuorum(members, nowNs, timeoutNs)
		require.Equal(rt, want, got)

		// Scramble term and position. Liveness quorum must not move.
		for _, m := range members {
			m.LeadershipTermID = rapid.Int64().Draw(rt, "scrambleTerm")
			m.LogPosition = drawQuorumLogPosition(rt, "scramblePos")
		}
		require.Equal(rt, got, election.HasActiveQuorum(members, nowNs, timeoutNs))
	})
}
