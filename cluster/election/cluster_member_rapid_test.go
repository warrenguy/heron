//go:build rapid

package election_test

import (
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
