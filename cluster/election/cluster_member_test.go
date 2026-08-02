package election_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/election"
)

func newTestMemberWithLog(
	id int32,
	leadershipTermID, logPosition, timeOfLastAppendPositionNs int64,
) *election.Member {
	return &election.Member{
		ID:                         id,
		LeadershipTermID:           leadershipTermID,
		LogPosition:                logPosition,
		TimeOfLastAppendPositionNs: timeOfLastAppendPositionNs,
	}
}

func memberPtrs(members ...*election.Member) []*election.Member {
	return members
}

func TestQuorumThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		memberCount int
		want        int
	}{
		{name: "one", memberCount: 1, want: 1},
		{name: "two", memberCount: 2, want: 2},
		{name: "three", memberCount: 3, want: 2},
		{name: "four", memberCount: 4, want: 3},
		{name: "five", memberCount: 5, want: 3},
		{name: "six", memberCount: 6, want: 4},
		{name: "seven", memberCount: 7, want: 4},
		// Java parity: (memberCount >> 1) + 1 yields 1 for zero; mirrors Java bit-shift
		// for negative inputs (e.g. -1 -> 0).
		{name: "zero", memberCount: 0, want: 1},
		{name: "negative", memberCount: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, election.QuorumThreshold(tt.memberCount))
		})
	}
}

// requireQuorumPosition builds a ranked scratch and asserts QuorumPosition.
//
// Params:
//   - members: cluster membership under test
//   - nowNs, timeoutNs: passed to QuorumPosition / IsActive (active when
//     TimeOfLastAppendPositionNs+timeoutNs > nowNs)
//   - want: expected return value
//
// Ranked length is QuorumThreshold(len(members)) (1 when n=0).
// Callers document their own k and expected meaning of want.
func requireQuorumPosition(
	t *testing.T,
	members []*election.Member,
	nowNs, timeoutNs, want int64,
) {
	t.Helper()
	ranked := make([]int64, election.QuorumThreshold(len(members)))
	got := election.QuorumPosition(members, ranked, nowNs, timeoutNs)
	require.Equal(t, want, got)
}

// TestQuorumPosition_threeMembers ranks LogPosition among three always-active
// members.
//
// Fixed setup:
//   - n = 3 members
//   - timeOfLastAppend = 0 for every member (all active under the window below)
//   - nowNs = 0, timeoutNs = 10
//   - leadership term = 0
//
// Ranking for this test:
//   - k = QuorumThreshold(3) = 2 (len of ranked scratch)
//   - k-th = k-th highest LogPosition after sort descending (index k-1);
//     missing ranks stay 0 (zero-fill); only strict > displaces a ranked slot
//
// Table row fields:
//   - pos[i]: LogPosition for member i
//   - want: expected QuorumPosition (= k-th under the rules above)
//
// Asserts via requireQuorumPosition. Permutation rows of {1,3,5} prove member
// order does not change the result.
func TestQuorumPosition_threeMembers(t *testing.T) {
	t.Parallel()

	// All members stay active: ts+timeout > now.
	const (
		nowNs     int64 = 0
		timeoutNs int64 = 10
	)

	tests := []struct {
		name string
		pos  [3]int64
		want int64
	}{
		{name: "all zero", pos: [3]int64{0, 0, 0}, want: 0},
		// One positive cannot fill k=2; floor stays 0.
		{name: "one at 123", pos: [3]int64{123, 0, 0}, want: 0},
		{name: "two at 123", pos: [3]int64{123, 123, 0}, want: 123},
		{name: "three at 123", pos: [3]int64{123, 123, 123}, want: 123},
		{name: "last two at 123", pos: [3]int64{0, 123, 123}, want: 123},
		{name: "only third at 123", pos: [3]int64{0, 0, 123}, want: 0},
		// Desc 200,123,0 -> k-th = 123.
		{name: "mixed 0 123 200", pos: [3]int64{0, 123, 200}, want: 123},
		// All six orders of {1,3,5}: k-th is always 3.
		{name: "5 3 1", pos: [3]int64{5, 3, 1}, want: 3},
		{name: "5 1 3", pos: [3]int64{5, 1, 3}, want: 3},
		{name: "1 3 5", pos: [3]int64{1, 3, 5}, want: 3},
		{name: "1 5 3", pos: [3]int64{1, 5, 3}, want: 3},
		{name: "3 1 5", pos: [3]int64{3, 1, 5}, want: 3},
		{name: "3 5 1", pos: [3]int64{3, 5, 1}, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := make([]*election.Member, 3)
			for i, p := range tt.pos {
				members[i] = newTestMemberWithLog(int32(i), 0, p, 0)
			}
			requireQuorumPosition(t, members, nowNs, timeoutNs, tt.want)
		})
	}
}

// TestQuorumPosition_activeOnly ranks five members with mixed activity.
// Inactive members are excluded before ranking.
//
// Fixed setup:
//   - n = 5 members
//   - leadership term = 0
//
// Ranking for this test:
//   - k = QuorumThreshold(5) = 3 (len of ranked scratch)
//   - k-th = k-th highest active LogPosition after sort descending (index k-1);
//     missing ranks stay 0 (zero-fill); only strict > displaces a ranked slot
//
// Table row fields:
//   - pos[i]: LogPosition for member i
//   - ts[i]: TimeOfLastAppendPositionNs for member i
//   - nowNs, timeoutNs: IsActive window (active when ts[i]+timeoutNs > nowNs)
//   - want: expected QuorumPosition (= k-th under the rules above)
//
// Asserts via requireQuorumPosition.
func TestQuorumPosition_activeOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pos       [5]int64
		ts        [5]int64
		nowNs     int64
		timeoutNs int64
		want      int64
	}{
		// All active; all positions zero.
		{
			name: "all zero",
			pos:  [5]int64{0, 0, 0, 0, 0}, ts: [5]int64{0, 0, 0, 0, 0},
			nowNs: 0, timeoutNs: 10, want: 0,
		},
		// Desc 5,4,3,2,1 -> k-th = 3.
		{
			name: "all active ascending",
			pos:  [5]int64{1, 2, 3, 4, 5}, ts: [5]int64{0, 0, 0, 0, 0},
			nowNs: 5, timeoutNs: 10, want: 3,
		},
		// Desc 5,3,2,2,1 -> k-th = 2.
		{
			name: "all active with ties",
			pos:  [5]int64{1, 2, 3, 2, 5}, ts: [5]int64{0, 0, 0, 0, 0},
			nowNs: 5, timeoutNs: 10, want: 2,
		},
		// now=5, timeout=5: active when ts > 0. Actives pos 1,3,5 -> k-th = 1.
		{
			name: "three active timeout 5",
			pos:  [5]int64{1, 2, 3, 2, 5}, ts: [5]int64{1, 0, 1, 0, 1},
			nowNs: 5, timeoutNs: 5, want: 1,
		},
		// Same activity mask; positions reordered; k-th still 1.
		{
			name: "three active reordered",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{1, 0, 1, 0, 1},
			nowNs: 5, timeoutNs: 5, want: 1,
		},
		// Only two actives: pad to k=3 with 0.
		{
			name: "two active yields zero",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{0, 1, 0, 1, 0},
			nowNs: 5, timeoutNs: 5, want: 0,
		},
		// Actives pos 2,2,1 -> k-th = 1.
		{
			name: "three active mixed times",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{0, 1, 0, 1, 1},
			nowNs: 5, timeoutNs: 5, want: 1,
		},
		// Actives pos 2,3,2 -> k-th = 2.
		{
			name: "three active higher quorum",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{0, 1, 1, 1, 0},
			nowNs: 5, timeoutNs: 5, want: 2,
		},
		// now=8, timeout=7: active when ts > 1. Actives m1,m2,m4 at 2,3,1 -> k-th = 1.
		{
			name: "three active late now",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{0, 2, 3, 1, 5},
			nowNs: 8, timeoutNs: 7, want: 1,
		},
		// All ts=0 under now=8, timeout=7: no actives.
		{
			name: "none active late now",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{0, 0, 0, 0, 0},
			nowNs: 8, timeoutNs: 7, want: 0,
		},
		// Only m0 active: one contributor, pad to k=3 -> 0.
		{
			name: "one active early ts",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{5, 0, 0, 0, 0},
			nowNs: 8, timeoutNs: 7, want: 0,
		},
		// Two actives (m0,m1): still below k=3 -> 0.
		{
			name: "two active still zero",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{5, 5, 0, 0, 0},
			nowNs: 8, timeoutNs: 7, want: 0,
		},
		// Actives pos 5,2,3 -> k-th = 2.
		{
			name: "three active at 5",
			pos:  [5]int64{5, 2, 3, 2, 1}, ts: [5]int64{5, 5, 5, 0, 0},
			nowNs: 8, timeoutNs: 7, want: 2,
		},
		// Actives pos 4,2,4 -> k-th = 2.
		{
			name: "three active ties at 4",
			pos:  [5]int64{4, 2, 4, 2, 1}, ts: [5]int64{5, 5, 5, 0, 0},
			nowNs: 8, timeoutNs: 7, want: 2,
		},
		// Four actives (m3 inactive); desc 4,4,3,2 -> k-th = 3.
		{
			name: "four active quorum 3",
			pos:  [5]int64{4, 2, 4, 2, 3}, ts: [5]int64{5, 5, 5, 0, 3},
			nowNs: 8, timeoutNs: 7, want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := make([]*election.Member, 5)
			for i := range members {
				members[i] = newTestMemberWithLog(int32(i), 0, tt.pos[i], tt.ts[i])
			}
			requireQuorumPosition(t, members, tt.nowNs, tt.timeoutNs, tt.want)
		})
	}
}

// TestQuorumPosition_edges covers contract edges outside the main tables.
//
// Ranking terms used in subtests:
//   - k = QuorumThreshold(n) unless a subtest passes an explicit ranked slice
//   - k-th = k-th highest active LogPosition after sort descending (index k-1);
//     missing ranks stay 0 (zero-fill); only strict > displaces a ranked slot
//
// Cases:
//   - empty/nil membership
//   - n=1 active and inactive
//   - LeadershipTermID ignored for ranking
//   - NullPosition cluster-start (zero-fill + strict >)
//   - MaxInt64 positions
//   - dirty ranked buffer zeroed on entry
//   - empty ranked panics
//
// requireQuorumPosition subtests pass (members, nowNs, timeoutNs, want).
// buffer zeroed and empty ranked panics call QuorumPosition directly.
func TestQuorumPosition_edges(t *testing.T) {
	t.Parallel()

	// High positions but timed out: nowNs=100, timeoutNs=1, ts=0 => inactive.
	t.Run("all inactive", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, memberPtrs(
			newTestMemberWithLog(0, 0, 100, 0),
			newTestMemberWithLog(1, 0, 200, 0),
			newTestMemberWithLog(2, 0, 300, 0),
		), 100, 1, 0)
	})

	// No members: QuorumThreshold(0)=1; result stays zero-filled.
	t.Run("nil members", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, nil, 0, 10, 0)
	})

	t.Run("empty members", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, []*election.Member{}, 0, 10, 0)
	})

	// n=1, k=1: sole active LogPosition is the quorum position.
	t.Run("single active n=1", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, memberPtrs(newTestMemberWithLog(0, 0, 42, 0)), 0, 10, 42)
	})

	// Same member, timed out (now=100, timeout=1, ts=0).
	t.Run("n=1 inactive", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, memberPtrs(newTestMemberWithLog(0, 0, 42, 0)), 100, 1, 0)
	})

	// newTestMemberWithLog(id, leadershipTermID, logPosition, timeOfLastAppend).
	// Terms 1 vs 99 must not filter; only activity and LogPosition rank.
	t.Run("term ignored", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			newTestMemberWithLog(0, 1, 100, 0),
			newTestMemberWithLog(1, 99, 100, 0),
			newTestMemberWithLog(2, 1, 50, 0),
		)

		// Drop member 2 so actives are two at pos 100 with different terms.
		members[2].TimeOfLastAppendPositionNs = -1000
		requireQuorumPosition(t, members, 0, 10, 100)
	})

	// All LogPosition=NullPosition (-1). Strict > vs zero-fill never inserts
	// negatives; result 0 (cluster-start shape).
	t.Run("null position active", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, memberPtrs(
			newTestMemberWithLog(0, 0, election.NullPosition, 0),
			newTestMemberWithLog(1, 0, election.NullPosition, 0),
			newTestMemberWithLog(2, 0, election.NullPosition, 0),
		), 0, 10, 0)
	})

	// Two actives at MaxInt64 fill k=2; no overflow in compare/insert.
	t.Run("max int64", func(t *testing.T) {
		t.Parallel()
		requireQuorumPosition(t, memberPtrs(
			newTestMemberWithLog(0, 0, math.MaxInt64, 0),
			newTestMemberWithLog(1, 0, math.MaxInt64, 0),
			newTestMemberWithLog(2, 0, 0, 0),
		), 0, 10, math.MaxInt64)
	})

	// Dirty caller-owned ranked must clear on entry. Members inactive under
	// (nowNs=100, timeoutNs=1) so only the zero-fill is observed.
	t.Run("buffer zeroed", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			newTestMemberWithLog(0, 0, 100, 0),
			newTestMemberWithLog(1, 0, 200, 0),
			newTestMemberWithLog(2, 0, 300, 0),
		)

		ranked := []int64{999, 888}
		got := election.QuorumPosition(members, ranked, 100, 1)
		require.Equal(t, int64(0), got)
		require.Equal(t, []int64{0, 0}, ranked)
	})

	// len(ranked)==0 is a programming error; access of last slot panics.
	t.Run("empty ranked panics", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(newTestMemberWithLog(0, 0, 42, 0))
		require.Panics(t, func() {
			election.QuorumPosition(members, []int64{}, 0, 10)
		})
	})
}

func TestCompareLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		lhsTerm int64
		lhsPos  int64
		rhsTerm int64
		rhsPos  int64
		want    int
	}{
		{
			name: "lhs newer term", lhsTerm: 5, lhsPos: 1000,
			rhsTerm: 3, rhsPos: 999999, want: 1,
		},
		{
			name: "rhs newer term", lhsTerm: -100, lhsPos: 99999,
			rhsTerm: 4, rhsPos: 0, want: -1,
		},
		{
			name: "equal term lhs newer pos", lhsTerm: 42, lhsPos: 371239192371239,
			rhsTerm: 42, rhsPos: 1001, want: 1,
		},
		{
			name: "equal term rhs newer pos", lhsTerm: 3, lhsPos: -777,
			rhsTerm: 3, rhsPos: 273291846723894, want: -1,
		},
		{
			name: "equal logs", lhsTerm: 1, lhsPos: 1024,
			rhsTerm: 1, rhsPos: 1024, want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := election.CompareLog(tt.lhsTerm, tt.lhsPos, tt.rhsTerm, tt.rhsPos)
			require.Equal(t, tt.want, got)

			lhs := newTestMemberWithLog(5, tt.lhsTerm, tt.lhsPos, 0)
			rhs := newTestMemberWithLog(100, tt.rhsTerm, tt.rhsPos, 0)
			require.Equal(t, tt.want, election.CompareLogMembers(lhs, rhs))
		})
	}
}

func TestWillVoteFor(t *testing.T) {
	t.Parallel()

	t.Run("no position", func(t *testing.T) {
		t.Parallel()
		member := newTestMemberWithLog(1, 0, election.NullPosition, 0)
		candidate := newTestMemberWithLog(2, 0, 100, 0)
		require.False(t, election.WillVoteFor(member, candidate))
		require.False(t, member.WillVoteFor(candidate))
	})

	t.Run("more log", func(t *testing.T) {
		t.Parallel()
		member := newTestMemberWithLog(1, 0, 500, 0)
		candidate := newTestMemberWithLog(2, 0, 100, 0)
		require.False(t, election.WillVoteFor(member, candidate))
		require.False(t, member.WillVoteFor(candidate))
	})

	for _, logPosition := range []int64{0, 900, 1024} {
		t.Run("less or equal log", func(t *testing.T) {
			t.Parallel()
			member := newTestMemberWithLog(1, 0, logPosition, 0)
			candidate := newTestMemberWithLog(2, 0, 1024, 0)
			require.True(t, election.WillVoteFor(member, candidate))
			require.True(t, member.WillVoteFor(candidate))
		})
	}

	t.Run("newer candidate term wins over position", func(t *testing.T) {
		t.Parallel()
		member := newTestMemberWithLog(1, 0, 1024, 0)
		candidate := newTestMemberWithLog(2, 1, 1024, 0)
		require.True(t, election.WillVoteFor(member, candidate))
		require.True(t, member.WillVoteFor(candidate))
	})

	t.Run("older candidate term loses despite position", func(t *testing.T) {
		t.Parallel()
		member := newTestMemberWithLog(1, 2, 0, 0)
		candidate := newTestMemberWithLog(2, 1, 999999, 0)
		require.False(t, election.WillVoteFor(member, candidate))
		require.False(t, member.WillVoteFor(candidate))
	})
}

func TestIsActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		timeOfLastAppendPositionNs int64
		nowNs                      int64
		timeoutNs                  int64
		want                       bool
	}{
		{
			name:                       "inactive at origin",
			timeOfLastAppendPositionNs: 0, nowNs: 0, timeoutNs: 0, want: false,
		},
		{
			name:                       "active within timeout",
			timeOfLastAppendPositionNs: 0, nowNs: 0, timeoutNs: 1, want: true,
		},
		{
			name:                       "inactive one past timeout",
			timeOfLastAppendPositionNs: 0, nowNs: 1, timeoutNs: 1, want: false,
		},
		{
			name:                       "inactive two past timeout",
			timeOfLastAppendPositionNs: 0, nowNs: 2, timeoutNs: 1, want: false,
		},
		{
			name:                       "inactive one before deadline",
			timeOfLastAppendPositionNs: 1, nowNs: 2, timeoutNs: 1, want: false,
		},
		{
			name:                       "active within five ns window",
			timeOfLastAppendPositionNs: 1, nowNs: 2, timeoutNs: 5, want: true,
		},
		{
			name:                       "active at deadline boundary",
			timeOfLastAppendPositionNs: 1, nowNs: 5, timeoutNs: 5, want: true,
		},
		{
			name:                       "inactive one past deadline",
			timeOfLastAppendPositionNs: 1, nowNs: 8, timeoutNs: 5, want: false,
		},
		{
			name:                       "active two before deadline",
			timeOfLastAppendPositionNs: 10, nowNs: 8, timeoutNs: 1, want: true,
		},
		{
			name:                       "active one ns before deadline",
			timeOfLastAppendPositionNs: 10, nowNs: 10, timeoutNs: 1, want: true,
		},
		{
			name:                       "inactive after deadline",
			timeOfLastAppendPositionNs: 10, nowNs: 12, timeoutNs: 1, want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			member := &election.Member{
				TimeOfLastAppendPositionNs: tt.timeOfLastAppendPositionNs,
			}
			require.Equal(t, tt.want, member.IsActive(tt.nowNs, tt.timeoutNs))
		})
	}
}

func TestHasReachedPosition(t *testing.T) {
	t.Parallel()

	t.Run("inactive", func(t *testing.T) {
		t.Parallel()
		member := newTestMemberWithLog(1, 42, 500, 0)
		require.False(t, member.HasReachedPosition(42, 500, 5, 3))
	})

	for _, leadershipTermID := range []int64{0, 10} {
		t.Run("term mismatch", func(t *testing.T) {
			t.Parallel()
			member := newTestMemberWithLog(1, 6, 500, 0)
			require.False(t, member.HasReachedPosition(leadershipTermID, 500, 5, 10))
		})
	}

	t.Run("log position less than target", func(t *testing.T) {
		t.Parallel()
		member := newTestMemberWithLog(1, 6, 100, 0)
		require.False(t, member.HasReachedPosition(6, 500, 5, 10))
	})

	for _, logPosition := range []int64{100, 1<<63 - 1} {
		t.Run("log position equal or greater", func(t *testing.T) {
			t.Parallel()
			member := newTestMemberWithLog(1, 42, logPosition, 10)
			require.True(t, member.HasReachedPosition(42, 100, 12, 5))
		})
	}
}

func TestIsUnanimousCandidate(t *testing.T) {
	t.Parallel()

	const gracefulClosedLeaderID = election.NullValue

	t.Run("member without log position", func(t *testing.T) {
		t.Parallel()
		candidate := newTestMemberWithLog(4, 100, 1000, 0)
		members := memberPtrs(
			newTestMemberWithLog(1, 2, 100, 0),
			newTestMemberWithLog(2, 8, election.NullPosition, 0),
			newTestMemberWithLog(3, 1, 1, 0),
		)
		require.False(t, election.IsUnanimousCandidate(members, candidate, int32(gracefulClosedLeaderID)))
	})

	t.Run("member with more up to date log", func(t *testing.T) {
		t.Parallel()
		candidate := newTestMemberWithLog(4, 10, 800, 0)
		members := memberPtrs(
			newTestMemberWithLog(1, 2, 100, 0),
			newTestMemberWithLog(2, 8, 6, 0),
			newTestMemberWithLog(3, 11, 1000, 0),
		)
		require.False(t, election.IsUnanimousCandidate(members, candidate, int32(gracefulClosedLeaderID)))
	})

	t.Run("graceful close excludes member", func(t *testing.T) {
		t.Parallel()
		candidate := newTestMemberWithLog(2, 2, 100, 0)
		members := memberPtrs(
			newTestMemberWithLog(1, 2, 100, 0),
			newTestMemberWithLog(2, 2, 100, 0),
		)
		require.False(t, election.IsUnanimousCandidate(members, candidate, 1))
	})

	t.Run("unanimous success", func(t *testing.T) {
		t.Parallel()
		candidate := newTestMemberWithLog(2, 10, 800, 0)
		members := memberPtrs(
			newTestMemberWithLog(10, 2, 100, 0),
			newTestMemberWithLog(20, 8, 6, 0),
			newTestMemberWithLog(30, 10, 800, 0),
		)
		require.True(t, election.IsUnanimousCandidate(members, candidate, int32(gracefulClosedLeaderID)))
	})
}

func TestIsQuorumCandidate(t *testing.T) {
	t.Parallel()

	candidate := newTestMemberWithLog(2, 10, 800, 0)

	t.Run("quorum not reached", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			newTestMemberWithLog(10, 2, 100, 0),
			newTestMemberWithLog(20, 18, 600, 0),
			newTestMemberWithLog(30, 10, 800, 0),
			newTestMemberWithLog(40, 19, 800, 0),
			newTestMemberWithLog(50, 10, 1000, 0),
		)
		require.False(t, election.IsQuorumCandidate(members, candidate))
	})

	t.Run("quorum reached", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			newTestMemberWithLog(10, 2, 100, 0),
			newTestMemberWithLog(20, 18, 600, 0),
			newTestMemberWithLog(30, 10, 800, 0),
			newTestMemberWithLog(40, 9, 800, 0),
			newTestMemberWithLog(50, 10, 700, 0),
		)
		require.True(t, election.IsQuorumCandidate(members, candidate))
	})
}

func TestIsQuorumLeader(t *testing.T) {
	t.Parallel()

	t.Run("quorum reached with null vote", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = -5
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID * 2, Vote: new(false)},
			&election.Member{ID: 3, CandidateTermID: candidateTermID},
			&election.Member{ID: 4, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 5, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.True(t, election.IsQuorumLeader(members, candidateTermID))
	})

	t.Run("negative vote", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 8
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID, Vote: new(false)},
			&election.Member{ID: 3, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.False(t, election.IsQuorumLeader(members, candidateTermID))
	})

	t.Run("quorum not reached", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 2
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID},
			&election.Member{ID: 3, CandidateTermID: candidateTermID + 5, Vote: new(true)},
		)
		require.False(t, election.IsQuorumLeader(members, candidateTermID))
	})
}

func TestIsUnanimousLeader(t *testing.T) {
	t.Parallel()

	const gracefulClosedLeaderID = election.NullMemberID

	t.Run("negative vote", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 42
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 3, CandidateTermID: candidateTermID, Vote: new(false)},
		)
		require.False(t, election.IsUnanimousLeader(members, candidateTermID, gracefulClosedLeaderID))
	})

	t.Run("graceful close", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 7
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.False(t, election.IsUnanimousLeader(members, candidateTermID, 1))
	})

	t.Run("null vote", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 2
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID},
			&election.Member{ID: 3, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.False(t, election.IsUnanimousLeader(members, candidateTermID, gracefulClosedLeaderID))
	})

	t.Run("wrong candidate term", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 2
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID + 1, Vote: new(true)},
			&election.Member{ID: 3, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.False(t, election.IsUnanimousLeader(members, candidateTermID, gracefulClosedLeaderID))
	})

	t.Run("unanimous success", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 42
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 3, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.True(t, election.IsUnanimousLeader(members, candidateTermID, gracefulClosedLeaderID))
	})

	t.Run("graceful close skip with unanimous remaining votes", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 9
		members := memberPtrs(
			&election.Member{ID: 1, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 2, CandidateTermID: candidateTermID, Vote: new(true)},
			&election.Member{ID: 3, CandidateTermID: candidateTermID, Vote: new(true)},
		)
		require.True(t, election.IsUnanimousLeader(members, candidateTermID, 1))
	})
}

func TestHasQuorumAtPosition(t *testing.T) {
	t.Parallel()

	t.Run("not a quorum", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			newTestMemberWithLog(10, 2, 100, 0),
			newTestMemberWithLog(20, 18, 600, 0),
			newTestMemberWithLog(30, 10, 800, 0),
			newTestMemberWithLog(40, 19, 800, 0),
			newTestMemberWithLog(50, 10, 1000, 0),
		)
		require.False(t, election.HasQuorumAtPosition(members, 10, 800, 0, 10))
	})

	t.Run("quorum at position", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			newTestMemberWithLog(10, 2, 100, 0),
			newTestMemberWithLog(20, 10, 600, 0),
			newTestMemberWithLog(30, 10, 800, 0),
			newTestMemberWithLog(40, 19, 800, 0),
			newTestMemberWithLog(50, 10, 1000, 0),
		)
		require.True(t, election.HasQuorumAtPosition(members, 10, 600, 5, 10))
	})
}

// TestHasActiveQuorum asserts free-function liveness quorum over IsActive.
//
// Window: nowNs = 100, timeoutNs = 10. Active when
// TimeOfLastAppendPositionNs+timeoutNs > nowNs. Active stamp 100. Stale stamp
// 0. Boundary stamp is nowNs-timeoutNs (90).
//
// Members pin non-zero LeadershipTermID and LogPosition. A rewrite that calls
// HasQuorumAtPosition(members, 0, 0, ...) fails term and position match.
//
// Single-member inactive stays false here. The always-true single-member rule
// lives on HostAdapter.HasActiveQuorum, not the free function.
func TestHasActiveQuorum(t *testing.T) {
	t.Parallel()

	const (
		nowNs     int64 = 100
		timeoutNs int64 = 10
		activeTS  int64 = 100
		staleTS   int64 = 0
		// Non-zero term and position. Wrong rewrite to HasQuorumAtPosition(0, 0)
		// drops every vote (term mismatch or position floor).
		termID int64 = 5
		logPos int64 = 100
	)

	tests := []struct {
		name string
		ts   []int64
		want bool
	}{
		{
			name: "all active",
			ts:   []int64{activeTS, activeTS, activeTS},
			want: true,
		},
		{
			name: "bare quorum",
			ts:   []int64{activeTS, activeTS, staleTS},
			want: true,
		},
		{
			name: "below quorum",
			ts:   []int64{activeTS, staleTS, staleTS},
			want: false,
		},
		{
			name: "none active",
			ts:   []int64{staleTS, staleTS, staleTS},
			want: false,
		},
		{
			name: "single member active",
			ts:   []int64{activeTS},
			want: true,
		},
		{
			name: "single member inactive",
			ts:   []int64{staleTS},
			want: false,
		},
		{
			name: "boundary",
			ts:   []int64{nowNs - timeoutNs, nowNs - timeoutNs, nowNs - timeoutNs},
			want: false,
		},
		{
			name: "even member count",
			ts:   []int64{activeTS, activeTS, staleTS, staleTS},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := make([]*election.Member, len(tt.ts))
			for i, stamp := range tt.ts {
				members[i] = newTestMemberWithLog(int32(i), termID, logPos, stamp)
			}
			require.Equal(t, tt.want, election.HasActiveQuorum(members, nowNs, timeoutNs))
			// Pin liveness-only. Non-zero term and position fail HasQuorumAtPosition(0, 0).
			if tt.want {
				require.False(t, election.HasQuorumAtPosition(members, 0, 0, nowNs, timeoutNs))
			}
		})
	}
}

func TestBecomeCandidate(t *testing.T) {
	t.Parallel()

	t.Run("matching candidate", func(t *testing.T) {
		t.Parallel()
		const candidateTermID int64 = 17
		members := memberPtrs(
			&election.Member{ID: 0, Vote: new(false), CandidateTermID: 3, IsBallotSent: true},
			&election.Member{ID: 1, Vote: new(true), CandidateTermID: 4, IsBallotSent: true},
			&election.Member{ID: 2, Vote: new(false), CandidateTermID: 5, IsBallotSent: false},
		)

		election.BecomeCandidate(members, candidateTermID, 1)

		require.NotNil(t, members[1].Vote)
		require.True(t, *members[1].Vote)
		require.Equal(t, candidateTermID, members[1].CandidateTermID)
		require.True(t, members[1].IsBallotSent)

		for _, idx := range []int{0, 2} {
			member := members[idx]
			require.Nil(t, member.Vote)
			require.Equal(t, election.NullValue, member.CandidateTermID)
			require.False(t, member.IsBallotSent)
		}
	})

	t.Run("unknown candidate member id", func(t *testing.T) {
		t.Parallel()
		members := memberPtrs(
			&election.Member{ID: 0, Vote: new(true), IsBallotSent: true},
			&election.Member{ID: 1, Vote: new(false), IsBallotSent: true},
		)

		election.BecomeCandidate(members, 99, 42)

		for _, member := range members {
			require.Nil(t, member.Vote)
			require.Equal(t, election.NullValue, member.CandidateTermID)
			require.False(t, member.IsBallotSent)
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		election.BecomeCandidate(nil, 1, 0)
		election.BecomeCandidate([]*election.Member{}, 1, 0)
	})
}

func TestReset(t *testing.T) {
	t.Parallel()

	member := &election.Member{
		ID:                         7,
		Vote:                       new(true),
		IsBallotSent:               true,
		IsLeader:                   true,
		HasTerminated:              true,
		LeadershipTermID:           11,
		CandidateTermID:            22,
		LogPosition:                33,
		TimeOfLastAppendPositionNs: 44,
	}

	member.Reset()

	require.Nil(t, member.Vote)
	require.False(t, member.IsBallotSent)
	require.False(t, member.IsLeader)
	require.False(t, member.HasTerminated)
	require.Equal(t, election.NullValue, member.LeadershipTermID)
	require.Equal(t, election.NullValue, member.CandidateTermID)
	require.Equal(t, election.NullPosition, member.LogPosition)
	require.Equal(t, int64(44), member.TimeOfLastAppendPositionNs)
}

func TestResetMembers(t *testing.T) {
	t.Parallel()

	members := memberPtrs(
		&election.Member{
			ID: 0, Vote: new(true), IsBallotSent: true, IsLeader: true, HasTerminated: true,
			LeadershipTermID: 11, CandidateTermID: 1, LogPosition: 10, TimeOfLastAppendPositionNs: 44,
		},
		&election.Member{
			ID: 1, Vote: new(false), IsBallotSent: true, IsLeader: true, HasTerminated: true,
			LeadershipTermID: 22, CandidateTermID: 2, LogPosition: 20, TimeOfLastAppendPositionNs: 55,
		},
	)

	election.ResetMembers(members)

	for _, member := range members {
		require.Nil(t, member.Vote)
		require.False(t, member.IsBallotSent)
		require.False(t, member.IsLeader)
		require.False(t, member.HasTerminated)
		require.Equal(t, election.NullValue, member.LeadershipTermID)
		require.Equal(t, election.NullValue, member.CandidateTermID)
		require.Equal(t, election.NullPosition, member.LogPosition)
	}
	require.Equal(t, int64(44), members[0].TimeOfLastAppendPositionNs)
	require.Equal(t, int64(55), members[1].TimeOfLastAppendPositionNs)
}

func TestNodeStateSemanticVersion(t *testing.T) {
	t.Parallel()
	require.Equal(t, cnc.ComposeSemanticVersion(0, 3, 0), election.NodeStateSemanticVersion)
}
