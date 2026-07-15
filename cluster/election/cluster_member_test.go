package election_test

import (
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
