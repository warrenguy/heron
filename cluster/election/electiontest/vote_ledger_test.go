package electiontest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
)

const ledgerTermID int64 = 7

func requireLedgerError(t *testing.T, err error, want string) {
	t.Helper()
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "electiontest: "), err.Error())
	require.ErrorContains(t, err, want)
}

func TestVoteLedger_AddGrant(t *testing.T) {
	tests := []struct {
		name   string
		grants [][2]int32
	}{
		{name: "second grant to the same candidate", grants: [][2]int32{{1, 0}, {1, 0}}},
		{name: "second grant to another candidate", grants: [][2]int32{{1, 0}, {1, 2}}},
		{name: "grant after the self-vote", grants: [][2]int32{{1, 1}, {1, 0}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ledger electiontest.VoteLedger
			first, second := tt.grants[0], tt.grants[1]
			require.NoError(t, ledger.AddGrant(first[0], first[1], ledgerTermID))

			// Another term and another voter stay independent.
			require.NoError(t, ledger.AddGrant(first[0], first[1], ledgerTermID+1))
			require.NoError(t, ledger.AddGrant(first[0]+1, second[1], ledgerTermID))

			err := ledger.AddGrant(second[0], second[1], ledgerTermID)
			requireLedgerError(t, err, "granted term")
		})
	}
}

func TestCheckGrantAnswers(t *testing.T) {
	const (
		voterID     int32 = 1
		candidateID int32 = 0
		logTermID   int64 = 3
		position    int64 = 100
	)
	vote := codecs.VoteFields{
		CandidateTermID:   ledgerTermID,
		CandidateMemberID: candidateID,
		FollowerMemberID:  voterID,
		Vote:              clusterwire.BooleanType_TRUE,
	}
	request := codecs.RequestVoteFields{
		LogLeadershipTermID: logTermID,
		LogPosition:         position,
		CandidateTermID:     ledgerTermID,
		CandidateMemberID:   candidateID,
	}
	tests := []struct {
		name          string
		vote          func(*codecs.VoteFields)
		request       func(*codecs.RequestVoteFields) *codecs.RequestVoteFields
		voterLogTerm  int64
		voterPosition int64
		want          string
	}{
		{
			name:          "wrong follower",
			vote:          func(v *codecs.VoteFields) { v.FollowerMemberID = 2 },
			voterLogTerm:  logTermID,
			voterPosition: position,
			want:          "names follower",
		},
		{
			name:          "missing request",
			request:       func(*codecs.RequestVoteFields) *codecs.RequestVoteFields { return nil },
			voterLogTerm:  logTermID,
			voterPosition: position,
			want:          "answers no RequestVote",
		},
		{
			name: "request from another candidate",
			request: func(r *codecs.RequestVoteFields) *codecs.RequestVoteFields {
				r.CandidateMemberID = 2
				return r
			},
			voterLogTerm:  logTermID,
			voterPosition: position,
			want:          "answers no RequestVote",
		},
		{
			name: "request at another candidate term",
			request: func(r *codecs.RequestVoteFields) *codecs.RequestVoteFields {
				r.CandidateTermID = ledgerTermID + 1
				return r
			},
			voterLogTerm:  logTermID,
			voterPosition: position,
			want:          "answers no RequestVote",
		},
		{
			name:          "voter ahead on log term",
			voterLogTerm:  logTermID + 1,
			voterPosition: 0,
			want:          "ahead of candidate",
		},
		{
			name:          "voter ahead on append position at an equal log term",
			voterLogTerm:  logTermID,
			voterPosition: position + 1,
			want:          "ahead of candidate",
		},
		{
			name:          "equal logs",
			voterLogTerm:  logTermID,
			voterPosition: position,
		},
		{
			name:          "candidate ahead",
			voterLogTerm:  logTermID - 1,
			voterPosition: position + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, r := vote, request
			if tt.vote != nil {
				tt.vote(&v)
			}
			req := &r
			if tt.request != nil {
				req = tt.request(&r)
			}
			err := electiontest.CheckGrantAnswers(voterID, v, req, tt.voterLogTerm, tt.voterPosition)
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			requireLedgerError(t, err, tt.want)
		})
	}
}

// ledgerMembers returns three members. Each id in votes holds a TRUE vote at
// ledgerTermID.
func ledgerMembers(votes ...int32) []*election.Member {
	members := election.MembersPtrs([]election.Member{
		{ID: 0, CandidateTermID: election.NullValue},
		{ID: 1, CandidateTermID: election.NullValue},
		{ID: 2, CandidateTermID: election.NullValue},
	})
	for _, id := range votes {
		members[id].Vote = new(true)
		members[id].CandidateTermID = ledgerTermID
	}
	return members
}

func TestVoteLedger_CheckCounted(t *testing.T) {
	type grant struct {
		voterID, candidateID int32
		termID               int64
	}
	tests := []struct {
		name   string
		grants []grant
		want   string
	}{
		{
			name:   "counted vote with no grant",
			grants: []grant{{0, 0, ledgerTermID}},
			want:   "without a grant",
		},
		{
			name:   "counted vote granted only at an older term",
			grants: []grant{{0, 0, ledgerTermID}, {1, 0, ledgerTermID - 1}},
			want:   "without a grant",
		},
		{
			name:   "counted vote granted to another candidate",
			grants: []grant{{0, 0, ledgerTermID}, {1, 2, ledgerTermID}},
			want:   "without a grant",
		},
		{
			name:   "every counted vote granted",
			grants: []grant{{0, 0, ledgerTermID}, {1, 0, ledgerTermID}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ledger electiontest.VoteLedger
			for _, g := range tt.grants {
				require.NoError(t, ledger.AddGrant(g.voterID, g.candidateID, g.termID))
			}
			err := ledger.CheckCounted(0, ledgerTermID, ledgerMembers(0, 1))
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			requireLedgerError(t, err, tt.want)
		})
	}
}

func TestVoteLedger_CheckWin(t *testing.T) {
	tests := []struct {
		name    string
		votes   []int32
		grants  []int32
		members func([]*election.Member)
		want    string
	}{
		{
			name:   "backed votes short of quorum",
			votes:  []int32{0},
			grants: []int32{0},
			want:   "short of quorum",
		},
		{
			name:   "TRUE vote at an older candidate term not counted",
			votes:  []int32{0},
			grants: []int32{0},
			members: func(members []*election.Member) {
				members[1].Vote = new(true)
				members[1].CandidateTermID = ledgerTermID - 1
			},
			want: "short of quorum",
		},
		{
			name:  "unbacked vote short of quorum reports the missing grant",
			votes: []int32{0},
			want:  "without a grant",
		},
		{name: "backed unanimous win", votes: []int32{0, 1, 2}, grants: []int32{0, 1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ledger electiontest.VoteLedger
			for _, id := range tt.grants {
				require.NoError(t, ledger.AddGrant(id, 0, ledgerTermID))
			}
			members := ledgerMembers(tt.votes...)
			if tt.members != nil {
				tt.members(members)
			}
			err := ledger.CheckWin(0, ledgerTermID, members)
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			requireLedgerError(t, err, tt.want)
		})
	}
}
