package electiontest

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

// RequestVoteTo returns the RequestVote fields of msg when msg is a
// RequestVote to voterID. Otherwise it returns nil. Callers pass the message
// in delivery, so a non-nil result is the request the voter answers.
func RequestVoteTo(msg election.SentMessage, voterID int32) *codecs.RequestVoteFields {
	if msg.Dst.MemberID != voterID || msg.TemplateID != clusterwire.RequestVoteSbeTemplateID {
		return nil
	}
	request := msg.Fields.(codecs.RequestVoteFields)
	return &request
}

type voteKey struct {
	voterID int32
	termID  int64
}

// VoteLedger stores vote grants.
//
// A grant is a TRUE vote. The ledger keys each grant on voter and candidate
// term. A member that enters CANDIDATE_BALLOT grants itself a vote. The zero
// value is ready to use.
type VoteLedger struct {
	grants map[voteKey]int32
}

// AddGrant stores the grant by voterID to candidateID at termID.
//
// Returns an error when voterID already holds a grant at termID.
func (l *VoteLedger) AddGrant(voterID, candidateID int32, termID int64) error {
	key := voteKey{voterID: voterID, termID: termID}
	if prior, ok := l.grants[key]; ok {
		return fmt.Errorf(
			"electiontest: member %d granted term %d to member %d, then to member %d",
			voterID, termID, prior, candidateID,
		)
	}
	if l.grants == nil {
		l.grants = make(map[voteKey]int32)
	}
	l.grants[key] = candidateID
	return nil
}

// CheckGrantAnswers checks one TRUE vote from voterID at send time.
//
// request is the RequestVote in delivery to voterID, or nil. voterLogTermID
// and voterAppendPosition are the voter log at send time.
//
// Returns an error when:
//   - vote names a follower other than voterID
//   - request is nil, or comes from another candidate or candidate term
//   - the voter log is ahead of the request log
func CheckGrantAnswers(
	voterID int32,
	vote codecs.VoteFields,
	request *codecs.RequestVoteFields,
	voterLogTermID, voterAppendPosition int64,
) error {
	if vote.FollowerMemberID != voterID {
		return fmt.Errorf(
			"electiontest: vote from member %d names follower %d",
			voterID, vote.FollowerMemberID,
		)
	}
	answered := request != nil &&
		request.CandidateMemberID == vote.CandidateMemberID &&
		request.CandidateTermID == vote.CandidateTermID
	if !answered {
		return fmt.Errorf(
			"electiontest: grant from member %d to member %d at term %d answers no RequestVote",
			voterID, vote.CandidateMemberID, vote.CandidateTermID,
		)
	}
	order := election.CompareLog(
		voterLogTermID,
		voterAppendPosition,
		request.LogLeadershipTermID,
		request.LogPosition,
	)
	if order > 0 {
		return fmt.Errorf(
			"electiontest: member %d log term %d position %d ahead of candidate %d log term %d position %d",
			voterID, voterLogTermID, voterAppendPosition,
			vote.CandidateMemberID, request.LogLeadershipTermID, request.LogPosition,
		)
	}
	return nil
}

// CheckCounted checks the votes that candidateID counts at termID.
//
// members is the member view of candidateID. A counted vote is a TRUE Vote at
// CandidateTermID termID.
//
// Returns an error when a counted vote has no ledger grant to candidateID at
// termID.
func (l *VoteLedger) CheckCounted(
	candidateID int32,
	termID int64,
	members []*election.Member,
) error {
	_, err := l.countBacked(candidateID, termID, members)
	return err
}

// CheckWin checks a ballot win by candidateID at termID.
//
// members is the member view of candidateID.
//
// Returns an error when:
//   - a counted vote has no ledger grant to candidateID at termID
//   - the counted votes fall short of the quorum threshold
func (l *VoteLedger) CheckWin(
	candidateID int32,
	termID int64,
	members []*election.Member,
) error {
	counted, err := l.countBacked(candidateID, termID, members)
	if err != nil {
		return err
	}
	threshold := election.QuorumThreshold(len(members))
	if counted < threshold {
		return fmt.Errorf(
			"electiontest: member %d won term %d on %d votes, short of quorum %d",
			candidateID, termID, counted, threshold,
		)
	}
	return nil
}

// countBacked returns how many votes candidateID counts at termID. Returns an
// error at the first counted vote without a ledger grant.
func (l *VoteLedger) countBacked(
	candidateID int32,
	termID int64,
	members []*election.Member,
) (int, error) {
	counted := 0
	for _, member := range members {
		votedYes := member.Vote != nil && *member.Vote
		if !votedYes || member.CandidateTermID != termID {
			continue
		}
		granted, ok := l.grants[voteKey{voterID: member.ID, termID: termID}]
		if !ok || granted != candidateID {
			return 0, fmt.Errorf(
				"electiontest: member %d counts a vote from member %d at term %d without a grant",
				candidateID, member.ID, termID,
			)
		}
		counted++
	}
	return counted, nil
}
