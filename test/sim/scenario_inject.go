package sim

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

// InjectCanvassPosition publishes a canvass position from fromID, enqueues delivery
// through the sim network, and drains the scheduler until the message lands.
// The caller advances time with AdvanceToPhase or RunSteps when a post-inject
// tick is required.
func InjectCanvassPosition(s *ElectionSim, fromID, toID int32, logTerm, logPos int64) error {
	return inject(
		s,
		fromID,
		toID,
		"canvass position",
		func(dst election.PublicationTarget) bool {
			s.Node(fromID).Publisher().CanvassPosition(
				dst,
				codecs.CanvassPositionFields{
					LogLeadershipTermID: logTerm,
					LogPosition:         logPos,
					LeadershipTermID:    logTerm,
					FollowerMemberID:    fromID,
				},
			)
			return true
		},
	)
}

// InjectAppendPosition publishes an append position from fromID, enqueues delivery
// through the sim network, and drains the scheduler until the message lands.
func InjectAppendPosition(s *ElectionSim, fromID, toID int32, term, pos int64) error {
	return inject(
		s,
		fromID,
		toID,
		"append position",
		func(dst election.PublicationTarget) bool {
			return s.Node(fromID).Publisher().AppendPosition(
				dst,
				codecs.AppendPositionFields{
					LeadershipTermID: term,
					LogPosition:      pos,
					FollowerMemberID: fromID,
				},
			)
		},
	)
}

// InjectVote publishes a ballot vote from the voter fromID for the candidate
// toID, enqueues delivery through the sim network, and drains the scheduler
// until the message lands.
func InjectVote(
	s *ElectionSim,
	fromID, toID int32,
	candidateTerm, logTerm, pos int64,
	vote bool,
) error {
	voteVal := clusterwire.BooleanType_FALSE
	if vote {
		voteVal = clusterwire.BooleanType_TRUE
	}
	return inject(
		s,
		fromID,
		toID,
		"vote",
		func(dst election.PublicationTarget) bool {
			s.Node(fromID).Publisher().PlaceVote(
				dst,
				codecs.VoteFields{
					CandidateTermID:     candidateTerm,
					LogLeadershipTermID: logTerm,
					LogPosition:         pos,
					CandidateMemberID:   toID,
					FollowerMemberID:    fromID,
					Vote:                voteVal,
				},
			)
			return true
		},
	)
}

// InjectRequestVote publishes a request vote for candidate fromID, enqueues
// delivery through the sim network, and drains the scheduler until the message
// lands.
func InjectRequestVote(
	s *ElectionSim,
	fromID, toID int32,
	candidateTerm, logTerm, pos int64,
) error {
	return inject(
		s,
		fromID,
		toID,
		"request vote",
		func(dst election.PublicationTarget) bool {
			return s.Node(fromID).Publisher().RequestVote(
				dst,
				codecs.RequestVoteFields{
					LogLeadershipTermID: logTerm,
					LogPosition:         pos,
					CandidateTermID:     candidateTerm,
					CandidateMemberID:   fromID,
				},
			)
		},
	)
}

func inject(
	s *ElectionSim,
	fromID, toID int32,
	kind string,
	publish func(dst election.PublicationTarget) bool,
) error {
	source := s.Node(fromID)
	if source == nil {
		return fmt.Errorf("sim: unknown inject source node %d", fromID)
	}
	target := s.Node(toID)
	if target == nil {
		return fmt.Errorf("sim: unknown inject destination node %d", toID)
	}
	before := len(source.Publisher().Sent)
	if !publish(election.PublicationTarget{MemberID: toID}) ||
		len(source.Publisher().Sent) != before+1 {
		return fmt.Errorf("sim: %s encode failed from %d to %d", kind, fromID, toID)
	}
	want := deliveredKey{from: fromID, index: before, gen: source.publishGen}
	delivered := func(*ElectionSim) bool { return target.hasDelivered(want) }
	if err := s.RunUntil(delivered); err != nil {
		return err
	}
	if !delivered(s) {
		return fmt.Errorf("sim: %s inject from %d to node %d did not deliver", kind, fromID, toID)
	}
	return nil
}
