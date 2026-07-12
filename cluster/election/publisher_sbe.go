package election

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// SbePublisher encodes consensus messages to SBE wire and records each send.
//
// Sent retains fields and wire for test assertions; maps to real publications
// when live publication wiring lands.
//
// SentHandler, when set, receives each recorded message (e.g. sim network
// routing).
type SbePublisher struct {
	ActingVersion uint16
	Sent          []SentMessage
	SentHandler   func(SentMessage)
}

var _ Publisher = (*SbePublisher)(nil)

func (p *SbePublisher) Reset() { p.Sent = p.Sent[:0] }

func publish[F any](
	p *SbePublisher,
	dst PublicationTarget,
	templateID uint16,
	blockLength uint16,
	ver uint16,
	fields F,
	encode func([]byte, int, uint16, F) (int, bool),
) bool {
	wireLen, ok := codecs.ClusterWireMessageLen(blockLength)
	if !ok {
		return false
	}
	wire := make([]byte, wireLen)
	wireEnd, ok := encode(wire, 0, ver, fields)
	if !ok {
		return false
	}
	msg := SentMessage{
		TemplateID: templateID,
		Dst:        dst,
		Fields:     fields,
		Wire:       wire[:wireEnd],
	}
	p.Sent = append(p.Sent, msg)
	if p.SentHandler != nil {
		p.SentHandler(msg)
	}
	return true
}

func (p *SbePublisher) CanvassPosition(
	dst PublicationTarget,
	logLeadershipTermID, logPosition, leadershipTermID int64,
	followerMemberID int32,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p, dst,
		clusterwire.CanvassPositionSbeTemplateID,
		codecs.CanvassPositionBlockLength(ver),
		ver,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: logLeadershipTermID,
			LogPosition:         logPosition,
			LeadershipTermID:    leadershipTermID,
			FollowerMemberID:    followerMemberID,
			ProtocolVersion: protocolVersionFor(
				ver,
				codecs.CanvassProtocolVersionMinVersion,
			),
		},
		codecs.EncodeCanvassPosition,
	)
}

func (p *SbePublisher) RequestVote(
	dst PublicationTarget,
	logLeadershipTermID, logPosition, candidateTermID int64,
	candidateMemberID int32,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publish(
		p, dst,
		clusterwire.RequestVoteSbeTemplateID,
		codecs.RequestVoteBlockLength(ver),
		ver,
		codecs.RequestVoteFields{
			LogLeadershipTermID: logLeadershipTermID,
			LogPosition:         logPosition,
			CandidateTermID:     candidateTermID,
			CandidateMemberID:   candidateMemberID,
			ProtocolVersion: protocolVersionFor(
				ver,
				codecs.RequestVoteProtocolVersionMinVersion,
			),
		},
		codecs.EncodeRequestVote,
	)
}

func (p *SbePublisher) PlaceVote(
	dst PublicationTarget,
	candidateTermID, logLeadershipTermID, logPosition int64,
	candidateMemberID, followerMemberID int32,
	vote bool,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p, dst,
		clusterwire.VoteSbeTemplateID,
		clusterwire.VoteSbeBlockLength,
		ver,
		codecs.VoteFields{
			CandidateTermID:     candidateTermID,
			LogLeadershipTermID: logLeadershipTermID,
			LogPosition:         logPosition,
			CandidateMemberID:   candidateMemberID,
			FollowerMemberID:    followerMemberID,
			Vote:                boolToClusterwire(vote),
		},
		codecs.EncodeVote,
	)
}

func (p *SbePublisher) NewLeadershipTerm(
	dst PublicationTarget,
	fields codecs.NewLeadershipTermFields,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p, dst,
		clusterwire.NewLeadershipTermSbeTemplateID,
		codecs.NewLeadershipTermBlockLength(ver),
		ver,
		fields,
		codecs.EncodeNewLeadershipTerm,
	)
}

func (p *SbePublisher) AppendPosition(
	dst PublicationTarget,
	leadershipTermID, logPosition int64,
	followerMemberID int32,
	flags uint8,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publish(
		p, dst,
		clusterwire.AppendPositionSbeTemplateID,
		codecs.AppendPositionBlockLength(ver),
		ver,
		codecs.AppendPositionFields{
			LeadershipTermID: leadershipTermID,
			LogPosition:      logPosition,
			FollowerMemberID: followerMemberID,
			Flags:            flags,
		},
		codecs.EncodeAppendPosition,
	)
}

func (p *SbePublisher) CommitPosition(
	dst PublicationTarget,
	leadershipTermID, logPosition int64,
	leaderMemberID int32,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p, dst,
		clusterwire.CommitPositionSbeTemplateID,
		clusterwire.CommitPositionSbeBlockLength,
		ver,
		codecs.CommitPositionFields{
			LeadershipTermID: leadershipTermID,
			LogPosition:      logPosition,
			LeaderMemberID:   leaderMemberID,
		},
		codecs.EncodeCommitPosition,
	)
}
