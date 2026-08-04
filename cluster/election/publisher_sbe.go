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

// publish encodes fields and records the send. varLens are optional
// variable-field payload lengths for [codecs.ClusterWireMessageLen].
func publish[F any](
	p *SbePublisher,
	dst PublicationTarget,
	templateID uint16,
	blockLength uint16,
	ver uint16,
	fields F,
	encode func([]byte, int, uint16, F) (int, bool),
	varLens ...int,
) bool {
	wireLen, ok := codecs.ClusterWireMessageLen(blockLength, varLens...)
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
	fields codecs.CanvassPositionFields,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p,
		dst,
		clusterwire.CanvassPositionSbeTemplateID,
		codecs.CanvassPositionBlockLength(ver),
		ver,
		applyCanvassProtocolVersion(ver, fields),
		codecs.EncodeCanvassPosition,
	)
}

func (p *SbePublisher) RequestVote(
	dst PublicationTarget,
	fields codecs.RequestVoteFields,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publish(
		p,
		dst,
		clusterwire.RequestVoteSbeTemplateID,
		codecs.RequestVoteBlockLength(ver),
		ver,
		applyRequestVoteProtocolVersion(ver, fields),
		codecs.EncodeRequestVote,
	)
}

func (p *SbePublisher) PlaceVote(
	dst PublicationTarget,
	fields codecs.VoteFields,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p,
		dst,
		clusterwire.VoteSbeTemplateID,
		clusterwire.VoteSbeBlockLength,
		ver,
		fields,
		codecs.EncodeVote,
	)
}

func (p *SbePublisher) NewLeadershipTerm(
	dst PublicationTarget,
	fields codecs.NewLeadershipTermFields,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p,
		dst,
		clusterwire.NewLeadershipTermSbeTemplateID,
		codecs.NewLeadershipTermBlockLength(ver),
		ver,
		fields,
		codecs.EncodeNewLeadershipTerm,
	)
}

func (p *SbePublisher) AppendPosition(
	dst PublicationTarget,
	fields codecs.AppendPositionFields,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publish(
		p,
		dst,
		clusterwire.AppendPositionSbeTemplateID,
		codecs.AppendPositionBlockLength(ver),
		ver,
		fields,
		codecs.EncodeAppendPosition,
	)
}

func (p *SbePublisher) CommitPosition(
	dst PublicationTarget,
	fields codecs.CommitPositionFields,
) {
	ver := actingVersion(p.ActingVersion)
	publish(
		p,
		dst,
		clusterwire.CommitPositionSbeTemplateID,
		clusterwire.CommitPositionSbeBlockLength,
		ver,
		fields,
		codecs.EncodeCommitPosition,
	)
}

func (p *SbePublisher) CatchupPosition(
	dst PublicationTarget,
	fields codecs.CatchupPositionFields,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publish(
		p,
		dst,
		clusterwire.CatchupPositionSbeTemplateID,
		clusterwire.CatchupPositionSbeBlockLength,
		ver,
		fields,
		codecs.EncodeCatchupPosition,
		len(fields.CatchupEndpoint),
	)
}
