package election

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// aeronPublisherSendAttempts is the TryClaim retry budget (SEND_ATTEMPTS).
const aeronPublisherSendAttempts = 3

// AeronPublisher implements Publisher over per-member exclusive publications.
//
// Concurrency: invoker-only. Same goroutine as Election.DoWork / Agent.DoWork
// and media-driver/client duty cycle. Not safe for concurrent use.
//
// Lifetime: caller owns ExclusivePublication (add, close, reconnect).
// SetPublication does not Close the previous handle. Missing publication for a
// MemberID is a silent no-op (void methods) or false (bool methods).
type AeronPublisher struct {
	// ActingVersion gates optional SBE fields (ProtocolVersion etc.).
	// Zero means use schema default via actingVersion() -- same as SbePublisher.
	ActingVersion uint16

	pubs map[int32]*aeron.ExclusivePublication
}

var _ Publisher = (*AeronPublisher)(nil)

// NewAeronPublisher returns an empty publisher. Bind peers via SetPublication.
func NewAeronPublisher() *AeronPublisher {
	return &AeronPublisher{
		pubs: make(map[int32]*aeron.ExclusivePublication),
	}
}

// SetPublication binds or replaces the exclusive consensus publication for memberID.
//
// Pass nil to unbind (does not Close the previous publication). Caller owns Close.
// Invoker-only; not safe concurrent with Offer/TryClaim on the same pubs.
func (p *AeronPublisher) SetPublication(memberID int32, pub *aeron.ExclusivePublication) {
	if pub == nil {
		delete(p.pubs, memberID)
		return
	}
	p.pubs[memberID] = pub
}

// Publication returns the bound exclusive publication for memberID, or nil.
func (p *AeronPublisher) Publication(memberID int32) *aeron.ExclusivePublication {
	return p.pubs[memberID]
}

// publishClaim encodes fields into a TryClaim reservation and commits.
//
// Returns true only after successful Commit. Returns false when:
//   - pub nil / missing member
//   - wire length cannot be computed or exceeds MaxPayloadLength
//   - all SEND_ATTEMPTS exhausted with retriable codes
//   - Closed / MaxPositionExceeded (stop immediately; no throw)
//   - encode fails or Commit fails (Abort best-effort)
//
// No idle or Client.DoWork inside. Tight spin of aeronPublisherSendAttempts only.
func publishClaim[F any](
	p *AeronPublisher,
	dst PublicationTarget,
	blockLength uint16,
	ver uint16,
	fields F,
	encode func([]byte, int, uint16, F) (int, bool),
) bool {
	pub := p.pubs[dst.MemberID]
	if pub == nil {
		return false
	}
	wireLen, ok := codecs.ClusterWireMessageLen(blockLength)
	if !ok || wireLen > pub.MaxPayloadLength() {
		return false
	}

	for range aeronPublisherSendAttempts {
		termIndex, frameOffset, position, err := pub.TryClaim(wireLen)
		if err != nil {
			// Fixed consensus sizes must fit MTU; fail closed on programmer path.
			return false
		}
		if position <= 0 {
			if !aeron.IsOfferRetriable(position) {
				return false
			}
			continue
		}

		termBuf := pub.TermBuffer(termIndex)
		payloadOff := frameOffset + logbuffer.OffsetFramePayload
		wireEnd, ok := encode(termBuf.Bytes(), payloadOff, ver, fields)
		if !ok || wireEnd-payloadOff != wireLen {
			_ = pub.Abort(termIndex, frameOffset)
			return false
		}
		if err := pub.Commit(termIndex, frameOffset); err != nil {
			_ = pub.Abort(termIndex, frameOffset)
			return false
		}
		return true
	}
	return false
}

func (p *AeronPublisher) CanvassPosition(
	dst PublicationTarget,
	fields codecs.CanvassPositionFields,
) {
	ver := actingVersion(p.ActingVersion)
	_ = publishClaim(
		p,
		dst,
		codecs.CanvassPositionBlockLength(ver),
		ver,
		applyCanvassProtocolVersion(ver, fields),
		codecs.EncodeCanvassPosition,
	)
}

func (p *AeronPublisher) RequestVote(
	dst PublicationTarget,
	fields codecs.RequestVoteFields,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publishClaim(
		p,
		dst,
		codecs.RequestVoteBlockLength(ver),
		ver,
		applyRequestVoteProtocolVersion(ver, fields),
		codecs.EncodeRequestVote,
	)
}

func (p *AeronPublisher) PlaceVote(
	dst PublicationTarget,
	fields codecs.VoteFields,
) {
	ver := actingVersion(p.ActingVersion)
	_ = publishClaim(
		p,
		dst,
		clusterwire.VoteSbeBlockLength,
		ver,
		fields,
		codecs.EncodeVote,
	)
}

func (p *AeronPublisher) NewLeadershipTerm(
	dst PublicationTarget,
	fields codecs.NewLeadershipTermFields,
) {
	ver := actingVersion(p.ActingVersion)
	_ = publishClaim(
		p,
		dst,
		codecs.NewLeadershipTermBlockLength(ver),
		ver,
		fields,
		codecs.EncodeNewLeadershipTerm,
	)
}

func (p *AeronPublisher) AppendPosition(
	dst PublicationTarget,
	fields codecs.AppendPositionFields,
) bool {
	ver := actingVersion(p.ActingVersion)
	return publishClaim(
		p,
		dst,
		codecs.AppendPositionBlockLength(ver),
		ver,
		fields,
		codecs.EncodeAppendPosition,
	)
}

func (p *AeronPublisher) CommitPosition(
	dst PublicationTarget,
	fields codecs.CommitPositionFields,
) {
	ver := actingVersion(p.ActingVersion)
	_ = publishClaim(
		p,
		dst,
		clusterwire.CommitPositionSbeBlockLength,
		ver,
		fields,
		codecs.EncodeCommitPosition,
	)
}
