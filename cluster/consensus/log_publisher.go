package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/buffer"
)

// LogPublisher appends cluster log frames on a single-writer exclusive publication.
//
// Concurrency: invoker-only. One goroutine owns Offer / TryClaim / Commit / Abort
// (ExclusivePublication contract).
//
// Lifetime: caller owns the ExclusivePublication. LogPublisher does not Close the
// publication on replace.
type LogPublisher struct {
	publication *aeron.ExclusivePublication
	// scratch is the encode buffer. SetPublication sizes it to MaxPayloadLength
	// for the single-frame hot path. Larger totals grow it up to
	// MaxMessageLength. Retained across appends.
	scratch []byte
}

// NewLogPublisher returns a publisher with no publication bound.
// Bind via SetPublication. Nil-safe Position and SessionID return 0 when unbound.
func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

// SetPublication binds or replaces the exclusive log publication.
//
// When pub is non-nil, grows scratch to at least MaxPayloadLength so later
// single-frame appends pass the payload length check without growing on the
// hot path.
//
// Does not close the previous publication. The caller owns lifecycle.
func (p *LogPublisher) SetPublication(pub *aeron.ExclusivePublication) {
	p.publication = pub
	if pub == nil {
		return
	}
	p.scratch = buffer.GrowScratch(p.scratch, pub.MaxPayloadLength())
}

// Publication returns the bound exclusive publication, or nil.
func (p *LogPublisher) Publication() *aeron.ExclusivePublication {
	return p.publication
}

// Position returns the publication position, or 0 when unbound.
func (p *LogPublisher) Position() int64 {
	if p == nil || p.publication == nil {
		return 0
	}
	return p.publication.Position()
}

// SessionID returns the publication session id, or 0 when unbound.
func (p *LogPublisher) SessionID() int32 {
	if p == nil || p.publication == nil {
		return 0
	}
	return p.publication.SessionID()
}

// scratchFor returns a buffer of length total for encoding. Totals above the
// current capacity grow scratch geometrically. The grown buffer is retained.
func (p *LogPublisher) scratchFor(total int) []byte {
	p.scratch = buffer.GrowScratch(p.scratch, total)
	return p.scratch
}

// offerEncoded offers the encoded bytes in buf under [aeron.SendAttempts].
//
// The caller must bind the publication and encode into buf before calling.
// Retry policy is [aeron.OfferAttempts].
func (p *LogPublisher) offerEncoded(buf []byte) int64 {
	return aeron.OfferAttempts(p.publication, buf, aeron.SendAttempts)
}

// AppendMessage encodes SessionMessageHeader + payload into scratch and offers
// the frame. Payloads above MaxPayloadLength fragment on the exclusive
// publication when the total fits MaxMessageLength.
//
// Returns stream position (>0) or aeron publication codes (NotConnected,
// BackPressured, AdminAction, Closed, MaxPositionExceeded,
// UnsupportedMessageLength). Nil publication -> Closed.
//
// Returns UnsupportedMessageLength without an Offer when headerLen+len(payload)
// exceeds MaxMessageLength.
//
// Offer retry policy is [LogPublisher.offerEncoded].
func (p *LogPublisher) AppendMessage(
	leadershipTermID int64,
	clusterSessionID int64,
	timestamp int64,
	payload []byte,
) int64 {
	if p == nil || p.publication == nil {
		return aeron.Closed
	}
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	total := headerLen + len(payload)
	if total > p.publication.MaxMessageLength() {
		return aeron.UnsupportedMessageLength
	}
	scratch := p.scratchFor(total)

	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: leadershipTermID,
		ClusterSessionID: clusterSessionID,
		Timestamp:        timestamp,
	}
	if _, err := codecs.EncodeIngressHeaderScratch(scratch, fields); err != nil {
		// Sized scratch after MaxMessageLength check should not fail; map like
		// client Offer encode path.
		return aeron.NotConnected
	}
	copy(scratch[headerLen:total], payload)
	return p.offerEncoded(scratch)
}

// AppendNewLeadershipTermEvent reserves a fixed 56-byte unfragmented claim,
// encodes NewLeadershipTermEvent (template 24) into the term buffer, and commits.
//
// LogPosition on the wire is the successful claim end position (pre-claim
// Position plus aligned fragment length).
//
// Returns false when receiver or publication is nil, or when encode fails.
// Claim retry, abort, and commit policy is [aeron.ExclusivePublication.ClaimFillCommit].
func (p *LogPublisher) AppendNewLeadershipTermEvent(
	leadershipTermID int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberID int32,
	logSessionID int32,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) bool {
	_, ok := appendClaimPos(
		p,
		codecs.NewLeadershipTermEventEncodedLen(),
		uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion),
		func(claimPos int64) codecs.NewLeadershipTermEventFields {
			return codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    leadershipTermID,
				LogPosition:         claimPos,
				Timestamp:           timestamp,
				TermBaseLogPosition: termBaseLogPosition,
				LeaderMemberID:      leaderMemberID,
				LogSessionID:        logSessionID,
				TimeUnit:            timeUnit,
				AppVersion:          appVersion,
			}
		},
		codecs.EncodeNewLeadershipTermEvent,
	)
	return ok
}

// AppendClusterAction reserves a fixed 40-byte unfragmented claim, encodes
// ClusterActionRequest (template 23) into the term buffer, and commits.
//
// LogPosition on the wire is the successful claim end position (pre-claim
// Position plus aligned fragment length).
//
// Returns the encoded logPosition and true on commit. Returns 0 and false
// when the receiver or publication is nil, encode fails, or the claim
// budget exhausts. Claim retry, abort, and commit policy is
// [aeron.ExclusivePublication.ClaimFillCommit].
func (p *LogPublisher) AppendClusterAction(
	leadershipTermID int64,
	timestamp int64,
	action clusterwire.ClusterAction,
	flags int32,
) (int64, bool) {
	return appendClaimPos(
		p,
		codecs.ClusterActionRequestEncodedLen(),
		uint16(clusterwire.ClusterActionRequestSbeSchemaVersion),
		func(claimPos int64) codecs.ClusterActionRequestFields {
			return codecs.ClusterActionRequestFields{
				LeadershipTermID: leadershipTermID,
				LogPosition:      claimPos,
				Timestamp:        timestamp,
				Action:           action,
				Flags:            flags,
			}
		},
		codecs.EncodeClusterActionRequest,
	)
}

// AppendSessionOpen encodes SessionOpenEvent (template 21) into scratch and
// offers the frame. Totals above MaxPayloadLength fragment when they fit
// MaxMessageLength.
//
// Behaviour: copies responseChannel and encodedPrincipal into scratch. Does not
// retain either slice. Does not validate field semantics.
//
// Returns stream position (>0) on success, or an aeron publication code:
//   - Closed when the receiver or publication is nil
//   - UnsupportedMessageLength when ClusterWireMessageLen fails or total
//     exceeds MaxMessageLength (does not call Offer)
//   - NotConnected when encode fails after the length checks
//   - Offer codes from [LogPublisher.offerEncoded] (same retry budget and
//     terminal-stop policy as AppendMessage)
func (p *LogPublisher) AppendSessionOpen(
	leadershipTermID int64,
	correlationID int64,
	clusterSessionID int64,
	timestamp int64,
	responseStreamID int32,
	responseChannel []byte,
	encodedPrincipal []byte,
) int64 {
	if p == nil || p.publication == nil {
		return aeron.Closed
	}
	total, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength,
		len(responseChannel),
		len(encodedPrincipal),
	)
	if !ok || total > p.publication.MaxMessageLength() {
		return aeron.UnsupportedMessageLength
	}
	scratch := p.scratchFor(total)

	fields := codecs.SessionOpenEventFields{
		LeadershipTermID: leadershipTermID,
		CorrelationID:    correlationID,
		ClusterSessionID: clusterSessionID,
		Timestamp:        timestamp,
		ResponseStreamID: responseStreamID,
		ResponseChannel:  responseChannel,
		EncodedPrincipal: encodedPrincipal,
	}
	actingVersion := uint16(clusterwire.SessionOpenEventSbeSchemaVersion)
	if _, encOK := codecs.EncodeSessionOpenEvent(scratch, 0, actingVersion, fields); !encOK {
		// Length checks already passed. Encode failure maps to NotConnected, same
		// as AppendMessage.
		return aeron.NotConnected
	}
	return p.offerEncoded(scratch)
}

// appendClaimPos encodes fields into a fixed-length claim and commits. The
// fields callback receives the successful claim end position so LogPosition
// can be written on the wire.
//
// Returns the claim end position and true on commit. Returns 0 and false
// when:
//   - the receiver or publication is nil
//   - encode fails
//   - the claim budget exhausts
//
// Claim retry, abort, and commit policy is
// [aeron.ExclusivePublication.ClaimFillCommit].
func appendClaimPos[F any](
	p *LogPublisher,
	length int,
	actingVersion uint16,
	fields func(claimPos int64) F,
	encode func([]byte, int, uint16, F) (int, bool),
) (int64, bool) {
	if p == nil {
		return 0, false
	}
	var claimPos int64
	ok := p.publication.ClaimFillCommit(
		length,
		aeron.SendAttempts,
		func(term []byte, payloadOff int, pos int64) bool {
			claimPos = pos
			wireEnd, encOK := encode(term, payloadOff, actingVersion, fields(pos))
			return encOK && wireEnd-payloadOff == length
		},
	)
	if !ok {
		return 0, false
	}
	return claimPos, true
}

// AppendSessionClose reserves a fixed 36-byte unfragmented claim. It encodes
// SessionCloseEvent (template 22) into the term buffer, then commits.
//
// Returns the claim end position and true on commit. Returns 0 and false
// when the receiver or publication is nil, encode fails, or the claim
// budget exhausts. Claim retry, abort, and commit policy is
// [aeron.ExclusivePublication.ClaimFillCommit].
func (p *LogPublisher) AppendSessionClose(
	leadershipTermID int64,
	clusterSessionID int64,
	timestamp int64,
	closeReason clusterwire.CloseReason,
) (int64, bool) {
	return appendClaimPos(
		p,
		codecs.SessionCloseEventEncodedLen(),
		uint16(clusterwire.SessionCloseEventSbeSchemaVersion),
		func(int64) codecs.SessionCloseEventFields {
			return codecs.SessionCloseEventFields{
				LeadershipTermID: leadershipTermID,
				ClusterSessionID: clusterSessionID,
				Timestamp:        timestamp,
				CloseReason:      closeReason,
			}
		},
		codecs.EncodeSessionCloseEvent,
	)
}

// AppendTimer reserves a fixed 32-byte unfragmented claim. It encodes
// TimerEvent (template 20) into the term buffer, then commits.
//
// Returns the claim end position and true on commit. Returns 0 and false
// when the receiver or publication is nil, encode fails, or the claim
// budget exhausts. Claim retry, abort, and commit policy is
// [aeron.ExclusivePublication.ClaimFillCommit].
func (p *LogPublisher) AppendTimer(
	leadershipTermID int64,
	correlationID int64,
	timestamp int64,
) (int64, bool) {
	return appendClaimPos(
		p,
		codecs.TimerEventEncodedLen(),
		uint16(clusterwire.TimerEventSbeSchemaVersion),
		func(int64) codecs.TimerEventFields {
			return codecs.TimerEventFields{
				LeadershipTermID: leadershipTermID,
				CorrelationID:    correlationID,
				Timestamp:        timestamp,
			}
		},
		codecs.EncodeTimerEvent,
	)
}
