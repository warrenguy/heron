package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// logPublisherSendAttempts is the Offer/TryClaim retry budget (SEND_ATTEMPTS).
const logPublisherSendAttempts = 3

// growEncodeScratch returns a buffer of length total. Grows geometrically when
// cap(scratch) is short. The caller retains the returned slice as scratch.
func growEncodeScratch(scratch []byte, total int) []byte {
	if cap(scratch) < total {
		return make([]byte, total, max(total, cap(scratch)*2))
	}
	return scratch[:total]
}

// offerWithOffset is the OfferWithOffset surface shared by Publication and
// ExclusivePublication.
type offerWithOffset interface {
	OfferWithOffset(buffer []byte, offset, length int) int64
}

// offerEncodedWithAttempts offers buf with logPublisherSendAttempts retries on
// retriable codes (BackPressured, AdminAction, NotConnected). Closed and
// MaxPositionExceeded stop immediately.
func offerEncodedWithAttempts(pub offerWithOffset, buf []byte) int64 {
	var position int64
	for range logPublisherSendAttempts {
		position = pub.OfferWithOffset(buf, 0, len(buf))
		if position > 0 || !aeron.IsOfferRetriable(position) {
			return position
		}
	}
	return position
}

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
	p.scratch = growEncodeScratch(p.scratch, pub.MaxPayloadLength())
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
	p.scratch = growEncodeScratch(p.scratch, total)
	return p.scratch
}

// offerEncoded offers the encoded bytes in buf with the log publisher retry
// budget.
//
// The caller must bind the publication and encode into buf before calling.
// Retries up to logPublisherSendAttempts on retriable codes (BackPressured,
// AdminAction, NotConnected). Closed and MaxPositionExceeded stop immediately.
func (p *LogPublisher) offerEncoded(buf []byte) int64 {
	return offerEncodedWithAttempts(p.publication, buf)
}

// AppendMessage encodes SessionMessageHeader + payload into scratch and offers
// the frame. Payloads above MaxPayloadLength fragment on the exclusive
// publication when the total fits MaxMessageLength.
//
// Returns stream position (>0) or aeron publication codes (NotConnected,
// BackPressured, AdminAction, Closed, MaxPositionExceeded,
// UnsupportedMessageLength). Nil publication -> Closed.
//
// headerLen+len(payload) > MaxMessageLength -> UnsupportedMessageLength without
// calling Offer. Keeps MaxPositionExceeded reserved for publication-position
// exhaustion.
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
	if p == nil {
		return false
	}
	length := codecs.NewLeadershipTermEventEncodedLen()
	actingVersion := uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion)

	return p.publication.ClaimFillCommit(
		length,
		logPublisherSendAttempts,
		func(term []byte, payloadOff int, claimPos int64) bool {
			fields := codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    leadershipTermID,
				LogPosition:         claimPos,
				Timestamp:           timestamp,
				TermBaseLogPosition: termBaseLogPosition,
				LeaderMemberID:      leaderMemberID,
				LogSessionID:        logSessionID,
				TimeUnit:            timeUnit,
				AppVersion:          appVersion,
			}
			wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(
				term,
				payloadOff,
				actingVersion,
				fields,
			)
			return ok && wireEnd-payloadOff == length
		},
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

// appendClaim encodes fields into a fixed-length claim and commits.
//
// Returns false when the receiver or publication is nil, or when encode fails.
// Claim retry, abort, and commit policy is [aeron.ExclusivePublication.ClaimFillCommit].
func appendClaim[F any](
	p *LogPublisher,
	length int,
	actingVersion uint16,
	fields F,
	encode func([]byte, int, uint16, F) (int, bool),
) bool {
	if p == nil {
		return false
	}
	return p.publication.ClaimFillCommit(
		length,
		logPublisherSendAttempts,
		func(term []byte, payloadOff int, _ int64) bool {
			wireEnd, ok := encode(term, payloadOff, actingVersion, fields)
			return ok && wireEnd-payloadOff == length
		},
	)
}

// AppendSessionClose reserves a fixed 36-byte unfragmented claim, encodes
// SessionCloseEvent (template 22) into the term buffer, and commits.
//
// Returns false when the receiver or publication is nil, or when encode fails.
// Claim retry, abort, and commit policy is [aeron.ExclusivePublication.ClaimFillCommit].
func (p *LogPublisher) AppendSessionClose(
	leadershipTermID int64,
	clusterSessionID int64,
	timestamp int64,
	closeReason clusterwire.CloseReason,
) bool {
	return appendClaim(
		p,
		codecs.SessionCloseEventEncodedLen(),
		uint16(clusterwire.SessionCloseEventSbeSchemaVersion),
		codecs.SessionCloseEventFields{
			LeadershipTermID: leadershipTermID,
			ClusterSessionID: clusterSessionID,
			Timestamp:        timestamp,
			CloseReason:      closeReason,
		},
		codecs.EncodeSessionCloseEvent,
	)
}

// AppendTimer reserves a fixed 32-byte unfragmented claim, encodes TimerEvent
// (template 20) into the term buffer, and commits.
//
// Returns false when the receiver or publication is nil, or when encode fails.
// Claim retry, abort, and commit policy is [aeron.ExclusivePublication.ClaimFillCommit].
func (p *LogPublisher) AppendTimer(
	leadershipTermID int64,
	correlationID int64,
	timestamp int64,
) bool {
	return appendClaim(
		p,
		codecs.TimerEventEncodedLen(),
		uint16(clusterwire.TimerEventSbeSchemaVersion),
		codecs.TimerEventFields{
			LeadershipTermID: leadershipTermID,
			CorrelationID:    correlationID,
			Timestamp:        timestamp,
		},
		codecs.EncodeTimerEvent,
	)
}
