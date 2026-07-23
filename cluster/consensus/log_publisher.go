package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// logPublisherSendAttempts is the Offer/TryClaim retry budget (SEND_ATTEMPTS).
const logPublisherSendAttempts = 3

// LogPublisher appends cluster log frames on a single-writer exclusive publication.
//
// Concurrency: invoker-only. One goroutine owns Offer / TryClaim / Commit / Abort
// (ExclusivePublication contract).
//
// Lifetime: caller owns the ExclusivePublication. LogPublisher does not Close the
// publication on replace.
type LogPublisher struct {
	publication *aeron.ExclusivePublication
	scratch     []byte
}

// NewLogPublisher returns a publisher with no publication bound.
// Bind via SetPublication. Nil-safe Position and SessionID return 0 when unbound.
func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

// SetPublication binds or replaces the exclusive log publication.
//
// Resizes scratch to MaxPayloadLength when pub is non-nil so later appends fit
// under a valid payload length check without growing on the hot path.
//
// Does not close the previous publication; the caller owns lifecycle.
func (p *LogPublisher) SetPublication(pub *aeron.ExclusivePublication) {
	p.publication = pub
	if pub == nil {
		return
	}
	need := pub.MaxPayloadLength()
	if cap(p.scratch) < need {
		p.scratch = make([]byte, need)
	} else {
		p.scratch = p.scratch[:need]
	}
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

// AppendMessage encodes SessionMessageHeader + payload into scratch and offers
// a single unfragmented frame.
//
// Returns stream position (>0) or aeron publication codes (NotConnected,
// BackPressured, AdminAction, Closed, MaxPositionExceeded,
// UnsupportedMessageLength). Nil publication -> Closed.
//
// headerLen+len(payload) > MaxPayloadLength -> UnsupportedMessageLength without
// calling Offer (same code Offer would return for oversize exclusive payload).
//
// Retries up to logPublisherSendAttempts on retriable codes (BackPressured,
// AdminAction, NotConnected). Closed and MaxPositionExceeded stop immediately.
func (p *LogPublisher) AppendMessage(
	leadershipTermID int64,
	clusterSessionID int64,
	timestamp int64,
	payload []byte,
) int64 {
	if p == nil || p.publication == nil {
		return aeron.Closed
	}
	pub := p.publication
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	total := headerLen + len(payload)
	if total > pub.MaxPayloadLength() {
		return aeron.UnsupportedMessageLength
	}
	if total > len(p.scratch) {
		// SetPublication skipped after bind - treat as closed publisher state.
		return aeron.Closed
	}

	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: leadershipTermID,
		ClusterSessionID: clusterSessionID,
		Timestamp:        timestamp,
	}
	if _, err := codecs.EncodeIngressHeaderScratch(p.scratch, fields); err != nil {
		// Sized scratch after MaxPayloadLength check should not fail; map like
		// client Offer encode path.
		return aeron.NotConnected
	}
	copy(p.scratch[headerLen:total], payload)

	var position int64
	for range logPublisherSendAttempts {
		position = pub.OfferWithOffset(p.scratch, 0, total)
		if position > 0 || !aeron.IsOfferRetriable(position) {
			return position
		}
	}
	return position
}

// AppendNewLeadershipTermEvent reserves a fixed 56-byte unfragmented claim,
// encodes NewLeadershipTermEvent (template 24) into the term buffer, and commits.
//
// LogPosition on the wire is the successful claim end position (equal to
// TryClaim reserved position on success; pre-claim Position plus aligned
// fragment length).
//
// Returns true only after successful Commit. Returns false when:
//   - nil receiver or nil publication
//   - all send attempts exhausted with position <= 0
//   - TryClaim returns a non-nil err (invalid length - should not happen for 56)
//   - encode fails (Abort best-effort)
//   - Commit fails (Abort best-effort)
//
// Closed and MaxPositionExceeded stop immediately without retries. Does not
// throw; callers may inspect publication state separately.
func (p *LogPublisher) AppendNewLeadershipTermEvent(
	leadershipTermID int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberID int32,
	logSessionID int32,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) bool {
	if p == nil || p.publication == nil {
		return false
	}
	pub := p.publication
	length := codecs.NewLeadershipTermEventEncodedLen()
	actingVersion := uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion)

	for range logPublisherSendAttempts {
		termIndex, frameOffset, position, err := pub.TryClaim(length)
		if err != nil {
			// Fixed length 56 with sane MTU; fail closed on programmer path.
			return false
		}
		if position <= 0 {
			if !aeron.IsOfferRetriable(position) {
				return false
			}
			continue
		}

		fields := codecs.NewLeadershipTermEventFields{
			LeadershipTermID:    leadershipTermID,
			LogPosition:         position,
			Timestamp:           timestamp,
			TermBaseLogPosition: termBaseLogPosition,
			LeaderMemberID:      leaderMemberID,
			LogSessionID:        logSessionID,
			TimeUnit:            timeUnit,
			AppVersion:          appVersion,
		}

		termBuf := pub.TermBuffer(termIndex)

		// Claim payload starts at frameOffset + DataHeaderLength (header pre-written).
		payloadOff := frameOffset + logbuffer.OffsetFramePayload
		wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(
			termBuf.Bytes(),
			payloadOff,
			actingVersion,
			fields,
		)
		if !ok || wireEnd-payloadOff != length {
			_ = pub.Abort(termIndex, frameOffset)
			return false
		}
		if err := pub.Commit(termIndex, frameOffset); err != nil {
			// Commit failed after fill; best-effort Abort if still in-flight.
			_ = pub.Abort(termIndex, frameOffset)
			return false
		}
		return true
	}
	return false
}
