package consensus

import (
	"github.com/warrenguy/heron/aeron"
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

// AppendPosition returns the publication position, or 0 when unbound.
//
// It satisfies [AppendPositionSource]: a bound publisher is the production
// append high-water source a leader ranks quorum against and a follower
// publishes as its append position. [AtomicAppendPosition] stays for unit and
// simulation inject.
func (p *LogPublisher) AppendPosition() int64 { return p.Position() }

var _ AppendPositionSource = (*LogPublisher)(nil)

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
