package client

import (
	"errors"
	"fmt"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

// AeronCluster is a connected cluster client session. Not safe for concurrent use.
//
// Methods other than accessors require a non-nil receiver. The application goroutine
// must invoke client.DoWork() on the transport *aeron.Client between Offer/TryClaim
// and PollEgress spins, matching ClusterContext invoker-only rules. Callers must
// CommitClaim or AbortClaim any in-flight TryClaim before Close.
type AeronCluster struct {
	ctx       ClusterContext
	aeron     *aeron.Client
	ownsAeron bool

	clusterSessionID int64
	leadershipTermID int64
	leaderMemberID   int32

	ingressPublication IngressPublication

	egressSubscription *aeron.Subscription
	egressImage        *aeron.Image
	egressAdapter      *EgressAdapter
	nanoClock          func() int64
	ingressMaxFrameLen int
	ingressScratch     []byte
	controlScratch     []byte
	closed             bool
}

// IngressClaim pins an in-flight TryClaim until CommitClaim or AbortClaim.
//
// The caller owns the claim between TryClaim success and commit/abort; Close does
// not finalize pending claims. Position is the stream position returned by the
// underlying TryClaim.
type IngressClaim struct {
	TermIndex     int
	FrameOffset   int
	PayloadLength int
	Position      int64
}

// NewAeronCluster constructs a session from a successful ConnectResult.
//
// Runs result.Context.Normalize().Validate() -- requires non-nil EgressHandler.
// Rejects nil result, nil required handles, closed ingress/subscription handles,
// or missing leader ingress publication. Transfers resource ownership from
// ConnectResult into AeronCluster; after success the result's transport handles
// are owned by the session -- do not Close them separately.
func NewAeronCluster(result *ConnectResult) (*AeronCluster, error) {
	if result == nil {
		return nil, errors.New("cluster client: nil connect result")
	}
	if result.IngressPublication == nil {
		return nil, errors.New("cluster client: ingress publication missing")
	}
	if result.EgressSubscription == nil {
		return nil, errors.New("cluster client: egress subscription missing")
	}
	if result.Aeron == nil {
		return nil, errors.New("cluster client: aeron client missing")
	}
	if result.IngressPublication.IsClosed() {
		return nil, errors.New("cluster client: ingress publication is closed")
	}
	if result.EgressSubscription.IsClosed() {
		return nil, errors.New("cluster client: egress subscription is closed")
	}
	if result.EgressImage != nil && result.EgressImage.IsClosed() {
		return nil, errors.New("cluster client: egress image is closed")
	}

	ctx := result.Context.Normalize()
	if err := ctx.Validate(); err != nil {
		return nil, err
	}

	memberMap := result.MemberIngressMap
	if !memberMap.IsChannelOnly() {
		ingress, err := memberMap.Require(result.LeaderMemberID)
		if err != nil {
			return nil, err
		}
		if ingress.Publication == nil {
			return nil, errors.New("cluster client: leader ingress publication missing")
		}
		if ingress.Publication != result.IngressPublication {
			return nil, errors.New("cluster client: leader ingress publication mismatch")
		}
	}

	adapter := NewEgressAdapterFromContext(
		ctx,
		ctx.EgressHandler,
		result.ClusterSessionID,
		result.EgressSubscription,
	)

	ingressScratchLen, ingressMaxFrameLen := ingressSessionLimits(result.IngressPublication)
	ingressScratch := make([]byte, ingressScratchLen)

	closeWireLen, closeOK := codecs.ClusterWireMessageLen(
		gensbe.SessionCloseRequestSbeBlockLength,
	)
	keepAliveWireLen, keepAliveOK := codecs.ClusterWireMessageLen(
		gensbe.SessionKeepAliveSbeBlockLength,
	)
	if !closeOK || !keepAliveOK {
		return nil, errors.New("cluster client: control frame wire length unavailable")
	}
	controlScratchLen := max(closeWireLen, keepAliveWireLen)

	nanoClock := resolveNanoClock(ctx.AeronContext.NanoClock)

	ac := &AeronCluster{
		ctx:                ctx,
		aeron:              result.Aeron,
		ownsAeron:          result.OwnsAeronClient,
		clusterSessionID:   result.ClusterSessionID,
		leadershipTermID:   result.LeadershipTermID,
		leaderMemberID:     result.LeaderMemberID,
		ingressPublication: result.IngressPublication,
		egressSubscription: result.EgressSubscription,
		egressImage:        result.EgressImage,
		egressAdapter:      adapter,
		nanoClock:          nanoClock,
		ingressMaxFrameLen: ingressMaxFrameLen,
		ingressScratch:     ingressScratch,
		controlScratch:     make([]byte, controlScratchLen),
	}

	result.IngressPublication = nil
	result.EgressSubscription = nil
	result.EgressImage = nil
	// Injected Aeron (OwnsAeronClient false) stays on result; session does not close it.
	if result.OwnsAeronClient {
		result.Aeron = nil
	}

	return ac, nil
}

// Offer prepends a SessionMessageHeader and offers payload via the leader ingress
// publication. Returns aeron publication status codes; no Go error on the hot path.
// EncodeIngressHeader failure maps to NotConnected; ingressScratch is sized at
// construct so encode cannot fail on valid session state. Exclusive publications
// return UnsupportedMessageLength when header+payload exceeds maxPayloadLength;
// concurrent publications may fragment larger totals via OfferWithOffset.
func (ac *AeronCluster) Offer(payload []byte) int64 {
	if ac.closed {
		return aeron.Closed
	}
	pub := ac.leaderIngress()
	if pub == nil {
		return aeron.NotConnected
	}
	totalLen := SessionHeaderLength() + len(payload)
	// Exclusive single-frame limit; concurrent Publication fragments above maxPayload.
	if ac.ingressMaxFrameLen > 0 && totalLen > ac.ingressMaxFrameLen {
		return aeron.UnsupportedMessageLength
	}
	if totalLen > len(ac.ingressScratch) {
		return aeron.MaxPositionExceeded
	}
	fields := ac.ingressHeaderFields()
	if _, encErr := codecs.EncodeIngressHeaderScratch(ac.ingressScratch, fields); encErr != nil {
		return aeron.NotConnected
	}
	copy(ac.ingressScratch[SessionHeaderLength():], payload)
	return pub.OfferWithOffset(ac.ingressScratch, 0, totalLen)
}

// TryClaim reserves ingress frame space for payloadLength application bytes plus the
// SessionMessageHeader. Like Aeron TryClaim, only a single unfragmented frame is
// produced; header+payload must fit ingressPublication.MaxPayloadLength(). Maximum
// application payload is MaxPayloadLength()-SessionHeaderLength(). Unlike Offer,
// which on concurrent *aeron.Publication may fragment totals larger than
// maxPayloadLength via OfferWithOffset, oversize TryClaim returns a non-nil err rather
// than fragmenting. Returns Aeron operational codes via position when err is nil;
// programmer errors (negative length, oversize claim) return a non-nil err.
func (ac *AeronCluster) TryClaim(payloadLength int) (IngressClaim, int64, error) {
	if ac.closed {
		return IngressClaim{}, aeron.Closed, nil
	}
	if payloadLength < 0 {
		return IngressClaim{}, 0, fmt.Errorf("cluster client: negative payload length %d", payloadLength)
	}
	pub := ac.leaderIngress()
	if pub == nil {
		return IngressClaim{}, aeron.NotConnected, nil
	}
	totalLen := SessionHeaderLength() + payloadLength
	termIndex, frameOffset, position, err := pub.TryClaim(totalLen)
	if err != nil {
		return IngressClaim{}, 0, err
	}
	if position <= 0 {
		return IngressClaim{}, position, nil
	}
	// TermBuffer from IngressPublication; claim length already validated by TryClaim.
	termBuf := pub.TermBuffer(termIndex)
	fields := ac.ingressHeaderFields()
	if _, encErr := codecs.EncodeIngressHeader(termBuf.Bytes(), frameOffset, fields); encErr != nil {
		_ = pub.Abort(termIndex, frameOffset)
		return IngressClaim{}, 0, encErr
	}
	claim := IngressClaim{
		TermIndex:     termIndex,
		FrameOffset:   frameOffset,
		PayloadLength: payloadLength,
		Position:      position,
	}
	return claim, position, nil
}

// CommitClaim copies payload into the claimed frame and commits the publication claim.
//
// Returns claim.Position on success. Operational/session state returns publication
// codes (Closed, NotConnected) with nil err; programmer errors and transport
// Commit/Abort failures return non-nil err. Uses ingressPublication directly for
// in-flight claims; does not re-check IsConnected via leaderIngress.
func (ac *AeronCluster) CommitClaim(claim IngressClaim, payload []byte) (int64, error) {
	if ac.closed {
		return aeron.Closed, nil
	}
	pub := ac.ingressPublication
	if pub == nil {
		return aeron.NotConnected, nil
	}
	if len(payload) != claim.PayloadLength {
		_ = pub.Abort(claim.TermIndex, claim.FrameOffset)
		return 0, fmt.Errorf(
			"cluster client: payload length %d does not match claimed %d",
			len(payload), claim.PayloadLength,
		)
	}
	termBuf := pub.TermBuffer(claim.TermIndex) // IngressPublication term buffer for claim write.
	payloadOffset := claim.FrameOffset + logbuffer.OffsetFramePayload + SessionHeaderLength()
	termBuf.PutBytes(payloadOffset, payload)
	if err := pub.Commit(claim.TermIndex, claim.FrameOffset); err != nil {
		return 0, err
	}
	return claim.Position, nil
}

// AbortClaim best-effort aborts an in-flight ingress claim.
func (ac *AeronCluster) AbortClaim(claim IngressClaim) error {
	if ac.closed || ac.ingressPublication == nil {
		return nil
	}
	return ac.ingressPublication.Abort(claim.TermIndex, claim.FrameOffset)
}

// PollEgress polls the egress subscription and dispatches to EgressHandler.
//
// When the connect-time egress image closes, the first poll frees its assembler
// buffer and clears reassembly state, then returns 0; subsequent polls continue
// on the subscription until Close.
func (ac *AeronCluster) PollEgress() int {
	if ac.closed || ac.egressAdapter == nil {
		return 0
	}
	if ac.egressImageUnavailable() {
		ac.teardownEgressImageState()
		return 0
	}
	return ac.egressAdapter.Poll()
}

// Close sends a best-effort SessionCloseRequest and releases session resources.
func (ac *AeronCluster) Close() error {
	if ac.closed {
		return nil
	}
	ac.closed = true
	ac.sendSessionCloseBestEffort()
	ac.teardownEgressImageState()
	if ac.ingressPublication != nil {
		_ = ac.ingressPublication.Close()
		ac.ingressPublication = nil
	}
	if ac.egressSubscription != nil {
		_ = ac.egressSubscription.Close()
		ac.egressSubscription = nil
	}
	if ac.ownsAeron && ac.aeron != nil {
		_ = ac.aeron.Close()
		ac.aeron = nil
	}
	return nil
}

// SendKeepAlive offers a SessionKeepAlive control frame via leader ingress.
func (ac *AeronCluster) SendKeepAlive() int64 {
	if ac.closed {
		return aeron.Closed
	}
	pub := ac.leaderIngress()
	if pub == nil {
		return aeron.NotConnected
	}
	actingVersion := uint16(gensbe.SessionKeepAliveSbeSchemaVersion)
	fields := codecs.SessionKeepAliveFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
	}
	end, ok := codecs.EncodeSessionKeepAlive(ac.controlScratch, 0, actingVersion, fields)
	if !ok {
		return aeron.NotConnected
	}
	return pub.Offer(ac.controlScratch[:end])
}

// IsClosed reports whether Close has been invoked.
func (ac *AeronCluster) IsClosed() bool {
	if ac == nil {
		return true
	}
	return ac.closed
}

// ClusterSessionID returns the connected cluster session id.
func (ac *AeronCluster) ClusterSessionID() int64 {
	if ac == nil {
		return 0
	}
	return ac.clusterSessionID
}

// LeadershipTermID returns the leadership term id from connect.
func (ac *AeronCluster) LeadershipTermID() int64 {
	if ac == nil {
		return 0
	}
	return ac.leadershipTermID
}

// LeaderMemberID returns the leader member id from connect.
func (ac *AeronCluster) LeaderMemberID() int32 {
	if ac == nil {
		return 0
	}
	return ac.leaderMemberID
}

// IngressPublication returns the leader ingress publication handle.
func (ac *AeronCluster) IngressPublication() IngressPublication {
	if ac == nil {
		return nil
	}
	return ac.ingressPublication
}

// EgressSubscription returns the egress subscription handle.
func (ac *AeronCluster) EgressSubscription() *aeron.Subscription {
	if ac == nil {
		return nil
	}
	return ac.egressSubscription
}

// Context returns the normalized cluster context used by the session.
func (ac *AeronCluster) Context() ClusterContext {
	if ac == nil {
		return ClusterContext{}
	}
	return ac.ctx
}

// Aeron returns the transport client. Nil after Close when the session owned the client.
func (ac *AeronCluster) Aeron() *aeron.Client {
	if ac == nil {
		return nil
	}
	return ac.aeron
}

func (ac *AeronCluster) ingressHeaderFields() codecs.IngressHeaderFields {
	return codecs.IngressHeaderFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
		Timestamp:        ac.nanoClock(),
	}
}

func (ac *AeronCluster) leaderIngress() IngressPublication {
	pub := ac.ingressPublication
	if pub == nil || !pub.IsConnected() {
		return nil
	}
	return pub
}

func (ac *AeronCluster) egressImageUnavailable() bool {
	return ac.egressImage != nil && ac.egressImage.IsClosed()
}

func (ac *AeronCluster) teardownEgressImageState() {
	if ac.egressAdapter == nil {
		return
	}
	// No egressImageTeardownDone flag: egressImage=nil after first pass makes
	// egressImageUnavailable false, so repeat calls only re-Clear adapter state.
	if ac.egressImage != nil {
		ac.egressAdapter.FreeSessionBuffer(ac.egressImage.SessionID())
	}
	ac.egressAdapter.Clear()
	ac.egressImage = nil
}

func (ac *AeronCluster) sendSessionCloseBestEffort() {
	// Close sets closed before this call; ac.closed is intentionally not checked here.
	if ac.ingressPublication == nil || ac.ingressPublication.IsClosed() {
		return
	}
	actingVersion := uint16(gensbe.SessionCloseRequestSbeSchemaVersion)
	fields := codecs.SessionCloseRequestFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
	}
	end, ok := codecs.EncodeSessionCloseRequest(ac.controlScratch, 0, actingVersion, fields)
	if !ok {
		return
	}
	_ = ac.ingressPublication.Offer(ac.controlScratch[:end])
}
