package client

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

// appIngressBlockedCode returns a non-zero publication code when app ingress must
// not proceed, or 0 when session guards pass. Only CONNECTED allows ingress.
func (ac *AeronCluster) appIngressBlockedCode() int64 {
	if ac.closed {
		return aeron.Closed
	}
	switch ac.sessionState {
	case SessionStateConnected:
		return 0
	case SessionStatePendingClose:
		return aeron.Closed
	case SessionStateAwaitNewLeader, SessionStateAwaitNewLeaderConnection:
	}
	return aeron.NotConnected
}

// leaderIngressPublication returns ac.ingressPublication when connected.
// When ingress is nil, returns NotConnected without tracking; PollEgress may
// drive handleDisconnected. When ingress exists but is disconnected and session
// is CONNECTED, tracks NotConnected before return. In AWAIT_NEW_LEADER_CONNECTION,
// trackIngressPublicationResult(NotConnected) is a no-op; recovery uses the
// member-map leader entry (recoveryLeaderIngress) and offerSessionKeepAlive instead.
func (ac *AeronCluster) leaderIngressPublication() (IngressPublication, int64) {
	pub := ac.ingressPublication
	if pub == nil || !pub.IsConnected() {
		if pub != nil && ac.sessionState == SessionStateConnected {
			ac.trackIngressPublicationResult(aeron.NotConnected)
		}
		return nil, aeron.NotConnected
	}
	return pub, 0
}

// leaderIngressForAppOffer is the app ingress entrypoint: session guards then
// leaderIngressPublication. Used by Offer, TryClaim, SendKeepAlive, and
// SendSnapshotRequest.
func (ac *AeronCluster) leaderIngressForAppOffer() (IngressPublication, int64) {
	if code := ac.appIngressBlockedCode(); code != 0 {
		return nil, code
	}
	return ac.leaderIngressPublication()
}

// trackIngressPublicationResult drives session-state transitions from ingress
// operational codes. Mirrors Java AeronCluster.trackIngressPublicationResult.
// In AWAIT_NEW_LEADER_CONNECTION only MaxPositionExceeded and positive positions
// alter state; Closed and NotConnected are intentional no-ops (recovery retries or
// pollTransportDisconnect handles transport loss).
func (ac *AeronCluster) trackIngressPublicationResult(result int64) {
	if ac.closed {
		return
	}
	switch ac.sessionState {
	case SessionStateConnected:
		switch result {
		case aeron.NotConnected, aeron.Closed:
			ac.handleDisconnected()
		case aeron.MaxPositionExceeded:
			ac.enterPendingClose()
		}
	case SessionStateAwaitNewLeaderConnection:
		switch result {
		case aeron.MaxPositionExceeded:
			ac.enterPendingClose()
		default:
			if result > 0 {
				ac.enterConnected()
			}
		}
	case SessionStateAwaitNewLeader, SessionStatePendingClose:
	}
}

// Offer prepends a SessionMessageHeader and offers payload via the leader ingress
// publication. Heron offers once on ingressScratch.
//
// The application goroutine must call client.DoWork between spins and re-invoke on
// BackPressured or AdminAction (invoker-only transport; no internal retry).
// Returns aeron publication status codes; no Go error on the hot path.
//
// EncodeIngressHeader failure maps to NotConnected; ingressScratch is sized at
// construct so encode cannot fail on valid session state. Exclusive publications
// return UnsupportedMessageLength when header+payload exceeds maxPayloadLength;
// concurrent publications may fragment larger totals via OfferWithOffset.
//
// Header plus payload above MaxMessageLength on a concurrent ingress returns
// UnsupportedMessageLength without an offer or state change. Publication-returned
// MaxPositionExceeded enters PENDING_CLOSE via trackIngressPublicationResult;
// PollEgress completes Close() via pollStateChanges.
//
// For paced retry, see IngressOfferUntilPositive (ingress_retry.go).
func (ac *AeronCluster) Offer(payload []byte) int64 {
	pub, code := ac.leaderIngressForAppOffer()
	if pub == nil {
		return code
	}
	totalLen := SessionHeaderLength() + len(payload)
	// Exclusive single-frame limit; concurrent Publication fragments above maxPayload.
	if ac.ingressMaxFrameLen > 0 && totalLen > ac.ingressMaxFrameLen {
		return aeron.UnsupportedMessageLength
	}
	if totalLen > len(ac.ingressScratch) {
		return aeron.UnsupportedMessageLength
	}
	fields := ac.ingressHeaderFields()
	if _, encErr := codecs.EncodeIngressHeaderScratch(ac.ingressScratch, fields); encErr != nil {
		return aeron.NotConnected
	}
	copy(ac.ingressScratch[SessionHeaderLength():], payload)
	position := pub.OfferWithOffset(ac.ingressScratch, 0, totalLen)
	if position < 0 {
		ac.trackIngressPublicationResult(position)
	}
	return position
}

// TryClaim reserves ingress frame space for payloadLength application bytes plus the
// SessionMessageHeader. Like Aeron TryClaim, only a single unfragmented frame is
// produced; header+payload must fit ingressPublication.MaxPayloadLength(). Maximum
// application payload is MaxPayloadLength()-SessionHeaderLength(). Unlike Offer,
// which on concurrent *aeron.Publication may fragment totals larger than
// maxPayloadLength via OfferWithOffset, oversize TryClaim returns a non-nil err rather
// than fragmenting. Returns Aeron operational codes via position when err is nil;
// programmer errors (negative length, oversize claim) return a non-nil err.
// EncodeIngressHeader failure best-effort aborts the claim; abort cleanup errors
// do not override the encode error.
//
// TryClaim/CommitClaim are not atomically retried: a failed CommitClaim or operational
// code on TryClaim leaves the claim state caller-owned.
//
// Do not wrap claim+commit in IngressOfferUntilPositive offerFn without explicit
// AbortClaim on retry paths.
func (ac *AeronCluster) TryClaim(payloadLength int) (IngressClaim, int64, error) {
	if payloadLength < 0 {
		return IngressClaim{}, 0, fmt.Errorf("cluster client: negative payload length %d", payloadLength)
	}
	pub, code := ac.leaderIngressForAppOffer()
	if pub == nil {
		return IngressClaim{}, code, nil
	}
	totalLen := SessionHeaderLength() + payloadLength
	termIndex, frameOffset, position, err := pub.TryClaim(totalLen)
	if err != nil {
		return IngressClaim{}, 0, err
	}
	if position < 0 {
		ac.trackIngressPublicationResult(position)
		return IngressClaim{}, position, nil
	}
	if position == 0 {
		return IngressClaim{}, 0, nil
	}
	// TermBuffer from IngressPublication; claim length already validated by TryClaim.
	termBuf := pub.TermBuffer(termIndex)
	fields := ac.ingressHeaderFields()
	if _, encErr := codecs.EncodeIngressHeader(termBuf.Bytes(), frameOffset, fields); encErr != nil {
		_ = abortIngressClaim(pub, termIndex, frameOffset)
		return IngressClaim{}, 0, encErr
	}
	claim := IngressClaim{
		TermIndex:        termIndex,
		FrameOffset:      frameOffset,
		PayloadLength:    payloadLength,
		Position:         position,
		leadershipTermID: ac.leadershipTermID,
		publication:      pub,
	}
	return claim, position, nil
}

// CommitClaim copies payload into the claimed frame and commits the publication claim.
//
// Returns claim.Position on success. Operational/session state returns publication
// codes (Closed, NotConnected) with nil err; programmer errors and transport Commit
// failures return non-nil err. Non-commit paths best-effort abort the claim (same as
// AbortClaim); abort cleanup errors do not override operational codes or programmer
// errors. Uses claim.publication and claim.leadershipTermID stamped at TryClaim;
// mismatches with the active leader ingress or leadership term abort and return
// NotConnected without session mutation. When CONNECTED and the stamped publication
// is the active ingress but closed, returns Closed and enters AWAIT_NEW_LEADER via
// trackIngressPublicationResult. In-flight claims on a disconnected-but-still-open
// active ingress return NotConnected without tracking; PollEgress pollTransportDisconnect
// detects that transport loss (intentional asymmetry with Offer/TryClaim).
//
// Claim+retry caveats: see TryClaim godoc.
func (ac *AeronCluster) CommitClaim(claim IngressClaim, payload []byte) (int64, error) {
	if ac.closed || ac.sessionState == SessionStatePendingClose {
		return aeron.Closed, nil
	}
	pub := claim.publication
	if pub == nil {
		return aeron.NotConnected, nil
	}
	var (
		code   int64
		commit bool
		err    error
	)
	switch {
	case pub.IsClosed():
		code = aeron.Closed
	case pub != ac.ingressPublication:
		code = aeron.NotConnected
	case claim.leadershipTermID != ac.leadershipTermID:
		code = aeron.NotConnected
	case len(payload) != claim.PayloadLength:
		err = fmt.Errorf(
			"cluster client: payload length %d does not match claimed %d",
			len(payload), claim.PayloadLength,
		)
	default:
		commit = true
	}
	if !commit {
		if !pub.IsClosed() {
			_ = abortIngressClaim(pub, claim.TermIndex, claim.FrameOffset)
		}
		if err != nil {
			return 0, err
		}
		if ac.sessionState == SessionStateConnected &&
			pub == ac.ingressPublication &&
			code == aeron.Closed {
			ac.trackIngressPublicationResult(code)
		}
		return code, nil
	}
	termBuf := pub.TermBuffer(claim.TermIndex) // IngressPublication term buffer for claim write.
	payloadOffset := claim.FrameOffset + logbuffer.OffsetFramePayload + SessionHeaderLength()
	termBuf.PutBytes(payloadOffset, payload)
	if err := pub.Commit(claim.TermIndex, claim.FrameOffset); err != nil {
		return 0, err
	}
	return claim.Position, nil
}

// AbortClaim best-effort aborts an in-flight ingress claim on claim.publication.
func (ac *AeronCluster) AbortClaim(claim IngressClaim) error {
	if ac.closed {
		return nil
	}
	pub := claim.publication
	if pub == nil || pub.IsClosed() {
		return nil
	}
	return abortIngressClaim(pub, claim.TermIndex, claim.FrameOffset)
}

func abortIngressClaim(pub IngressPublication, termIndex, frameOffset int) error {
	if pub == nil {
		return nil
	}
	return pub.Abort(termIndex, frameOffset)
}

// offerTrackedControlFrame encodes into controlScratch, offers on pub, and tracks
// the publication result. Guard failures (nil/closed pub, encode error) return
// NotConnected without tracking.
func (ac *AeronCluster) offerTrackedControlFrame(
	pub IngressPublication,
	encode func(scratch []byte) (end int, ok bool),
) int64 {
	if pub == nil || pub.IsClosed() {
		return aeron.NotConnected
	}
	end, ok := encode(ac.controlScratch)
	if !ok {
		return aeron.NotConnected
	}
	position := pub.Offer(ac.controlScratch[:end])
	ac.trackIngressPublicationResult(position)
	return position
}

// offerSessionKeepAlive offers SessionKeepAlive on pub and tracks the result.
func (ac *AeronCluster) offerSessionKeepAlive(pub IngressPublication) int64 {
	actingVersion := uint16(clusterwire.SessionKeepAliveSbeSchemaVersion)
	fields := codecs.SessionKeepAliveFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
	}
	return ac.offerTrackedControlFrame(pub, func(scratch []byte) (end int, ok bool) {
		return codecs.EncodeSessionKeepAlive(scratch, 0, actingVersion, fields)
	})
}

// SendKeepAlive offers one SessionKeepAlive control frame via leader ingress.
// Java sendKeepAlive retries tryClaim in a bounded loop; Heron offers once. The
// application goroutine must call client.DoWork between spins and re-invoke on
// BackPressured or AdminAction (invoker-only transport; no internal retry).
//
// Returns the publication stream position on success (>0) or Aeron publication
// status codes on failure; no Go error on the hot path. Session-guard codes
// (Closed, blocked NotConnected) return without tracking. When CONNECTED with a
// disconnected ingress publication, returns NotConnected and tracks via
// leaderIngressPublication, same as Offer and TryClaim.
//
// During leadership transition keep-alive may fail; keep calling PollEgress with
// DoWork between spins. Recovery keep-alives are sent from pollLeaderRecovery, not
// this API.
//
// For paced retry, see SendKeepAliveUntilPositive (ingress_retry.go).
func (ac *AeronCluster) SendKeepAlive() int64 {
	pub, code := ac.leaderIngressForAppOffer()
	if pub == nil {
		return code
	}
	return ac.offerSessionKeepAlive(pub)
}

// offerSnapshotRequest offers AdminRequest (template 26, SNAPSHOT, empty payload)
// on pub and tracks the result.
func (ac *AeronCluster) offerSnapshotRequest(
	pub IngressPublication,
	correlationID int64,
) int64 {
	actingVersion := uint16(clusterwire.AdminRequestSbeSchemaVersion)
	fields := codecs.AdminRequestFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
		CorrelationID:    correlationID,
		RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
		Payload:          nil,
	}
	return ac.offerTrackedControlFrame(pub, func(scratch []byte) (end int, ok bool) {
		return codecs.EncodeAdminRequest(scratch, 0, actingVersion, fields)
	})
}

// SendSnapshotRequest offers one AdminRequest (template 26, SNAPSHOT, empty
// payload) on the leader ingress publication. Java equivalent:
// AeronCluster.sendAdminRequestToTakeASnapshot; Heron offers once on
// controlScratch. Requires elevated cluster privileges; the consensus module may
// respond with AdminResponseCode_UNAUTHORISED_ACCESS.
//
// correlationID is caller-supplied and echoed on egress AdminResponse. Use
// Aeron().NextCorrelationID() or an application sequence; the client does not
// allocate or track correlation ids for admin.
//
// Returns publication stream position on success (>0) or Aeron publication status
// codes on failure (BackPressured, NotConnected, Closed, etc.); no Go error on the
// hot path. Session-guard codes (Closed, blocked NotConnected) return without
// tracking. When CONNECTED with a disconnected ingress publication, returns
// NotConnected and tracks via leaderIngressPublication, same as Offer and
// SendKeepAlive. Single offer only; caller must invoke Aeron().DoWork() between
// spins and re-invoke on BackPressured or AdminAction (invoker-only transport; no
// internal retry).
//
// During leadership transition admin may fail; keep calling PollEgress with DoWork
// between spins. Only SessionStateConnected with connected leader ingress proceeds.
//
// For paced retry, see IngressOfferUntilPositive (ingress_retry.go).
func (ac *AeronCluster) SendSnapshotRequest(correlationID int64) int64 {
	pub, code := ac.leaderIngressForAppOffer()
	if pub == nil {
		return code
	}
	return ac.offerSnapshotRequest(pub, correlationID)
}

func (ac *AeronCluster) ingressHeaderFields() codecs.IngressHeaderFields {
	return codecs.IngressHeaderFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
		Timestamp:        ac.nanoClock(),
	}
}

func (ac *AeronCluster) sendSessionCloseBestEffort() {
	// Close sets closed before this call; trackIngressPublicationResult is a no-op
	// when ac.closed, so offerTrackedControlFrame matches prior best-effort close.
	actingVersion := uint16(clusterwire.SessionCloseRequestSbeSchemaVersion)
	fields := codecs.SessionCloseRequestFields{
		LeadershipTermID: ac.leadershipTermID,
		ClusterSessionID: ac.clusterSessionID,
	}
	_ = ac.offerTrackedControlFrame(ac.ingressPublication, func(scratch []byte) (end int, ok bool) {
		return codecs.EncodeSessionCloseRequest(scratch, 0, actingVersion, fields)
	})
}
