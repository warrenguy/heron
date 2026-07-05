package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
)

const (
	// DefaultConnectEgressFragmentLimit limits connect egress polling to one terminal
	// fragment capture per Poll() tick, matching Java EgressPoller BREAK semantics.
	DefaultConnectEgressFragmentLimit = 1
)

// ConnectEgressPoller captures cluster connect egress events for AsyncConnect.
// Unlike EgressAdapter it does not dispatch to EgressHandler; terminal templates
// are stored on the poller until the next Poll reset.
//
// Not safe for concurrent use.
//
// Captured byte slices and EgressImage remain valid until the next Poll that
// begins with a prior pollComplete reset, or until the poller is discarded.
type ConnectEgressPoller struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	assembler     *aeron.FragmentAssembler

	pollComplete             bool
	templateID               int32
	clusterSessionID         int64
	correlationID            int64
	leadershipTermID         int64
	leaderMemberID           int32
	leaderHeartbeatTimeoutNs int64
	eventCode                EventCode
	detail                   []byte
	encodedChallenge         []byte
	egressImage              *aeron.Image
}

// NewConnectEgressPoller constructs a connect egress poller. fragmentLimit <= 0
// normalizes to DefaultConnectEgressFragmentLimit.
func NewConnectEgressPoller(
	subscription *aeron.Subscription,
	fragmentLimit int,
) *ConnectEgressPoller {
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultConnectEgressFragmentLimit
	}
	poller := &ConnectEgressPoller{
		subscription:  subscription,
		fragmentLimit: fragmentLimit,
		correlationID: conductor.NullCorrelationID,
	}
	poller.assembler = aeron.NewFragmentAssembler(func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		handleConnectEgressFragment(poller, buffer, offset, length, header)
	})
	return poller
}

// FragmentLimit reports the per-poll fragment budget passed to Subscription.Poll.
func (poller *ConnectEgressPoller) FragmentLimit() int {
	return poller.fragmentLimit
}

// Poll resets prior capture when pollComplete was set, then polls the subscription.
// Returns 0 when subscription is nil.
func (poller *ConnectEgressPoller) Poll() int {
	if poller.pollComplete {
		poller.resetCapture()
	}
	return pollEgressSubscription(
		poller.subscription,
		poller.assembler.Handler(),
		poller.fragmentLimit,
	)
}

// IsPollComplete reports whether the last poll captured a terminal connect template.
func (poller *ConnectEgressPoller) IsPollComplete() bool {
	return poller.pollComplete
}

// IsChallenged reports whether the last poll captured Challenge (7). Requires
// pollComplete; returns false when an unknown template was polled without capture.
func (poller *ConnectEgressPoller) IsChallenged() bool {
	return poller.pollComplete && poller.templateID == challengeSbeTemplateID
}

// TemplateID returns the captured SBE template id, or 0 when not captured.
func (poller *ConnectEgressPoller) TemplateID() int32 {
	return poller.templateID
}

// ClusterSessionID returns the captured cluster session id, or 0 when not captured.
func (poller *ConnectEgressPoller) ClusterSessionID() int64 {
	return poller.clusterSessionID
}

// CorrelationID returns the captured correlation id, or conductor.NullCorrelationID
// (-1) when not captured.
func (poller *ConnectEgressPoller) CorrelationID() int64 {
	return poller.correlationID
}

// LeadershipTermID returns the captured leadership term id, or 0 when not captured.
func (poller *ConnectEgressPoller) LeadershipTermID() int64 {
	return poller.leadershipTermID
}

// LeaderMemberID returns the captured leader member id, or 0 when not captured.
// Member id 0 is valid on the wire; read only after IsPollComplete with a
// template that sets this field (SessionEvent, NewLeaderEvent).
func (poller *ConnectEgressPoller) LeaderMemberID() int32 {
	return poller.leaderMemberID
}

// LeaderHeartbeatTimeoutNs returns the captured SessionEvent heartbeat timeout.
// Zero means absent when the encoder omitted the field (actingVersion >= 13).
// Pre-v13 clusters decode an absent field as math.MinInt64; ConnectResult
// normalizes absent values to zero. AsyncConnect must not treat 0 as Java
// NULL_VALUE (-1).
func (poller *ConnectEgressPoller) LeaderHeartbeatTimeoutNs() int64 {
	return poller.leaderHeartbeatTimeoutNs
}

// EventCode returns the captured SessionEvent code, or 0 when not captured.
// EventCodeOK is also 0; read only after IsPollComplete with SessionEvent.
func (poller *ConnectEgressPoller) EventCode() EventCode {
	return poller.eventCode
}

// Detail returns captured SessionEvent detail or NewLeader ingressEndpoints bytes.
// Nil when not captured. Safe to retain until the next Poll reset.
func (poller *ConnectEgressPoller) Detail() []byte {
	return poller.detail
}

// EncodedChallenge returns the captured Challenge payload. Nil when not captured
// or wire length is zero; AsyncConnect must treat nil like an empty challenge byte slice.
// Safe to retain until the next Poll reset.
func (poller *ConnectEgressPoller) EncodedChallenge() []byte {
	return poller.encodedChallenge
}

// EgressImage returns the Image from SessionEvent or NewLeaderEvent headers.
// Nil for Challenge, SessionMessageHeader, or when header context is unavailable.
func (poller *ConnectEgressPoller) EgressImage() *aeron.Image {
	return poller.egressImage
}

func (poller *ConnectEgressPoller) resetCapture() {
	poller.pollComplete = false
	poller.templateID = 0
	poller.clusterSessionID = 0
	poller.correlationID = conductor.NullCorrelationID
	poller.leadershipTermID = 0
	poller.leaderMemberID = 0
	poller.leaderHeartbeatTimeoutNs = 0
	poller.eventCode = 0
	poller.detail = nil
	poller.encodedChallenge = nil
	poller.egressImage = nil
}
