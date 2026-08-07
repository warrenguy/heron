package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// FragmentPoller delivers committed fragments from a subscription.
//
// [*aeron.Subscription] satisfies this interface as-built. The compile-time
// assertion below fails the build if either side moves.
//
// The seam exists so fragment translation is testable without a media driver: a
// zero-value [*aeron.Subscription] is safe to Poll but always reports zero
// fragments, so it can prove the guards and nothing else.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// [SubscriptionConsensusIngress] calls it only from the invoker goroutine.
type FragmentPoller interface {
	Poll(handler aeron.FragmentHandler, fragmentLimit int) int
}

var _ FragmentPoller = (*aeron.Subscription)(nil)

// SubscriptionConsensusIngress polls unfragmented SBE frames off a subscription
// and delivers each one to the agent duty cycle.
//
// Used for the consensus control stream and the client ingress stream.
//
// Behaviour: one Poll is one poller Poll. Each unfragmented DATA frame reaches
// onFragment as a view of that frame's payload span, at offset 0. Fragmented
// frames are skipped without reassembly. Every consensus message is sent
// through a claim that refuses to exceed the publication payload length, so no
// in-tree publisher can produce a fragmented frame. Client ingress uses the
// same unfragmented-only rule.
//
// Buffer ownership: the slice passed to onFragment is a zero-copy view of the
// driver's term buffer. It is valid only for the duration of the callback.
// Callers must decode inside the callback and must not retain, mutate, or
// append to it. This differs from [QueueConsensusIngress], which passes a
// buffer the caller enqueued and still owns.
//
// Concurrency: invoker-only. Poll must run on the same goroutine as
// [Agent.DoWork] and the transport client duty cycle, matching the
// Subscription contract. Not safe for concurrent use.
//
// Lifetime: the caller owns the subscription and closes it. This type closes
// nothing.
//
// Do not copy. The fragment handler binds to the construction receiver. A
// value copy has its own onFragment field, so Poll on the copy sets one field
// and handleBytes reads the other. Pass [*SubscriptionConsensusIngress] only.
type SubscriptionConsensusIngress struct {
	poller  FragmentPoller
	handler aeron.FragmentHandler

	// onFragment is set for the duration of one Poll and cleared on return.
	onFragment func(buf []byte, offset int)
}

var _ ConsensusIngress = (*SubscriptionConsensusIngress)(nil)

// NewSubscriptionConsensusIngress returns an ingress over poller.
//
// The fragment handler is bound once here, not per Poll, so a duty cycle
// builds no closure. A nil poller is allowed and makes every Poll report zero.
func NewSubscriptionConsensusIngress(poller FragmentPoller) *SubscriptionConsensusIngress {
	s := &SubscriptionConsensusIngress{poller: poller}
	s.handler = aeron.AdaptFragmentHandlerBytes(s.handleBytes)
	return s
}

// Poll drains up to fragmentLimit unfragmented SBE frames and calls onFragment
// for each.
//
// Returns the fragment count the poller reported. The count includes skipped
// fragmented and out-of-bounds frames as work: the stream position advanced
// past those frames either way.
//
// Returns 0 without polling when the receiver is nil, the poller is nil,
// onFragment is nil, or fragmentLimit is not positive. Shared early returns
// match [QueueConsensusIngress]. A nil poller is the extra seam check and also
// returns 0 without polling.
//
// Concurrency: invoker-only. Reentrant Poll from inside onFragment is
// unsupported. Reentrant Poll would overwrite the in-flight handler.
func (s *SubscriptionConsensusIngress) Poll(fragmentLimit int, onFragment func([]byte, int)) int {
	if s == nil || s.poller == nil || onFragment == nil || fragmentLimit <= 0 {
		return 0
	}
	s.onFragment = onFragment

	// Deferred, not inline: the fragment-handler panic recovery in Image.Poll is
	// conditional on a subscriber error handler being configured, and a fake
	// poller has none at all. A skipped clear would retain the caller's closure.
	// Named method avoids a per-Poll heap-allocated closure.
	defer s.clearOnFragment()

	return s.poller.Poll(s.handler, fragmentLimit)
}

func (s *SubscriptionConsensusIngress) clearOnFragment() {
	s.onFragment = nil
}

// handleBytes translates one adapted fragment payload into an onFragment call.
//
// Unfragmented only. Fragmented frames (missing UNFRAGMENTED) are skipped
// without reassembly. payload is the zero-copy span
// [aeron.AdaptFragmentHandlerBytes] already bounds-checked; nil marks an
// invalid or empty span and is skipped, so a decoder cannot read into the next
// frame. Delivery is at offset 0.
func (s *SubscriptionConsensusIngress) handleBytes(payload []byte, header *aeron.Header) {
	if header.Flags()&logbuffer.Unfragmented != logbuffer.Unfragmented {
		return
	}
	if payload == nil {
		return
	}
	s.onFragment(payload, 0)
}
