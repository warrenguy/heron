package consensus

import (
	"github.com/warrenguy/heron/aeron"
)

// ControlledFragmentPoller delivers committed fragments with controlled-action
// semantics.
//
// [*aeron.Subscription] satisfies this interface as-built via ControlledPoll.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// [SubscriptionClientIngress] calls it only from the invoker goroutine.
type ControlledFragmentPoller interface {
	ControlledPoll(handler aeron.ControlledFragmentHandler, fragmentLimit int) int
}

var _ ControlledFragmentPoller = (*aeron.Subscription)(nil)

// SubscriptionClientIngress polls client ingress frames with reassembly and
// controlled-action delivery.
//
// Behaviour: one Poll is one poller ControlledPoll through a
// [aeron.ControlledFragmentAssembler]. Unfragmented and reassembled frames
// reach onFragment as a payload view at offset 0. [aeron.ControlledAbort] from
// onFragment holds the current message for redelivery on a later Poll.
//
// Buffer ownership: the slice passed to onFragment is a zero-copy view of the
// driver's term buffer or the assembler buffer. The slice is valid only for the
// duration of the callback. Callers must decode inside the callback and must
// not retain, mutate, or append to the slice. This differs from
// [QueueClientIngress], which passes a buffer the caller enqueued and still
// owns.
//
// Concurrency: invoker-only. Poll must run on the same goroutine as
// [Agent.DoWork] and the transport client duty cycle. Not safe for concurrent
// use.
//
// Lifetime: the caller owns the subscription and closes it. This type closes
// nothing. Call FreeSessionBuffer on unavailable image for incomplete builders.
// Call Clear when the ingress is unbound.
//
// Do not copy. The assembler and fragment handler bind to the construction
// receiver. Pass [*SubscriptionClientIngress] only.
type SubscriptionClientIngress struct {
	poller    ControlledFragmentPoller
	assembler *aeron.ControlledFragmentAssembler
	// handler is the assembler ControlledFragmentHandler, cached at
	// construction so Poll does not allocate a method value each duty cycle.
	handler aeron.ControlledFragmentHandler

	// onFragment is set for the duration of one Poll and cleared on return.
	onFragment func(buf []byte, offset int) aeron.ControlledAction
}

var _ ClientIngress = (*SubscriptionClientIngress)(nil)

// NewSubscriptionClientIngress returns a controlled reassembling client
// ingress over poller.
//
// The assembler and handler bind once here, not per Poll. A nil poller is
// allowed and makes every Poll report zero.
func NewSubscriptionClientIngress(poller ControlledFragmentPoller) *SubscriptionClientIngress {
	s := &SubscriptionClientIngress{poller: poller}
	s.assembler = aeron.NewControlledFragmentAssembler(
		aeron.AdaptControlledFragmentHandlerBytes(s.handleFragment),
	)
	s.handler = s.assembler.Handler()
	return s
}

// Poll drains up to fragmentLimit wire fragments via ControlledPoll and calls
// onFragment once per whole reassembled message.
//
// fragmentLimit is the poller ControlledPoll budget, not a whole-message count.
// One large reassembled message can consume several fragment units for one
// onFragment call.
//
// Returns the fragment count the poller reported. Assembler middle fragments
// that do not reach the delegate still count when the image advanced past them.
//
// Returns 0 without polling when the receiver is nil, the poller is nil,
// onFragment is nil, or fragmentLimit is not positive.
//
// Concurrency: invoker-only. Reentrant Poll from inside onFragment is
// unsupported.
func (s *SubscriptionClientIngress) Poll(
	fragmentLimit int,
	onFragment func([]byte, int) aeron.ControlledAction,
) int {
	if s == nil || s.poller == nil || onFragment == nil || fragmentLimit <= 0 {
		return 0
	}
	s.onFragment = onFragment

	// Deferred, not inline: Image.ControlledPoll panic recovery is conditional
	// on a subscriber error handler, and a fake poller has none. A skipped
	// clear would retain the caller's closure. Named method avoids a per-Poll
	// heap-allocated closure.
	defer s.clearOnFragment()

	return s.poller.ControlledPoll(s.handler, fragmentLimit)
}

func (s *SubscriptionClientIngress) clearOnFragment() {
	s.onFragment = nil
}

// handleFragment is the assembler delegate. Delivers the payload view at
// offset 0. Drops a nil payload (zero length or out-of-bounds span).
func (s *SubscriptionClientIngress) handleFragment(
	payload []byte,
	_ *aeron.Header,
) aeron.ControlledAction {
	if s.onFragment == nil || payload == nil {
		return aeron.ControlledContinue
	}
	return s.onFragment(payload, 0)
}

// FreeSessionBuffer drops the reassembly builder for sessionID, in progress
// or idle.
//
// Behaviour: returns true when a builder was present and removed. Returns false
// when no builder existed for sessionID. An idle builder keeps its payload
// capacity until this call or Clear.
//
// Call FreeSessionBuffer from the unavailable-image handler for that session.
// Session-id reuse after an image drop must not reassemble against a stale
// builder.
//
// Concurrency: invoker-only. Same goroutine as Poll and [Agent.DoWork].
//
// A nil receiver returns false.
func (s *SubscriptionClientIngress) FreeSessionBuffer(sessionID int32) bool {
	if s == nil || s.assembler == nil {
		return false
	}
	return s.assembler.FreeSessionBuffer(sessionID)
}

// Clear drops every reassembly builder, in progress or idle.
//
// Behaviour: after Clear, FreeSessionBuffer returns false for every session id
// until a new BEGIN arrives.
//
// Concurrency: invoker-only. Same goroutine as Poll and [Agent.DoWork].
//
// A nil receiver is a silent no-op.
func (s *SubscriptionClientIngress) Clear() {
	if s == nil || s.assembler == nil {
		return
	}
	s.assembler.Clear()
}

// QueueClientIngress is an in-memory [ClientIngress] for unit inject.
// Each element is a full SBE message including the MessageHeader.
//
// Behaviour: [aeron.ControlledAbort] keeps the head queued so the same
// message redelivers on a later Poll. Other actions dequeue the head.
//
// Concurrency: invoker-only; not safe for concurrent Poll/Enqueue.
type QueueClientIngress struct {
	msgs [][]byte
}

var _ ClientIngress = (*QueueClientIngress)(nil)

// Enqueue appends one full message buffer for a later [QueueClientIngress.Poll].
func (q *QueueClientIngress) Enqueue(msg []byte) {
	if q == nil {
		return
	}
	q.msgs = append(q.msgs, msg)
}

// Poll drains up to fragmentLimit queued messages, calling onFragment for each
// at offset 0. Returns the number of fragments delivered and dequeued.
//
// [aeron.ControlledAbort] stops the batch and keeps the current head queued.
// Abort does not count toward the returned work count, matching
// [aeron.Image.ControlledPoll].
func (q *QueueClientIngress) Poll(
	fragmentLimit int,
	onFragment func([]byte, int) aeron.ControlledAction,
) int {
	if q == nil || onFragment == nil || fragmentLimit <= 0 {
		return 0
	}
	n := 0
	for n < fragmentLimit && len(q.msgs) > 0 {
		m := q.msgs[0]
		action := onFragment(m, 0)
		if action == aeron.ControlledAbort {
			// Hold head for redelivery. Do not advance or count.
			break
		}
		q.msgs = q.msgs[1:]
		n++
		if action == aeron.ControlledBreak {
			break
		}
	}
	return n
}

// Len returns the number of queued whole messages. Test helper surface.
func (q *QueueClientIngress) Len() int {
	if q == nil {
		return 0
	}
	return len(q.msgs)
}
