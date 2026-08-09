package consensus

import "github.com/warrenguy/heron/aeron"

// defaultConsensusIngressFragmentLimit is the max fragments drained per
// [Agent.pollConsensusIngress] poll (matches typical Aeron adapter limits).
const defaultConsensusIngressFragmentLimit = 10

// ConsensusIngress polls one duty-cycle batch of unfragmented SBE frames.
//
// Used for the consensus control stream. Client ingress uses [ClientIngress].
//
// Implementations: [QueueConsensusIngress], [SubscriptionConsensusIngress].
// The agent must not hold election types on this interface.
//
// Poll returns the work count (fragments handled). onFragment runs once per
// message frame payload (buffer and offset at MessageHeader).
//
// Concurrency: invoker-only. Called from [Agent.DoWork].
type ConsensusIngress interface {
	Poll(fragmentLimit int, onFragment func(buf []byte, offset int)) int
}

// QueueConsensusIngress is an in-memory [ConsensusIngress] for unit inject.
// Each element is a full SBE message including the MessageHeader.
//
// Drains through a [QueueClientIngress]. Each delivery returns
// ControlledContinue because ConsensusIngress has no poll action.
//
// Concurrency: invoker-only. Not safe for concurrent Poll or Enqueue.
type QueueConsensusIngress struct {
	queue QueueClientIngress
}

// Enqueue appends one full message buffer for a later [QueueConsensusIngress.Poll].
func (q *QueueConsensusIngress) Enqueue(msg []byte) {
	if q == nil {
		return
	}
	q.queue.Enqueue(msg)
}

// Poll drains up to fragmentLimit queued messages, calling onFragment for each
// at offset 0. Returns the number of fragments delivered.
func (q *QueueConsensusIngress) Poll(fragmentLimit int, onFragment func([]byte, int)) int {
	if q == nil || onFragment == nil {
		return 0
	}
	return q.queue.Poll(fragmentLimit, func(buf []byte, offset int) aeron.ControlledAction {
		onFragment(buf, offset)
		return aeron.ControlledContinue
	})
}

var _ ConsensusIngress = (*QueueConsensusIngress)(nil)
