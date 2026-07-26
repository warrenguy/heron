package consensus

// defaultConsensusIngressFragmentLimit is the max fragments drained per
// [Agent.pollConsensusIngress] poll (matches typical Aeron adapter limits).
const defaultConsensusIngressFragmentLimit = 10

// ConsensusIngress polls one duty-cycle batch of consensus-channel fragments.
//
// Implementations: [QueueConsensusIngress] (unit inject) and
// [SubscriptionConsensusIngress] (Aeron Subscription adapter). Agent never
// holds election types.
//
// Poll returns work count (fragments handled). onFragment is called per message
// frame payload (buffer + offset at MessageHeader).
//
// Concurrency: invoker-only from [Agent.DoWork].
type ConsensusIngress interface {
	Poll(fragmentLimit int, onFragment func(buf []byte, offset int)) int
}

// QueueConsensusIngress is an in-memory [ConsensusIngress] for unit inject.
// Each element is a full SBE message including the MessageHeader.
//
// Concurrency: invoker-only; not safe for concurrent Poll/Enqueue.
type QueueConsensusIngress struct {
	msgs [][]byte
}

// Enqueue appends one full message buffer for a later [QueueConsensusIngress.Poll].
func (q *QueueConsensusIngress) Enqueue(msg []byte) {
	if q == nil {
		return
	}
	q.msgs = append(q.msgs, msg)
}

// Poll drains up to fragmentLimit queued messages, calling onFragment for each
// at offset 0. Returns the number of fragments delivered.
func (q *QueueConsensusIngress) Poll(fragmentLimit int, onFragment func([]byte, int)) int {
	if q == nil || onFragment == nil || fragmentLimit <= 0 {
		return 0
	}
	n := 0
	for n < fragmentLimit && len(q.msgs) > 0 {
		m := q.msgs[0]
		q.msgs = q.msgs[1:]
		onFragment(m, 0)
		n++
	}
	return n
}

var _ ConsensusIngress = (*QueueConsensusIngress)(nil)
