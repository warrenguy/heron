package sim

import (
	"math/rand/v2"

	"github.com/warrenguy/heron/cluster/election"
)

// DeliverPayload is the scheduled inbound message for EventDeliver.
type DeliverPayload struct {
	From int32
	To   int32
	Msg  election.SentMessage

	// SentIndex is the message's position in the source publisher's Sent log,
	// stamped at enqueue so a delivery identifies the exact published message.
	SentIndex int

	// PublishGen is the source node's publish generation at enqueue time.
	//
	// Distinguishes post-reattach Sent indices from pre-crash publishes.
	PublishGen int
}

// deliveredKey identifies a delivered message by source, that source's
// publisher Sent index, and the source publish generation at enqueue.
//
// Lets inject wait for its own publish rather than any inbound traffic to the
// target. Generation invalidates pre-crash keys after attachLiveElection.
type deliveredKey struct {
	from  int32
	index int
	gen   int
}

// NetworkPolicy controls drop and latency for future nemesis fault injection.
type NetworkPolicy interface {
	Allow(from, to int32, msg election.SentMessage, rng *rand.Rand) (drop bool, latencyNs int64)
}

// AllowAllPolicy is the default network policy (no drops, zero latency).
type AllowAllPolicy struct{}

func (AllowAllPolicy) Allow(int32, int32, election.SentMessage, *rand.Rand) (bool, int64) {
	return false, 0
}

// focusOnlyPolicy drops every message not addressed to the focus node and
// defers focus-bound traffic to the inner policy.
type focusOnlyPolicy struct {
	focus int32
	inner NetworkPolicy
}

func (p focusOnlyPolicy) Allow(
	from, to int32,
	msg election.SentMessage,
	rng *rand.Rand,
) (bool, int64) {
	if to != p.focus {
		return true, 0
	}
	return p.inner.Allow(from, to, msg, rng)
}

// Network routes published wire messages between nodes.
type Network struct {
	nodes     map[int32]*Node
	scheduler *Scheduler
	sim       *ElectionSim
	policy    NetworkPolicy
	latencyNs int64
}

// NewNetwork wires a central message router to the orchestrator.
func NewNetwork(
	sim *ElectionSim,
	scheduler *Scheduler,
	policy NetworkPolicy,
	latencyNs int64,
) *Network {
	return &Network{
		nodes:     make(map[int32]*Node),
		scheduler: scheduler,
		sim:       sim,
		policy:    normalizeNetworkPolicy(policy),
		latencyNs: latencyNs,
	}
}

// Register adds a node endpoint for delivery.
func (n *Network) Register(node *Node) {
	n.nodes[node.id] = node
}

// Enqueue schedules delivery of a published message to its target member.
func (n *Network) Enqueue(from int32, msg election.SentMessage) {
	to := msg.Dst.MemberID
	entry := TimelineEntry{To: to, TemplateID: msg.TemplateID}
	drop, extraLatency := n.policy.Allow(from, to, msg, n.sim.rng)
	if drop {
		n.sim.appendTimelineNow("drop", from, entry)
		return
	}
	// Clamp: negative total schedules EventDeliver in the past and step()
	// SetNow regresses master time / node clocks / timeline.
	latency := max(n.latencyNs+extraLatency, 0)
	payload := &DeliverPayload{
		From:       from,
		To:         to,
		Msg:        msg,
		SentIndex:  n.sentIndex(from),
		PublishGen: n.publishGen(from),
	}
	n.sim.scheduleEvent(n.scheduler.Now()+latency, EventDeliver, to, payload)
	n.sim.appendTimelineNow("publish", from, entry)
}

// sentIndex returns the just-published message's position in the source
// publisher's Sent log.
//
// Publish appends before invoking the SentHandler that calls Enqueue, so the
// message is the final Sent entry.
func (n *Network) sentIndex(from int32) int {
	src, ok := n.nodes[from]
	if !ok {
		return -1
	}
	return len(src.publisher.Sent) - 1
}

// publishGen returns the source node's current publish generation.
func (n *Network) publishGen(from int32) int {
	src, ok := n.nodes[from]
	if !ok {
		return 0
	}
	return src.publishGen
}

// Deliver hands the recorded wire bytes to the destination Election's
// HandleConsensusMessage, exercising the production decode and dispatch path.
//
// Unknown or crashed (election-nil) destinations no-op without panic.
func (n *Network) Deliver(p DeliverPayload) bool {
	dst, ok := n.nodes[p.To]
	if !ok || dst.election == nil {
		return false
	}

	// Second election entry point besides Node.Tick; sync the node clock to
	// dispatch time so handlers read current time, not the last tick's.
	dst.clock.Set(n.scheduler.Now())
	delivered := dst.election.HandleConsensusMessage(p.Msg.Wire, 0)
	if delivered {
		dst.receivedMsgKeys = append(dst.receivedMsgKeys, deliveredKey{
			from:  p.From,
			index: p.SentIndex,
			gen:   p.PublishGen,
		})
	}
	return delivered
}

// setPolicy replaces the active network policy (outer, possibly focus-wrapped).
func (n *Network) setPolicy(policy NetworkPolicy) {
	n.policy = normalizeNetworkPolicy(policy)
}

// PolicyChain applies policies in order: first drop wins; latencies sum.
//
// Single-threaded sim: no mutex.
type PolicyChain []NetworkPolicy

func (c PolicyChain) Allow(
	from, to int32,
	msg election.SentMessage,
	rng *rand.Rand,
) (bool, int64) {
	var latency int64
	for _, p := range c {
		if p == nil {
			continue
		}
		drop, lat := p.Allow(from, to, msg, rng)
		latency += lat
		if drop {
			return true, latency
		}
	}
	return false, latency
}

// PartitionPolicy drops messages on cut directed edges.
//
// Single-threaded sim: no mutex.
type PartitionPolicy struct {
	cut map[[2]int32]struct{}
}

// NewPartitionPolicy returns an empty partition policy.
func NewPartitionPolicy() *PartitionPolicy {
	return &PartitionPolicy{cut: make(map[[2]int32]struct{})}
}

// Cut drops messages from -> to.
func (p *PartitionPolicy) Cut(from, to int32) {
	p.cut[[2]int32{from, to}] = struct{}{}
}

// Heal restores messages from -> to.
func (p *PartitionPolicy) Heal(from, to int32) {
	delete(p.cut, [2]int32{from, to})
}

// CutBidirectional cuts both directions between a and b.
func (p *PartitionPolicy) CutBidirectional(a, b int32) {
	p.Cut(a, b)
	p.Cut(b, a)
}

// HealBidirectional heals both directions between a and b.
func (p *PartitionPolicy) HealBidirectional(a, b int32) {
	p.Heal(a, b)
	p.Heal(b, a)
}

// IsCut reports whether from -> to is cut.
func (p *PartitionPolicy) IsCut(from, to int32) bool {
	_, ok := p.cut[[2]int32{from, to}]
	return ok
}

// IsBidirectionallyConnected reports neither directed edge is cut.
func (p *PartitionPolicy) IsBidirectionallyConnected(a, b int32) bool {
	return !p.IsCut(a, b) && !p.IsCut(b, a)
}

// Allow implements NetworkPolicy.
func (p *PartitionPolicy) Allow(
	from, to int32,
	_ election.SentMessage,
	_ *rand.Rand,
) (bool, int64) {
	return p.IsCut(from, to), 0
}

// LossyPolicy drops messages with DropProb and adds random latency in
// [LatencyMin, LatencyMax].
//
// No message duplication.
type LossyPolicy struct {
	DropProb   float64
	LatencyMin int64
	LatencyMax int64
}

// Allow implements NetworkPolicy.
func (p *LossyPolicy) Allow(
	_ int32,
	_ int32,
	_ election.SentMessage,
	rng *rand.Rand,
) (bool, int64) {
	if p.DropProb > 0 && rng != nil && rng.Float64() < p.DropProb {
		return true, 0
	}
	if rng == nil || p.LatencyMax <= p.LatencyMin {
		return false, p.LatencyMin
	}
	span := p.LatencyMax - p.LatencyMin
	return false, p.LatencyMin + rng.Int64N(span+1)
}
