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
	if policy == nil {
		policy = AllowAllPolicy{}
	}
	return &Network{
		nodes:     make(map[int32]*Node),
		scheduler: scheduler,
		sim:       sim,
		policy:    policy,
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
	latency := n.latencyNs + extraLatency
	payload := &DeliverPayload{
		From: from,
		To:   to,
		Msg:  msg,
	}
	n.sim.scheduleEvent(n.scheduler.Now()+latency, EventDeliver, to, payload)
	n.sim.appendTimelineNow("publish", from, entry)
}

// Deliver hands the recorded wire bytes to the destination Election's
// HandleConsensusMessage, exercising the production decode and dispatch path.
func (n *Network) Deliver(p DeliverPayload) bool {
	dst, ok := n.nodes[p.To]
	if !ok {
		return false
	}

	// Second election entry point besides Node.Tick; sync the node clock to
	// dispatch time so handlers read current time, not the last tick's.
	dst.clock.Set(n.scheduler.Now())
	delivered := dst.Election().HandleConsensusMessage(p.Msg.Wire, 0)
	if delivered {
		dst.receivedFromOrder = append(dst.receivedFromOrder, p.From)
	}
	return delivered
}
