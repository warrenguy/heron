package cluster

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// SubscriptionAdder registers subscriptions on a media driver.
//
// [*aeron.Client] satisfies this interface as-built. The argument order is stream
// id first, matching [aeron.Client.AddSubscription]; the compile-time assertion
// below fails the build if either side moves.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// [ConsensusSubscription] calls it only from the goroutine that opens the module.
type SubscriptionAdder interface {
	AddSubscription(
		streamID int32,
		channel string,
	) (*aeron.Subscription, error)
}

var _ SubscriptionAdder = (*aeron.Client)(nil)

// ConsensusSubscriptionDeps are the collaborators [OpenConsensusSubscription]
// needs. All three are required.
//
// Named for [election.ElectionDeps], which carries injected collaborators the
// same way.
type ConsensusSubscriptionDeps struct {
	// Adder registers the local consensus subscription. Production passes the
	// connected [*aeron.Client].
	Adder SubscriptionAdder

	// Topology supplies the local consensus bind channel and the consensus
	// stream id. [OpenConsensusSubscription] does not retain it.
	Topology *Topology

	// Agent receives the ingress. Pass the one agent the election and the host
	// adapter also hold.
	Agent *consensus.Agent
}

// ConsensusSubscription owns the member's inbound consensus control stream.
//
// Open with [OpenConsensusSubscription], which registers the subscription and
// wires the ingress into the agent before returning. There is no unbound state:
// an agent with a nil ingress silently receives nothing, which in a multi-member
// cluster is indistinguishable from every peer being down.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [consensus.Agent.DoWork] and the transport client duty cycle.
//
// Lifetime: this value owns the subscription and closes it. Close before closing
// the transport client when the process owns the client. Closing the client
// first is safe but leaves the agent briefly polling a closed subscription,
// which reports zero.
//
// Do not copy. Both fields are reference types, so a value copy is a silent
// alias: two values that each look like an owner and each look responsible for
// Close. Every method also takes a pointer receiver, so a copy taken to "avoid
// the pointer" bypasses the nil-receiver checks a [*ConsensusSubscription]
// caller relies on. Pass the [*ConsensusSubscription].
type ConsensusSubscription struct {
	agent        *consensus.Agent
	subscription *aeron.Subscription
}

var errAdderReturnedNoSubscription = errors.New("adder returned no subscription")

// requireSubscriptionDeps checks the collaborators every agent-bound
// subscription owner needs. [OpenConsensusSubscription] and
// [OpenClientIngressSubscription] share this check so both report the same
// errors.
func requireSubscriptionDeps(
	adder SubscriptionAdder,
	topology *Topology,
	agent *consensus.Agent,
) error {
	switch {
	case adder == nil:
		return errors.New("cluster: subscription adder is required")
	case topology == nil:
		return errors.New("cluster: topology is required")
	case agent == nil:
		return errors.New("cluster: consensus agent is required")
	}
	return nil
}

// addSubscriptionChecked adds one subscription.
//
// what names the stream in the wrapped error. Retry budget and nil-nil handling
// live in [addWithRetry].
func addSubscriptionChecked(
	adder SubscriptionAdder,
	streamID int32,
	channel string,
	what string,
) (*aeron.Subscription, error) {
	return addWithRetry(
		adder.AddSubscription,
		streamID,
		channel,
		what,
		errAdderReturnedNoSubscription,
	)
}

// OpenConsensusSubscription registers the local consensus subscription and wires
// a [consensus.SubscriptionConsensusIngress] into deps.Agent.
//
// The channel is [Topology.ConsensusSubscriptionChannel], already carrying this
// member's consensus endpoint, on [Topology.ConsensusStreamID].
//
// Open does not wait for a peer publication to appear. A cold cluster has no
// peers publishing yet, and the election re-sends on its own duty cycle.
//
// Errors when any dependency is nil and when the add fails. On error nothing is
// opened, the agent is untouched, and the returned value is nil.
func OpenConsensusSubscription(
	deps ConsensusSubscriptionDeps,
) (*ConsensusSubscription, error) {
	if err := requireSubscriptionDeps(deps.Adder, deps.Topology, deps.Agent); err != nil {
		return nil, err
	}

	sub, err := addSubscriptionChecked(
		deps.Adder,
		deps.Topology.ConsensusStreamID(),
		deps.Topology.ConsensusSubscriptionChannel(),
		"consensus subscription",
	)
	if err != nil {
		return nil, err
	}

	deps.Agent.SetIngress(consensus.NewSubscriptionConsensusIngress(sub))

	return &ConsensusSubscription{
		agent:        deps.Agent,
		subscription: sub,
	}, nil
}

// Subscription returns the bound subscription.
//
// Nil after Close and on a nil receiver. For read-only diagnostics such as
// IsConnected and ImageCount; do not Poll it directly, the agent owns polling.
//
// Concurrency: invoker-only. See [ConsensusSubscription].
func (c *ConsensusSubscription) Subscription() *aeron.Subscription {
	if c == nil {
		return nil
	}
	return c.subscription
}

// Close clears the agent ingress, then closes the subscription.
//
// Idempotent. After Close the value is inert: [ConsensusSubscription.Subscription]
// returns nil and further Close calls touch neither the agent nor any later
// rebound ingress. Safe on a nil receiver, a zero value, and after the transport
// client has already closed, which closes subscriptions itself.
//
// Returns the subscription close error, or nil.
func (c *ConsensusSubscription) Close() error {
	if c == nil || c.subscription == nil {
		return nil
	}
	c.agent.SetIngress(nil)
	err := c.subscription.Close()
	c.subscription = nil
	return err
}
