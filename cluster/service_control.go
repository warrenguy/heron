package cluster

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// ServiceControlSubscriptionDeps are the collaborators
// [OpenServiceControlSubscription] needs. All three are required.
type ServiceControlSubscriptionDeps struct {
	// Adder registers the local service control subscription. Production passes
	// the connected [*aeron.Client].
	Adder SubscriptionAdder

	// Topology supplies the service control channel and the consensus module
	// stream id. [OpenServiceControlSubscription] does not retain Topology.
	Topology *Topology

	// Agent receives the service control ingress. Pass the one agent the
	// election and the host adapter also hold.
	Agent *consensus.Agent
}

// ServiceControlSubscription owns the member's service control subscription
// (consensus module stream, default 105).
//
// Open with [OpenServiceControlSubscription]. That call registers the
// subscription and wires the ingress into the agent before returning.
//
// Behaviour: ServiceAck frames are fixed-block and unfragmented. The adapter
// skips fragmented frames without reassembly.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [consensus.Agent.DoWork] and the transport client duty cycle.
//
// Lifetime: this value owns the subscription and closes it. Close before
// closing the transport client when the process owns the client. Closing the
// client first is safe but leaves the agent briefly polling a closed
// subscription, which reports zero.
//
// Do not copy. Fields are reference types, so a value copy is a silent alias:
// two values each look like an owner and each look responsible for Close.
// Every method also takes a pointer receiver, so a copy taken to avoid the
// pointer bypasses the nil-receiver checks a [*ServiceControlSubscription]
// caller relies on. Pass the [*ServiceControlSubscription].
type ServiceControlSubscription struct {
	agent        *consensus.Agent
	subscription *aeron.Subscription
}

// OpenServiceControlSubscription registers the subscription on
// [Topology.ConsensusModuleStreamID] over [Topology.ServiceControlChannel] and
// wires a [consensus.SubscriptionConsensusIngress] into deps.Agent.
//
// Returns an error when:
//   - any dependency is nil
//   - the add fails
//
// On error nothing is opened. The agent is untouched. The returned value is nil.
func OpenServiceControlSubscription(
	deps ServiceControlSubscriptionDeps,
) (*ServiceControlSubscription, error) {
	if err := requireSubscriptionDeps(deps.Adder, deps.Topology, deps.Agent); err != nil {
		return nil, err
	}

	sub, err := addSubscriptionChecked(
		deps.Adder,
		deps.Topology.ConsensusModuleStreamID(),
		deps.Topology.ServiceControlChannel(),
		"service control subscription",
	)
	if err != nil {
		return nil, err
	}

	deps.Agent.SetServiceIngress(consensus.NewSubscriptionConsensusIngress(sub))

	return &ServiceControlSubscription{
		agent:        deps.Agent,
		subscription: sub,
	}, nil
}

// Subscription returns the bound subscription.
//
// Nil after Close and on a nil receiver. Use for read-only diagnostics such as
// IsConnected and ImageCount. Do not Poll the subscription directly. The agent
// owns polling.
//
// Concurrency: invoker-only. See [ServiceControlSubscription].
func (s *ServiceControlSubscription) Subscription() *aeron.Subscription {
	if s == nil {
		return nil
	}
	return s.subscription
}

// Close clears the agent service ingress, then closes the subscription.
//
// Idempotent. After Close the value is inert.
// [ServiceControlSubscription.Subscription] returns nil. Further Close calls
// touch neither the agent nor any later rebound ingress. Safe on a nil
// receiver, a zero value, and after the transport client has already closed
// the subscription.
//
// Returns the subscription close error, or nil.
func (s *ServiceControlSubscription) Close() error {
	if s == nil || s.subscription == nil {
		return nil
	}
	s.agent.SetServiceIngress(nil)
	err := s.subscription.Close()
	s.subscription = nil
	return err
}
