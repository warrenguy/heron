package cluster

import (
	"errors"

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

// ServiceControlPublicationDeps are the collaborators
// [OpenServiceControlPublication] needs. All three are required.
type ServiceControlPublicationDeps struct {
	// Adder registers the concurrent service control publication. Production
	// passes the connected [*aeron.Client].
	Adder PublicationAdder

	// Topology supplies the service control channel and the service stream id.
	// [OpenServiceControlPublication] does not retain Topology.
	Topology *Topology

	// Agent receives the ServiceProxy. Pass the one agent the election and the
	// host adapter also hold.
	Agent *consensus.Agent
}

// ServiceControlPublication owns the member's service control publication
// (service stream, default 104) and the [consensus.ServiceProxy] wired into the
// agent.
//
// Open with [OpenServiceControlPublication]. That call registers the
// publication, builds the proxy, and wires it into the agent before returning.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [consensus.Agent.DoWork] and the transport client duty cycle.
//
// Lifetime: this value owns the publication and closes it. Close before
// closing the transport client when the process owns the client. Closing the
// client first is safe but leaves the agent briefly holding a closed
// publication, which reports Closed on offer.
//
// Do not copy. Fields are reference types, so a value copy is a silent alias:
// two values each look like an owner and each look responsible for Close.
// Every method also takes a pointer receiver, so a copy taken to avoid the
// pointer bypasses the nil-receiver checks a [*ServiceControlPublication]
// caller relies on. Pass the [*ServiceControlPublication].
type ServiceControlPublication struct {
	agent       *consensus.Agent
	publication *aeron.Publication
}

// OpenServiceControlPublication registers the publication on
// [Topology.ServiceStreamID] over [Topology.ServiceControlChannel], builds a
// [consensus.ServiceProxy] over it, and wires the proxy into deps.Agent.
//
// Returns an error when:
//   - any dependency is nil
//   - the add fails
//
// On error nothing is opened. The agent is untouched. The returned value is
// nil.
func OpenServiceControlPublication(
	deps ServiceControlPublicationDeps,
) (*ServiceControlPublication, error) {
	if err := requirePublicationDeps(deps.Adder, deps.Topology, deps.Agent); err != nil {
		return nil, err
	}

	pub, err := addPublicationChecked(
		deps.Adder,
		deps.Topology.ServiceStreamID(),
		deps.Topology.ServiceControlChannel(),
		"service control publication",
	)
	if err != nil {
		return nil, err
	}

	proxy := consensus.NewServiceProxy()
	proxy.SetPublication(pub)
	deps.Agent.SetServiceProxy(proxy)

	return &ServiceControlPublication{
		agent:       deps.Agent,
		publication: pub,
	}, nil
}

// Publication returns the bound concurrent publication.
//
// Nil after Close and on a nil receiver. Use for read-only diagnostics such as
// IsConnected. Do not Offer on the publication directly. The agent owns the
// publish path.
//
// Concurrency: invoker-only. See [ServiceControlPublication].
func (p *ServiceControlPublication) Publication() *aeron.Publication {
	if p == nil {
		return nil
	}
	return p.publication
}

// Close clears the agent service proxy, then closes the publication.
//
// Idempotent. After Close the value is inert.
// [ServiceControlPublication.Publication] returns nil. Further Close calls
// touch neither the agent nor any later rebound proxy. Safe on a nil receiver,
// a zero value, and after the transport client has already closed the
// publication.
//
// Returns the publication close error, or nil.
func (p *ServiceControlPublication) Close() error {
	if p == nil || p.publication == nil {
		return nil
	}
	p.agent.SetServiceProxy(nil)
	err := p.publication.Close()
	p.publication = nil
	return err
}

// requirePublicationDeps checks the collaborators every agent-bound concurrent
// publication owner needs.
func requirePublicationDeps(
	adder PublicationAdder,
	topology *Topology,
	agent *consensus.Agent,
) error {
	switch {
	case adder == nil:
		return errors.New("cluster: publication adder is required")
	case topology == nil:
		return errors.New("cluster: topology is required")
	case agent == nil:
		return errors.New("cluster: consensus agent is required")
	}
	return nil
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
