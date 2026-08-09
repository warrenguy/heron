package cluster

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// ClientIngressSubscriptionDeps are the collaborators
// [OpenClientIngressSubscription] needs. All three are required.
type ClientIngressSubscriptionDeps struct {
	// Adder registers the local client ingress subscription. Production passes
	// the connected [*aeron.Client].
	Adder SubscriptionAdder

	// Topology supplies the local client ingress bind channel and the ingress
	// stream id. [OpenClientIngressSubscription] does not retain it.
	Topology *Topology

	// Agent receives the client ingress. Pass the one agent the election and
	// the host adapter also hold.
	Agent *consensus.Agent
}

// ClientIngressSubscription owns the member's inbound client ingress stream.
//
// Open with [OpenClientIngressSubscription]. That call registers the
// subscription and wires the ingress into the agent before returning.
//
// Behaviour: every member opens the subscription. Only a steady-state leader
// accepts connects, messages, keepalives, and closes. The agent dispatch owns
// that branch. The adapter reassembles fragmented ingress frames before
// dispatch.
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
// pointer bypasses the nil-receiver checks a [*ClientIngressSubscription]
// caller relies on. Pass the [*ClientIngressSubscription].
type ClientIngressSubscription struct {
	agent        *consensus.Agent
	subscription *aeron.Subscription
	// ingress is the concrete reassembling adapter wired into the agent. Close
	// uses this field to Clear assembler builders before unbind.
	ingress *consensus.SubscriptionClientIngress
}

// OpenClientIngressSubscription registers the local client ingress subscription
// on [Topology.IngressStreamID] and wires a
// [consensus.SubscriptionClientIngress] into deps.Agent as the client ingress.
//
// The channel is [Topology.IngressSubscriptionChannel]. That channel already
// carries this member's ingress endpoint.
//
// Behaviour: every member opens the subscription. Only a steady-state leader
// accepts connects, messages, keepalives, and closes. The agent dispatch owns
// that branch. The adapter reassembles fragmented ingress frames before
// dispatch. An unavailable image frees that session's reassembly builder.
//
// Returns an error when:
//   - any dependency is nil
//   - the add fails
//
// On error nothing is opened. The agent is untouched. The returned value is
// nil.
func OpenClientIngressSubscription(
	deps ClientIngressSubscriptionDeps,
) (*ClientIngressSubscription, error) {
	if err := requireSubscriptionDeps(deps.Adder, deps.Topology, deps.Agent); err != nil {
		return nil, err
	}

	sub, err := addSubscriptionChecked(
		deps.Adder,
		deps.Topology.IngressStreamID(),
		deps.Topology.IngressSubscriptionChannel(),
		"client ingress subscription",
	)
	if err != nil {
		return nil, err
	}

	ingress := consensus.NewSubscriptionClientIngress(sub)
	sub.SetUnavailableImageHandler(func(image *aeron.Image) {
		if image != nil {
			ingress.FreeSessionBuffer(image.SessionID())
		}
	})
	deps.Agent.SetClientIngress(ingress)

	return &ClientIngressSubscription{
		agent:        deps.Agent,
		subscription: sub,
		ingress:      ingress,
	}, nil
}

// Subscription returns the bound subscription.
//
// Nil after Close and on a nil receiver. For read-only diagnostics such as
// IsConnected and ImageCount. Do not Poll it directly. The agent owns polling.
//
// Concurrency: invoker-only. See [ClientIngressSubscription].
func (c *ClientIngressSubscription) Subscription() *aeron.Subscription {
	if c == nil {
		return nil
	}
	return c.subscription
}

// Close drops assembler builders, clears the agent client ingress, then closes
// the subscription.
//
// Idempotent. After Close the value is inert.
// [ClientIngressSubscription.Subscription] returns nil. Further Close calls
// touch neither the agent nor any later rebound ingress. Safe on a nil
// receiver, a zero value, and after the transport client has already closed
// the subscription.
//
// Returns the subscription close error, or nil.
func (c *ClientIngressSubscription) Close() error {
	if c == nil || c.subscription == nil {
		return nil
	}
	if c.ingress != nil {
		c.ingress.Clear()
		c.ingress = nil
	}
	c.agent.SetClientIngress(nil)
	err := c.subscription.Close()
	c.subscription = nil
	return err
}
