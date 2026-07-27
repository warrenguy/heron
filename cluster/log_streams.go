package cluster

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// LogStreamAdder registers both cluster log stream resources.
//
// [*aeron.Client] satisfies this interface as-built.
type LogStreamAdder interface {
	SubscriptionAdder
	ExclusivePublicationAdder
}

var _ LogStreamAdder = (*aeron.Client)(nil)

// LogStreamsDeps are the collaborators [OpenLogStreams] needs. All five are
// required.
type LogStreamsDeps struct {
	// Adder registers the cluster log subscription and publication.
	Adder LogStreamAdder

	// Topology supplies the cluster log channel and the log stream id. With a
	// shared log endpoint the channel is identical on every member.
	// [OpenLogStreams] does not retain it.
	Topology *Topology

	// Agent receives the log publisher, the log adapter, the append source, and
	// the log join source.
	Agent *consensus.Agent

	// LogPublisher receives the exclusive log publication. Construct with
	// [consensus.NewLogPublisher]. Caller-owned; [LogStreams] never closes it.
	LogPublisher *consensus.LogPublisher

	// LogAdapter is wired into the agent with no Image bound. Construct with
	// [consensus.NewLogAdapter]. The Image arrives with the follower log join,
	// which selects it by log session id. Caller-owned.
	LogAdapter *consensus.LogAdapter
}

// LogStreams owns the member's cluster log transport resources.
//
// Open with [OpenLogStreams], which registers the log subscription and the log
// exclusive publication and binds the publication, the adapter, the append
// source, and the log join source into the agent before returning.
//
// Behaviour: the publication must be bound before any election work runs.
// [consensus.Agent.AddLogPublication] reports the null session id until it is, and the
// transition that reads that id is the same one that publishes the new
// leadership term.
//
// The subscription opens before the publication. On a shared multicast group a
// member's own subscription is a receiver for its own publication, so opening it
// first removes the window where the publication has no receiver at all. Open
// still does not wait for connect.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [consensus.Agent.DoWork] and the transport client duty cycle.
//
// Lifetime: this value owns the two transport handles and closes them. It does
// not own the publisher or the adapter and never closes them. Close before
// closing the transport client when the process owns the client.
//
// Do not copy. Every field is a reference type; a value copy is a silent alias
// with two apparent owners.
type LogStreams struct {
	agent        *consensus.Agent
	logPublisher *consensus.LogPublisher
	publication  *aeron.ExclusivePublication
	subscription *aeron.Subscription
}

// OpenLogStreams registers the cluster log subscription and publication and
// binds the log path into deps.Agent.
//
// Both use [Topology.LogChannel] and [Topology.LogStreamID]. The subscription is
// added first, then the publication; both adds complete before anything is
// bound, so a failed add leaves the agent untouched.
//
// Binding is [consensus.Agent.SetLogPublisher], [consensus.Agent.SetLogAdapter],
// [consensus.Agent.SetAppendSource] with a [consensus.LogAppendPosition] over
// the agent's wired publisher and adapter, and [consensus.Agent.SetLogJoinSource]
// with a [consensus.SubscriptionLogJoinSource] over the log subscription. No
// Image is bound.
//
// Errors when any dependency is nil and when either add fails. On error every
// handle added so far is closed, the agent is untouched, and the returned value
// is nil.
func OpenLogStreams(
	deps LogStreamsDeps,
) (*LogStreams, error) {
	if deps.Adder == nil {
		return nil, errors.New("cluster: log stream adder is required")
	}
	if deps.Topology == nil {
		return nil, errors.New("cluster: topology is required")
	}
	if deps.Agent == nil {
		return nil, errors.New("cluster: consensus agent is required")
	}
	if deps.LogPublisher == nil {
		return nil, errors.New("cluster: log publisher is required")
	}
	if deps.LogAdapter == nil {
		return nil, errors.New("cluster: log adapter is required")
	}

	channel := deps.Topology.LogChannel()
	streamID := deps.Topology.LogStreamID()

	sub, err := addSubscriptionChecked(
		deps.Adder,
		streamID,
		channel,
		"cluster log subscription",
	)
	if err != nil {
		return nil, err
	}

	pub, err := addExclusivePublicationChecked(
		deps.Adder,
		streamID,
		channel,
		"cluster log publication",
	)
	if err != nil {
		_ = sub.Close()
		return nil, err
	}

	deps.LogPublisher.SetPublication(pub)
	deps.Agent.SetLogPublisher(deps.LogPublisher)
	deps.Agent.SetLogAdapter(deps.LogAdapter)
	deps.Agent.SetAppendSource(consensus.NewLogAppendPosition(deps.Agent))
	deps.Agent.SetLogJoinSource(consensus.NewSubscriptionLogJoinSource(sub))

	return &LogStreams{
		agent:        deps.Agent,
		logPublisher: deps.LogPublisher,
		publication:  pub,
		subscription: sub,
	}, nil
}

// Publication returns the cluster log exclusive publication.
//
// Nil after Close and on a nil receiver.
//
// Concurrency: invoker-only. See [LogStreams].
func (l *LogStreams) Publication() *aeron.ExclusivePublication {
	if l == nil {
		return nil
	}
	return l.publication
}

// Subscription returns the cluster log subscription.
//
// Nil after Close and on a nil receiver.
//
// The subscription carries one image per member that has published on the shared
// log channel, in no defined order, and that includes this member's own
// publication. Select by log session id; never index.
//
// Concurrency: invoker-only. See [LogStreams].
func (l *LogStreams) Subscription() *aeron.Subscription {
	if l == nil {
		return nil
	}
	return l.subscription
}

// Close clears the agent log join source, append source, log adapter, and log
// publisher, clears the publisher publication, then closes the publication and
// the subscription. The join source is cleared before the subscription it holds
// closes.
//
// Idempotent. When both transport handles are already nil, Close returns without
// touching the agent or the log publisher. The agent and log publisher fields are
// retained for the life of the value so the first Close can clear them safely.
//
// After Close the value is inert: both accessors return nil. Safe on a nil
// receiver, a zero value, and after the transport client has already closed.
//
// Returns the joined close errors, or nil.
func (l *LogStreams) Close() error {
	if l == nil || (l.publication == nil && l.subscription == nil) {
		return nil
	}

	l.agent.SetLogJoinSource(nil)
	l.agent.SetAppendSource(nil)
	l.agent.SetLogAdapter(nil)
	l.agent.SetLogPublisher(nil)
	l.logPublisher.SetPublication(nil)

	var err error
	if l.publication != nil {
		err = errors.Join(err, l.publication.Close())
		l.publication = nil
	}
	if l.subscription != nil {
		err = errors.Join(err, l.subscription.Close())
		l.subscription = nil
	}
	return err
}
