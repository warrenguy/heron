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

// LogStreamsDeps are the collaborators [OpenLogStreams] needs. Adder, Topology,
// Agent, LogPublisher, and LogAdapter are required. AppendSource is optional.
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

	// AppendSource is the append high-water source wired into the agent. Nil
	// binds [consensus.LogAppendPosition] over the wired publisher and adapter.
	// [OpenConsensusModule] supplies [consensus.RecordingPosAppendPosition] so
	// the agent ranks on durable recording state.
	AppendSource consensus.AppendPositionSource
}

// LogStreams owns the member's cluster log transport resources.
//
// Open with [OpenLogStreams], which registers the log subscription and the log
// exclusive publication and binds publisher, adapter, append source, and log
// join source into the agent before return.
//
// Behaviour: the publication must be bound before any election work runs.
// [consensus.Agent.AddLogPublication] reports the null session id until the
// publication is bound. The transition that reads that session id also publishes
// the new leadership term.
//
// The subscription opens before the publication. On a shared multicast group the
// member's own subscription receives its own publication. Opening the
// subscription first avoids a period with no receiver. Open does not wait for
// connect.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [consensus.Agent.DoWork] and the transport client duty cycle.
//
// Lifetime: owns the two transport handles and closes them. Does not own the
// publisher or the adapter; never closes them. Close before closing the
// transport client when the process owns the client.
//
// Do not copy. Every field is a reference type. A value copy shares the same
// handles; both values look like owners.
type LogStreams struct {
	agent        *consensus.Agent
	logPublisher *consensus.LogPublisher
	publication  *aeron.ExclusivePublication
	subscription *aeron.Subscription
}

// OpenLogStreams registers the cluster log subscription and publication and
// binds the log path into deps.Agent.
//
// Both use [Topology.LogChannel] and [Topology.LogStreamID].
//
// Open order for adds: subscription, then publication. Both adds complete
// before any bind. A failed add leaves the agent untouched.
//
// Bind steps:
//   - [consensus.Agent.SetLogPublisher]
//   - [consensus.Agent.SetLogAdapter]
//   - [consensus.Agent.SetAppendSource] with deps.AppendSource, or
//     [consensus.LogAppendPosition] over the wired publisher and adapter when
//     deps.AppendSource is nil
//   - [consensus.Agent.SetLogJoinSource] with [consensus.SubscriptionLogJoinSource]
//     over the log subscription
//
// No Image is bound at open.
//
// Returns an error when:
//   - any required dependency is nil
//   - either add fails
//
// On error: every handle added so far is closed. The agent is untouched. The
// returned value is nil.
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

	appendSource := deps.AppendSource
	if appendSource == nil {
		appendSource = consensus.NewLogAppendPosition(deps.Agent)
	}

	deps.LogPublisher.SetPublication(pub)
	deps.Agent.SetLogPublisher(deps.LogPublisher)
	deps.Agent.SetLogAdapter(deps.LogAdapter)
	deps.Agent.SetAppendSource(appendSource)
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
// log channel, including this member's own publication. Order is undefined.
// Select by log session id. Must not index.
//
// Concurrency: invoker-only. See [LogStreams].
func (l *LogStreams) Subscription() *aeron.Subscription {
	if l == nil {
		return nil
	}
	return l.subscription
}

// Close clears agent log join source, append source, log adapter, and log
// publisher, clears the publisher publication, then closes publication and
// subscription. Join source clears before the subscription it holds closes.
//
// Idempotent. When both transport handles are already nil, Close returns without
// touching the agent or the log publisher. Agent and log publisher fields stay
// for the life of the value so the first Close can clear them safely.
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
