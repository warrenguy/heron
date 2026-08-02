package cluster

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Exclusive log publication URI param prefixes. Names match Aeron CommonContext:
// init-term-id, term-id, term-offset, term-length, mtu.
const (
	channelInitTermIDParam = "init-term-id="
	channelTermIDParam     = "term-id="
	channelTermOffsetParam = "term-offset="
	channelTermLengthParam = "term-length="
	channelMTUParam        = "mtu="
)

// LogStreamAdder registers both cluster log stream resources.
//
// [*aeron.Client] satisfies this interface as-built.
type LogStreamAdder interface {
	SubscriptionAdder
	ExclusivePublicationAdder
}

var _ LogStreamAdder = (*aeron.Client)(nil)

// LogPublicationInitialPosition holds recovery geometry for the exclusive log
// publication channel.
//
// When non-nil on [LogStreamsDeps], [OpenLogStreams] writes init-term-id,
// term-id, term-offset, term-length, and mtu so the exclusive publication
// resumes at Position. Position zero is a valid seed for an empty recoverable
// log. A nil seed is cold start: the publication uses Topology.LogChannel
// unchanged.
//
// Applied at open only. Later election append advances do not rebuild the
// publication channel.
type LogPublicationInitialPosition struct {
	Position         int64
	InitialTermID    int32
	TermBufferLength int32
	MtuLength        int32
}

// LogStreamsDeps are the collaborators [OpenLogStreams] needs. Adder, Topology,
// Agent, LogPublisher, and LogAdapter are required. AppendSource and
// PublicationInitialPosition are optional.
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

	// PublicationInitialPosition seeds the exclusive log publication channel at
	// open when non-nil. Nil is cold start. The subscription always uses
	// Topology.LogChannel without seed. See [LogPublicationInitialPosition].
	PublicationInitialPosition *LogPublicationInitialPosition
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
// Both use [Topology.LogStreamID]. The subscription uses [Topology.LogChannel].
// The publication uses that channel, or a channel seeded from
// [LogStreamsDeps.PublicationInitialPosition] when the seed is non-nil. Seed
// lifetime is open-only; see [LogPublicationInitialPosition].
//
// Open order:
//  1. validate the publication channel seed
//  2. add the subscription
//  3. add the publication
//
// Both adds complete before any bind. A failed add leaves the agent untouched.
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
//   - the initial-position seed is invalid
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

	// Validate seed before any driver add. Pure geometry errors must not open a
	// subscription only to roll it back.
	pubChannel, err := logChannelWithInitialPosition(channel, deps.PublicationInitialPosition)
	if err != nil {
		return nil, err
	}

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
		pubChannel,
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

// logPublicationSeedFromPlan maps a recovery plan to exclusive log publication
// seed geometry for open.
//
// Behaviour:
//   - plan.Log == nil: returns nil (cold start)
//   - plan.Log != nil: returns a seed from the extent geometry at
//     [recordinglog.RecoveryPlan.AppendedOrCommittedPosition], including
//     position zero and active-extent restarts where AppendedLogPosition is
//     NullPosition
//
// Does not validate frame alignment or term length.
// [logChannelWithInitialPosition] validates those at open.
func logPublicationSeedFromPlan(
	plan recordinglog.RecoveryPlan,
) *LogPublicationInitialPosition {
	if plan.Log == nil {
		return nil
	}
	return &LogPublicationInitialPosition{
		Position:         plan.AppendedOrCommittedPosition(),
		InitialTermID:    plan.Log.Extent.InitialTermID,
		TermBufferLength: plan.Log.Extent.TermBufferLength,
		MtuLength:        plan.Log.Extent.MtuLength,
	}
}

// logChannelWithInitialPosition returns channel with exclusive-publication
// initial-position params from seed.
//
// Behaviour:
//   - seed == nil: returns channel unchanged (cold start)
//   - seed != nil: sets init-term-id, term-id, term-offset, term-length, and mtu
//     for seed.Position, including position zero
//
// Replaces an existing value when the channel already carries that param.
//
// Returns an error when:
//   - seed.Position is negative
//   - seed.Position is not frame-aligned
//   - seed.TermBufferLength fails [logbuffer.CheckTermLength]
//   - seed.MtuLength is non-positive, not frame-aligned, or above
//     MaxUDPPayloadLength
func logChannelWithInitialPosition(
	channel string,
	seed *LogPublicationInitialPosition,
) (string, error) {
	if seed == nil {
		return channel, nil
	}
	position := seed.Position
	if position < 0 {
		return "", fmt.Errorf("cluster: log initial position %d is invalid", position)
	}
	if position&(logbuffer.FrameAlignment-1) != 0 {
		return "", fmt.Errorf(
			"cluster: log initial position %d is not frame-aligned",
			position,
		)
	}
	if err := logbuffer.CheckTermLength(int(seed.TermBufferLength)); err != nil {
		return "", fmt.Errorf("cluster: log initial position term length: %w", err)
	}
	mtu := seed.MtuLength
	if mtu <= 0 || mtu > logbuffer.MaxUDPPayloadLength || mtu&(logbuffer.FrameAlignment-1) != 0 {
		return "", fmt.Errorf("cluster: log initial position mtu %d is invalid", mtu)
	}

	bitsToShift := logbuffer.PositionBitsToShift(int(seed.TermBufferLength))
	termID := logbuffer.ComputeTermIDFromPosition(position, bitsToShift, seed.InitialTermID)
	termOffset := logbuffer.ComputeTermOffsetFromPosition(position, bitsToShift)

	out := channel
	out = putOrReplaceChannelParam(
		out, channelInitTermIDParam, strconv.FormatInt(int64(seed.InitialTermID), 10),
	)
	out = putOrReplaceChannelParam(
		out, channelTermIDParam, strconv.FormatInt(int64(termID), 10),
	)
	out = putOrReplaceChannelParam(
		out, channelTermOffsetParam, strconv.FormatInt(int64(termOffset), 10),
	)
	out = putOrReplaceChannelParam(
		out, channelTermLengthParam, strconv.FormatInt(int64(seed.TermBufferLength), 10),
	)
	out = putOrReplaceChannelParam(
		out, channelMTUParam, strconv.FormatInt(int64(mtu), 10),
	)
	return out, nil
}

// putOrReplaceChannelParam sets param=value on channel. Appends when the param
// is absent. Replaces the value when present.
func putOrReplaceChannelParam(channel, param, value string) string {
	idx := aeron.ChannelParamIndex(channel, param, true)
	if idx < 0 {
		return aeron.AppendChannelParam(channel, param, value)
	}
	return aeron.ReplaceChannelParamValue(channel, idx, param, value)
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
