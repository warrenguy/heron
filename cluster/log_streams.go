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

// LogStreamAdder registers both cluster log stream resources and allocates
// correlation ids for manual-mode subscription tags.
//
// [*aeron.Client] satisfies this interface as-built.
type LogStreamAdder interface {
	SubscriptionAdder
	ExclusivePublicationAdder
	NextCorrelationID() int64
}

var _ LogStreamAdder = (*aeron.Client)(nil)

// Manual log subscription URI params. Names match Aeron ChannelUri and
// CommonContext.
const (
	channelControlModeParam = "control-mode="
	channelTagsParam        = "tags="
	channelRejoinParam      = "rejoin="
	channelAliasParam       = "alias="
	channelSoRcvbufParam    = "so-rcvbuf="
	channelRcvWndParam      = "rcv-wnd="

	logSubscriptionControlMode = "manual"
	logSubscriptionRejoin      = "false"
	logSubscriptionAlias       = "log-cm"
)

// LogPublicationInitialPosition holds recovery geometry for the exclusive log
// publication channel.
//
// When non-nil on [LogStreamsDeps], [OpenLogStreams] writes init-term-id,
// term-id, term-offset, term-length, and mtu so the exclusive publication
// resumes at Position. Position zero is a valid seed for an empty recoverable
// log. A nil seed is cold start: the publication uses Topology.LogChannel
// unchanged.
//
// Applied at open. [LogStreams.ReseedPublicationAt] takes a fresh seed when a
// new leader rebuilds the publication at the election append position.
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
	// open when non-nil. Nil is cold start. The subscription opens as
	// control-mode=manual with live Topology.LogChannel as the first receive
	// destination. See [LogPublicationInitialPosition].
	PublicationInitialPosition *LogPublicationInitialPosition
}

// LogStreams owns the member's cluster log transport resources.
//
// Open with [OpenLogStreams], which registers the log subscription and the log
// exclusive publication and binds publisher, adapter, append source, and log
// join source into the agent before return.
//
// Behaviour: the publication must be bound before any election work runs.
// [consensus.Agent.AddLogPublication] rebuilds an unbound publication through
// the reseed hook. Without a publisher it reports the null session id. The
// transition that reads that session id also publishes the new leadership term.
//
// The subscription opens as control-mode=manual before the publication. Open
// adds Topology.LogChannel as the first receive destination so the shared
// multicast group feeds the subscription. On that group the member's own
// subscription receives its own publication. Opening the subscription first
// avoids a period with no receiver. Open does not wait for connect.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [consensus.Agent.DoWork] and the transport client duty cycle.
//
// Lifetime: owns the two transport handles and closes them. Does not own the
// publisher or the adapter; never closes them. Close before closing the
// transport client when the process owns the client.
//
// Do not copy. A value copy shares the same handles; both values look like
// owners.
type LogStreams struct {
	agent             *consensus.Agent
	logPublisher      *consensus.LogPublisher
	publication       *aeron.ExclusivePublication
	subscription      *aeron.Subscription
	streamID          int32
	liveURI           string
	nextCorrelationID func() int64
	closed            bool
}

// OpenLogStreams registers the cluster log subscription and publication and
// binds the log path into deps.Agent.
//
// Both use [Topology.LogStreamID]. The subscription opens as control-mode=manual
// with unique tags, rejoin=false, and alias=log-cm. so-rcvbuf and rcv-wnd copy
// from [Topology.LogChannel] when that template carries them. The live multicast
// URI ([Topology.LogChannel]) is added as the first receive destination. The
// publication uses that channel, or a channel seeded from
// [LogStreamsDeps.PublicationInitialPosition] when the seed is non-nil. Seed
// lifetime is open-only; see [LogPublicationInitialPosition].
//
// Open order:
//  1. validate the publication channel seed
//  2. add the manual-mode subscription
//  3. add the live log channel as a receive destination
//  4. add the publication
//
// All adds complete before any bind. A failed add leaves the agent untouched.
//
// Bind steps:
//   - [consensus.Agent.SetLogPublisher]
//   - [consensus.Agent.SetLogAdapter]
//   - [consensus.Agent.SetAppendSource] with deps.AppendSource, or
//     [consensus.LogAppendPosition] over the wired publisher and adapter when
//     deps.AppendSource is nil
//   - [consensus.Agent.SetLogJoinSource] with
//     [consensus.SubscriptionLogJoinSource] over the log subscription
//   - [consensus.Agent.LiveLogDestinationSet] with the live log channel URI
//   - [consensus.Agent.SetLocalLogStream] with the resolved publication channel
//     and [Topology.LogStreamID] so the leader JoinLog can name the log
//
// No Image is bound at open.
//
// Returns an error when:
//   - any required dependency is nil
//   - the initial-position seed is invalid
//   - the subscription add, live destination add, or publication add fails
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

	liveURI := deps.Topology.LogChannel()
	streamID := deps.Topology.LogStreamID()
	nextCorrelationID := deps.Adder.NextCorrelationID

	// Validate seed before any driver add. Pure geometry errors must not open a
	// subscription only to roll it back.
	pubChannel, err := logChannelWithInitialPosition(liveURI, deps.PublicationInitialPosition)
	if err != nil {
		return nil, err
	}

	ls := &LogStreams{
		agent:             deps.Agent,
		logPublisher:      deps.LogPublisher,
		streamID:          streamID,
		liveURI:           liveURI,
		nextCorrelationID: nextCorrelationID,
	}

	sub, err := ls.addManualLogSubscription(deps.Adder, "cluster log subscription")
	if err != nil {
		return nil, err
	}

	if err := sub.AddDestination(liveURI); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf(
			"cluster: add live log destination on %s: %w",
			liveURI,
			err,
		)
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

	ls.bindLogPublication(pub)
	deps.Agent.SetLogPublisher(deps.LogPublisher)
	deps.Agent.SetLogAdapter(deps.LogAdapter)
	deps.Agent.SetAppendSource(appendSource)
	ls.bindLogSubscription(sub)
	deps.Agent.LiveLogDestinationSet(liveURI)
	deps.Agent.SetLocalLogStream(pubChannel, streamID)
	return ls, nil
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

// ReseedPublicationAt closes the exclusive log publication, if any, and opens a
// new one at seed.Position. Rebinds the log publisher.
//
// A new leader calls it when the publication is unbound or its position
// differs from the durable append high-water. The archive extends the LOCAL
// recording only when the publication join position equals the recording stop
// position.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - the receiver is nil or already closed
//   - seed is invalid
//   - the new exclusive publication cannot be added
//
// On add failure the old publication is closed and the publisher is unbound.
// The next call retries the add.
func (l *LogStreams) ReseedPublicationAt(
	adder ExclusivePublicationAdder,
	channel string,
	streamID int32,
	seed *LogPublicationInitialPosition,
) error {
	if l.isClosed() {
		return errors.New("cluster: log streams closed")
	}
	if adder == nil {
		return errors.New("cluster: log stream adder is required")
	}
	pubChannel, err := logChannelWithInitialPosition(channel, seed)
	if err != nil {
		return err
	}

	if l.publication != nil {
		// Unbind before close so offers cannot target a closed handle.
		l.logPublisher.SetPublication(nil)
		_ = l.publication.Close()
		l.publication = nil
	}

	pub, err := addExclusivePublicationChecked(
		adder,
		streamID,
		pubChannel,
		"cluster log publication reseed",
	)
	if err != nil {
		return err
	}
	l.bindLogPublication(pub)
	return nil
}

// Subscription returns the cluster log subscription.
//
// Nil after Close, on a nil receiver, and after a failed reseed and recovery.
//
// The subscription is control-mode=manual. Receive destinations (live multicast
// and catchup unicast) feed it. Images arrive per destination and session.
// Order is undefined. Select by log session id. Must not index.
//
// Concurrency: invoker-only. See [LogStreams].
func (l *LogStreams) Subscription() *aeron.Subscription {
	if l == nil {
		return nil
	}
	return l.subscription
}

// LiveURI returns the live log channel URI added as the initial receive
// destination at open.
//
// Empty after Close and on a nil receiver.
//
// Concurrency: invoker-only. See [LogStreams].
func (l *LogStreams) LiveURI() string {
	if l == nil {
		return ""
	}
	return l.liveURI
}

// ReseedSubscription opens a fresh control-mode=manual log subscription with
// new tags. Closes the bound subscription first, if any. Rebinds the agent log
// join source. The fresh subscription has no destinations.
//
// Used at catchup entry so late-join images from the prior endpoint cannot
// absorb same-session replay frames below their join position.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - the receiver is nil, a zero value, or closed
//   - adder is nil
//   - the correlation id allocator is nil
//   - the new subscription cannot be added
//
// Fail-closed: when the new add fails, ReseedSubscription opens a recovery
// subscription with fresh tags, rebinds the join source, and re-adds the live
// destination when liveURI is set. Returns the original add error. When
// recovery also fails the subscription stays nil and the join source stays
// unbound. The next call retries the add.
func (l *LogStreams) ReseedSubscription(adder SubscriptionAdder) error {
	if l.isClosed() {
		return errors.New("cluster: log streams closed")
	}
	if adder == nil {
		return errors.New("cluster: subscription adder is required")
	}
	if l.nextCorrelationID == nil {
		return errors.New("cluster: correlation id allocator is required")
	}

	if l.subscription != nil {
		// Unbind before close so join selection cannot target a closed handle.
		l.agent.SetLogJoinSource(nil)
		_ = l.subscription.Close()
		l.subscription = nil
	}

	sub, err := l.addManualLogSubscription(adder, "cluster log subscription reseed")
	if err != nil {
		return l.recoverSubscriptionAfterReseedFailure(adder, err)
	}
	l.bindLogSubscription(sub)
	return nil
}

// addManualLogSubscription opens one control-mode=manual log subscription.
func (l *LogStreams) addManualLogSubscription(
	adder SubscriptionAdder,
	what string,
) (*aeron.Subscription, error) {
	return addSubscriptionChecked(
		adder,
		l.streamID,
		manualLogSubscriptionChannel(l.nextCorrelationID, l.liveURI),
		what,
	)
}

// bindLogPublication stores pub and rebinds the log publisher.
func (l *LogStreams) bindLogPublication(pub *aeron.ExclusivePublication) {
	l.publication = pub
	l.logPublisher.SetPublication(pub)
}

// bindLogSubscription stores sub and rebinds the agent log join source.
func (l *LogStreams) bindLogSubscription(sub *aeron.Subscription) {
	l.subscription = sub
	l.agent.SetLogJoinSource(consensus.NewSubscriptionLogJoinSource(sub))
}

// recoverSubscriptionAfterReseedFailure re-opens a manual subscription and
// re-attaches the live destination after a failed reseed open. Returns
// reseedErr joined with any recovery error.
//
// When recovery cannot open a subscription, or cannot re-add the live
// destination, clears the live destination field so a later restore is not
// skipped for a stale real URI.
func (l *LogStreams) recoverSubscriptionAfterReseedFailure(
	adder SubscriptionAdder,
	reseedErr error,
) error {
	sub, err := l.addManualLogSubscription(adder, "cluster log subscription reseed recovery")
	if err != nil {
		l.agent.LiveLogDestinationSet("")
		return errors.Join(reseedErr, err)
	}
	l.bindLogSubscription(sub)

	live := l.liveURI
	if live != "" {
		if destErr := sub.AddDestination(live); destErr != nil {
			// AddDestination failed. Clear the live URI so restore re-adds.
			live = ""
			reseedErr = errors.Join(reseedErr, destErr)
		}
	}
	l.agent.LiveLogDestinationSet(live)
	return reseedErr
}

// manualLogSubscriptionChannel builds the control-mode=manual receive channel
// for the cluster log subscription.
//
// Shape: aeron:udp?control-mode=manual|tags=<t1>,<t2>|rejoin=false|alias=log-cm
// plus so-rcvbuf and rcv-wnd copied from liveURI when present. Tags come from
// two nextCorrelationID allocations so each open gets a fresh receive endpoint.
func manualLogSubscriptionChannel(
	nextCorrelationID func() int64,
	liveURI string,
) string {
	t1 := nextCorrelationID()
	t2 := nextCorrelationID()
	ch := "aeron:udp"
	ch = aeron.AppendChannelParam(ch, channelControlModeParam, logSubscriptionControlMode)
	ch = aeron.AppendChannelParam(
		ch,
		channelTagsParam,
		strconv.FormatInt(t1, 10)+","+strconv.FormatInt(t2, 10),
	)
	ch = aeron.AppendChannelParam(ch, channelRejoinParam, logSubscriptionRejoin)
	ch = aeron.AppendChannelParam(ch, channelAliasParam, logSubscriptionAlias)
	ch = copyChannelParamIfPresent(ch, liveURI, channelSoRcvbufParam)
	ch = copyChannelParamIfPresent(ch, liveURI, channelRcvWndParam)
	return ch
}

// copyChannelParamIfPresent appends param from src onto dst when src carries it.
func copyChannelParamIfPresent(dst, src, param string) string {
	idx := aeron.ChannelParamIndex(src, param, true)
	if idx < 0 {
		return dst
	}
	return aeron.AppendChannelParam(dst, param, aeron.ChannelParamValue(src, idx, param))
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
//   - seed.MtuLength fails [logbuffer.CheckMTULength]
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
	if err := logbuffer.CheckMTULength(int(seed.MtuLength)); err != nil {
		return "", fmt.Errorf("cluster: log initial position mtu length: %w", err)
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
		out, channelMTUParam, strconv.FormatInt(int64(seed.MtuLength), 10),
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

// Close clears agent binds, clears the publisher publication, then closes the
// publication and subscription.
//
// Behaviour: agent clear order is:
//   - log join source (before the subscription it holds closes)
//   - live destination
//   - catchup destination
//   - local log stream (JoinLog naming)
//   - append source
//   - log adapter
//   - log publisher
//
// Idempotent. A second Close returns without touching the agent or the log
// publisher.
//
// After Close the value is inert: both accessors return nil. Safe on a nil
// receiver, a zero value, and after the transport client has already closed.
//
// Returns the joined close errors, or nil.
func (l *LogStreams) Close() error {
	if l.isClosed() {
		return nil
	}
	l.closed = true

	l.agent.SetLogJoinSource(nil)
	l.agent.LiveLogDestinationSet("")
	l.agent.CatchupLogDestinationSet("")
	l.agent.SetLocalLogStream("", 0)
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
	l.liveURI = ""
	l.nextCorrelationID = nil
	return err
}

// isClosed reports whether l is a nil receiver, a zero value, or closed.
func (l *LogStreams) isClosed() bool {
	return l == nil || l.closed || l.agent == nil
}
