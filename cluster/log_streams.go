package cluster

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/recordinglog"
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

// Log publication and subscription URI params. Names match Aeron ChannelUri
// and CommonContext.
const (
	channelTagsParam                    = "tags="
	channelAliasParam                   = "alias="
	channelSoRcvbufParam                = "so-rcvbuf="
	channelRcvWndParam                  = "rcv-wnd="
	channelSpiesSimulateConnectionParam = "ssc="

	logSubscriptionControlMode = "manual"
	logSubscriptionRejoin      = "false"
	logSubscriptionAlias       = "log-cm"
	logPublicationAlias        = "log"
)

// LogPublicationInitialPosition holds recovery geometry for the exclusive log
// publication channel.
//
// When non-nil on [LogStreamsDeps], [OpenLogStreams] writes init-term-id,
// term-id, term-offset, term-length, and mtu so the exclusive publication
// resumes at Position. Position zero is a valid seed for an empty recoverable
// log. A nil seed is cold start: the publication uses
// [Topology.LogPublicationChannel] with no seed params.
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

	// Topology supplies the log receive channel, the publication channel, the
	// publication destinations, and the log stream id. [OpenLogStreams] copies
	// the mode and the destinations. It does not retain the Topology.
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
	// open when non-nil. See [LogPublicationInitialPosition].
	PublicationInitialPosition *LogPublicationInitialPosition
}

// LogStreams owns the member's cluster log transport resources.
//
// Open with [OpenLogStreams]. Open registers the log subscription and the log
// exclusive publication. Open binds the publisher, adapter, append source,
// and log join source into the agent before return.
//
// Behaviour: the publication must be bound before any election work runs.
// [consensus.Agent.AddLogPublication] rebuilds an unbound publication through
// the reseed hook. Without a publisher it reports the null session id. The
// transition that reads that session id also publishes the new leadership term.
//
// The subscription opens as control-mode=manual before the publication. Open
// adds this member's log channel as the first receive destination. On a shared
// log endpoint the member's own subscription receives its own publication.
// With distinct log endpoints the publication sends to each peer, and this
// member receives those peers on its own endpoint. Opening the subscription
// first avoids a period with no receiver. Open does not wait for connect.
//
// Concurrency: call from the goroutine that runs [consensus.Agent.DoWork].
// Not safe from other goroutines.
//
// Lifetime: owns the two transport handles and closes them. Does not own the
// publisher or the adapter. Never closes them. Close before closing the
// transport client when the process owns the client.
//
// Do not copy. A value copy shares the same handles. Both values look like
// owners.
type LogStreams struct {
	agent                   *consensus.Agent
	logPublisher            *consensus.LogPublisher
	publication             *aeron.ExclusivePublication
	subscription            *aeron.Subscription
	streamID                int32
	liveURI                 string
	nextCorrelationID       func() int64
	logMDC                  bool
	publicationDestinations []string
	closed                  bool
}

// OpenLogStreams registers the cluster log subscription and publication and
// binds the log path into deps.Agent.
//
// Both use [Topology.LogStreamID]. The subscription opens as control-mode=manual
// with unique tags, rejoin=false, and alias=log-cm. so-rcvbuf and rcv-wnd copy
// from [Topology.LogChannel] when that channel carries them. [Topology.LogChannel]
// is added as the first receive destination. The publication uses
// [Topology.LogPublicationChannel], or that channel seeded from
// [LogStreamsDeps.PublicationInitialPosition] when the seed is non-nil. Seed
// lifetime is open-only. See [LogPublicationInitialPosition].
//
// In manual multi-destination-cast mode the publication also gets fresh tags
// and alias=log. The leader JoinLog channel names that publication by its
// channel tag. Shared mode keeps [Topology.LogChannel] as the publication
// channel.
//
// Open order:
//  1. validate the publication channel seed
//  2. add the manual-mode subscription
//  3. add the live log channel as a receive destination
//  4. add the publication
//  5. manual multi-destination-cast only: add one publication destination per peer
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
//   - [consensus.Agent.SetLocalLogStream] with the leader JoinLog channel
//     and [Topology.LogStreamID]
//   - [consensus.Agent.SetLeaderLogRecordingChannel] with that same channel,
//     or empty in shared mode
//
// No Image is bound at open.
//
// Returns an error when:
//   - any required dependency is nil
//   - the initial-position seed is invalid
//   - the subscription add, live destination add, publication add, or a
//     publication destination add fails
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
	seededChannel, err := logChannelWithInitialPosition(
		deps.Topology.LogPublicationChannel(),
		deps.PublicationInitialPosition,
	)
	if err != nil {
		return nil, err
	}

	ls := &LogStreams{
		agent:                   deps.Agent,
		logPublisher:            deps.LogPublisher,
		streamID:                streamID,
		liveURI:                 liveURI,
		nextCorrelationID:       nextCorrelationID,
		logMDC:                  deps.Topology.IsLogMDC(),
		publicationDestinations: deps.Topology.LogPublicationDestinations(),
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

	pub, leaderChannel, err := ls.addLogPublication(
		deps.Adder,
		seededChannel,
		streamID,
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
	ls.bindLeaderLogChannels(seededChannel, leaderChannel, streamID, true)
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
// Pass [Topology.LogPublicationChannel] as channel. A new leader reseeds when
// the publication is unbound or its position differs from the durable append
// high-water. The archive extends the LOCAL recording only when the
// publication join position equals the recording stop position.
//
// Manual multi-destination-cast mode draws fresh tags and adds one send
// destination per peer. On success the leader JoinLog channel and the LOCAL
// recording channel name the new channel tag.
//
// Shared mode does not change the JoinLog channel set at open. It keeps the
// recording channel empty.
//
// Concurrency: call from the goroutine that runs [consensus.Agent.DoWork].
// Not safe from other goroutines.
//
// Returns an error when:
//   - the receiver is nil or already closed
//   - adder is nil
//   - the correlation id allocator is nil
//   - seed is invalid
//   - the new exclusive publication cannot be added
//   - a publication destination cannot be added
//
// On add failure the old publication stays closed and the publisher stays
// unbound. Both agent channels keep the previous tag. The next call retries
// every add.
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
	if l.nextCorrelationID == nil {
		return errors.New("cluster: correlation id allocator is required")
	}
	seededChannel, err := logChannelWithInitialPosition(channel, seed)
	if err != nil {
		return err
	}

	if l.publication != nil {
		// Unbind before close so offers cannot target a closed handle.
		l.logPublisher.SetPublication(nil)
		_ = l.publication.Close()
		l.publication = nil
	}

	pub, leaderChannel, err := l.addLogPublication(
		adder,
		seededChannel,
		streamID,
		"cluster log publication reseed",
	)
	if err != nil {
		return err
	}
	l.bindLogPublication(pub)
	l.bindLeaderLogChannels(seededChannel, leaderChannel, streamID, false)
	return nil
}

// Subscription returns the cluster log subscription.
//
// Nil after Close, on a nil receiver, and after a failed reseed and recovery.
//
// The subscription is control-mode=manual. Receive destinations (live and
// catchup) feed it. Images arrive per destination and session.
// Order is undefined. Select by log session id. Must not index.
//
// Concurrency: call from the goroutine that runs [consensus.Agent.DoWork].
// Not safe from other goroutines. See [LogStreams].
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
// absorb same-session replay frames below their join position. Used at
// prepare for a new term so a fresh receive endpoint can image a session the
// old endpoint holds in cool-down.
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

// addLogPublication adds the exclusive log publication on channel, which
// already carries any initial-position seed.
//
// Returns the publication and the leader channel. The leader channel is
// empty in shared mode. On a destination error the new publication is closed.
func (l *LogStreams) addLogPublication(
	adder ExclusivePublicationAdder,
	channel string,
	streamID int32,
	what string,
) (*aeron.ExclusivePublication, string, error) {
	pubChannel := channel
	leaderChannel := ""
	if l.logMDC {
		channelTag, tags := drawChannelTags(l.nextCorrelationID)
		pubChannel = aeron.SetChannelParam(pubChannel, channelTagsParam, tags)
		pubChannel = aeron.SetChannelParam(pubChannel, channelAliasParam, logPublicationAlias)
		leaderChannel = aeron.AppendChannelParam(udpChannelPrefix, channelTagsParam, channelTag)
		leaderChannel = aeron.AppendChannelParam(
			leaderChannel,
			channelAliasParam,
			logPublicationAlias,
		)
	}

	pub, err := addExclusivePublicationChecked(adder, streamID, pubChannel, what)
	if err != nil {
		return nil, "", err
	}

	// Shared mode holds no destinations.
	for _, dest := range l.publicationDestinations {
		if destErr := pub.AddDestination(dest); destErr != nil {
			_ = pub.Close()
			return nil, "", fmt.Errorf(
				"cluster: add log publication destination on %s: %w",
				dest,
				destErr,
			)
		}
	}
	return pub, leaderChannel, nil
}

// bindLeaderLogChannels records the leader JoinLog channel and the LOCAL
// recording channel after a successful publication add.
func (l *LogStreams) bindLeaderLogChannels(
	pubChannel string,
	leaderChannel string,
	streamID int32,
	atOpen bool,
) {
	if l.logMDC {
		l.agent.SetLocalLogStream(leaderChannel, streamID)
		l.agent.SetLeaderLogRecordingChannel(leaderChannel)
		return
	}

	if atOpen {
		l.agent.SetLocalLogStream(pubChannel, streamID)
	}

	l.agent.SetLeaderLogRecordingChannel("")
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
	_, tags := drawChannelTags(nextCorrelationID)
	ch := "aeron:udp"
	ch = aeron.AppendChannelParam(ch, aeron.ChannelControlModeParam, logSubscriptionControlMode)
	ch = aeron.AppendChannelParam(ch, channelTagsParam, tags)
	ch = aeron.AppendChannelParam(ch, aeron.ChannelRejoinParam, logSubscriptionRejoin)
	ch = aeron.AppendChannelParam(ch, channelAliasParam, logSubscriptionAlias)
	ch = copyChannelParamIfPresent(ch, liveURI, channelSoRcvbufParam)
	ch = copyChannelParamIfPresent(ch, liveURI, channelRcvWndParam)
	return ch
}

// drawChannelTags allocates two fresh tags from nextCorrelationID. Returns
// the first tag and the tags= value "<first>,<second>".
func drawChannelTags(nextCorrelationID func() int64) (first, tags string) {
	first = strconv.FormatInt(nextCorrelationID(), 10)
	return first, first + "," + strconv.FormatInt(nextCorrelationID(), 10)
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
	out = aeron.SetChannelParam(
		out, channelInitTermIDParam, strconv.FormatInt(int64(seed.InitialTermID), 10),
	)
	out = aeron.SetChannelParam(
		out, channelTermIDParam, strconv.FormatInt(int64(termID), 10),
	)
	out = aeron.SetChannelParam(
		out, channelTermOffsetParam, strconv.FormatInt(int64(termOffset), 10),
	)
	out = aeron.SetChannelParam(
		out, channelTermLengthParam, strconv.FormatInt(int64(seed.TermBufferLength), 10),
	)
	out = aeron.SetChannelParam(
		out, channelMTUParam, strconv.FormatInt(int64(seed.MtuLength), 10),
	)
	return out, nil
}

// Close clears agent binds, clears the publisher publication, then closes the
// publication and subscription.
//
// Behaviour: agent clear order is:
//   - log join source (before the subscription it holds closes)
//   - live destination
//   - catchup destination
//   - local log stream (JoinLog naming)
//   - leader LOCAL recording channel
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
	l.agent.SetLeaderLogRecordingChannel("")
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
