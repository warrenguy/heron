package cluster

import (
	"context"
	"errors"
	"math"
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// catchupControl is the production [consensus.CatchupControl].
//
// Behaviour:
//   - Follower: reseeds the log subscription and adds a unicast catchup
//     destination for the leader archive replay. Ensure clears the live
//     destination while catchup is active. Only the catchup feed is attached
//     until live is restored on teardown, or added when catchup is near live.
//   - Prepare for a new term: replaces the log subscription with a fresh one
//     that carries only the live destination.
//   - Leader: starts an archive replay of the local cluster log recording
//     toward the follower catchup endpoint with the log publication session id.
//     Length is math.MaxInt64 so an active recording is followed.
//
// Concurrency: invoker-only.
//
// Ownership: owns every catchup replay this control started and the catchup
// destination URI recorded on the agent. Close with the module, before the
// archive client closes. Does not own the log subscription, archive client,
// transport client, or agent.
type catchupControl struct {
	ctx context.Context

	// control issues StartReplay / StopReplay.
	control replayControl

	// adder reseeds the log subscription.
	adder SubscriptionAdder

	// agent supplies the local log recording id and holds destination URIs.
	// Not closed by this type.
	agent *consensus.Agent

	// streams owns the log subscription this control reseeds and attaches
	// destinations to. Not closed by this type.
	streams *LogStreams

	// logStreamID is the cluster log stream id (same as the live log).
	logStreamID int32

	// logChannel is the cluster log channel template the live log and its
	// recording use. Catchup builds on the template so term geometry matches.
	// Also the live URI restored after catchup teardown when live is absent.
	logChannel string

	// replaySessionIDs are the leader-side catchup replays started and not yet
	// stopped. StopCatchupReplay removes one id. StopCatchupReplays drains this
	// slice.
	replaySessionIDs []int64
}

var _ consensus.CatchupControl = (*catchupControl)(nil)

// newCatchupControl builds a catchup control. All fields are required.
func newCatchupControl(
	ctx context.Context,
	control replayControl,
	adder SubscriptionAdder,
	agent *consensus.Agent,
	streams *LogStreams,
	logStreamID int32,
	logChannel string,
) *catchupControl {
	return &catchupControl{
		ctx:         moduleControlContext(ctx),
		control:     control,
		adder:       adder,
		agent:       agent,
		streams:     streams,
		logStreamID: logStreamID,
		logChannel:  logChannel,
	}
}

// EnsureCatchupLogDestination reseeds the log subscription and adds the
// catchup receive destination when needed.
//
// Destination is the configured cluster log channel with
// endpoint=<catchupEndpoint>.
//
// The log subscription is not session-filtered. The leader replay channel
// carries session-id. Join selection keys on the log session id through the
// single log join source.
//
// Idempotent once open: a non-empty agent catchup destination returns that URI
// and touches nothing.
//
// On failure after reseed: restores the live destination when the live feed is
// absent so the member is not left with zero destinations. Reseed itself is
// fail-closed (see [LogStreams.ReseedSubscription]).
//
// Returns the destination URI, or empty on failure.
func (c *catchupControl) EnsureCatchupLogDestination(catchupEndpoint string) string {
	if c == nil || catchupEndpoint == "" || c.agent == nil || c.streams == nil {
		return ""
	}
	if dest := c.agent.CatchupLogDestination(); dest != "" {
		return dest
	}
	if err := c.reseedWithLiveCleared(); err != nil {
		return ""
	}

	channel := catchupDestinationURI(c.logChannel, catchupEndpoint)
	if channel == "" {
		_ = c.restoreLiveLogDestination()
		return ""
	}
	sub := c.logSubscription()
	if sub == nil {
		return ""
	}
	if err := sub.AddDestination(channel); err != nil {
		_ = c.restoreLiveLogDestination()
		return ""
	}
	c.agent.CatchupLogDestinationSet(channel)
	return channel
}

// RemoveCatchupLogDestination removes the catchup receive destination and
// restores the live destination when the live feed is absent. Idempotent.
func (c *catchupControl) RemoveCatchupLogDestination() error {
	return c.removeCatchupDestination(true)
}

// removeCatchupDestination removes the catchup destination URI.
//
// When restoreLive is true and LiveLogDestination is empty, re-adds the live log
// channel. The agent records the URI. Module close passes restoreLive false.
//
// CatchupLogDestination clears only after a successful RemoveDestination, or
// when no subscription is present to remove. A remove error leaves
// CatchupLogDestination set. A later call retries the same URI.
func (c *catchupControl) removeCatchupDestination(restoreLive bool) error {
	if c == nil || c.agent == nil {
		return nil
	}
	dest := c.agent.CatchupLogDestination()
	if dest != "" {
		if sub := c.logSubscription(); sub != nil {
			if err := sub.RemoveDestination(dest); err != nil {
				return err
			}
		}
		c.agent.CatchupLogDestinationSet("")
	}
	if restoreLive {
		return c.restoreLiveLogDestination()
	}
	return nil
}

// logSubscription returns the log subscription owned by streams, or nil.
func (c *catchupControl) logSubscription() *aeron.Subscription {
	if c == nil || c.streams == nil {
		return nil
	}
	return c.streams.Subscription()
}

// AddLiveLogDestination adds the live log channel as a receive destination.
// No-op when the live feed is already attached. Idempotent.
//
// On AddDestination failure LiveLogDestination stays empty. The next tick
// retries. The catchup feed keeps running.
//
// Concurrency: invoker-only.
func (c *catchupControl) AddLiveLogDestination() error {
	return c.restoreLiveLogDestination()
}

// restoreLiveLogDestination re-adds the live log channel.
// No-op when the live feed is already attached.
func (c *catchupControl) restoreLiveLogDestination() error {
	if c == nil || c.agent == nil {
		return nil
	}
	if c.agent.LiveLogDestination() != "" {
		return nil
	}
	liveURI := c.logChannel
	if liveURI == "" && c.streams != nil {
		liveURI = c.streams.LiveURI()
	}
	if liveURI == "" {
		return nil
	}
	sub := c.logSubscription()
	if sub == nil {
		return nil
	}
	if err := sub.AddDestination(liveURI); err != nil {
		return err
	}
	c.agent.LiveLogDestinationSet(liveURI)
	return nil
}

// ReseedLogSubscription replaces the log subscription with a fresh
// control-mode=manual subscription that carries only the live destination.
// Clears both destination fields first: the close drops every destination of
// the old subscription.
//
// A failed reseed keeps the recovery subscription. The live add then runs
// only when recovery left the live destination unset.
//
// Returns the reseed error or the live destination add error.
func (c *catchupControl) ReseedLogSubscription() error {
	if c == nil || c.agent == nil || c.streams == nil {
		return nil
	}
	c.agent.CatchupLogDestinationSet("")
	if err := c.reseedWithLiveCleared(); err != nil {
		return err
	}
	return c.restoreLiveLogDestination()
}

// reseedWithLiveCleared clears the live destination field, then replaces the
// log subscription. The fresh subscription has no destinations.
//
// Clears live first so a stale URI cannot skip the restore after the reseed.
// On a reseed error, restores live when recovery opened a subscription
// without it. Returns the reseed error.
func (c *catchupControl) reseedWithLiveCleared() error {
	c.agent.LiveLogDestinationSet("")
	if err := c.streams.ReseedSubscription(c.adder); err != nil {
		_ = c.restoreLiveLogDestination()
		return err
	}
	return nil
}

// StartCatchupReplay starts an archive replay of the local log recording and
// retains the replay session id for StopCatchupReplay / StopCatchupReplays.
func (c *catchupControl) StartCatchupReplay(
	logPosition int64,
	catchupEndpoint string,
	logSessionID int32,
) (int64, error) {
	recordingID := c.agent.LocalLogRecordingID()
	if recordingID < 0 {
		return 0, errNoLocalLogRecording
	}
	channel := catchupReplayChannel(c.logChannel, catchupEndpoint, logSessionID)
	replaySessionID, err := c.control.StartReplay(
		c.ctx,
		recordingID,
		logPosition,
		math.MaxInt64,
		c.logStreamID,
		[]byte(channel),
	)
	if err != nil {
		return 0, err
	}
	c.replaySessionIDs = append(c.replaySessionIDs, replaySessionID)
	return replaySessionID, nil
}

// StopCatchupReplay stops one retained catchup replay by session id.
//
// Idempotent when the session is not retained. Removes the session from the
// ledger only after StopReplay succeeds. A failed stop can be retried.
//
// Concurrency: invoker-only.
func (c *catchupControl) StopCatchupReplay(replaySessionID int64) error {
	if c == nil {
		return nil
	}
	i := slices.Index(c.replaySessionIDs, replaySessionID)
	if i < 0 {
		return nil
	}
	if err := c.control.StopReplay(c.ctx, replaySessionID); err != nil {
		return err
	}
	c.replaySessionIDs = slices.Delete(c.replaySessionIDs, i, i+1)
	return nil
}

// StopCatchupReplays stops every retained catchup replay. Idempotent.
//
// Drops a session id once stop has been attempted. A stop that fails is not
// retried. The archive either never had the session or is gone. A retained id
// would stop a later replay that reused the number.
func (c *catchupControl) StopCatchupReplays() error {
	if c == nil || len(c.replaySessionIDs) == 0 {
		return nil
	}
	sessionIDs := c.replaySessionIDs
	c.replaySessionIDs = nil

	var err error
	for _, replaySessionID := range sessionIDs {
		err = errors.Join(err, c.control.StopReplay(c.ctx, replaySessionID))
	}
	return err
}

// close stops leader-side replays and removes the catchup destination without
// restoring the live feed. Idempotent. Call before the archive client closes.
// StopReplay needs the archive control session. Live restore is wasted work
// during module shutdown and may block on a closing client.
func (c *catchupControl) close() error {
	if c == nil {
		return nil
	}
	return errors.Join(c.StopCatchupReplays(), c.removeCatchupDestination(false))
}

// errNoLocalLogRecording reports no local log recording for catchup replay.
var errNoLocalLogRecording = errors.New("cluster: catchup: no local log recording")

// catchupDestinationURI is the follower unicast receive channel for catchup.
//
// Builds on logChannel, the configured cluster log channel template, so term
// geometry matches the live log and the recording taken from that channel.
// Returns empty when the endpoint cannot be applied to the channel.
func catchupDestinationURI(logChannel, catchupEndpoint string) string {
	ch, err := BuildChannelWithEndpoint(logChannel, catchupEndpoint)
	if err != nil {
		return ""
	}
	return ch
}

// catchupReplayChannel is the leader archive replay channel toward a follower
// catchup endpoint. Carries the log publication session id, linger=0, eos=false,
// and alias=log-catchup for driver-tool observability.
//
// Length on StartCatchupReplay is math.MaxInt64 so an active recording is
// followed (AeronArchive REPLAY_ALL_AND_FOLLOW / Long.MAX_VALUE).
func catchupReplayChannel(
	logChannel string,
	catchupEndpoint string,
	logSessionID int32,
) string {
	ch := catchupDestinationURI(logChannel, catchupEndpoint)
	if logSessionID != -1 {
		ch = sessionScopedLogChannel(ch, logSessionID)
	}
	ch = aeron.AppendChannelParam(ch, "linger=", "0")
	ch = aeron.AppendChannelParam(ch, "eos=", "false")
	ch = aeron.AppendChannelParam(ch, channelAliasParam, "log-catchup")
	return ch
}
