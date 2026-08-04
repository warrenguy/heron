package cluster

import (
	"context"
	"errors"
	"math"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// catchupControl is the production [consensus.CatchupControl].
//
// Behaviour: on the follower, opens a unicast subscription on the member
// catchup endpoint so the leader archive can replay the log gap. Join
// selection prefers that subscription over the live multicast log subscription
// when matching by session. On the leader, starts an archive replay of the
// local cluster log recording toward the follower catchup endpoint with the
// log publication session id. Length is math.MaxInt64 so an active recording
// is followed.
//
// Concurrency: invoker-only.
//
// Ownership: owns the catchup subscription and every catchup replay this
// control started. Close with the module, before the archive client closes.
// Does not own the archive client, transport client, or agent.
type catchupControl struct {
	ctx context.Context

	// control issues StartReplay / StopReplay.
	control replayControl

	// adder registers the catchup subscription.
	adder SubscriptionAdder

	// agent supplies the local log recording id and takes the catchup join
	// source. Not closed by this type.
	agent *consensus.Agent

	// liveJoin is the live cluster log join source. Prefer catchup images over
	// liveJoin while a catchup subscription is open.
	liveJoin consensus.LogJoinSource

	// logStreamID is the cluster log stream id (same as the live log).
	logStreamID int32

	// logChannel is the cluster log channel template the live log and its
	// recording use. Catchup builds on the template so term geometry matches.
	logChannel string

	// catchupSub is the follower catchup receive subscription. Nil until open.
	catchupSub *aeron.Subscription

	// catchupChannel is the destination URI catchupSub was opened on. Empty
	// while catchupSub is nil.
	catchupChannel string

	// replaySessionIDs are the leader-side catchup replays started and not yet
	// stopped. StopCatchupReplays drains this slice.
	replaySessionIDs []int64
}

var _ consensus.CatchupControl = (*catchupControl)(nil)

// newCatchupControl builds a catchup control. All fields are required.
func newCatchupControl(
	ctx context.Context,
	control replayControl,
	adder SubscriptionAdder,
	agent *consensus.Agent,
	liveJoin consensus.LogJoinSource,
	logStreamID int32,
	logChannel string,
) *catchupControl {
	return &catchupControl{
		ctx:         moduleControlContext(ctx),
		control:     control,
		adder:       adder,
		agent:       agent,
		liveJoin:    liveJoin,
		logStreamID: logStreamID,
		logChannel:  logChannel,
	}
}

// EnsureCatchupLogDestination opens the catchup receive subscription when
// needed and installs a join source that prefers catchup images.
//
// Destination is the configured cluster log channel with
// endpoint=<catchupEndpoint>.
//
// The subscription is not session-filtered. The leader replay channel carries
// session-id. Join selection keys on the log session id through the join
// source.
//
// Returns the destination URI, or empty on open failure.
func (c *catchupControl) EnsureCatchupLogDestination(catchupEndpoint string) string {
	if c == nil || catchupEndpoint == "" {
		return ""
	}
	if c.catchupSub != nil {
		return c.catchupChannel
	}
	channel := catchupDestinationURI(c.logChannel, catchupEndpoint)

	sub, err := addSubscriptionChecked(
		c.adder,
		c.logStreamID,
		channel,
		"catchup log subscription",
	)
	if err != nil {
		return ""
	}
	c.catchupSub = sub
	c.catchupChannel = channel
	// Agent holds the join source until RemoveCatchupLogDestination restores
	// liveJoin.
	c.agent.SetLogJoinSource(&preferCatchupJoinSource{
		catchup: consensus.NewSubscriptionLogJoinSource(sub),
		live:    c.liveJoin,
	})
	return channel
}

// RemoveCatchupLogDestination closes the catchup receive subscription and
// restores the live log join source. Idempotent.
func (c *catchupControl) RemoveCatchupLogDestination() error {
	if c == nil || c.catchupSub == nil {
		return nil
	}
	// Restore the live join source before closing the catchup sub.
	if c.liveJoin != nil {
		c.agent.SetLogJoinSource(c.liveJoin)
	}
	err := c.catchupSub.Close()
	c.catchupSub = nil
	c.catchupChannel = ""
	return err
}

// StartCatchupReplay starts an archive replay of the local log recording and
// retains the replay session id for StopCatchupReplays.
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

// close stops leader-side replays and releases the catchup subscription. Idempotent.
// Call before the archive client closes. StopReplay needs the archive control
// session.
func (c *catchupControl) close() error {
	if c == nil {
		return nil
	}
	return errors.Join(c.StopCatchupReplays(), c.RemoveCatchupLogDestination())
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
// catchup endpoint, with the log publication session id and no linger.
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
	return ch
}

// preferCatchupJoinSource prefers an open catchup image over the live log
// image for the same log session id.
//
// Behaviour: when the catchup source has an open image for the session, Ready
// and Image return that image. Otherwise the live source is used. A late-join
// live image with a join position above the follower log position is still
// reported by the live source. The election await path rejects a join position
// mismatch and waits for the catchup image.
//
// Concurrency: invoker-only.
type preferCatchupJoinSource struct {
	catchup consensus.LogJoinSource
	live    consensus.LogJoinSource
}

var _ consensus.LogJoinSource = (*preferCatchupJoinSource)(nil)

func (s *preferCatchupJoinSource) Image(logSessionID int32) *aeron.Image {
	if s == nil {
		return nil
	}
	if s.catchup != nil {
		if img := s.catchup.Image(logSessionID); img != nil {
			return img
		}
	}
	if s.live != nil {
		return s.live.Image(logSessionID)
	}
	return nil
}

func (s *preferCatchupJoinSource) Ready(
	logSessionID int32,
) (consensus.FollowerLogJoinParams, bool) {
	if s == nil {
		return consensus.FollowerLogJoinParams{}, false
	}
	if s.catchup != nil {
		if params, ok := s.catchup.Ready(logSessionID); ok {
			return params, true
		}
	}
	if s.live != nil {
		return s.live.Ready(logSessionID)
	}
	return consensus.FollowerLogJoinParams{}, false
}
