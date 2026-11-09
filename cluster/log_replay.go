package cluster

import (
	"context"
	"errors"
	"fmt"

	"luas.fast/aeron"
	"luas.fast/archive"
	"luas.fast/cluster/consensus"
)

const (
	// replayEndedShortReason names a short close of the replay image before
	// the bound. Ordinary end-of-replay, not a construction fault.
	replayEndedShortReason = "replay ended before the bound"

	// replayEndOfStreamReason names a replay image whose publisher ended the
	// stream below the stop position.
	replayEndOfStreamReason = "replay image reached end of stream before the bound"

	// replayStalledReason is the bound-image mode stall below the stop.
	// The image does not deliver for one ImageAwaitNs.
	replayStalledReason = "replay image delivered nothing before the deadline"

	// servicesNotJoinedReason names a replay whose services did not ack the
	// replay JoinLog before the deadline.
	servicesNotJoinedReason = "services did not join the log replay"

	replayHoldExpiredReason = "replay image closed before the services acked the snapshot action"
)

// ErrReplayHoldExpired reports an election log replay held at a snapshot
// wait after its image closed. A service still owed its ack one ImageAwaitNs
// later.
var ErrReplayHoldExpired = errors.New("cluster: replay hold expired after the image closed")

// replayControl is the archive control surface a cluster log replay needs.
//
// [*archive.Client] satisfies this interface. The compile-time assertion below
// fails the build when Client or this interface diverge.
//
// Concurrency: implementations block the caller until the archive replies.
type replayControl interface {
	StartReplay(
		ctx context.Context,
		recordingID int64,
		position int64,
		length int64,
		replayStreamID int32,
		replayChannel []byte,
	) (int64, error)

	StopReplay(ctx context.Context, replaySessionID int64) error
}

var _ replayControl = (*archive.Client)(nil)

// logReplayDeps carries the collaborators a cluster log replay needs.
//
// The caller must set every field except Ctx and ErrorHandler.
type logReplayDeps struct {
	// Ctx carries context values into the archive control calls. [newLogReplay]
	// detaches cancel, matching the log recording control. Nil takes
	// context.Background().
	Ctx context.Context

	// Control issues the replay start and stop control requests.
	Control replayControl

	// Adder registers the replay subscription. Production passes the connected
	// [*aeron.Client].
	Adder SubscriptionAdder

	// Agent supplies the local log recording id and the module log adapter.
	// The replay drains through the adapter, which feeds every frame to the
	// module and raises commit. The agent also sends the replay JoinLog to the
	// services. Construction reads the recording id once and picks the mode
	// from the adapter state. Each drain reads the adapter position and image
	// state again.
	Agent *consensus.Agent

	// ReplayChannel and ReplayStreamID come from [Topology].
	ReplayChannel  string
	ReplayStreamID int32

	// ImageAwaitNs is the cap on each wait:
	//   - the wait for the replay image
	//   - the wait for the services to join, after the image bind
	//   - in bound-image mode, each wait for the image to deliver
	//   - a queued snapshot wait after the replay image closes short of its
	//     end of stream
	//
	// The caller must set a positive ImageAwaitNs. OpenConsensusModule
	// passes the election leader heartbeat timeout.
	//
	// This package does not default ImageAwaitNs. The default lives on
	// election.ElectionConfig, which this file must not import.
	ImageAwaitNs int64

	// ErrorHandler receives [ErrReplayHoldExpired] before the replay ends the
	// member. Nil skips the report. OpenConsensusModule sets the module error
	// handler.
	ErrorHandler func(error)
}

// logReplay replays this member's own cluster log through the module log
// adapter.
//
// Behaviour:
//   - Bound-image mode drains an open image the adapter already holds at
//     the replay start. The services read the range from their own log
//     image. A bound image that does not deliver for one ImageAwaitNs ends
//     the replay.
//   - Archive mode replays the local recording through an archive session.
//     It drains only after every service acks the replay JoinLog. Past the
//     services deadline the replay finishes at its start.
//   - End of stream below the stop ends the replay in either mode.
//   - A pending snapshot wait or an unresolved module snapshot take holds
//     the drain at the action. End of stream or the stall deadline ends a
//     held replay only after the hold clears.
//   - A hold that expires on a closed image ends the member. IsDone stays
//     false. [replayProgress.stepHoldDeadline] states the expiry rule.
//
// Concurrency: call from the goroutine that runs [logReplay.DoWork]. Not safe
// from other goroutines.
//
// Lifetime: the helper does not outlive the election state that built it. The
// election builds the helper on entry to a replay state. The election closes
// the helper on every exit.
//
// Ownership: owns the archive replay session, the replay subscription, and the
// replay image binding. Does not own the archive client, the transport client,
// the agent, or a bound-image mode image.
type logReplay struct {
	deps            logReplayDeps
	progress        replayProgress
	replaySessionID int64
	subscription    *aeron.Subscription
	image           *aeron.Image
	imageBound      bool
	deadlineNs      int64
	closed          bool
	closeErr        error
}

var _ consensus.LogReplay = (*logReplay)(nil)

// newLogReplay picks the replay mode.
//
// The helper is never nil. A construction failure returns a helper that is
// done at startPosition. The election then retries instead of recording a
// position the replay did not reach.
//
// Concurrency: call from the goroutine that runs [logReplay.DoWork]. Not safe
// from other goroutines. The call blocks that goroutine for the archive
// control response and the subscription registration.
func newLogReplay(
	deps logReplayDeps,
	startPosition int64,
	stopPosition int64,
) *logReplay {
	deps.Ctx = moduleControlContext(deps.Ctx)

	r := &logReplay{
		deps: deps,
		progress: replayProgress{
			startPosition: startPosition,
			stopPosition:  stopPosition,
			position:      startPosition,
			holdTimeoutNs: deps.ImageAwaitNs,
		},
	}

	if stopPosition <= startPosition {
		r.progress.markDone("replay bound not above start position")
		return r
	}

	adapter := deps.Agent.LogAdapter()
	if !adapter.IsImageClosed() && adapter.Position() == startPosition {
		r.image = adapter.Image()
		r.imageBound = true
		r.progress.stallTimeoutNs = deps.ImageAwaitNs
		return r
	}

	recordingID := deps.Agent.LocalLogRecordingID()
	if recordingID < 0 {
		r.progress.markDone("no local log recording")
		return r
	}

	span := stopPosition - startPosition
	sessionID, err := deps.Control.StartReplay(
		deps.Ctx,
		recordingID,
		startPosition,
		span,
		deps.ReplayStreamID,
		[]byte(deps.ReplayChannel),
	)
	if err != nil {
		r.progress.markDone("start replay refused: " + err.Error())
		return r
	}
	r.replaySessionID = sessionID

	// Archive returns a composite session id. The subscription image carries
	// only the low 32 bits. Match archive.FindReplayImage and the production
	// cluster log path.
	//nolint:gosec // G115: the image carries the low 32 bits of the session id
	channel := sessionScopedLogChannel(deps.ReplayChannel, int32(uint32(sessionID)))

	sub, err := addSubscriptionChecked(
		deps.Adder,
		deps.ReplayStreamID,
		channel,
		"replay subscription",
	)
	if err != nil {
		stopErr := deps.Control.StopReplay(deps.Ctx, sessionID)
		r.closeErr = errors.Join(r.closeErr, stopErr)
		r.progress.markDone(err.Error())
		return r
	}
	r.subscription = sub
	return r
}

// DoWork drives one await, join, or drain step.
//
// Returns the fragment count from one drain batch, 1 when DoWork first binds
// the image, and 0 otherwise.
func (r *logReplay) DoWork(nowNs int64) int {
	if r.closed || r.progress.done || r.progress.holdExpired {
		return 0
	}
	if r.imageBound {
		return r.drain(nowNs)
	}

	// Factory has no clock. Set the image-await deadline on the first DoWork
	// call. The image bind resets it for the services wait.
	if r.deadlineNs == 0 {
		r.deadlineNs = nowNs + r.deps.ImageAwaitNs
	}

	if r.image == nil {
		return r.awaitImage(nowNs)
	}
	agent := r.deps.Agent
	if !agent.IsReplayLogJoined() || agent.HasPendingJoinLog() {
		if nowNs >= r.deadlineNs {
			r.progress.markDone(servicesNotJoinedReason)
		}
		return 0
	}
	return r.drain(nowNs)
}

func (r *logReplay) awaitImage(nowNs int64) int {
	image, found, err := archive.FindReplayImage(r.subscription, r.replaySessionID)
	if err != nil {
		r.progress.markDone("find replay image: " + err.Error())
		return 0
	}
	if !found {
		if nowNs >= r.deadlineNs {
			r.progress.markDone("replay image did not appear")
		}
		return 0
	}
	if image.JoinPosition() != r.progress.startPosition {
		r.progress.markDone("replay image join position mismatch")
		return 0
	}
	r.image = image
	agent := r.deps.Agent
	agent.BindReplayLog(image)
	agent.PublishReplayJoinLog(consensus.ReplayLogJoin{
		StartPosition: r.progress.startPosition,
		StopPosition:  r.progress.stopPosition,
		LogSessionID:  image.SessionID(),
		LogStreamID:   r.deps.ReplayStreamID,
		LogChannel:    sessionScopedLogChannel(r.deps.ReplayChannel, image.SessionID()),
	})
	r.deadlineNs = nowNs + r.deps.ImageAwaitNs
	return 1
}

// drain polls one batch through the module log adapter.
//
// Behaviour:
//   - An adapter that no longer holds the replay image counts as closed.
//     drain skips the poll, so a different image does not feed the module,
//     raise commit, or advance the replay.
//   - drain reads the hold kind after the poll. The poll can queue a
//     snapshot wait.
func (r *logReplay) drain(nowNs int64) int {
	agent := r.deps.Agent
	adapter := agent.LogAdapter()

	if adapter.Image() != r.image {
		r.observeDrain(nowNs, r.progress.position, replayImageClosed)
		return 0
	}

	received := agent.ReplayLogPoll(r.progress.stopPosition, nowNs)
	r.observeDrain(nowNs, adapter.Position(), replayImageStateOf(adapter))

	return received
}

// observeDrain stores one drain observation. An expired hold ends the member.
// [logReplay.DoWork] does not drain after an expiry, so the fault report runs
// once.
func (r *logReplay) observeDrain(
	nowNs, adapterPosition int64,
	image replayImageState,
) {
	agent := r.deps.Agent

	r.progress.observe(nowNs, adapterPosition, image, replayHoldOf(agent))

	if !r.progress.holdExpired {
		return
	}

	err := fmt.Errorf("%w: snapshot wait at position %d", ErrReplayHoldExpired, r.progress.position)

	if r.deps.ErrorHandler != nil {
		r.deps.ErrorHandler(err)
	}

	agent.TerminateOnError(err)
}

// replayImageStateOf reports closed for a nil adapter or a nil image.
func replayImageStateOf(adapter *consensus.LogAdapter) replayImageState {
	switch {
	case adapter.IsLogEndOfStream():
		return replayImageEndOfStream
	case adapter.IsImageClosed():
		return replayImageClosed
	}

	return replayImageOpen
}

// Position reports how far the replay has run.
func (r *logReplay) Position() int64 {
	return r.progress.position
}

// IsDone reports whether the replay has finished. Once true, stays true.
func (r *logReplay) IsDone() bool {
	return r.progress.done
}

// Close stops the archive replay. It closes a retained subscription.
//
// Behaviour:
//   - sets closed before touching the session or subscription so a later
//     DoWork must not treat the helper's own teardown as an external stop
//   - archive mode: calls [consensus.Agent.UnbindReplayLog] once the image
//     was bound. The adapter drops the image only while it still holds it.
//     The call also drops an unpublished replay JoinLog
//   - bound-image mode calls [consensus.Agent.UnbindReplayLog] only after
//     end of stream or the stall deadline
//   - issues StopReplay when a live session may remain: the session was
//     started and the replay did not reach its bound. The stop blocks on the
//     module archive client, so it first cancels a module snapshot take in
//     flight through [consensus.Agent.CloseSnapshotTake]. The take shares
//     that client and may hold an open handle
//   - skips the stop await when a length-bounded session already delivered
//     its full range
//   - closes the subscription when construction retained one
//
// Idempotent.
//
// Concurrency: invoker-only. May block for the archive control response.
func (r *logReplay) Close() {
	if r.closed {
		return
	}
	r.closed = true
	if r.image != nil && (!r.imageBound || r.progress.hasEndedWithoutClose()) {
		r.deps.Agent.UnbindReplayLog(r.image)
	}
	r.image = nil

	// A retained subscription means construction started a session and did not
	// stop the session inline.
	if r.subscription == nil {
		return
	}
	if r.progress.position < r.progress.stopPosition {
		r.deps.Agent.CloseSnapshotTake()
		// No liveness pre-check. StopReplay on an unknown session replies OK.
		r.closeErr = errors.Join(
			r.closeErr,
			r.deps.Control.StopReplay(r.deps.Ctx, r.replaySessionID),
		)
	}
	r.closeErr = errors.Join(r.closeErr, r.subscription.Close())
	r.subscription = nil
}

// reason returns why the replay stopped. Returns the empty string unless the
// replay ended short or a hold expired.
func (r *logReplay) reason() string {
	return r.progress.reason
}

// closeError returns the joined StopReplay and subscription Close error, or nil
// when both succeeded.
func (r *logReplay) closeError() error {
	return r.closeErr
}

// replayImageState reports whether the replay image can still deliver
// frames below the stop position.
type replayImageState uint8

const (
	// replayImageOpen is an open image below its end of stream.
	replayImageOpen replayImageState = iota

	// replayImageEndOfStream is an open or closed image whose subscriber
	// position reached the end-of-stream position its publisher sent.
	replayImageEndOfStream

	// replayImageClosed is an image closed short of its end of stream, or an
	// adapter that no longer holds the replay image.
	replayImageClosed
)

// replayHold reports what holds the replay at a snapshot action.
type replayHold uint8

const (
	replayHoldNone replayHold = iota

	// replayHoldServiceAcks is a queued snapshot wait. A service still owes
	// its ack.
	replayHoldServiceAcks

	// replayHoldSnapshotTake is a module snapshot take after the wait pops.
	replayHoldSnapshotTake
)

// replayHoldOf checks the queued wait first.
// [consensus.Agent.HasPendingSnapshotAction] also reports that wait.
func replayHoldOf(agent *consensus.Agent) replayHold {
	switch {
	case agent.HasPendingSnapshotWait():
		return replayHoldServiceAcks
	case agent.HasPendingSnapshotAction():
		return replayHoldSnapshotTake
	}
	return replayHoldNone
}

// replayProgress tracks how far a cluster log replay has run and why the
// replay stopped.
//
// Behaviour:
//   - position never goes backwards
//   - position never exceeds stopPosition
//   - done marks once. After done, position stops moving.
//   - holdExpired marks once. After that mark, position, done, and reason
//     stay unchanged.
//
// Zero stallTimeoutNs turns the stall deadline off. stallDeadlineNs stays
// zero until [replayProgress.observe] sets it.
//
// holdTimeoutNs is positive.
type replayProgress struct {
	startPosition   int64
	stopPosition    int64
	position        int64
	done            bool
	reason          string
	stallTimeoutNs  int64
	stallDeadlineNs int64
	holdTimeoutNs   int64
	holdDeadlineNs  int64
	holdExpired     bool
}

// observe stores one drained batch. nowNs is the caller clock.
// adapterPosition is the adapter position after the batch. An expiry does not
// finish the replay.
func (p *replayProgress) observe(
	nowNs, adapterPosition int64,
	image replayImageState,
	hold replayHold,
) {
	if p.done || p.holdExpired {
		return
	}

	previous := p.position
	p.position = min(max(p.position, adapterPosition), p.stopPosition)

	if p.stepHoldDeadline(nowNs, image, hold) {
		p.holdExpired = true
		p.reason = replayHoldExpiredReason

		return
	}

	held := hold != replayHoldNone

	if p.position >= p.stopPosition {
		if !held {
			p.markDone("")
		}

		return
	}

	if held {
		// Not read when stallTimeoutNs is zero.
		p.stallDeadlineNs = nowNs + p.stallTimeoutNs

		return
	}

	switch image {
	case replayImageEndOfStream:
		p.markDone(replayEndOfStreamReason)
	case replayImageClosed:
		p.markDone(replayEndedShortReason)

	case replayImageOpen:
		switch {
		case p.stallTimeoutNs == 0:
		case p.position > previous || p.stallDeadlineNs == 0:
			p.stallDeadlineNs = nowNs + p.stallTimeoutNs
		case nowNs >= p.stallDeadlineNs:
			p.markDone(replayStalledReason)
		}
	}
}

// stepHoldDeadline advances the hold deadline.
//
// Behaviour:
//   - A service-ack hold on [replayImageClosed] expires one holdTimeoutNs
//     after the first such call.
//   - A close at end of stream does not expire. Each service still on the
//     image has reached the action and owes only the ack after its take.
//   - A call without that service-ack hold clears the deadline.
//
// Returns true when nowNs reaches the deadline.
func (p *replayProgress) stepHoldDeadline(
	nowNs int64,
	image replayImageState,
	hold replayHold,
) bool {
	closedAck := hold == replayHoldServiceAcks && image == replayImageClosed
	if !closedAck {
		p.holdDeadlineNs = 0

		return false
	}

	if p.holdDeadlineNs == 0 {
		p.holdDeadlineNs = nowNs + p.holdTimeoutNs

		return false
	}

	return nowNs >= p.holdDeadlineNs
}

// hasEndedWithoutClose reports a replay done short at end of stream or the
// stall deadline.
func (p *replayProgress) hasEndedWithoutClose() bool {
	return p.done &&
		(p.reason == replayEndOfStreamReason || p.reason == replayStalledReason)
}

// markDone marks the replay finished at the position already recorded. Idempotent:
// second and later calls change nothing.
func (p *replayProgress) markDone(reason string) {
	if p.done {
		return
	}
	p.done = true
	p.reason = reason
}
