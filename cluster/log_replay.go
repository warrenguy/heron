package cluster

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/consensus"
)

const (
	// replayEndedShortReason names a short close of the replay image before
	// the bound. Ordinary end-of-replay, not a construction fault.
	replayEndedShortReason = "replay ended before the bound"

	// servicesNotJoinedReason names a replay whose services did not ack the
	// replay JoinLog before the deadline.
	servicesNotJoinedReason = "services did not join the log replay"
)

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

// logReplayDeps are the collaborators a cluster log replay needs. All fields
// are required except Ctx, which takes a default when nil.
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

	// ImageAwaitNs bounds the wait for the replay image. From the image bind,
	// it also bounds the wait for the services to join the replay. The field
	// is required and must be positive. OpenConsensusModule passes the
	// election leader heartbeat timeout.
	//
	// This package does not default ImageAwaitNs. The default lives on
	// election.ElectionConfig, which this file must not import.
	ImageAwaitNs int64
}

// logReplay replays this member's own cluster log through the module log
// adapter. The adapter feeds every frame to the module and raises commit.
//
// Behaviour:
//   - Bound-image mode: the adapter holds an open image at the replay start.
//     DoWork drains that image. No archive replay runs, and the services
//     read the range from their own log image.
//   - Archive mode: the constructor starts the archive replay and registers
//     the subscription. DoWork finds the replay image and binds it into the
//     adapter. It sends the replay JoinLog to the services. Once every
//     service acked the JoinLog, DoWork drains one bounded batch per call.
//   - A pending snapshot wait holds the drain at the action. The replay is
//     not done at the bound while the wait pends, or while the module
//     snapshot take of the popped wait is unresolved.
//   - Position reports how far the replay ran. IsDone is sticky.
//
// Concurrency: invoker-only.
//
// Lifetime: the helper does not outlive the election state that built it. The
// election builds the helper on entry to a replay state and closes the helper
// on every exit.
//
// Ownership: owns the archive replay session, the replay subscription and the
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

// newLogReplay picks the replay mode. Archive mode starts the archive replay
// and registers the session-filtered subscription.
//
// Behaviour: builds one replay over the member's own log. The checks run in
// this order:
//  1. stopPosition at or below startPosition: done at startPosition.
//  2. The module log adapter holds an open image at startPosition:
//     bound-image mode.
//  3. No local log recording: done at startPosition.
//  4. Otherwise archive mode.
//
// The helper is never nil. A construction failure returns a helper that is
// done at startPosition. The election then retries instead of recording a
// position the replay did not reach.
//
// Concurrency: invoker-only. Blocks the invoker for the archive control
// response and the subscription registration.
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

// DoWork drives one await, join or drain step.
//
// Behaviour:
//   - Bound-image mode drains on every call.
//   - Archive mode awaits the image first. On discovery DoWork binds the
//     image, sends the replay JoinLog and sets the services deadline.
//   - Archive mode then drains nothing until the replay JoinLog publishes
//     and every service acked it. Past the services deadline the replay is
//     done at its start.
//
// Returns:
//   - 0 when closed or done, when still awaiting the image or the services
//     inside the bound, or when a drain receives nothing
//   - 1 when the image is first bound
//   - the fragment count from one drain batch
func (r *logReplay) DoWork(nowNs int64) int {
	if r.closed || r.progress.done {
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
	// A marked service ack failure reports the replay joined. An unpublished
	// replay JoinLog has reached no service, so the drain waits and the
	// deadline keeps running.
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

// drain polls one batch through the module log adapter, then records the
// adapter position.
//
// Behaviour: the adapter no longer holding the replay image counts as a closed
// image. drain then skips the poll, so another image never feeds the module,
// raises commit or advances the replay.
func (r *logReplay) drain(nowNs int64) int {
	agent := r.deps.Agent
	adapter := agent.LogAdapter()
	if adapter.Image() != r.image {
		r.progress.observe(r.progress.position, true, agent.HasPendingSnapshotAction())
		return 0
	}
	received := agent.ReplayLogPoll(r.progress.stopPosition, nowNs)
	r.progress.observe(adapter.Position(), adapter.IsImageClosed(), agent.HasPendingSnapshotAction())
	return received
}

// Position reports how far the replay has run.
func (r *logReplay) Position() int64 {
	return r.progress.position
}

// IsDone reports whether the replay has finished. Once true, stays true.
func (r *logReplay) IsDone() bool {
	return r.progress.done
}

// Close unbinds the replay image, stops the archive replay session and closes
// the subscription.
//
// Behaviour:
//   - sets closed before touching the session or subscription so a later
//     DoWork must not treat the helper's own teardown as an external stop
//   - archive mode: calls [consensus.Agent.UnbindReplayLog] once the image
//     was bound. The adapter drops the image only while it still holds it.
//     The call also drops an unpublished replay JoinLog
//   - bound-image mode leaves the adapter image bound
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
	if r.image != nil && !r.imageBound {
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

// reason returns why the replay stopped. Returns the empty string when the
// replay reached its bound or is still running.
func (r *logReplay) reason() string {
	return r.progress.reason
}

// closeError returns the joined StopReplay and subscription Close error, or nil
// when both succeeded.
func (r *logReplay) closeError() error {
	return r.closeErr
}

// replayProgress tracks how far a cluster log replay has run and why the replay
// stopped.
//
// Behaviour: position never goes backwards and never exceeds stopPosition. done
// is sticky: once true, done stays true and position stops moving.
type replayProgress struct {
	startPosition int64
	stopPosition  int64
	position      int64
	done          bool
	reason        string
}

// observe records one drained batch. adapterPosition is the adapter position
// after the batch. closedShort reports a closed replay image. held reports a
// pending snapshot action, as [consensus.Agent.HasPendingSnapshotAction] does.
//
// Behaviour:
//   - At the bound and not held: done.
//   - At the bound and held: not done. A later call checks again.
//   - Closed below the bound: done short, whatever held reports.
func (p *replayProgress) observe(adapterPosition int64, closedShort, held bool) {
	if p.done {
		return
	}
	p.position = min(max(p.position, adapterPosition), p.stopPosition)
	if p.position >= p.stopPosition {
		if !held {
			p.markDone("")
		}
		return
	}
	if closedShort {
		p.markDone(replayEndedShortReason)
	}
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
