package cluster

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/consensus"
)

// replayEndedShortReason names a short close of the replay image before the
// bound. Ordinary end-of-replay, not a construction fault.
const replayEndedShortReason = "replay ended before the bound"

// discardReplayFragment advances the replay image position and reads nothing
// from the fragment. No production path wires a cluster log decode handler. The
// production cluster log poll discards payloads the same way.
var discardReplayFragment = aeron.AdaptFragmentHandlerBytes(
	func(payload []byte, header *aeron.Header) {},
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
// are required except Ctx and FragmentLimit, which take defaults when zero.
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

	// Agent supplies the local log recording id and takes the replayed commit
	// position. Construction reads the recording id once. Open does not re-read
	// the id.
	Agent *consensus.Agent

	// ReplayChannel and ReplayStreamID come from [Topology].
	ReplayChannel  string
	ReplayStreamID int32

	// FragmentLimit caps one duty-cycle drain. Zero takes
	// consensus.DefaultLogFragmentLimit.
	FragmentLimit int

	// ImageAwaitNs bounds the wait for the replay image. The field is required
	// and must be positive. OpenConsensusModule passes the election leader
	// heartbeat timeout.
	//
	// This package does not default ImageAwaitNs. The default lives on
	// election.ElectionConfig, which this file must not import.
	ImageAwaitNs int64
}

// logReplay replays this member's own cluster log recording into the member's
// commit position.
//
// Behaviour: the constructor starts the archive replay and registers the
// subscription. DoWork finds the replay image, then drains one bounded batch
// per call. Position reports how far the replay ran. IsDone is sticky.
//
// Concurrency: invoker-only.
//
// Lifetime: the helper does not outlive the election state that built it. The
// election builds the helper on entry to a replay state and closes the helper
// on every exit.
//
// Ownership: owns the archive replay session and the replay subscription. Does
// not own the archive client, the transport client, or the agent.
type logReplay struct {
	deps            logReplayDeps
	progress        replayProgress
	replaySessionID int64
	subscription    *aeron.Subscription
	image           *aeron.Image
	deadlineNs      int64
	closed          bool
	closeErr        error
}

var _ consensus.LogReplay = (*logReplay)(nil)

// newLogReplay starts the archive replay and registers the session-filtered
// subscription.
//
// Behaviour: builds one replay over the member's own local log recording. The
// helper is never nil. A construction failure returns a helper that is done at
// startPosition. The election then retries instead of recording a position the
// replay did not reach.
//
// Concurrency: invoker-only. Blocks the invoker for the archive control
// response and the subscription registration.
func newLogReplay(
	deps logReplayDeps,
	startPosition int64,
	stopPosition int64,
) *logReplay {
	if deps.Ctx == nil {
		deps.Ctx = context.Background()
	} else {
		deps.Ctx = context.WithoutCancel(deps.Ctx)
	}
	if deps.FragmentLimit <= 0 {
		deps.FragmentLimit = consensus.DefaultLogFragmentLimit
	}

	r := &logReplay{
		deps: deps,
		progress: replayProgress{
			startPosition: startPosition,
			stopPosition:  stopPosition,
			position:      startPosition,
		},
	}

	recordingID := deps.Agent.LocalLogRecordingID()
	if recordingID < 0 {
		r.progress.markDone("no local log recording")
		return r
	}

	if stopPosition <= startPosition {
		r.progress.markDone("replay bound not above start position")
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

// DoWork drives one await or drain step.
//
// Returns:
//   - 0 when closed or done, when still awaiting the image inside the bound, or
//     when a drain receives nothing
//   - 1 when the image is first bound
//   - the fragment count from one drain batch
func (r *logReplay) DoWork(nowNs int64) int {
	if r.closed || r.progress.done {
		return 0
	}

	// Factory has no clock. Record the image-await deadline on the first DoWork
	// call.
	if r.deadlineNs == 0 {
		r.deadlineNs = nowNs + r.deps.ImageAwaitNs
	}

	if r.image == nil {
		return r.awaitImage(nowNs)
	}
	return r.drain()
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
	return 1
}

func (r *logReplay) drain() int {
	// stopPosition 0 disables the archive position bound check. The poll done
	// flag then means only "drained this call", not "reached the replay bound".
	// replayProgress.observe owns the bound test.
	received, _, err := archive.PollReplayFragments(
		r.image,
		r.deps.FragmentLimit,
		0,
		0,
		discardReplayFragment,
	)

	// err != nil means the image is closed. A nil image or nil handler is
	// unreachable after a successful bind with the package-level handler.
	r.progress.observe(r.image.Position(), err != nil)
	_ = r.deps.Agent.ProposeMaxCommit(r.progress.position)
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

// Close stops the archive replay session and closes the subscription.
//
// Behaviour:
//   - sets closed before touching the session or subscription so a later
//     DoWork must not treat the helper's own teardown as an external stop
//   - issues StopReplay when a live session may remain: the session was
//     started and the replay did not reach its bound
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
	r.image = nil

	// A retained subscription means construction started a session and did not
	// stop the session inline.
	if r.subscription == nil {
		return
	}
	if r.progress.position < r.progress.stopPosition {
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

// observe records one drained batch. imagePosition is the replay image position
// after the batch. closedShort reports that the poll refused because the image
// is closed.
func (p *replayProgress) observe(imagePosition int64, closedShort bool) {
	if p.done {
		return
	}
	p.position = min(max(p.position, imagePosition), p.stopPosition)
	if p.position >= p.stopPosition {
		p.markDone("")
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
