package consensus

import (
	"luas.fast/aeron"
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
)

// logJoinUnpublished is the logJoinWaitsAhead value while a tracked JoinLog
// waits for a successful publish.
const logJoinUnpublished = -1

// ReplayLogJoin holds the JoinLog fields for one archive replay.
type ReplayLogJoin struct {
	// StartPosition is the replay start. The JoinLog sends it as LogPosition.
	StartPosition int64

	// StopPosition is the replay stop. The JoinLog sends it as
	// MaxLogPosition.
	StopPosition int64

	// LogSessionID is the session id of the replay image.
	LogSessionID int32

	// LogStreamID is the replay stream id.
	LogStreamID int32

	// LogChannel is the session-scoped replay channel the module subscribed.
	LogChannel string
}

// LogAdapter returns the wired module log adapter, or nil.
//
// Concurrency: invoker-only.
func (a *Agent) LogAdapter() *LogAdapter {
	return a.logAdapter
}

// BindReplayLog binds image into the module log adapter. The adapter clears
// its fragment assembler.
//
// A nil adapter ignores the call.
//
// Concurrency: invoker-only.
//
// Ownership: does not own image or the image it replaces. The caller closes
// both.
func (a *Agent) BindReplayLog(image *aeron.Image) {
	if a.logAdapter == nil {
		return
	}
	a.logAdapter.SetImage(image)
}

// ReplayLogPoll polls the module log adapter under stopPosition. It then
// raises commit to the adapter position.
//
// Behaviour:
//   - A nil adapter or an adapter with no image returns 0.
//   - Outside ACTIVE and SUSPENDED, the call does not poll and returns 0.
//   - A pending snapshot wait lowers the bound to its log position. After
//     the pop, the module snapshot take holds the bound there until the take
//     resolves.
//   - On a non-leader, a pending JoinLog publish lowers the bound to the
//     JoinLog position.
//   - nowNs becomes the session learn time for the poll.
//
// Concurrency: invoker-only.
//
// Returns the fragment count.
func (a *Agent) ReplayLogPoll(stopPosition, nowNs int64) int {
	adapter := a.logAdapter
	if adapter == nil || adapter.Image() == nil || !a.isActiveOrSuspended() {
		return 0
	}
	return a.pollBoundLog(adapter, stopPosition, nowNs)
}

// UnbindReplayLog clears the replay join tracking. It then clears the adapter
// image when the adapter still holds image.
//
// Behaviour: an unpublished replay JoinLog drops, so a dead replay session does
// not reach a service. A published replay JoinLog keeps its wait.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
//
// Ownership: does not own image. The caller closes it.
func (a *Agent) UnbindReplayLog(image *aeron.Image) {
	if a.logJoinWaitsAhead == logJoinUnpublished {
		a.pendingJoinLog = nil
	}
	a.logJoinWaitsAhead = 0
	adapter := a.logAdapter
	if adapter != nil && adapter.Image() == image {
		adapter.SetImage(nil)
	}
}

// PublishReplayJoinLog publishes the replay JoinLog, or stores it for retry.
//
// Behaviour:
//   - On a nil service proxy, the call is a silent no-op.
//   - The call replaces any stored pending JoinLog.
//   - A successful publish queues one wait at the replay start, unless
//     serviceAckFailure is marked.
//   - A terminal publish code marks [Agent.ServicePublishFailure].
//   - While serviceAckFailure is nil, [Agent.IsReplayLogJoined] reports false
//     until the replay JoinLog wait pops. A terminal publish code,
//     [Agent.UnbindReplayLog], or a new election clears the tracking earlier.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
func (a *Agent) PublishReplayJoinLog(join ReplayLogJoin) {
	if a.serviceProxy == nil {
		return
	}
	a.logJoinWaitsAhead = logJoinUnpublished
	a.publishOrQueueJoinLog(codecs.JoinLogFields{
		LogPosition:    join.StartPosition,
		MaxLogPosition: join.StopPosition,
		MemberID:       a.thisMemberID,
		LogSessionID:   join.LogSessionID,
		LogStreamID:    join.LogStreamID,
		IsStartup:      clusterwire.BooleanType_TRUE,
		Role:           RoleFollower.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(join.LogChannel),
	})
}

// IsReplayLogJoined reports whether every service acked the replay JoinLog.
//
// Behaviour:
//   - Reports false once serviceAckFailure is marked.
//   - Otherwise reports true when the agent does not track a JoinLog.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
func (a *Agent) IsReplayLogJoined() bool {
	return a.serviceAckFailure == nil && a.logJoinWaitsAhead == 0
}

// markLogJoinPublished starts the join count once a tracked JoinLog publishes.
// Its wait is the last queued entry, so the count is the FIFO length. While
// serviceAckFailure is marked, publishJoinLog does not queue a wait, so the
// count is 0.
func (a *Agent) markLogJoinPublished() {
	if a.logJoinWaitsAhead != logJoinUnpublished {
		return
	}
	if a.serviceAckFailure != nil {
		a.logJoinWaitsAhead = 0
		return
	}
	a.logJoinWaitsAhead = len(a.serviceAckWaits)
}
