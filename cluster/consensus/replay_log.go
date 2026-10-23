package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// replayJoinUnpublished is the replayJoinWaitsAhead value while the replay
// JoinLog waits for a successful publish.
const replayJoinUnpublished = -1

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
	if adapter == nil || adapter.Image() == nil {
		return 0
	}
	return a.pollBoundLog(adapter, stopPosition, nowNs)
}

// UnbindReplayLog clears the replay join tracking. It then clears the adapter
// image when the adapter still holds image.
//
// Behaviour:
//   - The tracking clear does not depend on the image match.
//   - The adapter keeps the image position after the clear.
//   - [Agent.IsReplayLogJoined] reports true after the call.
//   - An unpublished replay JoinLog drops, so a dead replay session reaches
//     no service. A published replay JoinLog keeps its wait.
//
// Concurrency: invoker-only.
//
// Ownership: does not own image. The caller closes it.
func (a *Agent) UnbindReplayLog(image *aeron.Image) {
	if a.replayJoinWaitsAhead == replayJoinUnpublished {
		a.pendingJoinLog = nil
	}
	a.replayJoinWaitsAhead = 0
	adapter := a.logAdapter
	if adapter != nil && adapter.Image() == image {
		adapter.SetImage(nil)
	}
}

// PublishReplayJoinLog publishes the replay JoinLog, or stores it for retry.
//
// Behaviour:
//   - A nil service proxy changes nothing.
//   - The JoinLog carries the join fields, this member id, IsStartup TRUE,
//     role follower and IsStandby FALSE.
//   - The call replaces any stored pending JoinLog.
//   - A successful publish queues one wait at the replay start, unless
//     serviceAckFailure is marked.
//   - A terminal publish code marks [Agent.ServicePublishFailure].
//   - While serviceAckFailure is nil, [Agent.IsReplayLogJoined] reports
//     false until the replay JoinLog wait pops, a terminal publish code
//     lands, [Agent.UnbindReplayLog] runs, or a new election prepares
//     through [Agent.PrepareForNewLeadership] or enterElection.
//
// Concurrency: invoker-only.
func (a *Agent) PublishReplayJoinLog(join ReplayLogJoin) {
	if a.serviceProxy == nil {
		return
	}
	a.replayJoinWaitsAhead = replayJoinUnpublished
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
//   - Reports true when no replay JoinLog is tracked.
//   - Reports true once serviceAckFailure is marked. No wait pops after the
//     mark.
//
// Concurrency: invoker-only.
func (a *Agent) IsReplayLogJoined() bool {
	return a.replayJoinWaitsAhead == 0 || a.serviceAckFailure != nil
}

// markReplayJoinPublished starts the replay join count once the replay JoinLog
// publishes. The count is the FIFO length, so the replay JoinLog wait pops
// last. A marked serviceAckFailure queues no wait, so the count is 0.
func (a *Agent) markReplayJoinPublished() {
	if a.replayJoinWaitsAhead != replayJoinUnpublished {
		return
	}
	if a.serviceAckFailure != nil {
		a.replayJoinWaitsAhead = 0
		return
	}
	a.replayJoinWaitsAhead = len(a.serviceAckWaits)
}
