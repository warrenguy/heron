package consensus

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

// DefaultLeaderHeartbeatTimeoutNs bounds the settled quiet wait before a
// non-leader re-enters an election (10s).
//
// The quiet path reads timeOfLastLogUpdateNs (settled log-update clock). The
// quiet path does not read timeOfLastLeaderUpdateNs (ingress leader-update
// clock / canvass base).
//
// DefaultElectionConfig in the election package uses this constant for
// LeaderHeartbeatTimeoutNs so the settled agent and the election share one
// timeout value.
const DefaultLeaderHeartbeatTimeoutNs int64 = 10_000_000_000

// slowTickIntervalNs paces the settled-state timeout checks (10ms).
const slowTickIntervalNs int64 = 10_000_000

// ElectionTrigger names the condition that installed the current election.
//
// Behaviour:
//   - [ElectionTriggerBootstrap]: open-time install
//   - all other values: settled-state re-install; each maps to one condition
//   - [ElectionTriggerNone]: no election installed yet
type ElectionTrigger int32

const (
	// ElectionTriggerNone is the value before any election is installed.
	ElectionTriggerNone ElectionTrigger = iota

	// ElectionTriggerBootstrap is the install the assembly performs at open.
	ElectionTriggerBootstrap

	// ElectionTriggerLeaderHeartbeatTimeout is a non-leader whose
	// timeOfLastLogUpdateNs is older than the leader heartbeat timeout.
	ElectionTriggerLeaderHeartbeatTimeout

	// ElectionTriggerInactiveFollowerQuorum is a leader whose members no longer
	// reach a quorum of active append positions.
	//
	// The constant name matches the upstream reason string. The check is
	// active-member quorum over the full member set (this member included), not
	// followers only.
	ElectionTriggerInactiveFollowerQuorum

	// ElectionTriggerHigherTerm is a settled member that received a leadership
	// term above its own.
	ElectionTriggerHigherTerm

	// ElectionTriggerLogRecordingStopped is a settled member whose own log
	// recording reported STOP.
	ElectionTriggerLogRecordingStopped

	// ElectionTriggerLogImageClosed is a non-leader whose log adapter holds a
	// closed image or no image.
	ElectionTriggerLogImageClosed
)

var electionTriggerNames = [...]string{
	"NONE",
	"BOOTSTRAP",
	"LEADER_HEARTBEAT_TIMEOUT",
	"INACTIVE_FOLLOWER_QUORUM",
	"HIGHER_TERM",
	"LOG_RECORDING_STOPPED",
	"LOG_IMAGE_CLOSED",
}

// String returns the trigger name, or Trigger(n) for an unknown value.
func (t ElectionTrigger) String() string {
	if t < 0 || int(t) >= len(electionTriggerNames) {
		return fmt.Sprintf("Trigger(%d)", int32(t))
	}
	return electionTriggerNames[t]
}

// ElectionSeed carries the settled state a re-entered election starts from.
//
// Behaviour: the agent fills every field from agent state. The factory resolves
// term base and append-position fallback. The agent does not hold those
// fallbacks.
type ElectionSeed struct {
	// LeadershipTermID is the term the member currently holds.
	LeadershipTermID int64

	// CommitPosition is the mode-1 commit value.
	CommitPosition int64

	// AppendPosition is the local append high-water. Meaningful only when
	// AppendPositionReady is true.
	AppendPosition int64

	// AppendPositionReady reports whether AppendPosition may be used. False
	// means the factory must substitute its own fallback.
	AppendPositionReady bool

	// GracefulClosedLeaderID is the leader whose log reached end of stream
	// when the trigger fired. Else the null member id.
	GracefulClosedLeaderID int32
}

// ElectionFactory builds a fresh [ElectionRunner] from settled state.
//
// Behaviour:
//   - retains election config, election deps, and recovery plan (via the
//     cluster factory wired at open)
//   - must not panic
//   - returns nil to refuse the install
//
// Concurrency: invoker-only. Called from [Agent.DoWork].
//
// Ownership: the agent owns the returned runner until [Agent.ElectionComplete]
// or [Agent.SetElection] with a nil runner clears it. The factory does not own
// the runner. The factory does not own the retained config, deps, or plan.
type ElectionFactory func(seed ElectionSeed) ElectionRunner

// SetElectionFactory wires or clears the settled-state election factory.
//
// Behaviour: with a factory wired, a settled member re-installs an election
// when a trigger fires. Nil clears the factory. After clear, the member runs
// only the open-time election and does not re-install.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Lifetime: assembly wires this after the open-time election install. Nil
// clears the factory. The assembly or caller owns clear order. This method does
// not bind a module Close sequence.
func (a *Agent) SetElectionFactory(f ElectionFactory) {
	a.electionFactory = f
}

// ElectionCount returns the number of elections installed through
// [Agent.installElection].
//
// Behaviour:
//   - each non-nil [Agent.SetElection] and each successful settled re-install
//     adds one
//   - a refused re-entry adds none
//   - [Agent.SetElection](nil) and [Agent.ElectionComplete] add none
//   - construct inject of [AgentConfig.Election] does not count unless the
//     install path runs later
//   - monotonic for the life of the agent
//
// Concurrency: invoker-only.
func (a *Agent) ElectionCount() int64 {
	return a.electionCount
}

// LastElectionTrigger returns the trigger that installed the newest election.
//
// Behaviour: [ElectionTriggerNone] until the first install through
// [Agent.installElection]. The value is not cleared by
// [Agent.ElectionComplete], and not cleared by [Agent.SetElection] with a nil
// runner, so it still names the cause of the newest election a member
// installed.
//
// Concurrency: invoker-only.
func (a *Agent) LastElectionTrigger() ElectionTrigger {
	return a.lastElectionTrigger
}

// installElection is the one place that writes a.election for an install.
//
// A nil runner clears the field and does not bump count or trigger.
//
// Clears prepareInProgress so a fresh or cleared runner starts a new prepare
// episode with a full teardown. Consensus ingress poll resumes.
func (a *Agent) installElection(runner ElectionRunner, trigger ElectionTrigger) {
	a.prepareInProgress = false
	a.election = runner
	if runner == nil {
		return
	}
	a.electionCount++
	a.lastElectionTrigger = trigger
}

// appendPositionForSeed reads the local append high-water for an election seed.
//
// Behaviour: nil source reports not-ready with position 0. A wired source is
// read directly. Does not call [Agent.resolveAppendPosition], so a seed read
// cannot mark append-rank failure.
//
// Returns:
//   - position, true: source reported a usable high-water
//   - 0, false: no source, or source reported not-ready
func (a *Agent) appendPositionForSeed() (int64, bool) {
	if a.appendSource == nil {
		return 0, false
	}
	return a.appendSource.AppendPosition()
}

// enterElection installs a settled-state election for trigger when allowed.
//
// Behaviour:
//   - refuses when an election is already live or no factory is wired
//   - reads the append seed while the current role still applies
//   - refuses when the factory returns nil. Role and pendingJoinLog stay
//     unchanged
//   - after a non-nil runner returns: drains the log adapter under
//     followerLogPollBound, clears pendingJoinLog, sets role to follower,
//     then installs
//   - isLogEndOfStream seeds GracefulClosedLeaderID from leaderMemberID,
//     else the null member id. A leader holds the null id, so a leader
//     install seeds the null id
//
// Role flip order: append read first, then RoleFollower. Cluster append
// sources are role-sensitive. A leader must report the publication
// position. After RoleFollower, the follower image path returns 0.
//
// Returns: 1 when a runner was installed, else 0.
func (a *Agent) enterElection(
	trigger ElectionTrigger,
	isLogEndOfStream bool,
	nowNs int64,
) int {
	if a.election != nil || a.electionFactory == nil {
		return 0
	}
	// Read append while still leader when role is leader: cluster append
	// sources are role-sensitive (LogAppendPosition).
	position, ready := a.appendPositionForSeed()
	gracefulClosedLeaderID := nullMemberID
	if isLogEndOfStream {
		gracefulClosedLeaderID = a.leaderMemberID
	}
	runner := a.electionFactory(ElectionSeed{
		LeadershipTermID:       a.leadershipTermID,
		CommitPosition:         a.CommitPosition(),
		AppendPosition:         position,
		AppendPositionReady:    ready,
		GracefulClosedLeaderID: gracefulClosedLeaderID,
	})
	if runner == nil {
		return 0
	}
	// Drain committed frames before the runner can unbind the image.
	a.logPollNowNs = nowNs
	a.drainLogBacklog(a.followerLogPollBound(position))
	// Clear a superseded unpublished JoinLog at settled re-entry before the new
	// election runs. Clear at install. Does not wait for election Init.
	// PrepareForNewLeadership still clears on mid-election re-Init and when a
	// runner never reaches Prepare.
	a.pendingJoinLog = nil
	a.RoleSet(RoleFollower)
	a.installElection(runner, trigger)
	return 1
}

// slowTickIfDue paces settled-state timeout checks and runs slowTickWork when
// due.
//
// Behaviour: returns 0 when nowNs is before the deadline. After slowTickWork,
// advances the deadline by 1ns when work was done, else by slowTickIntervalNs.
// Does not call noteDoWorkStep.
func (a *Agent) slowTickIfDue(nowNs int64) int {
	if nowNs < a.slowTickDeadlineNs {
		return 0
	}
	work := a.slowTickWork(nowNs)
	if work > 0 {
		a.slowTickDeadlineNs = nowNs + 1
	} else {
		a.slowTickDeadlineNs = nowNs + slowTickIntervalNs
	}
	return work
}

// slowTickWork runs the settled timeout triggers when no election is live.
//
// Behaviour: returns 0 while an election is live so the deadline still advances
// from slowTickIfDue. A leader checks active-member quorum. Any other role
// checks the leader heartbeat timeout.
func (a *Agent) slowTickWork(nowNs int64) int {
	if a.election != nil {
		return 0
	}
	if a.role == RoleLeader {
		return a.activeQuorumTick(nowNs)
	}
	return a.leaderWentQuietTick(nowNs)
}

// activeQuorumTick re-enters election when a leader loses its active quorum.
//
// Behaviour:
//   - nil positions: returns 0
//   - HasActiveQuorum true: returns 0
//   - refused enterElection: returns 0
//   - HasActiveQuorum counts all members (this member included), not
//     followers only
//
// Returns 1 when an election was installed.
func (a *Agent) activeQuorumTick(nowNs int64) int {
	if a.positions == nil {
		return 0
	}
	if a.positions.HasActiveQuorum(nowNs, a.leaderHeartbeatTimeoutNs) {
		return 0
	}
	return a.enterElection(ElectionTriggerInactiveFollowerQuorum, false, nowNs)
}

// leaderWentQuietTick re-enters election when a non-leader's settled
// log-update clock is older than the leader heartbeat timeout.
//
// Behaviour: returns 0 when nowNs is still inside the timeout window from
// timeOfLastLogUpdateNs. That clock is not timeOfLastLeaderUpdateNs (canvass
// base). A refused enterElection returns 0.
//
// Returns 1 when an election was installed.
func (a *Agent) leaderWentQuietTick(nowNs int64) int {
	if nowNs < a.timeOfLastLogUpdateNs+a.leaderHeartbeatTimeoutNs {
		return 0
	}
	return a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, nowNs)
}

// logStopTick re-enters an election when the log recording stopped or a
// non-leader's log image closed.
//
// Behaviour:
//   - consumes the recording STOP mark on every tick. The mark drops even
//     when no runner installs
//   - a STOP mark installs for any role. A closed or missing image installs
//     for a non-leader only. A STOP mark wins in the same tick
//   - end of stream seeds GracefulClosedLeaderID: the STOP position equals
//     the image end-of-stream position, or the image reached it
//
// Returns: 1 when a runner was installed, else 0.
func (a *Agent) logStopTick(nowNs int64) int {
	stopPosition, stopped := a.consumeLogRecordingStop()
	if a.election != nil {
		return 0
	}
	if stopped {
		isLogEndOfStream := a.logAdapter.IsLogEndOfStreamAt(stopPosition)
		return a.enterElection(ElectionTriggerLogRecordingStopped, isLogEndOfStream, nowNs)
	}
	if a.isFollowerLogImageClosed() {
		isLogEndOfStream := a.logAdapter.IsLogEndOfStream()
		return a.enterElection(ElectionTriggerLogImageClosed, isLogEndOfStream, nowNs)
	}
	return 0
}

// consumeLogRecordingStop takes the recording STOP mark. A nil recording
// control reports no mark.
func (a *Agent) consumeLogRecordingStop() (stopPosition int64, stopped bool) {
	if a.recordingControl == nil {
		return recordinglog.NullPosition, false
	}
	return a.recordingControl.ConsumeStopSignal()
}

// isFollowerLogImageClosed reports a non-leader whose log adapter holds a
// closed image or no image. A nil adapter reports false.
//
// A leader may still hold the follower image of an earlier term, so the role
// check comes first.
func (a *Agent) isFollowerLogImageClosed() bool {
	return a.role != RoleLeader && a.logAdapter != nil && a.logAdapter.IsImageClosed()
}
