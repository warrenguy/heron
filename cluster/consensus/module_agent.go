package consensus

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Package-level truncate refusals. Identity-comparable with errors.Is.
var (
	errNoLogTruncateControl = errors.New(
		"consensus: truncate log entry: no log truncate control",
	)
	errTruncateWhileRecording = errors.New(
		"consensus: truncate log entry: cluster log recording is active",
	)
	errTruncateUnknownRecording = errors.New(
		"consensus: truncate log entry: no cluster log recording id",
	)
	errTruncatePositionMisaligned = errors.New(
		"consensus: truncate log entry: position not frame aligned",
	)
)

// nullValue mirrors aeron cnc NullValue (-1) for recording IDs and null session.
//
// Local unexported const - do not import election.
const nullValue = -1

// nullMemberID is the unset member-id sentinel. Same numeric value as
// election.NullMemberID / cnc.NullValue. Member id 0 is valid - never treat
// zero as "unset".
const nullMemberID int32 = int32(nullValue)

// DefaultLeaderHeartbeatIntervalNs is the default commit re-publish and
// follower append cadence (200ms).
const DefaultLeaderHeartbeatIntervalNs int64 = 200_000_000

// DefaultAppendPositionNotReadyTimeoutNs bounds a not-ready append high-water
// run (5s).
//
// The run may span inactive periods. Fail-closed marks only while a recording
// is active at the crossing tick. Same order of magnitude as the archive
// control response timeout that bounds the start which allocates the counter.
const DefaultAppendPositionNotReadyTimeoutNs int64 = 5_000_000_000

// ErrAppendPositionUnavailable reports that a cluster log recording is active and
// its recording-position counter has been absent longer than
// [AgentConfig.AppendPositionNotReadyTimeoutNs].
//
// Causes:
//   - the archive stopped the recording without the member observing the stop
//   - the archive never allocated the counter for a start it acknowledged
//   - the counters reader is reading a different media driver than the archive
//
// [Agent.AppendRankFailure] returns this sentinel wrapped with the recording id
// and the run length.
//
// A member that reports this error:
//   - has stopped ranking
//   - has stopped publishing its append position
//
// Callers must not treat that member as a live peer.
var ErrAppendPositionUnavailable = errors.New(
	"consensus: log recording is active and its recording position counter is absent",
)

// Agent is the consensus module duty-cycle owner.
//
//   - Implements [AgentHandler]
//   - Exposes host-delegate methods the election host adapter forwards
//     (member-free host duties)
//   - Holds consensus-facing interfaces for election, positions, egress,
//     ingress, and append high-water ([ElectionRunner], [PositionTable],
//     [ConsensusEgress], [ConsensusIngress], [AppendPositionSource]); never
//     election concrete types
//
// Concurrency: invoker-only. Caller runs media-driver/client duty cycle and
// [Agent.DoWork] on one goroutine. Agent does not spawn threads and does not
// own a Client field.
//
// Lifetime: the host adapter outlives the election as implementer of positions
// and egress. [Agent.ElectionComplete] clears the election runner and stamps
// timeOfLastLogUpdateNs (settled log-update clock). With an [ElectionFactory]
// wired, a settled member may install a further election after that clear.
// [Agent.SetElectionFactory] wires or clears the factory.
//
// Positions and egress may be set at construct or later via
// [Agent.SetPositions] and [Agent.SetEgress]. Agent must exist before the host
// adapter can take *Agent. Constructor may also set them for unit tests that
// skip the host adapter.
type Agent struct {
	// election is nil in steady-state after [Agent.ElectionComplete].
	election ElectionRunner

	// positions / egress: usually the same host adapter after SetPositions /
	// SetEgress.
	positions PositionTable
	egress    ConsensusEgress

	// mode-1 commit value. Nil commit: [Agent.ProposeMaxCommit] and
	// [Agent.CommitPosition] return 0.
	commit *atomic.Int64

	role                     Role
	timeOfLastLeaderUpdateNs int64
	localLogRecordingID      int64

	// recordingControl starts and stops the cluster log recording. Nil skips
	// recording in both joins and in PrepareForNewLeadership.
	//
	// [LogRecordingControl.IsActive] owns live-vs-inactive. The agent does not
	// keep a second active flag.
	//
	// localLogRecordingID is kept after a stop so a term entry still names the
	// recording that carried the term.
	recordingControl LogRecordingControl

	// truncateControl truncates the cluster log recording. Nil refuses every
	// truncate.
	truncateControl LogTruncateControl

	// leadershipTermID is the current term for publish and ingress term gates.
	//
	// Default 0 is a valid term id (first term) - not a sentinel.
	leadershipTermID int64

	// thisMemberID is this node's cluster member id for steady leader stamp.
	//
	// Default 0 is a valid member id; tests that need "unset" pass
	// nullMemberID (-1). HostAdapter construct seeds from ThisMember.ID.
	thisMemberID int32

	// leaderMemberID is the known leader id for steady notify gates and
	// AppendPosition publish target (HostAdapter reads this store). Default 0
	// is a valid member id; use nullMemberID (-1) for unset.
	leaderMemberID int32

	// timeOfLastLogUpdateNs is the settled log-update clock.
	//
	// Writers: [Agent.UpdateLeaderPosition], same-term follower notify, and
	// [Agent.ElectionComplete].
	//
	// Readers: the commit re-publish gate and the settled quiet-tick timeout
	// (leaderWentQuietTick).
	//
	// Distinct from timeOfLastLeaderUpdateNs. That field is the ingress
	// leader-update clock and the canvass base via Host.TimeOfLastLeaderUpdateNs.
	timeOfLastLogUpdateNs int64

	// leaderHeartbeatIntervalNs is the commit re-publish and follower
	// AppendPosition send cadence.
	leaderHeartbeatIntervalNs int64

	// notifiedCommitPosition is the follower-visible commit high-water.
	//
	// Written by steady ingress CommitPosition and NewLeadershipTerm
	// handlers; [Agent.NotifiedCommitPosition] exposes it.
	notifiedCommitPosition int64

	// ingress polls consensus channel fragments. Nil: pollConsensusIngress
	// returns 0 (unit tests inject via [QueueConsensusIngress] or direct
	// handler calls).
	ingress ConsensusIngress

	// ingressHandler is onIngressFragment cached once at construction so
	// pollConsensusIngress does not allocate a method value each duty cycle.
	ingressHandler func(buf []byte, offset int)

	// ingressPollNowNs is the duty-cycle clock for the current
	// pollConsensusIngress batch. Set before Poll; read by onIngressFragment
	// so the fragment handler is a method value (no per-poll closure).
	ingressPollNowNs int64

	// appendSource supplies local log append high-water for leader rank and
	// follower AppendPosition publish. Nil: leader consensusWork returns 0.
	appendSource AppendPositionSource

	// appendNotReadySinceNs is the first tick of the open not-ready run from
	// appendSource, or nullValue when the last read was ready. A ready read
	// clears the field. A run that spans a recording stop and a new start ends
	// when the new counter appears.
	appendNotReadySinceNs int64

	// appendNotReadyTimeoutNs bounds a not-ready run while a cluster log
	// recording is active. Past the bound the agent marks appendRankFailure.
	appendNotReadyTimeoutNs int64

	// appendRankFailure is the first marked append-rank failure, or nil until
	// marked. Marked once. Later ticks do not replace the cause.
	appendRankFailure error

	// lastPublishedAppendPosition is the last successfully published follower
	// append high-water (advanced path only).
	lastPublishedAppendPosition int64

	// timeOfLastAppendPositionSendNs gates follower AppendPosition re-publish
	// on the heartbeat interval.
	timeOfLastAppendPositionSendNs int64

	// logPublisher appends leadership-term and session frames on the exclusive
	// log publication. Nil until wired via config or [Agent.SetLogPublisher].
	logPublisher *LogPublisher

	// logAdapter polls the live log Image under the follower commit bound.
	//
	// Nil: followerConsensusWork skips poll and proposeMax from adapter
	// (AppendPosition interval path still runs).
	//
	// CatchupPoll uses the same adapter when an Image is bound.
	logAdapter *LogAdapter

	// joinSource supplies the live cluster log image for the follower log join.
	//
	// Nil: FollowerLogJoinReady and JoinLogAsFollower both report not-ready.
	joinSource LogJoinSource

	// logReplicationFactory optional inject for [Agent.NewLogReplication].
	//
	// Nil: NewLogReplication returns nil.
	logReplicationFactory LogReplicationFactory

	// logReplayFactory optional inject for [Agent.NewLogReplay].
	//
	// Nil: NewLogReplay returns done-stub at appendPosition.
	logReplayFactory LogReplayFactory

	// catchupControl opens follower catchup receive and starts leader-side
	// archive catchup replay. Nil: EnsureCatchupLogDestination and
	// OnCatchupPosition are silent no-ops.
	catchupControl CatchupControl

	// catchupReplayActive is true after a successful OnCatchupPosition start
	// until the replay is stopped. Blocks a start for another follower while a
	// replay publication is active.
	catchupReplayActive bool

	// catchupReplayMemberID is the follower the active replay serves. Valid
	// only while catchupReplayActive.
	catchupReplayMemberID int32

	// logPublicationReseed rebuilds the exclusive log publication at a position
	// when a follower becomes leader. Nil: JoinLogAsLeader does not reseed.
	logPublicationReseed LogPublicationReseed

	// termBaseLogPosition is the term base written into NewLeadershipTermEvent.
	//
	// Seeded by [Agent.SetTermBaseLogPosition] or [Agent.JoinLogAsLeader]
	// logPosition.
	termBaseLogPosition int64

	// appVersion is the application version for NewLeadershipTermEvent.
	//
	// Default 0 is valid on the wire.
	appVersion int32

	// leaderHeartbeatTimeoutNs bounds the settled quiet wait on
	// timeOfLastLogUpdateNs before a non-leader re-enters an election
	// (leaderWentQuietTick).
	//
	// A settled leader passes the same value to PositionTable.HasActiveQuorum
	// as the active-quorum timeout window (activeQuorumTick).
	leaderHeartbeatTimeoutNs int64

	// slowTickDeadlineNs is the next nowNs at which slowTickIfDue evaluates.
	// Zero means the first tick always evaluates.
	slowTickDeadlineNs int64

	// electionFactory builds a replacement ElectionRunner for a settled
	// member. Nil: no settled-state re-install happens.
	electionFactory ElectionFactory

	// electionCount counts installed elections, open-time install included.
	electionCount int64

	// lastElectionTrigger names the condition that installed the newest
	// election.
	lastElectionTrigger ElectionTrigger
}

// AgentConfig is the constructor config. Nil interface fields are allowed for
// unit tests that exercise Agent host-delegate stubs only, and for production
// bootstrap before the host adapter exists (then call [Agent.SetPositions] /
// [Agent.SetEgress] / [Agent.SetElection]).
//
// Member ids: zero is a valid member id. Constructor stores ThisMemberID and
// LeaderMemberID as given - never rewrites 0 to -1. Production: HostAdapter
// construct seeds ThisMemberID from ThisMember.ID and LeaderMemberID from
// config (including 0 / -1). Unit tests that need "unset" pass -1
// (nullMemberID) explicitly.
type AgentConfig struct {
	Election  ElectionRunner  // optional until election wired
	Positions PositionTable   // optional at construct; set via [Agent.SetPositions]
	Egress    ConsensusEgress // optional at construct; set via [Agent.SetEgress]

	// Commit is mode-1 injected *atomic.Int64. Nil commit:
	// [Agent.ProposeMaxCommit] and [Agent.CommitPosition] return 0 (no store).
	//
	// Production bootstrap passes non-nil.
	Commit *atomic.Int64

	// Role seeds [Agent.Role] / [Agent.RoleSet]. Default [RoleFollower] (zero value).
	Role Role

	// LeaderHeartbeatIntervalNs: zero -> DefaultLeaderHeartbeatIntervalNs
	// (200_000_000). Negative panics at NewAgent.
	LeaderHeartbeatIntervalNs int64

	// LeaderHeartbeatTimeoutNs: zero -> DefaultLeaderHeartbeatTimeoutNs
	// (10_000_000_000). Negative panics at NewAgent.
	LeaderHeartbeatTimeoutNs int64

	// AppendPositionNotReadyTimeoutNs: zero ->
	// DefaultAppendPositionNotReadyTimeoutNs (5_000_000_000). Negative panics at
	// NewAgent.
	AppendPositionNotReadyTimeoutNs int64

	// LeadershipTermID seeds a.leadershipTermID (default 0 is a valid term).
	LeadershipTermID int64

	// ThisMemberID seeds a.thisMemberID as given. Zero is valid.
	//
	// HostAdapter construct overwrites via [Agent.SetThisMemberID] from
	// ThisMember.ID when the host is wired.
	ThisMemberID int32

	// LeaderMemberID seeds a.leaderMemberID as given. Zero is valid; use -1
	// for unset until leader install. HostAdapter construct overwrites via
	// [Agent.SetLeaderMemberID].
	LeaderMemberID int32

	// Ingress optional; nil until subscription wired. Tests inject
	// [QueueConsensusIngress].
	Ingress ConsensusIngress

	// AppendSource optional mode-1 inject for leader rank and follower
	// AppendPosition publish. Nil until wired via config or [Agent.SetAppendSource].
	AppendSource AppendPositionSource

	// LogPublisher optional inject for log append and NewLeadershipTermEvent.
	//
	// Nil until wired via config or [Agent.SetLogPublisher].
	LogPublisher *LogPublisher

	// LogAdapter optional inject for follower log poll. Nil until wired via
	// config or [Agent.SetLogAdapter].
	LogAdapter *LogAdapter

	// LogJoinSource optional inject for the follower log join. Nil until the
	// cluster log subscription is wired; both join methods report not-ready
	// until then.
	LogJoinSource LogJoinSource

	// AppVersion seeds a.appVersion for NewLeadershipTermEvent (default 0 valid).
	AppVersion int32

	// TermBaseLogPosition seeds a.termBaseLogPosition (default 0). Also set by
	// [Agent.JoinLogAsLeader] logPosition or [Agent.SetTermBaseLogPosition].
	TermBaseLogPosition int64
}

// NewAgent constructs an [Agent] from cfg. localLogRecordingID defaults to
// nullValue. LeaderHeartbeatIntervalNs zero becomes
// DefaultLeaderHeartbeatIntervalNs; negative panics.
// LeaderHeartbeatTimeoutNs zero becomes DefaultLeaderHeartbeatTimeoutNs;
// negative panics. AppendPositionNotReadyTimeoutNs zero becomes
// DefaultAppendPositionNotReadyTimeoutNs; negative panics.
//
// Member ids are stored as given (0 is valid; -1 is unset).
//
// Concurrency: invoker-only after construction; see [Agent].
//
// Lifetime: caller owns the returned Agent. Positions, egress, election, and
// election factory may be wired later via [Agent.SetPositions],
// [Agent.SetEgress], [Agent.SetElection], and [Agent.SetElectionFactory]. With
// a factory wired, a settled member may install further elections after
// [Agent.ElectionComplete].
func NewAgent(cfg AgentConfig) *Agent {
	interval := cfg.LeaderHeartbeatIntervalNs
	if interval == 0 {
		interval = DefaultLeaderHeartbeatIntervalNs
	}
	if interval < 0 {
		panic("consensus: negative LeaderHeartbeatIntervalNs")
	}
	timeout := cfg.LeaderHeartbeatTimeoutNs
	if timeout == 0 {
		timeout = DefaultLeaderHeartbeatTimeoutNs
	}
	if timeout < 0 {
		panic("consensus: negative LeaderHeartbeatTimeoutNs")
	}
	appendTimeout := cfg.AppendPositionNotReadyTimeoutNs
	if appendTimeout == 0 {
		appendTimeout = DefaultAppendPositionNotReadyTimeoutNs
	}
	if appendTimeout < 0 {
		panic("consensus: negative AppendPositionNotReadyTimeoutNs")
	}
	a := &Agent{
		election:                  cfg.Election,
		positions:                 cfg.Positions,
		egress:                    cfg.Egress,
		commit:                    cfg.Commit,
		role:                      cfg.Role,
		localLogRecordingID:       nullValue,
		leadershipTermID:          cfg.LeadershipTermID,
		thisMemberID:              cfg.ThisMemberID,
		leaderMemberID:            cfg.LeaderMemberID,
		leaderHeartbeatIntervalNs: interval,
		leaderHeartbeatTimeoutNs:  timeout,
		ingress:                   cfg.Ingress,
		appendSource:              cfg.AppendSource,
		appendNotReadySinceNs:     nullValue,
		appendNotReadyTimeoutNs:   appendTimeout,
		logPublisher:              cfg.LogPublisher,
		logAdapter:                cfg.LogAdapter,
		joinSource:                cfg.LogJoinSource,
		termBaseLogPosition:       cfg.TermBaseLogPosition,
		appVersion:                cfg.AppVersion,
	}
	a.ingressHandler = a.onIngressFragment
	return a
}

// AppendRankFailure returns the marked append-rank failure, or nil.
//
// Behaviour:
//   - non-nil once a cluster log recording has been active with no
//     recording-position counter longer than the configured bound
//   - marked once: first cause kept; later ticks do not replace it
//   - a later ready source report does not clear the marked failure
//   - ranking and publish stay stopped after mark
//
// Callers must poll this value. Nothing in [cluster.Runner] reads it. The duty
// cycle keeps running after the failure is set: transport is still serviced and
// consensus ingress is still polled.
//
// A member with a non-nil failure contributes nothing to rank or append
// publish. Callers must not treat that member as a live peer.
//
// Concurrency: invoker-only.
func (a *Agent) AppendRankFailure() error { return a.appendRankFailure }

// resolveAppendPosition reads the wired append high-water once and applies the
// not-ready policy.
//
// Returns the position and whether the caller may use that position.
//
// When [Agent.AppendRankFailure] is marked, freezes the member for the process
// life:
//   - always not-ready
//   - does not read the source
func (a *Agent) resolveAppendPosition(nowNs int64) (int64, bool) {
	if a.appendRankFailure != nil {
		return 0, false
	}
	if a.appendSource == nil {
		a.appendNotReadySinceNs = nullValue
		return 0, true
	}
	position, ready := a.appendSource.AppendPosition()
	if ready {
		a.appendNotReadySinceNs = nullValue
		return position, true
	}
	if a.appendNotReadySinceNs == nullValue {
		a.appendNotReadySinceNs = nowNs
	}
	timedOut := nowNs-a.appendNotReadySinceNs >= a.appendNotReadyTimeoutNs
	if a.IsRecordingActive() && timedOut {
		a.appendRankFailure = fmt.Errorf(
			"%w: recording_id=%d not_ready_ns=%d",
			ErrAppendPositionUnavailable,
			a.LocalLogRecordingID(),
			nowNs-a.appendNotReadySinceNs,
		)
	}
	return 0, false
}

// SetPositions sets the [PositionTable] used for ranking and member updates.
//
// Idempotent replace.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Production bootstrap passes non-nil. Nil is allowed; ranking becomes a no-op
// until rewired.
func (a *Agent) SetPositions(positions PositionTable) {
	a.positions = positions
}

// SetEgress sets the [ConsensusEgress] used for commit/append publish.
//
// Idempotent replace.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Production bootstrap passes non-nil. Nil is allowed; egress becomes a no-op
// until rewired.
func (a *Agent) SetEgress(egress ConsensusEgress) {
	a.egress = egress
}

// SetElection wires or clears the [ElectionRunner] for open bootstrap and
// assembly clear.
//
// Behaviour: a non-nil runner installs as [ElectionTriggerBootstrap] and
// increments [Agent.ElectionCount]. A nil runner clears the field only and does
// not change count or trigger. [Agent.ElectionComplete] clears the runner
// directly and does not call SetElection.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetElection(runner ElectionRunner) {
	a.installElection(runner, ElectionTriggerBootstrap)
}

// SetIngress replaces the consensus ingress poller.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Nil is allowed; [Agent.pollConsensusIngress] returns 0 until rewired.
func (a *Agent) SetIngress(ingress ConsensusIngress) { a.ingress = ingress }

// Ingress returns the wired [ConsensusIngress], or nil if unset.
func (a *Agent) Ingress() ConsensusIngress { return a.ingress }

// Election returns the current [ElectionRunner], or nil in steady-state.
func (a *Agent) Election() ElectionRunner { return a.election }

// Positions returns the wired [PositionTable], or nil if unset.
func (a *Agent) Positions() PositionTable { return a.positions }

// Egress returns the wired [ConsensusEgress], or nil if unset.
func (a *Agent) Egress() ConsensusEgress { return a.egress }

// Role returns the current cluster node [Role].
func (a *Agent) Role() Role { return a.role }

// RoleSet sets the current cluster node [Role].
func (a *Agent) RoleSet(role Role) { a.role = role }

// CommitPosition returns the current commit value, or 0 if commit is nil.
func (a *Agent) CommitPosition() int64 {
	if a.commit == nil {
		return 0
	}
	return a.commit.Load()
}

// ProposeMaxCommit advances the commit value to max(current, pos) and returns
// the value after the update. Nil commit pointer: returns 0 (no store).
//
// Concurrency: invoker-only writer; load then conditional store (no CAS).
// *atomic.Int64 is for external readers of the same value, not concurrent max
// writers.
func (a *Agent) ProposeMaxCommit(pos int64) int64 {
	if a.commit == nil {
		return 0
	}
	cur := a.commit.Load()
	if pos <= cur {
		return cur
	}
	a.commit.Store(pos)
	return pos
}

// UpdateLeaderPosition applies the leader commit gate.
//
// Preconditions: host adapter (or test) has stamped thisMember when using the
// election-host path. Agent does not stamp member state. Host keeps the
// three-arg form (stamps append, then forwards quorum here).
//
// Gate: quorumPosition > commit OR nowNs >= timeOfLastLogUpdateNs + interval.
//
// On pass: PublishCommitPosition(quorum, leadershipTermID), ProposeMaxCommit,
// set timeOfLastLogUpdateNs, sweepUncommitted (no-op stub), return 1.
//
// Does NOT write timeOfLastLeaderUpdateNs (ingress only).
//
// Nil commit: still may publish and return 1 on gate pass. ProposeMax is a
// no-op on a missing counter, but work and clock still advance.
func (a *Agent) UpdateLeaderPosition(nowNs, quorumPosition int64) int {
	commit := a.CommitPosition()
	intervalDue := nowNs >= a.timeOfLastLogUpdateNs+a.leaderHeartbeatIntervalNs
	if quorumPosition <= commit && !intervalDue {
		return 0
	}

	if a.egress != nil {
		a.egress.PublishCommitPosition(quorumPosition, a.leadershipTermID)
	}
	_ = a.ProposeMaxCommit(quorumPosition)
	a.timeOfLastLogUpdateNs = nowNs
	a.sweepUncommittedEntriesTo(quorumPosition)
	return 1
}

func (a *Agent) sweepUncommittedEntriesTo(quorumPosition int64) {
	// No-op until client sessions / uncommitted entry lists exist.
	_ = quorumPosition
}

// TimeOfLastLeaderUpdateNs returns the last inbound leader-update clock.
//
// Written only on inbound NewLeadershipTerm / CommitPosition (ingress path).
func (a *Agent) TimeOfLastLeaderUpdateNs() int64 {
	return a.timeOfLastLeaderUpdateNs
}

// LeadershipTermID returns the current leadership term.
func (a *Agent) LeadershipTermID() int64 { return a.leadershipTermID }

// SetLeadershipTermID is the Agent-local term setter (bootstrap, host install
// handoff, tests). No egress.
func (a *Agent) SetLeadershipTermID(id int64) { a.leadershipTermID = id }

// ThisMemberID returns this node's cluster member id. Zero is a valid member
// id; -1 means unset only when bootstrap or a caller stored -1.
func (a *Agent) ThisMemberID() int32 { return a.thisMemberID }

// SetThisMemberID sets this node's cluster member id (steady leader stamp via
// [PositionTable.UpdateMemberPosition]). Used by HostAdapter construct and
// tests. Member id 0 is valid; use -1 for unset.
func (a *Agent) SetThisMemberID(id int32) { a.thisMemberID = id }

// LeaderMemberID returns the stored leader member id. Zero is a valid member
// id; -1 means unset only when bootstrap or a caller stored -1.
func (a *Agent) LeaderMemberID() int32 { return a.leaderMemberID }

// SetLeaderMemberID sets the known leader id (steady notify gates and
// AppendPosition target). Used by JoinLogAsLeader, HostAdapter install
// handoff, and tests. Member id 0 is valid; use -1 for unset.
func (a *Agent) SetLeaderMemberID(id int32) { a.leaderMemberID = id }

// NotifiedCommitPosition returns the follower-visible commit high-water.
//
// Steady-state CommitPosition / NewLeadershipTerm handlers advance this via
// max with the inbound position when term and leader gates pass.
func (a *Agent) NotifiedCommitPosition() int64 { return a.notifiedCommitPosition }

// doneLogReplay is an immediately-complete [LogReplay] at a fixed position.
//
// Avoids livelock on leader-replay gap when archive helpers are not yet wired.
type doneLogReplay struct{ position int64 }

func (r *doneLogReplay) DoWork(int64) int { return 0 }
func (r *doneLogReplay) Position() int64  { return r.position }
func (r *doneLogReplay) IsDone() bool     { return true }
func (r *doneLogReplay) Close()           {}

var _ LogReplay = (*doneLogReplay)(nil)

// LogReplayFactory builds a [LogReplay] helper for [Agent.NewLogReplay].
//
// Nil factory field: NewLogReplay returns a done-stub at appendPosition. A
// non-nil factory that returns nil also falls through to the done-stub (avoids
// soft livelock on leader-replay construction).
//
// A factory that cannot build a helper must return a helper that is done at the
// start position. The factory must not return nil: the nil fallthrough reports
// appendPosition.
type LogReplayFactory func(logPosition, appendPosition int64) LogReplay

// LogReplicationFactory builds a follower [LogReplication] helper for
// [Agent.NewLogReplication].
//
// Nil factory: NewLogReplication returns nil.
type LogReplicationFactory func(
	leaderArchiveEndpoint, responseArchiveEndpoint string,
	leaderRecordingID, stopPosition, nowNs int64,
) LogReplication

// SetLogReplayFactory wires or clears the optional [LogReplayFactory].
//
// Nil restores the done-stub default for [Agent.NewLogReplay].
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetLogReplayFactory(f LogReplayFactory) { a.logReplayFactory = f }

// SetLogReplicationFactory wires or clears the optional [LogReplicationFactory].
//
// Nil restores the default nil return from [Agent.NewLogReplication].
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetLogReplicationFactory(f LogReplicationFactory) { a.logReplicationFactory = f }

// NewLogReplay returns a [LogReplay] for the logPosition/appendPosition gap.
//
// When a [LogReplayFactory] is set and returns non-nil, returns that result.
//
// Nil factory field or factory result falls through to a done-stub at
// appendPosition ([LogReplay.IsDone] true, [LogReplay.DoWork] 0).
func (a *Agent) NewLogReplay(logPosition, appendPosition int64) LogReplay {
	if f := a.logReplayFactory; f != nil {
		if r := f(logPosition, appendPosition); r != nil {
			return r
		}
	}
	return &doneLogReplay{position: appendPosition}
}

// NewLogReplication returns a follower [LogReplication] helper, or nil.
//
// When a [LogReplicationFactory] is set, returns the factory result. Otherwise
// returns nil (full archive replicate mesh not wired).
func (a *Agent) NewLogReplication(
	leaderArchiveEndpoint string,
	responseArchiveEndpoint string,
	leaderRecordingID int64,
	stopPosition int64,
	nowNs int64,
) LogReplication {
	f := a.logReplicationFactory
	if f == nil {
		return nil
	}
	return f(
		leaderArchiveEndpoint,
		responseArchiveEndpoint,
		leaderRecordingID,
		stopPosition,
		nowNs,
	)
}

// CatchupPoll polls catchup/live log progress under limitPosition.
//
// When a [LogAdapter] Image is bound, polls with bound = limitPosition (same
// BoundedControlledPoll substrate as followerConsensusWork) and advances
// commit via [Agent.ProposeMaxCommit] from adapter position. Returns 0 when
// adapter or image is nil, or when the bound yields no work.
//
// Concurrency: invoker-only. Election-live catchup is the sole consumer while
// an election runner is set; steady followerConsensusWork does not run then.
//
// nowNs is reserved for future pacing and ignored here.
func (a *Agent) CatchupPoll(limitPosition, nowNs int64) int {
	_ = nowNs
	adapter := a.logAdapter
	if adapter == nil || adapter.Image() == nil {
		return 0
	}
	return a.pollLogAndProposeMax(adapter, limitPosition)
}

// liveAddMaxWindow is ReplayMerge.LIVE_ADD_MAX_WINDOW (32 MiB). Caps the
// near-live window so a large term buffer does not force a wide catchup gap.
const liveAddMaxWindow int64 = 32 * 1024 * 1024

// CatchupInitiated notes the follower append-position cadence time so catchup
// progress publish does not wait a full heartbeat interval.
//
// Concurrency: invoker-only.
func (a *Agent) CatchupInitiated(nowNs int64) {
	a.timeOfLastAppendPositionSendNs = nowNs
}

// StopAllCatchups stops every leader-side catchup replay and clears
// catchupReplayActive so a later OnCatchupPosition may start a new replay.
//
// Behaviour: clears the active gate whether or not the archive stop succeeds. A
// stop failure must not strand the gate for the life of the Agent.
//
// Concurrency: invoker-only. Blocks for the archive control response.
func (a *Agent) StopAllCatchups() {
	a.catchupReplayActive = false
	a.catchupReplayMemberID = nullMemberID
	if a.catchupControl == nil {
		return
	}
	_ = a.catchupControl.StopCatchupReplays()
}

// RemoveCatchupLogDestination closes the follower catchup receive path.
//
// Catchup completion does not call this. The receive path stays open for the
// bound log adapter, which still reads the catchup image. Prepare on the next
// Init closes it.
//
// Concurrency: invoker-only.
func (a *Agent) RemoveCatchupLogDestination() {
	if a.catchupControl == nil {
		return
	}
	_ = a.catchupControl.RemoveCatchupLogDestination()
}

// IsCatchupNearLive reports whether the bound log image is within one quarter
// term (capped at 32 MiB) of position.
//
// Returns false when no image is bound. Matches ConsensusModuleAgent
// isCatchupNearLive: localPosition >= position - min(termBufferLength/4,
// 32MiB).
//
// Concurrency: invoker-only.
func (a *Agent) IsCatchupNearLive(position int64) bool {
	if a.logAdapter == nil {
		return false
	}
	image := a.logAdapter.Image()
	if image == nil {
		return false
	}
	window := min(int64(image.TermBufferLength())>>2, liveAddMaxWindow)
	return image.Position() >= position-window
}

// IsLogJoinedForSession reports whether the bound log image carries
// logSessionID.
//
// Callers use this after catchup. Live-log await must skip only when the
// adapter is already on the leader-advertised session, not a prior term's
// image.
func (a *Agent) IsLogJoinedForSession(logSessionID int32) bool {
	if a.logAdapter == nil {
		return false
	}
	image := a.logAdapter.Image()
	return image != nil && image.SessionID() == logSessionID
}

// CatchupControl opens the follower catchup receive path and starts
// leader-side archive replay for a catchup request.
//
// Concurrency: invoker-only. Blocks for archive control and subscription add.
//
// Ownership: implementations own any subscription they open and must close
// that subscription with the module.
type CatchupControl interface {
	// EnsureCatchupLogDestination opens a catchup receive subscription for
	// catchupEndpoint. Returns the destination channel URI, or empty when not
	// ready. Idempotent once open.
	EnsureCatchupLogDestination(catchupEndpoint string) string

	// RemoveCatchupLogDestination closes the catchup receive subscription and
	// restores the live log join source. Idempotent.
	RemoveCatchupLogDestination() error

	// StartCatchupReplay starts an archive replay of the local log recording
	// from logPosition toward catchupEndpoint with logSessionID. Returns the
	// replay session id, or an error.
	StartCatchupReplay(
		logPosition int64,
		catchupEndpoint string,
		logSessionID int32,
	) (replaySessionID int64, err error)

	// StopCatchupReplays stops every archive replay this control started. Idempotent.
	StopCatchupReplays() error
}

// SetCatchupControl wires or clears the [CatchupControl].
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetCatchupControl(c CatchupControl) { a.catchupControl = c }

// EnsureCatchupLogDestination opens the follower catchup receive path.
//
// The control owns the open state and is idempotent once open, so this
// delegates on every call rather than caching the URI.
//
// Returns empty when no control is wired or the open is not ready.
func (a *Agent) EnsureCatchupLogDestination(catchupEndpoint string) string {
	if a.catchupControl == nil {
		return ""
	}
	return a.catchupControl.EnsureCatchupLogDestination(catchupEndpoint)
}

// OnCatchupPosition starts a leader-side archive catchup replay for a
// follower.
//
// Behaviour:
//   - refuses unless this member is the leader
//   - refuses when fields.LeadershipTermID is above this member's leadership
//     term
//   - refuses when CatchupEndpoint is empty
//   - refuses while a replay for another follower is active
//   - stops the active replay first when the same follower asks again, so a
//     follower that abandoned a catchup can retry within the term
//   - refuses when no [CatchupControl] is wired, no log publisher is bound, or
//     the local log recording id is null
//   - starts one replay per call
//
// One follower at a time. A second follower is refused until the active replay
// stops through [Agent.PrepareForNewLeadership] or [Agent.StopAllCatchups]. The
// replay follows the active recording, so it stays the served follower's log
// feed for the rest of the term.
//
// Concurrency: invoker-only. Blocks for the archive control response.
func (a *Agent) OnCatchupPosition(fields codecs.CatchupPositionFields) {
	if a.role != RoleLeader {
		return
	}
	if fields.LeadershipTermID > a.leadershipTermID {
		return
	}
	if fields.CatchupEndpoint == "" {
		return
	}
	if a.catchupReplayActive {
		if a.catchupReplayMemberID != fields.FollowerMemberID {
			return
		}
		a.StopAllCatchups()
	}
	if a.catchupControl == nil {
		return
	}
	if a.localLogRecordingID < 0 {
		return
	}
	// Session id -1 is the null sentinel. Other negative values are valid
	// Aeron session ids.
	logSessionID, bound := a.boundLogSessionID()
	if !bound || logSessionID == int32(nullValue) {
		return
	}
	_, err := a.catchupControl.StartCatchupReplay(
		fields.LogPosition,
		fields.CatchupEndpoint,
		logSessionID,
	)
	if err != nil {
		return
	}
	a.catchupReplayActive = true
	a.catchupReplayMemberID = fields.FollowerMemberID
}

// SetLogPublisher wires or clears the [LogPublisher] used for log append and
// NewLeadershipTermEvent.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetLogPublisher(p *LogPublisher) { a.logPublisher = p }

// SetLogAdapter wires or clears the [LogAdapter] used for follower log poll.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetLogAdapter(adapter *LogAdapter) { a.logAdapter = adapter }

// SetTermBaseLogPosition sets the term base written into NewLeadershipTermEvent.
//
// Prefer [Agent.JoinLogAsLeader] on the leader-join path; use this setter for
// tests or bootstrap that skip join.
func (a *Agent) SetTermBaseLogPosition(pos int64) { a.termBaseLogPosition = pos }

// AddLogPublication returns the bound log publication session id when a
// [LogPublisher] with a publication is already injected; otherwise null
// session id (-1).
//
// When a [LogPublicationReseed] is wired and the publication position is below
// appendPosition, reseeds the publication at appendPosition first so the
// returned session id is the one followers will join. Election captures this
// id when entering leader log replication and publishes it in
// NewLeadershipTerm.
//
// Required inject order: [Agent.SetLogPublisher] and
// [LogPublisher.SetPublication] before any caller that stores the returned
// session id. A bind after this call leaves the caller with -1 while a later
// NewLeadershipTermEvent would write a real session id.
//
// On reseed failure re-reads the bound session id. A reseed that unbinds the
// publisher before failing returns null session id, never the pre-reseed id
// that the publication no longer holds. The caller may still fail at
// JoinLogAsLeader when LOCAL extend refuses.
func (a *Agent) AddLogPublication(appendPosition int64) int32 {
	sessionID, bound := a.boundLogSessionID()
	if !bound {
		return sessionID
	}
	if reseeded, did, _ := a.reseedLogPublicationIfBehind(appendPosition); did {
		return reseeded
	}

	// A failed reseed closes the old publication before opening the new one. Use
	// the session id the publisher holds now, not the id captured before the
	// attempt.
	sessionID, _ = a.boundLogSessionID()
	return sessionID
}

// boundLogSessionID returns the log publication session id when a
// [LogPublisher] with a publication is bound. Returns null session id (-1)
// and false otherwise.
func (a *Agent) boundLogSessionID() (sessionID int32, ok bool) {
	if a.logPublisher == nil || a.logPublisher.Publication() == nil {
		return int32(nullValue), false
	}
	return a.logPublisher.SessionID(), true
}

// reseedLogPublicationIfBehind rebuilds the log publication when a reseed
// hook is wired and the current publication position is below target.
//
// Returns:
//   - sessionID, true, nil: reseed ran and succeeded
//   - 0, false, nil: reseed did not run (no hook, unbound publisher, or
//     position already at or above target)
//   - 0, false, err: reseed ran and the hook failed
func (a *Agent) reseedLogPublicationIfBehind(
	position int64,
) (sessionID int32, did bool, err error) {
	if a.logPublicationReseed == nil || a.logPublisher == nil || a.logPublisher.Publication() == nil {
		return 0, false, nil
	}
	if a.logPublisher.Position() >= position {
		return 0, false, nil
	}
	sessionID, err = a.logPublicationReseed(position)
	if err != nil {
		return 0, false, err
	}
	return sessionID, true, nil
}

// AppendNewLeadershipTermEvent appends a NewLeadershipTermEvent via the wired
// [LogPublisher]. Returns false when logPublisher is nil or the append fails.
//
// TimeUnit is always NANOS (SBE zero is MILLIS - never treat Go zero as unset).
func (a *Agent) AppendNewLeadershipTermEvent(nowNs int64) bool {
	if a.logPublisher == nil {
		return false
	}
	return a.logPublisher.AppendNewLeadershipTermEvent(
		a.leadershipTermID,
		nowNs,
		a.termBaseLogPosition,
		a.thisMemberID,
		a.logPublisher.SessionID(),
		clusterwire.ClusterTimeUnit_NANOS,
		a.appVersion,
	)
}

// PollArchiveEvents is a stub; returns 0.
func (a *Agent) PollArchiveEvents() int { return 0 }

// LocalLogRecordingID returns the local log recording id (default nullValue).
func (a *Agent) LocalLogRecordingID() int64 { return a.localLogRecordingID }

// IsRecordingActive reports whether this member's cluster log recording is
// running now.
//
// Behaviour:
//   - false when no [LogRecordingControl] is wired
//   - otherwise the value of [LogRecordingControl.IsActive]
//
// [LogRecordingControl.IsActive] is false before the first successful start
// and after a clean stop. The control reports true after a successful start
// until the next clean stop.
//
// [Agent.LocalLogRecordingID] keeps the last non-null recording id across a
// stop. A non-null id with IsRecordingActive false is expected and is not a
// fault.
//
// Callers that rank on a recording-position counter must read
// IsRecordingActive, not [Agent.LocalLogRecordingID]. A retained id with no
// live recording has no counter. That state must not read as a missing
// counter under live recording.
//
// Concurrency: invoker-only.
func (a *Agent) IsRecordingActive() bool {
	return a.recordingControl != nil && a.recordingControl.IsActive()
}

// SetLogRecordingID sets the local log recording id.
//
// Behaviour:
//   - stores recordingID when no cluster log recording is running
//   - does not overwrite while [Agent.IsRecordingActive] is true
//   - a recording id this agent established from an awaited START signal cannot
//     be renamed by a caller that learned a different id elsewhere
//
// Concurrency: invoker-only.
func (a *Agent) SetLogRecordingID(recordingID int64) {
	if a.IsRecordingActive() {
		return
	}
	a.localLogRecordingID = recordingID
}

// TruncateLogEntry removes every recorded byte at or above logPosition and
// unbinds the log adapter so no position above logPosition remains.
//
// Behaviour:
//   - refuses first when the control is missing, the recording is active, the
//     recording id is null, or logPosition is misaligned
//   - truncates the cluster log recording through [LogTruncateControl]
//   - unbinds the log adapter at logPosition
//   - does not write recording.log; the election lowers the term entry
//   - does not stop a recording
//
// The truncate control stores logPosition as the recording stop position. A
// later [Agent.PrepareForNewLeadership] derives that position, not the
// pre-truncate extent. leadershipTermID is accepted for host-signature parity
// and is unused.
//
// Concurrency: invoker-only. Blocks the invoker for the archive reply.
//
// Returns an error when:
//   - no [LogTruncateControl] is wired
//   - [Agent.IsRecordingActive] reports true
//   - [Agent.LocalLogRecordingID] is the null sentinel
//   - logPosition is negative or is not aligned to [logbuffer.FrameAlignment]
//   - the archive refuses the truncate or the wait times out
//
// On error the log adapter is untouched, the truncate control leaves the
// recording stop position unchanged, and the caller must not lower any
// position. Archive refuse detail, including the data-header partial-update
// case, lives on archive.Client.TruncateRecording.
func (a *Agent) TruncateLogEntry(leadershipTermID, logPosition int64) error {
	_ = leadershipTermID
	if a.truncateControl == nil {
		return errNoLogTruncateControl
	}
	if a.IsRecordingActive() {
		return errTruncateWhileRecording
	}
	recordingID := a.localLogRecordingID
	if recordingID == nullValue {
		return errTruncateUnknownRecording
	}
	if logPosition < 0 || logPosition&(logbuffer.FrameAlignment-1) != 0 {
		return fmt.Errorf("%w: %d", errTruncatePositionMisaligned, logPosition)
	}
	if _, err := a.truncateControl.TruncateLogRecording(recordingID, logPosition); err != nil {
		return err
	}
	a.logAdapter.DisconnectAt(logPosition)
	return nil
}

// AwaitLocalSocketsClosed is a no-op stub.
func (a *Agent) AwaitLocalSocketsClosed(registrationID int64) {}

// FollowerLogJoinReady reports the follower log join parameters for the leader's
// log session id.
//
// Returns false until a [LogJoinSource] is wired and reports an open cluster log
// image for logSessionID. The election waits and retries while this is false,
// and gives up on its leader heartbeat timeout.
//
// IsLeaderStartup is left false; the election sets it before joining.
func (a *Agent) FollowerLogJoinReady(
	logSessionID int32,
) (FollowerLogJoinParams, bool) {
	if a.joinSource == nil {
		return FollowerLogJoinParams{}, false
	}
	return a.joinSource.Ready(logSessionID)
}

// PrepareForNewLeadership resets election leadership state and tears down
// catchup on both halves before any recording stop.
//
// Behaviour:
//   - sets role [RoleFollower]
//   - clears the known leader
//   - resets follower append cadence fields
//   - stops leader-side catchup replays and clears catchupReplayActive
//   - closes the follower catchup receive path
//   - keeps commit
//   - stops the cluster log recording when a control is wired
//   - returns the last-append position on success
//
// Log and ingress tear-down stay for full agent wiring.
//
// Role, known leader, cadence fields, and catchup teardown always run before
// any recording stop attempt, including when the stop later fails. Stop runs
// whenever a control is wired, not only when [LogRecordingControl.IsActive] is
// true. Stop is idempotent when nothing needs draining. That path drains a
// dirty start that never became active.
//
// Catchup teardown runs here, not on the CATCHUP exit. At that exit the log
// adapter still reads the catchup image. No live image replaces the catchup
// image yet. Reaching prepare through the catchup timeout path leaves both
// catchup halves installed otherwise.
//
// Append-rank failure state always clears on prepare. A not-ready run that
// marked while a prior leadership term's recording was active must not freeze
// ranking across a leadership transition. The next duty cycle may open a fresh
// not-ready run against the recording that prepare starts later.
//
// Returns (first matching branch):
//   - logPosition, nil: control is nil, or localLogRecordingID is negative
//   - derived archive stop position, nil: retained recording id is
//     non-negative and [LogRecordingControl.StopPositionFor] succeeds. After a
//     truncate the control reports the truncate position, so prepare does not
//     re-raise above that bound
//   - [recordinglog.NullPosition], err: stop fails. When the recording was
//     active the control stays active so a later prepare retries the stop.
//     Caller must not adopt lastAppendPosition
//   - [recordinglog.NullPosition], err: StopPositionFor fails after a clean
//     stop. The control is inactive. A later prepare retries derivation.
//     Caller must not adopt lastAppendPosition
func (a *Agent) PrepareForNewLeadership(
	logPosition int64,
	nowNs int64,
) (
	lastAppendPosition int64,
	err error,
) {
	a.RoleSet(RoleFollower)
	a.SetLeaderMemberID(nullMemberID)
	a.lastPublishedAppendPosition = 0
	a.timeOfLastAppendPositionSendNs = 0
	a.appendRankFailure = nil
	a.appendNotReadySinceNs = nullValue
	a.StopAllCatchups()
	a.RemoveCatchupLogDestination()
	_ = nowNs

	if a.recordingControl == nil {
		return logPosition, nil
	}
	if stopErr := a.recordingControl.StopLogRecording(); stopErr != nil {
		return recordinglog.NullPosition, stopErr
	}
	if a.localLogRecordingID < 0 {
		return logPosition, nil
	}

	stopPosition, err := a.recordingControl.StopPositionFor(a.localLogRecordingID)
	if err != nil {
		return recordinglog.NullPosition, err
	}
	return stopPosition, nil
}

// LogPublicationReseed rebuilds the exclusive log publication so its join
// position matches the durable append high-water. Returns the new session id.
//
// Used by [Agent.JoinLogAsLeader] when a former follower becomes leader and the
// existing exclusive publication sits below logPosition.
type LogPublicationReseed func(position int64) (sessionID int32, err error)

// SetLogPublicationReseed wires or clears the [LogPublicationReseed] hook.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetLogPublicationReseed(f LogPublicationReseed) {
	a.logPublicationReseed = f
}

// JoinLogAsLeader sets role [RoleLeader], records the leadership term, stores
// logPosition as the NewLeadershipTermEvent term base, clears the follower
// append target (leaderMemberID to nullMemberID), and starts the LOCAL cluster
// log recording when a control is wired.
//
// Role and term fields write before the recording start.
//
// When a [LogPublicationReseed] is wired and the bound exclusive publication
// position is below logPosition, reseeds the publication at logPosition before
// starting the recording so LOCAL extend can match the recording stop.
//
// A failed start still keeps the agent in the leader role for the term install
// path.
//
// Returns an error when:
//   - a control is wired, the recording is not already active, and the log
//     publisher or its publication is unbound
//   - reseed fails
//   - the start fails
//
// Nil control skips recording and returns nil.
//
// isStartup is accepted for host-signature parity; the startup branch lands
// later.
func (a *Agent) JoinLogAsLeader(
	leadershipTermID int64,
	logPosition int64,
	logSessionID int32,
	isStartup bool,
) error {
	a.RoleSet(RoleLeader)
	a.leadershipTermID = leadershipTermID
	a.SetLeaderMemberID(nullMemberID)
	a.termBaseLogPosition = logPosition
	_ = isStartup

	if a.IsRecordingActive() || a.recordingControl == nil {
		return nil
	}
	if a.logPublisher == nil || a.logPublisher.Publication() == nil {
		return errors.New("consensus: join log as leader: log publication is not bound")
	}
	// Reseed may already have run in AddLogPublication when entering leader
	// log replication. Re-run only when the publication is still behind.
	sessionID := logSessionID
	if reseeded, did, reseedErr := a.reseedLogPublicationIfBehind(logPosition); reseedErr != nil {
		return reseedErr
	} else if did {
		sessionID = reseeded
	}
	// Empty recording channel: control uses the configured live log channel.
	return a.adoptLogRecording(archivewire.SourceLocation_LOCAL, sessionID, "")
}

// JoinLogAsFollower binds the live cluster log image for params.LogSessionID
// into the wired [LogAdapter], starts the REMOTE cluster log recording when a
// control is wired, and sets role [RoleFollower].
//
// Returns:
//   - joined false, nil: no [LogJoinSource] or no [LogAdapter] is wired, or the
//     source no longer holds an open image for that session (election retries
//     until leader heartbeat timeout)
//   - joined false, err: the recording cannot be started, or a live recording
//     cannot be stopped after the image is gone post-start
//   - joined true, nil: the image is bound
//
// The image is looked up before start and again after. The second check is
// required: the start await drives transport DoWork and can close the
// pre-start image. A live recording is stopped when that re-fetch is nil.
// Clean stop leaves the join free to retry; stop error fails closed for
// prepare drain.
//
// Does not touch the known leader id: the term install already set it.
//
// Clearing the known leader id would stop follower commit notification and
// append reporting.
// nowNs is accepted for handler-signature parity and is unused.
func (a *Agent) JoinLogAsFollower(
	params FollowerLogJoinParams,
	nowNs int64,
) (
	joined bool,
	err error,
) {
	_ = nowNs
	if a.joinSource == nil || a.logAdapter == nil {
		return false, nil
	}
	if a.joinSource.Image(params.LogSessionID) == nil {
		return false, nil
	}

	// Skip start while a recording is already active (idempotent re-join).
	//
	// Prepare stops between terms before the next join starts a new one.
	//
	// Pass params.LogChannel so catchup joins record the catchup destination
	// (image join at the follower log position), not the live multicast
	// late-join image.
	if !a.IsRecordingActive() && a.recordingControl != nil {
		if startErr := a.adoptLogRecording(
			archivewire.SourceLocation_REMOTE,
			params.LogSessionID,
			params.LogChannel,
		); startErr != nil {
			return false, startErr
		}
	}

	image := a.joinSource.Image(params.LogSessionID)
	if image == nil {
		if stopErr := a.stopLogRecordingAfterVanishedImage(); stopErr != nil {
			return false, stopErr
		}
		return false, nil
	}

	a.logAdapter.SetImage(image)
	a.RoleSet(RoleFollower)
	return true, nil
}

// stopLogRecordingAfterVanishedImage stops a live recording after the join
// image is gone post-start.
//
// No-op when nothing is live.
//
// On stop failure the control stays active so prepare can retry drain.
//
// Returns the stop error, or nil on success or no-op.
func (a *Agent) stopLogRecordingAfterVanishedImage() error {
	if !a.IsRecordingActive() {
		return nil
	}
	return a.recordingControl.StopLogRecording()
}

// adoptLogRecording starts the cluster log recording and stores the catalog
// id.
//
// Caller must ensure:
//   - recordingControl is non-nil
//   - the recording is not already active
//
// recordingChannel is the base channel for the recording (no session-id).
// Empty uses the control's configured live log channel.
//
// After a successful start the control reports [LogRecordingControl.IsActive]
// true.
//
// Returns an error when StartLogRecording fails. The control stays inactive.
func (a *Agent) adoptLogRecording(
	sourceLocation archivewire.SourceLocation,
	logSessionID int32,
	recordingChannel string,
) error {
	recordingID, err := a.recordingControl.StartLogRecording(
		sourceLocation,
		logSessionID,
		recordingChannel,
	)
	if err != nil {
		return err
	}
	a.localLogRecordingID = recordingID
	return nil
}

// ElectionComplete clears the [ElectionRunner] and stamps
// timeOfLastLogUpdateNs (settled log-update clock). [PositionTable] and
// [ConsensusEgress] stay wired.
//
// Behaviour:
//   - leader: stamps nowNs minus one heartbeat interval so the first steady
//     tick publishes a commit heartbeat
//   - any other role: stamps nowNs so the quiet-tick trigger waits one full
//     leader heartbeat timeout
//   - without a stamp the field stays 0; a duty cycle on an absolute clock
//     re-enters election on the next tick
//   - does not write timeOfLastLeaderUpdateNs
//   - does not call [Agent.SetElection]
//
// Concurrency: invoker-only.
func (a *Agent) ElectionComplete(nowNs int64) {
	a.election = nil
	if a.role == RoleLeader {
		a.timeOfLastLogUpdateNs = nowNs - a.leaderHeartbeatIntervalNs
		return
	}
	a.timeOfLastLogUpdateNs = nowNs
}

// DoWork runs one duty-cycle tick:
//   - slow tick
//   - consensus ingress poll (always, even when election non-nil)
//   - [ElectionRunner.DoWork] when an election is live, else consensusWork
//
// Concurrency: invoker-only; see [Agent].
func (a *Agent) DoWork(nowNs int64) int {
	work := 0
	noteDoWorkStep("slowTick")
	work += a.slowTickIfDue(nowNs)
	noteDoWorkStep("pollIngress")
	work += a.pollConsensusIngress(nowNs)
	if a.election != nil {
		noteDoWorkStep("election.DoWork")
		work += a.election.DoWork(nowNs)
	} else {
		noteDoWorkStep("consensusWork")
		work += a.consensusWork(nowNs)
	}
	return work
}

func (a *Agent) pollConsensusIngress(nowNs int64) int {
	if a.ingress == nil {
		return 0
	}
	a.ingressPollNowNs = nowNs
	return a.ingress.Poll(defaultConsensusIngressFragmentLimit, a.ingressHandler)
}

func (a *Agent) onIngressFragment(buf []byte, offset int) {
	a.handleConsensusFragment(buf, offset, a.ingressPollNowNs)
}

// handleConsensusFragment routes one consensus frame.
//
// When election is live: stamp leader-update clock for NewLeadershipTerm /
// CommitPosition (term >= current), then
// [ElectionRunner.HandleConsensusMessage]. When election is nil: steady-state
// AppendPosition / CommitPosition / NewLeadershipTerm / CanvassPosition
// dispatch.
func (a *Agent) handleConsensusFragment(buf []byte, offset int, nowNs int64) {
	if a.election != nil {
		a.noteLeaderUpdateClockFromFragment(buf, offset, nowNs)
		_ = a.election.HandleConsensusMessage(buf, offset)
		return
	}
	a.handleSteadyConsensusFragment(buf, offset, nowNs)
}

// noteLeaderUpdateClockFromFragment peeks MessageHeader and stamps
// timeOfLastLeaderUpdateNs for NewLeadershipTerm or CommitPosition when
// decoded leadershipTermID >= a.leadershipTermID. Used on the election
// ingress path before HandleConsensusMessage (clock first, then dispatch).
func (a *Agent) noteLeaderUpdateClockFromFragment(buf []byte, offset int, nowNs int64) {
	templateID, ok := consensusMessageTemplateID(buf, offset)
	if !ok {
		return
	}
	switch templateID {
	case clusterwire.NewLeadershipTermSbeTemplateID:
		fields, ok := codecs.TryDecodeNewLeadershipTermAt(buf, offset)
		if ok {
			a.stampLeaderUpdateClock(fields.LeadershipTermID, nowNs)
		}
	case clusterwire.CommitPositionSbeTemplateID:
		fields, ok := codecs.TryDecodeCommitPositionAt(buf, offset)
		if ok {
			a.stampLeaderUpdateClock(fields.LeadershipTermID, nowNs)
		}
	}
}

// consensusMessageTemplateID returns the SBE template id when the MessageHeader
// is valid for the cluster schema.
func consensusMessageTemplateID(buf []byte, offset int) (templateID uint16, ok bool) {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(buf, offset)
	if !ok || schemaID != clusterwire.MessageHeaderSbeSchemaID {
		return 0, false
	}
	return templateID, true
}

// stampLeaderUpdateClock sets timeOfLastLeaderUpdateNs when termID is current
// or higher.
func (a *Agent) stampLeaderUpdateClock(termID, nowNs int64) {
	if termID >= a.leadershipTermID {
		a.timeOfLastLeaderUpdateNs = nowNs
	}
}

func (a *Agent) handleSteadyConsensusFragment(buf []byte, offset int, nowNs int64) {
	templateID, ok := consensusMessageTemplateID(buf, offset)
	if !ok {
		return
	}
	switch templateID {
	case clusterwire.AppendPositionSbeTemplateID:
		if fields, ok := codecs.TryDecodeAppendPositionAt(buf, offset); ok {
			a.onSteadyAppendPosition(fields, nowNs)
		}
	case clusterwire.CommitPositionSbeTemplateID:
		if fields, ok := codecs.TryDecodeCommitPositionAt(buf, offset); ok {
			a.onSteadyCommitPosition(fields, nowNs)
		}
	case clusterwire.NewLeadershipTermSbeTemplateID:
		if fields, ok := codecs.TryDecodeNewLeadershipTermAt(buf, offset); ok {
			a.onSteadyNewLeadershipTerm(fields, nowNs)
		}
	case clusterwire.CanvassPositionSbeTemplateID:
		if fields, ok := codecs.TryDecodeCanvassPositionAt(buf, offset); ok {
			a.onSteadyCanvassPosition(fields, nowNs)
		}
	case clusterwire.CatchupPositionSbeTemplateID:
		if fields, ok := codecs.TryDecodeCatchupPositionAt(buf, offset); ok {
			// Settled leader: start catchup replay.
			a.OnCatchupPosition(fields)
		}
	}
}

// onSteadyCanvassPosition answers a canvass from a settled leader.
//
// Behaviour:
//   - refuses unless this member is the leader
//   - refuses when the canvasser's log leadership term is above this member's
//     leadership term
//   - on acceptance, updates the canvasser's log position
//   - on acceptance, publishes this leader's current leadership term to the
//     canvasser
//
// Echo and refuse:
//   - the published term echoes the canvasser's own log leadership term
//   - a canvasser installs a term only when that echo matches the value it
//     published
//   - a refused canvasser gets no position update and no publish
//
// Concurrency: invoker-only; see [Agent].
func (a *Agent) onSteadyCanvassPosition(fields codecs.CanvassPositionFields, nowNs int64) {
	_ = nowNs
	if a.role != RoleLeader {
		return
	}
	if fields.LogLeadershipTermID > a.leadershipTermID {
		return
	}
	if a.positions == nil || a.egress == nil {
		return
	}
	a.positions.UpdateMemberPosition(
		fields.FollowerMemberID,
		fields.LogLeadershipTermID,
		fields.LogPosition,
	)

	_ = a.egress.PublishNewLeadershipTerm(CanvassAnswer{
		FollowerMemberID:    fields.FollowerMemberID,
		LogLeadershipTermID: fields.LogLeadershipTermID,
		LeadershipTermID:    a.leadershipTermID,
		LogPosition:         a.logPublisher.Position(),
		CommitPosition:      a.CommitPosition(),
		LeaderRecordingID:   a.localLogRecordingID,
		LogSessionID:        a.AddLogPublication(0),
	})
}

// onSteadyAppendPosition applies a follower AppendPosition on the leader only.
//
// Higher-term appends are ignored (no member update).
func (a *Agent) onSteadyAppendPosition(fields codecs.AppendPositionFields, nowNs int64) {
	_ = nowNs
	if a.role != RoleLeader {
		return
	}
	if fields.LeadershipTermID > a.leadershipTermID {
		return
	}
	if a.positions == nil {
		return
	}
	a.positions.UpdateMemberPosition(
		fields.FollowerMemberID,
		fields.LeadershipTermID,
		fields.LogPosition,
	)
}

// onSteadyCommitPosition stamps the leader-update clock on term >= current,
// and advances follower notified commit only for same-term CommitPosition from
// the known leader (leaderMemberID must not be null).
func (a *Agent) onSteadyCommitPosition(fields codecs.CommitPositionFields, nowNs int64) {
	a.applySteadyLeaderCommitNotify(
		fields.LeadershipTermID,
		fields.LeaderMemberID,
		fields.LogPosition,
		nowNs,
	)
}

// onSteadyNewLeadershipTerm stamps the leader-update clock on term >= current.
//
// Same-term follower notify from a known leader advances notified commit from
// the NewLeadershipTerm commit field; higher term invokes
// [Agent.onUnexpectedHigherTerm].
func (a *Agent) onSteadyNewLeadershipTerm(fields codecs.NewLeadershipTermFields, nowNs int64) {
	a.applySteadyLeaderCommitNotify(
		fields.LeadershipTermID,
		fields.LeaderMemberID,
		fields.CommitPosition,
		nowNs,
	)
}

// applySteadyLeaderCommitNotify is the shared steady-state gate for
// CommitPosition and NewLeadershipTerm:
//
//   - term >= current: stamp timeOfLastLeaderUpdateNs
//   - term > current: [Agent.onUnexpectedHigherTerm], no notify
//   - term < current: no stamp, no notify
//   - same term: follower notify only from known leader
//
// Fail-closed: null or mismatched leaderMemberID must not advance notified
// commit.
func (a *Agent) applySteadyLeaderCommitNotify(
	termID int64,
	leaderMemberID int32,
	commitPosition int64,
	nowNs int64,
) {
	switch {
	case termID > a.leadershipTermID:
		a.stampLeaderUpdateClock(termID, nowNs)
		a.onUnexpectedHigherTerm(nowNs, termID)
		return
	case termID < a.leadershipTermID:
		return
	}

	a.stampLeaderUpdateClock(termID, nowNs)

	// Same term: follower notify only from known leader.
	if a.role != RoleFollower {
		return
	}

	// Fail-closed: unknown or mismatched leader must not advance notified commit.
	if a.leaderMemberID == nullMemberID || leaderMemberID != a.leaderMemberID {
		return
	}
	a.notifiedCommitPosition = max(a.notifiedCommitPosition, commitPosition)
	a.timeOfLastLogUpdateNs = nowNs
}

// onUnexpectedHigherTerm re-enters election when a settled member sees a
// leadership term above its own.
//
// Behaviour: records the observation, then installs via
// [ElectionTriggerHigherTerm] when no election is live and a factory is wired.
// The install return is discarded: fragment poll already counts the work.
//
// Concurrency: invoker-only.
func (a *Agent) onUnexpectedHigherTerm(nowNs, term int64) {
	noteUnexpectedHigherTerm(nowNs, term)
	a.enterElection(ElectionTriggerHigherTerm)
}

func (a *Agent) consensusWork(nowNs int64) int {
	if a.role == RoleLeader {
		return a.leaderConsensusWork(nowNs)
	}
	return a.followerConsensusWork(nowNs)
}

// leaderConsensusWork stamps this member (when set), ranks quorum, and runs the
// commit gate. ACTIVE timer poll is not in this path.
func (a *Agent) leaderConsensusWork(nowNs int64) int {
	if a.appendSource == nil || a.positions == nil {
		return 0
	}
	appendPos, ready := a.resolveAppendPosition(nowNs)
	if !ready {
		return 0
	}

	// Steady stamp via PositionTable only. Skip when thisMemberID is unset;
	// rank and gate still run.
	if a.thisMemberID != nullMemberID {
		a.positions.UpdateMemberPosition(a.thisMemberID, a.leadershipTermID, appendPos)
	}
	quorum := a.positions.QuorumPositionBoundedByLeaderLog(appendPos, nowNs)
	return a.UpdateLeaderPosition(nowNs, quorum)
}

// followerConsensusWork polls the log under the commit/append bound, advances
// local commit from adapter position, then publishes AppendPosition on interval.
//
// The append high-water is read once per tick. That value is shared by the poll
// bound and the publish. The source may scan the log image's received frames, so
// one read per tick is the budget.
//
// Not-ready read:
//   - freezes the poll bound at the adapter position
//   - publishes nothing
func (a *Agent) followerConsensusWork(nowNs int64) int {
	appendPos, ready := a.resolveAppendPosition(nowNs)
	work := 0
	if a.logAdapter != nil {
		var bound int64
		if ready {
			bound = a.followerLogPollBound(appendPos)
		} else {
			bound = a.logAdapter.Position()
		}
		work += a.pollLogAndProposeMax(a.logAdapter, bound)
	}
	if ready {
		work += a.updateFollowerPosition(nowNs, appendPos)
	}
	return work
}

// pollLogAndProposeMax polls adapter under boundPosition and advances commit
// from adapter position. ProposeMax runs even when poll returns 0 (position
// may already match prior consume). Caller must pass non-nil adapter.
func (a *Agent) pollLogAndProposeMax(adapter *LogAdapter, boundPosition int64) int {
	work := adapter.Poll(boundPosition)
	_ = a.ProposeMaxCommit(adapter.Position())
	return work
}

// followerLogPollBound is min(notifiedCommit, appendPos).
//
// Nil AppendPositionSource: bound is notifiedCommit only, whatever appendPos
// the caller read.
func (a *Agent) followerLogPollBound(appendPos int64) int64 {
	if a.appendSource == nil {
		return a.notifiedCommitPosition
	}
	return min(a.notifiedCommitPosition, appendPos)
}

func (a *Agent) updateFollowerPosition(nowNs, appendPos int64) int {
	if a.egress == nil {
		return 0
	}
	position := max(appendPos, a.lastPublishedAppendPosition)
	advanced := position > a.lastPublishedAppendPosition
	intervalDue := nowNs >= a.timeOfLastAppendPositionSendNs+a.leaderHeartbeatIntervalNs
	if !advanced && !intervalDue {
		return 0
	}
	ok := a.egress.PublishAppendPosition(a.leadershipTermID, position, 0 /* flags none */)
	if !ok {
		return 0
	}
	if advanced {
		a.lastPublishedAppendPosition = position
	}
	a.timeOfLastAppendPositionSendNs = nowNs
	return 1
}

var _ AgentHandler = (*Agent)(nil)
