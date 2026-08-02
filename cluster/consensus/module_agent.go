package consensus

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/recordinglog"
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
	liveLogDestination       string
	catchupLogDestination    string
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

var _ LogReplay = (*doneLogReplay)(nil)

// LogReplayFactory builds a [LogReplay] helper for [Agent.NewLogReplay].
//
// Nil factory field: NewLogReplay returns a done-stub at appendPosition.
//
// A non-nil factory that returns nil also falls through to the done-stub
// (avoids soft livelock on leader-replay construction).
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

// CatchupInitiated is a no-op host hook.
//
// Real catchup start (subscription / replicate mesh) is not performed here.
func (a *Agent) CatchupInitiated(int64) {}

// StopAllCatchups is a no-op host hook.
//
// Stop mesh for live catchup sessions is not performed here.
func (a *Agent) StopAllCatchups() {}

// IsCatchupNearLive reports whether catchup can skip to log-init.
//
// Production Agent always returns false. Alternate ElectionHost
// implementations (for example sim) may report near-live.
func (a *Agent) IsCatchupNearLive(int64) bool { return false }

// LiveLogDestination returns the live log destination channel URI.
func (a *Agent) LiveLogDestination() string { return a.liveLogDestination }

// LiveLogDestinationSet sets the live log destination channel URI.
func (a *Agent) LiveLogDestinationSet(destination string) {
	a.liveLogDestination = destination
}

// CatchupLogDestination returns the catchup log destination channel URI.
func (a *Agent) CatchupLogDestination() string { return a.catchupLogDestination }

// CatchupLogDestinationSet sets the catchup log destination channel URI.
func (a *Agent) CatchupLogDestinationSet(destination string) {
	a.catchupLogDestination = destination
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
// [LogPublisher] with a publication is already injected; otherwise null session
// id (-1). Does not create a publication or parse channel/MDC settings.
//
// Required inject order: [Agent.SetLogPublisher] and [LogPublisher.SetPublication]
// before any caller that stores the returned session id (for example election
// leader-log replication). Bind after this call leaves the caller with -1 while
// later NewLeadershipTermEvent would write a real session id.
//
// appendPosition is reserved for initialPosition wiring and ignored here.
func (a *Agent) AddLogPublication(appendPosition int64) int32 {
	_ = appendPosition
	if a.logPublisher != nil && a.logPublisher.Publication() != nil {
		return a.logPublisher.SessionID()
	}
	return int32(nullValue)
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

// TruncateLogEntry is a no-op stub.
func (a *Agent) TruncateLogEntry(leadershipTermID, logPosition int64) {}

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

// PrepareForNewLeadership sets role [RoleFollower], clears the known leader,
// resets follower append cadence state, stops the cluster log recording when a
// control is wired, and returns the last-append position.
//
// Commit is kept.
//
// Log and ingress tear-down land with full agent wiring.
//
// Role, known leader, and cadence fields always reset before any stop attempt,
// including when the stop later fails.
//
// Stop runs whenever a control is wired, not only when
// [LogRecordingControl.IsActive] is true. Stop is idempotent when nothing
// needs draining. That path drains a dirty start that never became active.
//
// Returns:
//   - archive stop position, nil: a live recording was stopped
//     ([LogRecordingControl.IsActive] was true before the stop)
//   - logPosition, nil: no live recording, control is nil, or stop was a clean
//     no-op
//   - [recordinglog.NullPosition], err: stop failed; when the recording was
//     active the control stays active so a later prepare retries the stop
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
	_ = nowNs

	if a.recordingControl == nil {
		return logPosition, nil
	}

	wasActive := a.recordingControl.IsActive()
	stopPosition, err := a.recordingControl.StopLogRecording()
	if err != nil {
		return recordinglog.NullPosition, err
	}
	if wasActive {
		return stopPosition, nil
	}
	return logPosition, nil
}

// JoinLogAsLeader sets role [RoleLeader], records the leadership term, stores
// logPosition as the NewLeadershipTermEvent term base, clears the follower
// append target (leaderMemberID -> nullMemberID), and starts the LOCAL cluster
// log recording when a control is wired.
//
// Role and term fields write before the recording start.
//
// A failed start still leaves the agent in the leader role for the term install path.
//
// Returns an error when:
//   - a control is wired, the recording is not already active, and the log
//     publisher or its publication is unbound
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
	return a.adoptLogRecording(archivewire.SourceLocation_LOCAL, logSessionID)
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
	if !a.IsRecordingActive() && a.recordingControl != nil {
		if startErr := a.adoptLogRecording(
			archivewire.SourceLocation_REMOTE,
			params.LogSessionID,
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
	_, err := a.recordingControl.StopLogRecording()
	return err
}

// adoptLogRecording starts the cluster log recording and stores the catalog id.
//
// Caller must ensure:
//   - recordingControl is non-nil
//   - the recording is not already active
//
// After a successful start the control reports [LogRecordingControl.IsActive]
// true.
//
// Returns an error when StartLogRecording fails. The control stays inactive.
func (a *Agent) adoptLogRecording(
	sourceLocation archivewire.SourceLocation,
	logSessionID int32,
) error {
	recordingID, err := a.recordingControl.StartLogRecording(
		sourceLocation,
		logSessionID,
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
