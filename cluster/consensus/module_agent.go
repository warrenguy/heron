package consensus

import "sync/atomic"

// nullValue mirrors aeron cnc NullValue (-1) for recording IDs and null session.
// Local unexported const - do not import election.
const nullValue = -1

// Agent is the consensus module duty-cycle owner.
//
//   - Implements [AgentHandler]
//   - Exposes host-delegate methods the election host adapter forwards
//     (member-free host duties)
//   - Holds only consensus-facing interfaces for election, positions, and
//     egress ([ElectionRunner], [PositionTable], [ConsensusEgress]); never
//     election concrete types
//
// Concurrency: invoker-only. Caller runs media-driver/client duty cycle and
// [Agent.DoWork] on one goroutine. Agent does not spawn threads and does not
// own a Client field.
//
// Lifetime: the host adapter outlives the election as implementer of positions
// and egress. [Agent.ElectionComplete] clears the election runner field only.
//
// Positions and egress may be set at construct or later via [Agent.SetPositions]
// / [Agent.SetEgress] (Agent must exist before the host adapter can take
// *Agent). Constructor may also set them for unit tests that skip the host
// adapter.
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
}

// AgentConfig is the constructor bag. Nil interface fields are legal for unit
// tests that exercise Agent host-delegate stubs only, and for production
// bootstrap before the host adapter exists (then call [Agent.SetPositions] /
// [Agent.SetEgress] / [Agent.SetElection]).
type AgentConfig struct {
	Election  ElectionRunner  // optional until election wired
	Positions PositionTable   // optional at construct; set via [Agent.SetPositions]
	Egress    ConsensusEgress // optional at construct; set via [Agent.SetEgress]
	// Commit is mode-1 injected *atomic.Int64. Nil commit:
	// [Agent.ProposeMaxCommit] and [Agent.CommitPosition] return 0 (no store).
	// Production bootstrap passes non-nil.
	Commit *atomic.Int64
	// Role seeds [Agent.Role] / [Agent.RoleSet]. Default [RoleFollower] (zero value).
	Role Role
}

// NewAgent constructs an [Agent] from cfg. localLogRecordingID defaults to
// nullValue.
//
// Concurrency: invoker-only after construction; see [Agent].
//
// Lifetime: caller owns the returned Agent; positions, egress, and election
// may be wired later via [Agent.SetPositions] / [Agent.SetEgress] /
// [Agent.SetElection].
func NewAgent(cfg AgentConfig) *Agent {
	return &Agent{
		election:            cfg.Election,
		positions:           cfg.Positions,
		egress:              cfg.Egress,
		commit:              cfg.Commit,
		role:                cfg.Role,
		localLogRecordingID: nullValue,
	}
}

// SetPositions sets the [PositionTable] used for ranking and member updates.
// Idempotent replace.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Production bootstrap passes non-nil. Nil is legal; ranking becomes a no-op
// until rewired.
func (a *Agent) SetPositions(positions PositionTable) {
	a.positions = positions
}

// SetEgress sets the [ConsensusEgress] used for commit/append publish.
// Idempotent replace.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Production bootstrap passes non-nil. Nil is legal; egress becomes a no-op
// until rewired.
func (a *Agent) SetEgress(egress ConsensusEgress) {
	a.egress = egress
}

// SetElection wires or clears the [ElectionRunner] (bootstrap /
// [Agent.ElectionComplete]).
func (a *Agent) SetElection(runner ElectionRunner) { a.election = runner }

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

// UpdateLeaderPosition proposes max of quorum onto the commit value when commit
// is bound. Returns 1 when the commit advanced, else 0. Nil commit returns 0
// without claiming work.
//
// nowNs and appendPosition are ignored in this skeleton; the host adapter stamps
// member append state before calling this path.
//
// Do not write a.timeOfLastLeaderUpdateNs here.
//
// aeron ConsensusModuleAgent.updateLeaderPosition never touches that clock; it
// is set only on inbound NewLeadershipTerm / CommitPosition. Writing it here
// would delay canvass nomination when the host adapter delegates both methods
// to Agent ([Agent.TimeOfLastLeaderUpdateNs] is the canvass base).
func (a *Agent) UpdateLeaderPosition(nowNs, appendPosition, quorumPosition int64) int {
	prev := a.CommitPosition()
	next := a.ProposeMaxCommit(quorumPosition)
	if next != prev {
		return 1
	}
	return 0
}

// TimeOfLastLeaderUpdateNs returns the last inbound leader-update clock.
// This skeleton never writes the field; it stays 0 until ingress wiring lands.
func (a *Agent) TimeOfLastLeaderUpdateNs() int64 {
	return a.timeOfLastLeaderUpdateNs
}

// doneLogReplay is an immediately-complete [LogReplay] at a fixed position.
// Avoids livelock on leader-replay gap when archive helpers are not yet wired.
type doneLogReplay struct{ position int64 }

func (r *doneLogReplay) DoWork(int64) int { return 0 }
func (r *doneLogReplay) Position() int64  { return r.position }
func (r *doneLogReplay) IsDone() bool     { return true }

var _ LogReplay = (*doneLogReplay)(nil)

// NewLogReplay returns a done-stub at appendPosition ([LogReplay.IsDone] true,
// [LogReplay.DoWork] 0).
func (a *Agent) NewLogReplay(_, appendPosition int64) LogReplay {
	return &doneLogReplay{position: appendPosition}
}

// NewLogReplication returns nil until archive-backed follower [LogReplication]
// lands.
func (a *Agent) NewLogReplication(
	leaderArchiveEndpoint string,
	responseArchiveEndpoint string,
	leaderRecordingID int64,
	stopPosition int64,
	nowNs int64,
) LogReplication {
	return nil
}

// CatchupPoll is a no-op stub; returns 0.
func (a *Agent) CatchupPoll(limitPosition, nowNs int64) int { return 0 }

// CatchupInitiated is a no-op stub.
func (a *Agent) CatchupInitiated(nowNs int64) {}

// StopAllCatchups is a no-op stub.
func (a *Agent) StopAllCatchups() {}

// IsCatchupNearLive is a no-op stub; returns false.
func (a *Agent) IsCatchupNearLive(position int64) bool { return false }

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

// AddLogPublication is a stub; returns null session id (-1).
func (a *Agent) AddLogPublication(appendPosition int64) int32 {
	return int32(nullValue)
}

// AppendNewLeadershipTermEvent is a stub; returns false.
func (a *Agent) AppendNewLeadershipTermEvent(nowNs int64) bool { return false }

// PollArchiveEvents is a stub; returns 0.
func (a *Agent) PollArchiveEvents() int { return 0 }

// LocalLogRecordingID returns the local log recording id (default nullValue).
func (a *Agent) LocalLogRecordingID() int64 { return a.localLogRecordingID }

// SetLogRecordingID sets the local log recording id.
func (a *Agent) SetLogRecordingID(recordingID int64) {
	a.localLogRecordingID = recordingID
}

// TruncateLogEntry is a no-op stub.
func (a *Agent) TruncateLogEntry(leadershipTermID, logPosition int64) {}

// AwaitLocalSocketsClosed is a no-op stub.
func (a *Agent) AwaitLocalSocketsClosed(registrationID int64) {}

// FollowerLogJoinReady is a stub; returns zero [FollowerLogJoinParams] and false.
func (a *Agent) FollowerLogJoinReady(logSessionID int32) (FollowerLogJoinParams, bool) {
	return FollowerLogJoinParams{}, false
}

// PrepareForNewLeadership sets role [RoleFollower] and returns logPosition as
// the last-append placeholder. Log/ingress tear-down lands with full agent
// wiring.
func (a *Agent) PrepareForNewLeadership(logPosition, nowNs int64) int64 {
	a.RoleSet(RoleFollower)
	return logPosition
}

// JoinLogAsLeader sets role [RoleLeader]. Service readiness blocks land with
// full wiring.
func (a *Agent) JoinLogAsLeader(
	leadershipTermID int64,
	logPosition int64,
	logSessionID int32,
	isStartup bool,
) {
	a.RoleSet(RoleLeader)
}

// JoinLogAsFollower is a skeleton; reports not ready.
func (a *Agent) JoinLogAsFollower(params FollowerLogJoinParams, nowNs int64) bool {
	return false
}

// ElectionComplete clears the [ElectionRunner]; [PositionTable] and
// [ConsensusEgress] stay wired.
func (a *Agent) ElectionComplete(nowNs int64) {
	a.election = nil
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

func (a *Agent) slowTickIfDue(nowNs int64) int { return 0 }

func (a *Agent) pollConsensusIngress(nowNs int64) int {
	// When wired:
	//   - election non-nil: [ElectionRunner.HandleConsensusMessage]
	//   - else: steady-state decode AppendPosition / CommitPosition
	return 0
}

func (a *Agent) consensusWork(nowNs int64) int {
	// When wired:
	//   - leader: rank and commit
	//   - follower: append-position interval
	return 0
}

var _ AgentHandler = (*Agent)(nil)
