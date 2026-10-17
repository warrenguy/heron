// Package consensus implements the cluster consensus module agent.
//
// Agent owns the consensus module duty cycle. LogPublisher appends cluster log
// frames. LogAdapter polls the cluster log. Package cluster assembles these
// parts. Package election runs the election state machine.
package consensus

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/heron/aeron"
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
//   - Holds consensus-facing interfaces only. Must not hold election concrete
//     types.
//
// Concurrency: invoker-only. Caller runs the media-driver/client duty cycle and
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

	// prepareInProgress is true while a PrepareForNewLeadership episode is open.
	// Teardown runs on the first call of an episode. Consensus ingress poll skips
	// while prepareInProgress is true.
	prepareInProgress bool

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

	// clientIngress polls client ingress messages. Nil: pollClientIngress
	// returns 0.
	clientIngress ClientIngress

	// clientIngressHandler is handleClientIngressFragment cached once at
	// construction so pollClientIngress does not allocate a method value each
	// duty cycle.
	clientIngressHandler func(buf []byte, offset int) aeron.ControlledAction

	// clientIngressPollNowNs is the duty-cycle clock for the current
	// pollClientIngress batch. Set before Poll. Read by
	// handleClientIngressFragment so the fragment handler is a method value
	// (no per-poll closure).
	clientIngressPollNowNs int64

	// sessionTimeoutNs bounds pre-open deadlines, egress-event deadlines,
	// open-session idle close, and non-leader closing reclaim.
	sessionTimeoutNs int64

	// sessionConnectHandler receives leader-accepted connects. Nil runs
	// acceptSessionConnect. A non-nil handler replaces acceptSessionConnect.
	sessionConnectHandler SessionConnectHandler

	// sessions is the client session registry. The Agent constructs it. Pure
	// state with no transport resources.
	sessions *sessionManager

	// authenticator decides whether client sessions open. Never nil after
	// NewAgent. A nil AgentConfig.Authenticator installs acceptAllAuthenticator.
	authenticator Authenticator

	// sessionAuthProxy is the reused SessionProxy handed to authenticator
	// handlers. proxyFor rebinds the proxy for each call.
	sessionAuthProxy sessionAuthProxy

	// egressAdder registers concurrent per-session egress publications. Nil
	// leaves accepted sessions pending with no add. Redirect and reject entries
	// drop at queue time when the adder is nil.
	egressAdder EgressPublicationAdder

	// ingressEndpoints is the cluster ingress-endpoints string stored as
	// SessionEvent REDIRECT detail ("0=host:port,1=host:port").
	ingressEndpoints string

	// egressScratch holds one SessionEvent, Challenge, or NewLeaderEvent
	// encode for the egress offer helpers.
	egressScratch []byte

	// egressEvents is the capped REDIRECT/ERROR queue. Each entry owns a
	// short-lived egress publication.
	egressEvents []*egressEvent

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
	// [Agent.PrepareForNewLeadership] clears the field so ranking may start a
	// fresh not-ready run in the next term.
	appendRankFailure error

	// lastPublishedAppendPosition is the last successfully published follower
	// unflagged append high-water (advanced path only). Catchup-flagged
	// publishes must not raise this floor. After election close the steady path
	// re-publishes max(recorded, lastPublished) unflagged into the quorum.
	lastPublishedAppendPosition int64

	// lastCatchupPublishedPosition is the last catchup-flagged position
	// published. Used for CatchupPoll cadence only and never as a steady floor.
	lastCatchupPublishedPosition int64

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

	// logPollNowNs is the duty-cycle clock for the current log adapter poll.
	// Set before Poll. handleSessionOpenEvent reads the field so learn stores
	// the current activity time.
	logPollNowNs int64

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

	// liveLogDestination is the live multicast URI added as a receive
	// destination on the log subscription. Empty when the live feed is not
	// attached.
	liveLogDestination string

	// catchupLogDestination is the catchup unicast URI currently added as a
	// receive destination on the log subscription. Empty when catchup is not
	// active.
	catchupLogDestination string

	// catchupReplays maps follower member id to that follower's replay state
	// (replay session id and stopped flag). Empty when no catchup is active. One
	// entry per follower. Distinct followers may catch up concurrently.
	catchupReplays map[int32]catchupReplay

	// logPublicationReseed rebuilds the exclusive log publication for
	// AddLogPublication. Nil disables the reseed.
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

	// serviceAcks holds per-service ServiceAck queues. Always non-nil after
	// NewAgent. With zero services the tracker has zero queues and hasReached
	// returns true.
	serviceAcks *serviceAckTracker

	// serviceAckID is the expected next ack id. Starts at 0. Advances only in
	// removeServiceAckHead.
	serviceAckID int64

	// serviceAckFailure is the first marked receive-time out-of-sequence
	// failure, or nil until marked. Full contract on [Agent.ServiceAckFailure].
	serviceAckFailure error

	// serviceIngress polls service control fragments (stream 105). Nil means
	// pollServiceControl returns 0.
	serviceIngress ConsensusIngress

	// serviceIngressHandler is handleServiceControlFragment cached once at
	// construction so pollServiceControl does not allocate a method value each
	// duty cycle.
	serviceIngressHandler func(buf []byte, offset int)

	// serviceProxy publishes JoinLog and RequestServiceAck (stream 104). Nil
	// means no service control publication is wired. JoinLog publish is a
	// silent no-op until a proxy is set. RequestServiceAck returns
	// ErrServiceControlUnbound.
	serviceProxy *ServiceProxy

	// pendingJoinLog is one JoinLog awaiting a successful publish, or nil.
	// Stored on a retriable publish outcome. Retried by retryPendingJoinLog
	// after the election or steady branch. Replaced by a newer join. Cleared by
	// PrepareForNewLeadership.
	pendingJoinLog *codecs.JoinLogFields

	// serviceAckWaits holds log positions of published coordination points
	// whose acks are not yet consumed. FIFO. The first entry pairs with
	// serviceAckID.
	serviceAckWaits []int64

	// snapshotWaitPosition is the log position of a pending snapshot action wait
	// in the current leadership term, or 0 when none. PrepareForNewLeadership
	// retires the wait and clears this field.
	snapshotWaitPosition int64

	// abandonedSnapshotPositions holds snapshot wait log positions retired by
	// PrepareForNewLeadership. The slice grows one entry per retired wait. The
	// slice is never cleared. handleServiceAck drops a ServiceAck at a retired
	// position unless the first queued coordination wait uses the same position.
	abandonedSnapshotPositions []int64

	// abandonedSnapshotAckIDs stores dropped ServiceAck ids at retired snapshot
	// positions, one slice per service. serviceAckID advances when every service
	// has dropped the current expected id.
	abandonedSnapshotAckIDs [][]int64

	// servicePublishFailure is the first terminal publish failure on the stream
	// 104 publish path, or nil. Marked once. Distinct from serviceAckFailure on
	// the stream 105 receive path.
	servicePublishFailure error

	// localLogChannel and localLogStreamID name the log for the leader JoinLog.
	// SetLocalLogStream sets both from the resolved log publication channel and
	// log stream id.
	localLogChannel  string
	localLogStreamID int32

	// timers is the member-local wheel plus correlation maps. Always non-nil
	// after NewAgent.
	timers *timerService

	// expiredTimerCountByCorrelationID counts committed TimerEvents that found
	// no timer to cancel, per correlation id. handleScheduleTimer consumes one
	// count per request and deletes the entry at zero. Never reset.
	expiredTimerCountByCorrelationID map[int64]int64

	// timerExpiryHandler is handleTimerExpiry cached once at construction so
	// timerWork does not allocate a method value each duty cycle.
	timerExpiryHandler func(correlationID, now int64) bool

	// timerFailure is the first timer registry failure, or nil until marked.
	// Marked once. Later refusals do not replace the cause.
	timerFailure error
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

	// Authenticator decides whether client sessions open. Nil runs the
	// accept-all default: empty principal, never challenges, never rejects.
	//
	// Ownership: the agent does not own the authenticator. No Close duty.
	Authenticator Authenticator

	// SessionTimeoutNs: zero -> DefaultSessionTimeoutNs (10s). Negative panics
	// at NewAgent.
	//
	// One value bounds pre-open egress, queued egress events, challenge waits,
	// open-session idle close, and non-leader closing reclaim.
	SessionTimeoutNs int64

	// ServiceCount sizes the ServiceAck tracker. Zero for a member without
	// clustered services. A negative value panics at NewAgent.
	ServiceCount int
}

// NewAgent constructs an [Agent] from cfg.
//
// Defaults and panics:
//   - localLogRecordingID zero becomes nullValue
//   - LeaderHeartbeatIntervalNs zero becomes
//     DefaultLeaderHeartbeatIntervalNs; negative panics
//   - LeaderHeartbeatTimeoutNs zero becomes
//     DefaultLeaderHeartbeatTimeoutNs; negative panics
//   - AppendPositionNotReadyTimeoutNs zero becomes
//     DefaultAppendPositionNotReadyTimeoutNs; negative panics
//   - SessionTimeoutNs zero becomes DefaultSessionTimeoutNs; negative panics
//   - ServiceCount negative panics; zero builds a ServiceAck tracker with zero
//     queues (hasReached returns true)
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
	sessionTimeout := cfg.SessionTimeoutNs
	if sessionTimeout == 0 {
		sessionTimeout = DefaultSessionTimeoutNs
	}
	if sessionTimeout < 0 {
		panic("consensus: negative SessionTimeoutNs")
	}
	if cfg.ServiceCount < 0 {
		panic("consensus: negative ServiceCount")
	}
	auth := cfg.Authenticator
	if auth == nil {
		auth = acceptAllAuthenticator{}
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
		sessionTimeoutNs:          sessionTimeout,
		ingress:                   cfg.Ingress,
		appendSource:              cfg.AppendSource,
		appendNotReadySinceNs:     nullValue,
		appendNotReadyTimeoutNs:   appendTimeout,
		logPublisher:              cfg.LogPublisher,
		joinSource:                cfg.LogJoinSource,
		termBaseLogPosition:       cfg.TermBaseLogPosition,
		appVersion:                cfg.AppVersion,
		catchupReplays:            make(map[int32]catchupReplay),
		sessions:                  newSessionManager(),
		authenticator:             auth,
		serviceAcks:               newServiceAckTracker(cfg.ServiceCount),
		timers:                    newTimerService(),

		expiredTimerCountByCorrelationID: make(map[int64]int64),
	}
	a.sessionAuthProxy.agent = a
	a.ingressHandler = a.onIngressFragment
	a.clientIngressHandler = a.handleClientIngressFragment
	a.serviceIngressHandler = a.handleServiceControlFragment
	a.timerExpiryHandler = a.handleTimerExpiry
	a.SetLogAdapter(cfg.LogAdapter)
	return a
}

// AppendRankFailure returns the marked append-rank failure, or nil.
//
// Behaviour:
//   - non-nil once a cluster log recording has been active with no
//     recording-position counter longer than the configured bound
//   - marked once: first cause kept; later ticks do not replace it
//   - a later ready source report does not clear the marked failure
//   - [Agent.PrepareForNewLeadership] clears the marked failure
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
// Behaviour:
//   - non-nil runner: installs as [ElectionTriggerBootstrap] and increments
//     [Agent.ElectionCount]
//   - nil runner: clears the field only. Does not change count or trigger
//   - always: clears prepareInProgress through [Agent.installElection]
//
// [Agent.ElectionComplete] clears the runner directly. ElectionComplete does
// not call SetElection.
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

// SetLeadershipTermID sets the Agent-local leadership term. Does not send
// egress.
func (a *Agent) SetLeadershipTermID(id int64) { a.leadershipTermID = id }

// ThisMemberID returns this node's cluster member id. Zero is a valid member
// id; -1 means unset only when bootstrap or a caller stored -1.
func (a *Agent) ThisMemberID() int32 { return a.thisMemberID }

// SetThisMemberID sets this node's cluster member id. Steady leader stamp uses
// [PositionTable.UpdateMemberPosition]. Member id 0 is valid. Use -1 for
// unset.
func (a *Agent) SetThisMemberID(id int32) { a.thisMemberID = id }

// LeaderMemberID returns the stored leader member id. Zero is a valid member
// id; -1 means unset only when bootstrap or a caller stored -1.
func (a *Agent) LeaderMemberID() int32 { return a.leaderMemberID }

// SetLeaderMemberID sets the known leader id for steady notify gates and the
// AppendPosition target. Member id 0 is valid. Use -1 for unset.
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

// AppendPosition flag bits on the consensus AppendPosition wire field.
const (
	// AppendPositionFlagNone marks a steady (non-catchup) AppendPosition.
	AppendPositionFlagNone uint8 = 0

	// AppendPositionFlagCatchup marks progress during catchup. Leaders exclude
	// these positions from the commit quorum. Use is limited to catchup
	// completion tracking and liveness.
	AppendPositionFlagCatchup uint8 = 1
)

// isCatchupAppendPosition reports whether flags mark a catchup AppendPosition.
func isCatchupAppendPosition(flags uint8) bool {
	return flags&AppendPositionFlagCatchup != 0
}

// CatchupPoll polls catchup/live log progress under limitPosition.
//
// When a [LogAdapter] Image is bound, polls with bound = limitPosition (same
// BoundedControlledPoll substrate as followerConsensusWork) and advances
// commit via [Agent.ProposeMaxCommit] from adapter position. Then publishes
// the adapter position under [AppendPositionFlagCatchup] so the leader can
// observe catchup progress.
//
// The published position is the consumed merged-image position, not a recorded
// append position. Heron follower recording is channel-based and does not see
// unicast catchup frames, so the recorded high-water stalls during catchup.
//
// Returns 0 when adapter or image is nil.
//
// Concurrency: invoker-only. Election-live catchup is the sole consumer while
// an election runner is set. Steady followerConsensusWork does not run then.
func (a *Agent) CatchupPoll(limitPosition, nowNs int64) int {
	adapter := a.logAdapter
	if adapter == nil || adapter.Image() == nil {
		return 0
	}
	a.logPollNowNs = nowNs
	work := a.pollLogAndProposeMax(adapter, limitPosition)
	work += a.updateFollowerPosition(nowNs, adapter.Position(), AppendPositionFlagCatchup)
	return work
}

// liveAddMaxWindow is ReplayMerge.LIVE_ADD_MAX_WINDOW (32 MiB). Caps the
// near-live window so a large term buffer does not force a wide catchup gap.
const liveAddMaxWindow int64 = 32 * 1024 * 1024

// CatchupInitiated notes the follower append-position cadence time so catchup
// progress publish does not wait a full heartbeat interval. Clears the catchup
// publish high-water so the first CatchupPoll is treated as advanced.
//
// Concurrency: invoker-only.
func (a *Agent) CatchupInitiated(nowNs int64) {
	a.timeOfLastAppendPositionSendNs = nowNs
	a.lastCatchupPublishedPosition = 0
}

// StopAllCatchups stops every leader-side catchup replay and drains
// catchupReplays so a later OnCatchupPosition may start new replays.
//
// Behaviour: clears the map whether or not the archive stop succeeds. A stop
// failure must not leave stale map entries for the life of the Agent.
//
// Concurrency: invoker-only. Blocks for the archive control response.
func (a *Agent) StopAllCatchups() {
	clear(a.catchupReplays)
	if a.catchupControl == nil {
		return
	}
	_ = a.catchupControl.StopCatchupReplays()
}

// CatchupReplayCount returns how many leader-side catchup replays the per-member
// map currently tracks.
//
// Idle is zero. Concurrent catchup of two followers reports two while both
// replays are live. TrackCatchupCompletion deletes an entry only after
// StopCatchup publishes successfully.
//
// Concurrency: invoker-only.
func (a *Agent) CatchupReplayCount() int {
	return len(a.catchupReplays)
}

// RemoveCatchupLogDestination removes the catchup receive destination and
// restores the live destination when the live feed is absent.
//
// Behaviour: StopCatchup dispatch and [Agent.PrepareForNewLeadership] call
// this. Idempotent when the catchup destination is already absent.
//
// Concurrency: invoker-only.
func (a *Agent) RemoveCatchupLogDestination() {
	if a.catchupControl == nil {
		return
	}
	_ = a.catchupControl.RemoveCatchupLogDestination()
}

// LiveLogDestination returns the live log receive-destination URI, or empty
// when the live feed is not attached.
//
// Concurrency: invoker-only.
func (a *Agent) LiveLogDestination() string {
	return a.liveLogDestination
}

// LiveLogDestinationSet records or clears the live log receive-destination URI.
//
// Empty clears. Does not add or remove the destination on the subscription.
//
// Concurrency: invoker-only.
func (a *Agent) LiveLogDestinationSet(uri string) {
	a.liveLogDestination = uri
}

// AddLiveLogDestination adds the live log channel as a receive destination.
// No-op when the live feed is already attached. Idempotent.
//
// On failure LiveLogDestination stays empty. The next tick retries.
// The catchup feed keeps running.
//
// Concurrency: invoker-only.
func (a *Agent) AddLiveLogDestination() {
	if a.catchupControl == nil {
		return
	}
	_ = a.catchupControl.AddLiveLogDestination()
}

// CatchupLogDestination returns the catchup receive-destination URI, or empty
// when catchup is not active.
//
// Concurrency: invoker-only.
func (a *Agent) CatchupLogDestination() string {
	return a.catchupLogDestination
}

// CatchupLogDestinationSet records or clears the catchup receive-destination
// URI.
//
// Empty clears. Does not add or remove the destination on the subscription.
//
// Concurrency: invoker-only.
func (a *Agent) CatchupLogDestinationSet(uri string) {
	a.catchupLogDestination = uri
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

// CatchupControl attaches the follower catchup feed as a receive destination
// on the log subscription and starts leader-side archive replay for a catchup
// request.
//
// Concurrency: invoker-only. Blocks for archive control, subscription reseed,
// and destination commands.
//
// Ownership: implementations own destination state they attach and must clear
// that state with the module. They do not own the log subscription.
type CatchupControl interface {
	// EnsureCatchupLogDestination reseeds the log subscription and adds a
	// catchup receive destination for catchupEndpoint. Returns the destination
	// channel URI, or empty when not ready. Idempotent once open.
	EnsureCatchupLogDestination(catchupEndpoint string) string

	// RemoveCatchupLogDestination removes the catchup receive destination and
	// restores the live destination when the live feed is absent. Idempotent.
	RemoveCatchupLogDestination() error

	// AddLiveLogDestination adds the live log channel as a receive destination.
	// No-op when the live feed is already attached. Idempotent.
	//
	// On failure LiveLogDestination stays empty. The next tick retries.
	AddLiveLogDestination() error

	// StartCatchupReplay starts an archive replay of the local log recording
	// from logPosition toward catchupEndpoint with logSessionID. Returns the
	// replay session id, or an error.
	StartCatchupReplay(
		logPosition int64,
		catchupEndpoint string,
		logSessionID int32,
	) (replaySessionID int64, err error)

	// StopCatchupReplay stops one archive replay by session id. Idempotent when
	// the session is not retained. Removes the session from the control ledger
	// only after a successful stop. A failed stop can be retried.
	StopCatchupReplay(replaySessionID int64) error

	// StopCatchupReplays stops every archive replay this control started. Idempotent.
	StopCatchupReplays() error
}

// SetCatchupControl wires or clears the [CatchupControl].
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetCatchupControl(c CatchupControl) { a.catchupControl = c }

// EnsureCatchupLogDestination attaches the follower catchup receive destination.
//
// The control owns the open state and is idempotent once open, so this method
// delegates on every call rather than caching the URI.
//
// Returns empty when no control is wired or the open is not ready.
func (a *Agent) EnsureCatchupLogDestination(catchupEndpoint string) string {
	if a.catchupControl == nil {
		return ""
	}
	return a.catchupControl.EnsureCatchupLogDestination(catchupEndpoint)
}

// catchupReplay is the leader's per-follower replay state.
//
// stopped records that the archive stop succeeded. A failed publish after stop
// retries the publish alone and must not re-stop the archive session.
type catchupReplay struct {
	replaySessionID int64
	stopped         bool
}

// TrackCatchupCompletion stops a leader-side catchup replay when logPosition
// reaches the leader log publication position, then publishes StopCatchup.
//
// Behaviour:
//   - returns when no map entry exists and flags are not catchup
//   - when stop is still needed, or there is no map entry, returns if
//     logPublisher is nil or logPosition is below the leader publication
//     position
//   - when a map entry is active and not yet stopped, stops that archive
//     session
//   - a stop failure keeps the entry so the next AppendPosition retries the
//     stop
//   - after stop succeeds or was already stopped, publishes StopCatchup
//     without re-checking logPosition against the leader publication
//   - deletes the map entry only on publish success; a failed publish retries
//     the publish alone even if the leader log has advanced
//
// An active map entry qualifies even without the catchup flag. A follower that
// already detached still drives cleanup to completion.
//
// Concurrency: invoker-only. May block for the archive control response.
func (a *Agent) TrackCatchupCompletion(memberID int32, logPosition int64, flags uint8) {
	replay, active := a.catchupReplays[memberID]
	if !active && !isCatchupAppendPosition(flags) {
		return
	}
	needStop := active && !replay.stopped
	// Position gate covers the archive-stop half and the no-entry flagged publish
	// path. Once stop has latched, publish retry must not re-check position.
	if a.logPublisher == nil {
		return
	}
	if (needStop || !active) && logPosition < a.logPublisher.Position() {
		return
	}
	if needStop {
		if a.catchupControl == nil ||
			a.catchupControl.StopCatchupReplay(replay.replaySessionID) != nil {
			// Entry stays active. The next AppendPosition from this member
			// retries the stop, flagged or not.
			return
		}
		replay.stopped = true
		a.catchupReplays[memberID] = replay
	}
	if a.egress != nil && a.egress.PublishStopCatchup(a.leadershipTermID, memberID) {
		delete(a.catchupReplays, memberID)
	}
}

// OnCatchupPosition starts a leader-side archive catchup replay for a
// follower.
//
// Behaviour:
//   - refuses unless this member is the leader
//   - refuses when fields.LeadershipTermID is above this member's leadership
//     term
//   - refuses when CatchupEndpoint is empty
//   - on a same-member request, stops that follower's active replay first so a
//     retry can replace an abandoned catchup within the term
//   - a same-member stop failure keeps the map entry and returns without a
//     new start; the ledger session id stays tracked for retry
//   - refuses when no [CatchupControl] is wired, no log publisher is bound, or
//     the local log recording id is null
//   - starts one replay per call and stores it under the follower member id
//   - allows concurrent catchups: each follower gets its own map entry
//
// The replay follows the active recording. Each map entry remains that
// follower's log feed until single-session stop, [Agent.StopAllCatchups], or
// [Agent.PrepareForNewLeadership].
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
	if a.catchupControl == nil {
		// Deletes the stale map entry so a later control wire does not inherit it.
		delete(a.catchupReplays, fields.FollowerMemberID)
		return
	}
	if existing, ok := a.catchupReplays[fields.FollowerMemberID]; ok {
		// Same-member restart: stop the stale replay before starting a new one. Keep
		// the map entry on stop failure so the ledger session stays tracked.
		if err := a.catchupControl.StopCatchupReplay(existing.replaySessionID); err != nil {
			return
		}
		delete(a.catchupReplays, fields.FollowerMemberID)
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
	replaySessionID, err := a.catchupControl.StartCatchupReplay(
		fields.LogPosition,
		fields.CatchupEndpoint,
		logSessionID,
	)
	if err != nil {
		return
	}
	a.catchupReplays[fields.FollowerMemberID] = catchupReplay{
		replaySessionID: replaySessionID,
	}
}

// SetLogPublisher wires or clears the [LogPublisher] used for log append and
// NewLeadershipTermEvent.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
func (a *Agent) SetLogPublisher(p *LogPublisher) { a.logPublisher = p }

// SetLogAdapter wires or clears the [LogAdapter] used for follower log poll.
//
// Behaviour:
//   - A non-nil adapter receives this agent as its session event sink and
//     its timer event sink. Committed SessionOpenEvent and SessionCloseEvent
//     frames then update the registry. Committed TimerEvent frames cancel or
//     count their timer.
//   - A nil adapter clears the field only. The previous adapter keeps both
//     sinks.
//
// Concurrency: invoker-only. Must not race with [Agent.DoWork].
func (a *Agent) SetLogAdapter(adapter *LogAdapter) {
	a.logAdapter = adapter
	if adapter != nil {
		adapter.SetSessionEventSink(a)
		adapter.setTimerEventSink(a)
	}
}

// SetTermBaseLogPosition sets the term base written into NewLeadershipTermEvent.
//
// Prefer [Agent.JoinLogAsLeader] on the leader-join path; use this setter for
// tests or bootstrap that skip join.
func (a *Agent) SetTermBaseLogPosition(pos int64) { a.termBaseLogPosition = pos }

// AddLogPublication reseeds the log publication at appendPosition when the
// publication is unbound or its position differs. Returns the session id the
// publisher holds afterwards.
//
// The reseed needs a [LogPublisher], a [LogPublicationReseed] hook and a known
// recording id. Without them the bound publication stays.
//
// Required inject order: [Agent.SetLogPublisher] and
// [LogPublisher.SetPublication] before any caller that stores the returned
// session id. A bind after this call leaves the caller with -1 while a later
// NewLeadershipTermEvent would write a real session id.
//
// Returns:
//   - session id, nil: the bound or reseeded publication
//   - null session id (-1), nil: the publisher is unbound and no reseed ran
//   - session id the publisher holds now, err: the reseed hook failed
func (a *Agent) AddLogPublication(appendPosition int64) (int32, error) {
	reseeded, ok, err := a.reseedLogPublicationAt(appendPosition)
	if ok {
		return reseeded, nil
	}

	// A failed reseed may have closed the old publication. Report the session
	// id the publisher holds now.
	sessionID, _ := a.boundLogSessionID()
	return sessionID, err
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

// reseedLogPublicationAt runs the reseed hook at target when the publication
// is unbound or its position differs from target.
//
// Skips the reseed without a hook, a publisher or a known recording id. With
// no recording id the LOCAL start opens a fresh recording at any join position.
//
// Returns:
//   - session id, true, nil: the hook succeeded
//   - 0, false, nil: no reseed ran
//   - 0, false, err: the hook failed
func (a *Agent) reseedLogPublicationAt(target int64) (sessionID int32, ok bool, err error) {
	if !a.canReseedLogPublication() {
		return 0, false, nil
	}
	bound := a.logPublisher.Publication() != nil
	if bound && a.logPublisher.Position() == target {
		return 0, false, nil
	}
	sessionID, err = a.logPublicationReseed(target)
	if err != nil {
		return 0, false, err
	}
	return sessionID, true, nil
}

// canReseedLogPublication reports whether a reseed hook and a [LogPublisher]
// are wired and the local recording id is known.
func (a *Agent) canReseedLogPublication() bool {
	return a.logPublicationReseed != nil &&
		a.logPublisher != nil &&
		a.localLogRecordingID >= 0
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

// PrepareForNewLeadership resets election leadership state, then polls the
// cluster log recording stop. Does not reset commit.
//
// First call of an episode runs teardown. Later calls poll only.
//
// nowNs is the deadline clock for [LogRecordingControl.PollStopLogRecording].
//
// Returns:
//   - lastAppendPosition, prepared true, nil: stop proof is held
//   - [recordinglog.NullPosition], prepared false, nil: poll again
//   - [recordinglog.NullPosition], prepared false, err: fail closed
//
// Caller must not adopt lastAppendPosition unless prepared is true.
func (a *Agent) PrepareForNewLeadership(
	logPosition int64,
	nowNs int64,
) (
	lastAppendPosition int64,
	prepared bool,
	err error,
) {
	if !a.prepareInProgress {
		a.prepareInProgress = true
		a.teardownForNewLeadership()
	}
	lastAppendPosition, prepared, err = a.pollPrepareStop(logPosition, nowNs)
	if prepared || err != nil {
		a.prepareInProgress = false
	}
	return lastAppendPosition, prepared, err
}

// IsPrepareInProgress reports whether a PrepareForNewLeadership episode is open.
func (a *Agent) IsPrepareInProgress() bool {
	return a.prepareInProgress
}

// teardownForNewLeadership runs first-call prepare teardown. Does not write
// prepareInProgress. Does not poll the recording stop.
//
// Catchup teardown runs here, not on the CATCHUP election exit. At the CATCHUP
// exit the log adapter still reads the catchup image.
func (a *Agent) teardownForNewLeadership() {
	a.RoleSet(RoleFollower)
	a.SetLeaderMemberID(nullMemberID)
	a.lastPublishedAppendPosition = 0
	a.lastCatchupPublishedPosition = 0
	a.timeOfLastAppendPositionSendNs = 0
	a.appendRankFailure = nil
	a.appendNotReadySinceNs = nullValue
	a.pendingJoinLog = nil
	a.retireSnapshotWait()
	a.StopAllCatchups()
	a.RemoveCatchupLogDestination()
	a.releaseSessionsForNewTerm()
}

// pollPrepareStop advances one recording stop poll for prepare. Does not write
// prepareInProgress. Nil control returns logPosition prepared. A non-negative
// retained recording id returns the archive stop when the poll is done.
func (a *Agent) pollPrepareStop(logPosition, nowNs int64) (int64, bool, error) {
	if a.recordingControl == nil {
		return logPosition, true, nil
	}
	stopDone, stopErr := a.recordingControl.PollStopLogRecording(nowNs)
	if stopErr != nil {
		return recordinglog.NullPosition, false, stopErr
	}
	if !stopDone {
		return recordinglog.NullPosition, false, nil
	}
	if a.localLogRecordingID < 0 {
		return logPosition, true, nil
	}
	stopPosition, err := a.recordingControl.StopPositionFor(a.localLogRecordingID)
	if err != nil {
		return recordinglog.NullPosition, false, err
	}
	return stopPosition, true, nil
}

// LogPublicationReseed rebuilds the exclusive log publication so its join
// position matches the durable append high-water. Returns the new session id.
//
// [Agent.AddLogPublication] calls it after an election win when the
// publication is unbound or its position differs from the append position.
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
// With a [LogPublicationReseed] wired and a known recording id, the recording
// start requires the publication at logPosition under logSessionID.
// [Agent.AddLogPublication] reseeds it there earlier. The archive extends the
// LOCAL recording only when the publication join position equals the recording
// stop position.
//
// On success, publishes JoinLog (template 40) when a service proxy is bound.
// Publish failure does not fail JoinLogAsLeader. Retriable outcomes store a
// pending JoinLog for [Agent.retryPendingJoinLog]. Terminal outcomes mark
// [Agent.ServicePublishFailure] once.
//
// A failed start still keeps the agent in the leader role for the term install
// path and publishes no JoinLog.
//
// Returns an error when:
//   - a control is wired, the recording is not already active, and the log
//     publisher or its publication is unbound
//   - the publication fails the recording start check above
//   - the start fails
//
// Nil control skips recording and returns nil. JoinLog still publishes on that
// success path when a proxy is bound.
//
// isStartup is written into the JoinLog IsStartup field.
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

	sessionID, err := a.startLeaderLogRecording(logPosition, logSessionID)
	if err != nil {
		return err
	}
	a.publishLeaderJoinLog(logPosition, sessionID, isStartup)
	return nil
}

// startLeaderLogRecording starts the LOCAL cluster log recording and returns
// the log session id the leader JoinLog advertises.
//
// Behaviour: an active recording or a nil recordingControl skips the start and
// returns logSessionID unchanged. With a reseed hook and a known recording id,
// the start checks the publication against logPosition and logSessionID. The
// start adopts the LOCAL recording with an empty channel, so control uses the
// configured live log channel.
//
// The check never reseeds. A reseed here changes the session id after the
// NewLeadershipTerm fan-out, so no follower can join.
//
// Returns an error when:
//   - the log publisher or its publication is unbound
//   - the check finds a position or session id mismatch
//   - the recording start fails
func (a *Agent) startLeaderLogRecording(
	logPosition int64,
	logSessionID int32,
) (int32, error) {
	if a.IsRecordingActive() || a.recordingControl == nil {
		return logSessionID, nil
	}
	sessionID, bound := a.boundLogSessionID()
	if !bound {
		return 0, errors.New("consensus: join log as leader: log publication is not bound")
	}
	if a.canReseedLogPublication() {
		position := a.logPublisher.Position()
		mismatch := position != logPosition || sessionID != logSessionID
		if mismatch {
			return 0, fmt.Errorf(
				"consensus: join log as leader: log publication %d at %d, election session %d at %d",
				sessionID, position, logSessionID, logPosition,
			)
		}
	}
	if err := a.adoptLogRecording(archivewire.SourceLocation_LOCAL, logSessionID, ""); err != nil {
		return 0, err
	}
	return logSessionID, nil
}

// JoinLogAsFollower binds the live cluster log image for params.LogSessionID
// into the wired [LogAdapter], starts the REMOTE cluster log recording when a
// control is wired, and sets role [RoleFollower].
//
// params.IsBoundRejoin true routes to [Agent.rejoinBoundLog]: no image
// selection, no rebind, recording restart only. Bound re-join publishes no
// JoinLog: the image, recording, and log spec are unchanged.
//
// On a successful fresh bind, publishes JoinLog when a service proxy is bound.
// Publish failure does not fail JoinLogAsFollower. Same pending and terminal
// policy as [Agent.JoinLogAsLeader].
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
// Clean stop leaves the join free to retry. Stop error fails closed for
// prepare drain.
//
// Does not touch the known leader id: the term install already set it.
// Clearing the known leader id would stop follower commit notification and
// append reporting.
//
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
	if params.IsBoundRejoin {
		return a.rejoinBoundLog(params)
	}
	if a.joinSource.Image(params.LogSessionID) == nil {
		return false, nil
	}

	if startErr := a.startFollowerLogRecording(params); startErr != nil {
		return false, startErr
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
	a.publishFollowerJoinLog(params)
	return true, nil
}

// startFollowerLogRecording starts the REMOTE cluster log recording for
// params. Skips the start while a recording is already active (idempotent
// re-join) or no control is wired. Prepare stops between terms before the
// next join starts a new one.
//
// Passes params.LogChannel so catchup joins record the catchup destination
// (image join at the follower log position), not the live multicast late-join
// image.
func (a *Agent) startFollowerLogRecording(params FollowerLogJoinParams) error {
	if a.IsRecordingActive() || a.recordingControl == nil {
		return nil
	}
	return a.adoptLogRecording(
		archivewire.SourceLocation_REMOTE,
		params.LogSessionID,
		params.LogChannel,
	)
}

// rejoinBoundLog restarts the REMOTE recording for the image the adapter
// already holds. Runs no image selection. Never rebinds the adapter.
//
// The bound image is checked before the start and again after. The start
// await drives transport DoWork and can close the bound image. A recording
// started for an image that closed during the await is stopped again.
//
// Returns:
//   - joined false, nil: the bound image is not open on params.LogSessionID.
//     The election routes to the position-checked await.
//   - joined false, err: the start or the post-close stop failed
//   - joined true, nil: the bound image remains. The recording is active when a
//     control is wired.
func (a *Agent) rejoinBoundLog(params FollowerLogJoinParams) (
	joined bool,
	err error,
) {
	if !a.isBoundLogImageOpen(params.LogSessionID) {
		return false, nil
	}
	if startErr := a.startFollowerLogRecording(params); startErr != nil {
		return false, startErr
	}
	if !a.isBoundLogImageOpen(params.LogSessionID) {
		if stopErr := a.stopLogRecordingAfterVanishedImage(); stopErr != nil {
			return false, stopErr
		}
		return false, nil
	}
	a.RoleSet(RoleFollower)
	return true, nil
}

// isBoundLogImageOpen reports whether the adapter's bound image is open and
// carries logSessionID.
func (a *Agent) isBoundLogImageOpen(logSessionID int32) bool {
	image := a.logAdapter.Image()
	return image != nil && !image.IsClosed() && image.SessionID() == logSessionID
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

// ElectionComplete clears the [ElectionRunner] and stamps timeOfLastLogUpdateNs
// (settled log-update clock). [PositionTable] and [ConsensusEgress] stay wired.
//
// Behaviour:
//   - leader: stamps nowNs minus one heartbeat interval so the first steady
//     tick publishes a commit heartbeat. Stamps the timer wheel current time
//     to nowNs. Winding expires nothing. Drains the log and rebinds
//     open-session egress
//   - any other role: stamps nowNs so the quiet-tick trigger waits one full
//     leader heartbeat timeout. Does not touch session state
//   - without a write to timeOfLastLogUpdateNs the field stays 0. A duty
//     cycle on an absolute clock re-enters election on the next tick
//   - does not write timeOfLastLeaderUpdateNs
//   - does not call [Agent.SetElection]
//
// Concurrency: invoker-only.
func (a *Agent) ElectionComplete(nowNs int64) {
	a.election = nil
	if a.role == RoleLeader {
		a.timeOfLastLogUpdateNs = nowNs - a.leaderHeartbeatIntervalNs
		a.timers.currentTime(nowNs)
		a.logPollNowNs = nowNs
		a.drainLogBacklog(a.CommitPosition())
		a.rebindOpenSessions(nowNs)
		return
	}
	a.timeOfLastLogUpdateNs = nowNs
}

// DoWork runs one duty-cycle tick:
//   - slow tick
//   - consensus ingress poll when no prepare episode is in progress
//   - client ingress poll when no election is live
//   - service control poll (always; ServiceAck, ScheduleTimer, and CancelTimer
//     dispatch; every role; while election is live)
//   - service control work (always; consume first wait after poll; every role;
//     while election is live)
//   - [ElectionRunner.DoWork] when an election is live, else sessionWork then
//     timerWork then consensusWork
//   - pending JoinLog retry after that branch so PrepareForNewLeadership can
//     drop a superseded pending before any offer
//
// Prepare starts after the consensus ingress poll of the first episode tick.
// That tick is the one INIT drop. Later pending ticks skip the poll. A stop
// or derivation error ends the episode. The next tick polls once, then
// election.DoWork starts a fresh episode.
//
// Settled sessionWork order: egressEventWork, newLeaderEventWork,
// sessionPendingWork, sessionCloseWork, okPendingWork. See [Agent.sessionWork].
//
// Concurrency: invoker-only. See [Agent].
func (a *Agent) DoWork(nowNs int64) int {
	work := 0
	noteDoWorkStep("slowTick")
	work += a.slowTickIfDue(nowNs)
	noteDoWorkStep("pollIngress")
	if !a.prepareInProgress {
		work += a.pollConsensusIngress(nowNs)
	}
	noteDoWorkStep("pollClientIngress")
	work += a.pollClientIngress(nowNs)
	noteDoWorkStep("pollServiceControl")
	work += a.pollServiceControl()
	noteDoWorkStep("serviceControlWork")
	work += a.serviceControlWork()
	if a.election != nil {
		noteDoWorkStep("election.DoWork")
		work += a.election.DoWork(nowNs)
	} else {
		noteDoWorkStep("sessionWork")
		work += a.sessionWork(nowNs)
		noteDoWorkStep("timerWork")
		work += a.timerWork(nowNs)
		noteDoWorkStep("consensusWork")
		work += a.consensusWork(nowNs)
	}
	noteDoWorkStep("retryPendingJoinLog")
	work += a.retryPendingJoinLog()
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
	case clusterwire.StopCatchupSbeTemplateID:
		if fields, ok := codecs.TryDecodeStopCatchupAt(buf, offset); ok {
			a.onSteadyStopCatchup(fields)
		}
	}
}

// onSteadyStopCatchup detaches the catchup receive destination after election
// close when a leader StopCatchup arrives late.
//
// Same term and follower-member guard as [election.Election.OnStopCatchup].
// Removing an already-removed destination is a no-op.
func (a *Agent) onSteadyStopCatchup(fields codecs.StopCatchupFields) {
	if fields.LeadershipTermID != a.leadershipTermID ||
		fields.FollowerMemberID != a.thisMemberID {
		return
	}
	a.RemoveCatchupLogDestination()
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
//   - sends the bound log session id without a reseed
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

	logSessionID, _ := a.boundLogSessionID()
	_ = a.egress.PublishNewLeadershipTerm(CanvassAnswer{
		FollowerMemberID:    fields.FollowerMemberID,
		LogLeadershipTermID: fields.LogLeadershipTermID,
		LeadershipTermID:    a.leadershipTermID,
		LogPosition:         a.logPublisher.Position(),
		CommitPosition:      a.CommitPosition(),
		LeaderRecordingID:   a.localLogRecordingID,
		LogSessionID:        logSessionID,
	})
}

// onSteadyAppendPosition applies a follower AppendPosition on the leader only.
//
// Behaviour:
//   - higher-term appends are ignored (no member update)
//   - catchup-flagged positions skip the position-table update, refresh
//     liveness only, and drive [Agent.TrackCatchupCompletion]
//   - unflagged positions update the table when set
//   - TrackCatchupCompletion always runs so a live map entry finishes cleanup
//     even when positions is nil
func (a *Agent) onSteadyAppendPosition(fields codecs.AppendPositionFields, nowNs int64) {
	_ = nowNs
	if a.role != RoleLeader {
		return
	}
	if fields.LeadershipTermID > a.leadershipTermID {
		return
	}
	if a.positions != nil {
		if isCatchupAppendPosition(fields.Flags) {
			a.positions.RefreshMemberLiveness(fields.FollowerMemberID)
		} else {
			a.positions.UpdateMemberPosition(
				fields.FollowerMemberID,
				fields.LeadershipTermID,
				fields.LogPosition,
			)
		}
	}
	a.TrackCatchupCompletion(fields.FollowerMemberID, fields.LogPosition, fields.Flags)
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
	a.enterElection(ElectionTriggerHigherTerm, nowNs)
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
		a.logPollNowNs = nowNs
		var bound int64
		if ready {
			bound = a.followerLogPollBound(appendPos)
		} else {
			bound = a.logAdapter.Position()
		}
		work += a.pollLogAndProposeMax(a.logAdapter, bound)
	}
	if ready {
		work += a.updateFollowerPosition(nowNs, appendPos, AppendPositionFlagNone)
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

func (a *Agent) updateFollowerPosition(nowNs, appendPos int64, flags uint8) int {
	if a.egress == nil {
		return 0
	}
	// Steady floors on lastPublished so interval re-publish keeps the unflagged
	// high-water. Catchup publishes the adapter position alone and cadences on
	// lastCatchupPublishedPosition so progress does not raise the steady floor.
	catchup := isCatchupAppendPosition(flags)
	position := appendPos
	lastPublished := a.lastCatchupPublishedPosition
	if !catchup {
		position = max(appendPos, a.lastPublishedAppendPosition)
		lastPublished = a.lastPublishedAppendPosition
	}
	advanced := position > lastPublished
	intervalDue := nowNs >= a.timeOfLastAppendPositionSendNs+a.leaderHeartbeatIntervalNs
	if !advanced && !intervalDue {
		return 0
	}
	if !a.egress.PublishAppendPosition(a.leadershipTermID, position, flags) {
		return 0
	}
	if advanced {
		if catchup {
			a.lastCatchupPublishedPosition = position
		} else {
			a.lastPublishedAppendPosition = position
		}
	}
	a.timeOfLastAppendPositionSendNs = nowNs
	return 1
}

var _ AgentHandler = (*Agent)(nil)
