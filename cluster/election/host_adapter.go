package election

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/consensus"
)

// HostAdapter bridges ElectionHost duties to *consensus.Agent and implements
// the reverse-facing PositionTable and ConsensusEgress held by Agent.
//
//   - Implements [ElectionHost]
//   - Implements [consensus.PositionTable] and [consensus.ConsensusEgress]
//     (same instance via [consensus.Agent.SetPositions] /
//     [consensus.Agent.SetEgress])
//
// Concurrency: invoker-only. Same goroutine as Agent.DoWork / Election.DoWork
// and media-driver/client duty cycle. Not safe for concurrent use.
//
// Lifetime: outlives Election after Agent.ElectionComplete. Continues to serve
// Agent.positions and Agent.egress for steady-state ranking and publish.
//
// Ownership:
//   - shares the same []*Member slice pointers Election uses
//   - owns ranked scratch buffer and the Publisher used for Commit/Append egress
//
// Invariant: after NewHostAdapter:
//   - Members length and member IDs are fixed (do not append, remove, or replace)
//   - ranked length and memberByID are sized/built once at construct
//
// Dynamic membership is out of scope.
type HostAdapter struct {
	agent *consensus.Agent
	pub   Publisher

	members    []*Member
	memberByID map[int32]*Member
	thisMember *Member
	ranked     []int64

	// leaderHeartbeatTimeoutNs is the IsActive window passed to QuorumPosition
	// (aeron leaderHeartbeatTimeoutNs). Distinct from LeaderHeartbeatIntervalNs
	// (publish cadence on Election).
	leaderHeartbeatTimeoutNs int64

	// nanoClock stamps TimeOfLastAppendPositionNs on member position updates.
	nanoClock func() int64
}

// HostAdapterConfig is the constructor bag.
type HostAdapterConfig struct {
	// Agent is required. HostAdapter holds the concrete pointer.
	Agent *consensus.Agent

	// Publisher is required.
	//
	// Same instance should be passed to ElectionDeps.Publisher for ballot and
	// NewLeadershipTerm paths. HostAdapter uses it for Commit/Append egress only.
	// Agent never holds Publisher.
	Publisher Publisher

	// Members is required and non-empty.
	//
	// Shared with ElectionDeps.Members (same slice, same *Member pointers).
	// HostAdapter does not copy members.
	//
	// Length and IDs are fixed after construct (see HostAdapter invariant).
	Members []*Member

	// ThisMember is required. Must be pointer-identical to one element of
	// Members (m == ThisMember for some m in Members).
	//
	// ID-only match of a clone is a panic: UpdateLeaderPosition stamps this
	// pointer while QuorumPosition ranks the slice element -- they must be the
	// same *Member.
	//
	// Pass the same thisMember pointer into HostAdapter and ElectionDeps.
	// Construct seeds Agent.ThisMemberID from ThisMember.ID (steady leader
	// stamp store).
	ThisMember *Member

	// LeaderHeartbeatTimeoutNs is required and must be positive.
	//
	// Feeds QuorumPosition activity filter. Use
	// ElectionConfig.LeaderHeartbeatTimeoutNs at bootstrap.
	LeaderHeartbeatTimeoutNs int64

	// NanoClock is required. Stamps member append times.
	//
	// Share ElectionDeps.NanoClock.
	NanoClock func() int64

	// LeaderMemberID is optional. Pass NullMemberID when unset (member id 0 is
	// a valid leader). Constructor seeds Agent via SetLeaderMemberID (single
	// store; HostAdapter has no separate leader field).
	LeaderMemberID int32
}

// NewHostAdapter validates cfg and builds lookup map + ranked scratch.
//
// Panics on:
//   - nil Agent, nil Publisher, nil ThisMember, nil NanoClock
//   - empty Members
//   - non-positive LeaderHeartbeatTimeoutNs
//   - duplicate member IDs in Members
//   - ThisMember not pointer-identical to any Members element
//
// Panic style matches NewElection / validateElectionDeps (construction-time
// programmer errors).
//
// Lifetime: after success, Members length/IDs and ranked size are fixed for
// the lifetime of the adapter. ranked length is QuorumThreshold(len(Members)),
// allocated once and reused by every QuorumPositionBoundedByLeaderLog call
// (zeroed inside QuorumPosition).
func NewHostAdapter(cfg HostAdapterConfig) *HostAdapter {
	if cfg.Agent == nil {
		panic("election: nil Agent")
	}
	if cfg.Publisher == nil {
		panic("election: nil Publisher")
	}
	if cfg.ThisMember == nil {
		panic("election: nil ThisMember")
	}
	if cfg.NanoClock == nil {
		panic("election: nil NanoClock")
	}
	if len(cfg.Members) == 0 {
		panic("election: empty Members")
	}
	if cfg.LeaderHeartbeatTimeoutNs <= 0 {
		panic("election: non-positive LeaderHeartbeatTimeoutNs")
	}

	memberByID := make(map[int32]*Member, len(cfg.Members))
	thisInMembers := false
	for _, m := range cfg.Members {
		if _, dup := memberByID[m.ID]; dup {
			panic("election: duplicate member id")
		}
		memberByID[m.ID] = m
		if m == cfg.ThisMember {
			thisInMembers = true
		}
	}
	if !thisInMembers {
		panic("election: ThisMember not in Members")
	}

	h := &HostAdapter{
		agent:                    cfg.Agent,
		pub:                      cfg.Publisher,
		members:                  cfg.Members,
		memberByID:               memberByID,
		thisMember:               cfg.ThisMember,
		ranked:                   make([]int64, QuorumThreshold(len(cfg.Members))),
		leaderHeartbeatTimeoutNs: cfg.LeaderHeartbeatTimeoutNs,
		nanoClock:                cfg.NanoClock,
	}
	// Agent stores for self id (steady stamp) and leader id (append target).
	h.agent.SetThisMemberID(cfg.ThisMember.ID)
	h.agent.SetLeaderMemberID(cfg.LeaderMemberID)
	return h
}

// SetLeaderMemberID sets Agent leader id (PublishAppendPosition target and
// steady notify gates). Clear with NullMemberID when this node is leader or
// the leader is unknown. Member id 0 is valid.
//
// Callers: installLeadershipTerm after follower adopt; tests.
func (h *HostAdapter) SetLeaderMemberID(id int32) {
	h.agent.SetLeaderMemberID(id)
}

// ApplyLeadershipTerm forwards the adopted leadership term onto Agent for
// steady ingress gates after ElectionComplete (follower install handoff).
func (h *HostAdapter) ApplyLeadershipTerm(term int64) {
	h.agent.SetLeadershipTermID(term)
}

// LeaderMemberID returns the current AppendPosition target (Agent store).
func (h *HostAdapter) LeaderMemberID() int32 { return h.agent.LeaderMemberID() }

func (h *HostAdapter) QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64 {
	return min(
		QuorumPosition(h.members, h.ranked, nowNs, h.leaderHeartbeatTimeoutNs),
		appendPosition,
	)
}

func (h *HostAdapter) UpdateMemberLogPosition(
	member *Member,
	leadershipTermID, logPosition int64,
) {
	if member == nil {
		return
	}
	member.LeadershipTermID = leadershipTermID
	member.LogPosition = logPosition
	member.TimeOfLastAppendPositionNs = h.nanoClock()
}

func (h *HostAdapter) UpdateMemberPosition(
	memberID int32,
	leadershipTermID, logPosition int64,
) {
	h.UpdateMemberLogPosition(h.memberByID[memberID], leadershipTermID, logPosition)
}

func (h *HostAdapter) UpdateLeaderPosition(nowNs, appendPosition, quorumPosition int64) int {
	h.thisMember.LogPosition = appendPosition
	h.thisMember.TimeOfLastAppendPositionNs = nowNs
	return h.agent.UpdateLeaderPosition(nowNs, quorumPosition)
}

func (h *HostAdapter) PublishCommitPosition(commitPosition, leadershipTermID int64) {
	leaderID := h.thisMember.ID
	for _, member := range h.members {
		if member.ID == leaderID {
			continue
		}
		h.pub.CommitPosition(
			PublicationTarget{MemberID: member.ID},
			codecs.CommitPositionFields{
				LeadershipTermID: leadershipTermID,
				LogPosition:      commitPosition,
				LeaderMemberID:   leaderID,
			},
		)
	}
}

func (h *HostAdapter) PublishAppendPosition(
	leadershipTermID, logPosition int64,
	flags uint8,
) bool {
	leaderID := h.agent.LeaderMemberID()
	if leaderID == NullMemberID || leaderID == h.thisMember.ID {
		return false
	}
	return h.pub.AppendPosition(
		PublicationTarget{MemberID: leaderID},
		codecs.AppendPositionFields{
			LeadershipTermID: leadershipTermID,
			LogPosition:      logPosition,
			FollowerMemberID: h.thisMember.ID,
			Flags:            flags,
		},
	)
}

// TrackCatchupCompletion is a no-op until archive stop/catchup is wired.
//
// Kept so [ElectionHost] is fully implemented.
func (h *HostAdapter) TrackCatchupCompletion(*Member, int64, uint8) {}

func (h *HostAdapter) Role() consensus.Role { return h.agent.Role() }

func (h *HostAdapter) RoleSet(role consensus.Role) { h.agent.RoleSet(role) }

func (h *HostAdapter) TimeOfLastLeaderUpdateNs() int64 {
	return h.agent.TimeOfLastLeaderUpdateNs()
}

func (h *HostAdapter) NewLogReplay(logPosition, appendPosition int64) LogReplay {
	return h.agent.NewLogReplay(logPosition, appendPosition)
}

func (h *HostAdapter) NewLogReplication(
	leaderArchiveEndpoint, responseArchiveEndpoint string,
	leaderRecordingID, stopPosition, nowNs int64,
) LogReplication {
	return h.agent.NewLogReplication(
		leaderArchiveEndpoint,
		responseArchiveEndpoint,
		leaderRecordingID,
		stopPosition,
		nowNs,
	)
}

func (h *HostAdapter) CatchupPoll(limitPosition, nowNs int64) int {
	return h.agent.CatchupPoll(limitPosition, nowNs)
}

func (h *HostAdapter) CatchupInitiated(nowNs int64) {
	h.agent.CatchupInitiated(nowNs)
}

func (h *HostAdapter) StopAllCatchups() { h.agent.StopAllCatchups() }

func (h *HostAdapter) IsCatchupNearLive(position int64) bool {
	return h.agent.IsCatchupNearLive(position)
}

func (h *HostAdapter) LiveLogDestination() string {
	return h.agent.LiveLogDestination()
}

func (h *HostAdapter) LiveLogDestinationSet(destination string) {
	h.agent.LiveLogDestinationSet(destination)
}

func (h *HostAdapter) CatchupLogDestination() string {
	return h.agent.CatchupLogDestination()
}

func (h *HostAdapter) CatchupLogDestinationSet(destination string) {
	h.agent.CatchupLogDestinationSet(destination)
}

func (h *HostAdapter) AddLogPublication(appendPosition int64) int32 {
	return h.agent.AddLogPublication(appendPosition)
}

func (h *HostAdapter) AppendNewLeadershipTermEvent(nowNs int64) bool {
	return h.agent.AppendNewLeadershipTermEvent(nowNs)
}

func (h *HostAdapter) PollArchiveEvents() int {
	return h.agent.PollArchiveEvents()
}

func (h *HostAdapter) LocalLogRecordingID() int64 {
	return h.agent.LocalLogRecordingID()
}

func (h *HostAdapter) SetLogRecordingID(recordingID int64) {
	h.agent.SetLogRecordingID(recordingID)
}

func (h *HostAdapter) TruncateLogEntry(leadershipTermID, logPosition int64) {
	h.agent.TruncateLogEntry(leadershipTermID, logPosition)
}

func (h *HostAdapter) AwaitLocalSocketsClosed(registrationID int64) {
	h.agent.AwaitLocalSocketsClosed(registrationID)
}

func (h *HostAdapter) FollowerLogJoinReady(
	logSessionID int32,
) (consensus.FollowerLogJoinParams, bool) {
	return h.agent.FollowerLogJoinReady(logSessionID)
}

var (
	_ ElectionHost              = (*HostAdapter)(nil)
	_ consensus.PositionTable   = (*HostAdapter)(nil)
	_ consensus.ConsensusEgress = (*HostAdapter)(nil)
)
