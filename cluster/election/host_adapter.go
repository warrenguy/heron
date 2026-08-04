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
//   - owns ranked scratch buffer and the Publisher used for Commit/Append
//     egress and settled canvass NewLeadershipTerm answers
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

	// epochClock stamps Timestamp on a settled canvass answer.
	epochClock func() int64

	// recordingLogLookup resolves term base and successor fields on a settled
	// canvass answer. Nil makes PublishNewLeadershipTerm report false.
	recordingLogLookup RecordingLogLookup

	// appVersion is written into a settled canvass answer.
	appVersion int32
}

// HostAdapterConfig is the constructor config.
type HostAdapterConfig struct {
	// Agent is required. HostAdapter holds the concrete pointer.
	Agent *consensus.Agent

	// Publisher is required.
	//
	// Same instance should be passed to ElectionDeps.Publisher for ballot and
	// election NewLeadershipTerm paths. HostAdapter uses it for Commit/Append
	// egress and settled canvass NewLeadershipTerm answers.
	//
	// Agent never holds Publisher.
	Publisher Publisher

	// Members is required and non-empty.
	//
	// Shared with ElectionDeps.Members (same slice, same *Member pointers).
	//
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
	//
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

	// EpochClock is required. Stamps the timestamp on a settled canvass answer.
	//
	// Share ElectionDeps.EpochClock.
	// A second clock would put a different timestamp on an answer than on the
	// election's own publishes.
	EpochClock func() int64

	// RecordingLogLookup resolves term base and successor-term fields on a settled
	// canvass answer.
	//
	// Optional. A nil lookup makes PublishNewLeadershipTerm return false.
	//
	// Share ElectionDeps.RecordingLogLookup.
	RecordingLogLookup RecordingLogLookup

	// AppVersion is the application version written into a settled canvass
	// answer. Zero is valid on the wire.
	//
	// Share ElectionConfig.AppVersion.
	AppVersion int32
}

// NewHostAdapter validates cfg and builds lookup map + ranked scratch.
//
// Panics on:
//   - nil Agent, nil Publisher, nil ThisMember, nil NanoClock, nil EpochClock
//   - empty Members
//   - non-positive LeaderHeartbeatTimeoutNs
//   - duplicate member IDs in Members
//   - ThisMember not pointer-identical to any Members element
//
// Does not panic on a nil RecordingLogLookup: PublishNewLeadershipTerm reports
// false when the lookup is nil.
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
	if cfg.EpochClock == nil {
		panic("election: nil EpochClock")
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
		epochClock:               cfg.EpochClock,
		recordingLogLookup:       cfg.RecordingLogLookup,
		appVersion:               cfg.AppVersion,
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

// HasActiveQuorum reports whether a quorum of members reported an append
// position within timeoutNs of nowNs.
//
// Behaviour:
//   - a single-member cluster always returns true
//   - a multi-member cluster delegates to free HasActiveQuorum over h.members
//   - timeoutNs comes from the caller; does not read leaderHeartbeatTimeoutNs
//
// A settled leader stamps this member every tick. The stamp keeps the leader
// active while the stamp falls inside timeoutNs.
//
// Aeron checks member count at the call site. This method folds the
// single-member case so the agent always makes one call.
func (h *HostAdapter) HasActiveQuorum(nowNs, timeoutNs int64) bool {
	if len(h.members) <= 1 {
		return true
	}
	return HasActiveQuorum(h.members, nowNs, timeoutNs)
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

// PublishNewLeadershipTerm answers one member's canvass with this leader's
// current leadership term.
//
// Behaviour:
//   - resolves term base from the recording log entry for
//     answer.LeadershipTermID
//   - when the canvasser is already at answer.LeadershipTermID, sets all three
//     successor-term fields to null
//   - when the canvasser lags, resolves successor fields from the entry one
//     term above the canvasser's log leadership term
//   - when that next entry is absent, uses this leader's term, the resolved
//     term base, and a null log position
//   - stamps the epoch timestamp and this member's id
//   - publishes to the canvasser
//
// This path owns successor resolution for a settled canvass answer.
// Election NewLeadershipTerm field assembly uses different rules and stays
// separate.
//
// Concurrency: invoker-only; see [HostAdapter].
//
// Returns false without publishing when:
//   - the recording-log lookup is nil
//   - the canvasser is unknown
//   - the canvasser is this member
//   - answer.LogSessionID is NullMemberID
//   - this leader's current term has no recording-log entry
func (h *HostAdapter) PublishNewLeadershipTerm(answer consensus.CanvassAnswer) bool {
	if h.recordingLogLookup == nil {
		return false
	}
	member := h.memberByID[answer.FollowerMemberID]
	if member == nil || member.ID == h.thisMember.ID {
		return false
	}
	if answer.LogSessionID == NullMemberID {
		return false
	}
	current := h.recordingLogLookup.FindTermEntry(answer.LeadershipTermID)
	if current == nil {
		return false
	}
	termBaseLogPosition := current.TermBaseLogPosition
	nextLeadershipTermID := int64(NullValue)
	nextTermBaseLogPosition := int64(NullPosition)
	nextLogPosition := int64(NullPosition)
	if answer.LogLeadershipTermID < answer.LeadershipTermID {
		next := h.recordingLogLookup.FindTermEntry(answer.LogLeadershipTermID + 1)
		if next != nil {
			nextLeadershipTermID = next.LeadershipTermID
			nextTermBaseLogPosition = next.TermBaseLogPosition
			nextLogPosition = next.LogPosition
		} else {
			nextLeadershipTermID = answer.LeadershipTermID
			nextTermBaseLogPosition = termBaseLogPosition
		}
	}
	fields := codecs.NewLeadershipTermFields{
		LogLeadershipTermID:     answer.LogLeadershipTermID,
		NextLeadershipTermID:    nextLeadershipTermID,
		NextTermBaseLogPosition: nextTermBaseLogPosition,
		NextLogPosition:         nextLogPosition,
		LeadershipTermID:        answer.LeadershipTermID,
		TermBaseLogPosition:     termBaseLogPosition,
		LogPosition:             answer.LogPosition,
		CommitPosition:          answer.CommitPosition,
		LeaderRecordingID:       answer.LeaderRecordingID,
		Timestamp:               h.epochClock(),
		LeaderMemberID:          h.thisMember.ID,
		LogSessionID:            answer.LogSessionID,
		AppVersion:              h.appVersion,
		IsStartup:               boolToClusterwire(false),
	}
	h.pub.NewLeadershipTerm(
		PublicationTarget{MemberID: answer.FollowerMemberID},
		fields,
	)
	return true
}

// TrackCatchupCompletion is a no-op.
//
// A leader-side catchup replay follows the active recording, so it is the
// returning follower's live log feed until the next prepare. Stopping it at the
// caught-up position strands that follower: its log adapter is bound to the
// catchup image, and no live image replaces it. Stopping on completion needs a
// follower-side handover to the live log first.
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

func (h *HostAdapter) IsCatchupNearLive(position int64) bool {
	return h.agent.IsCatchupNearLive(position)
}

func (h *HostAdapter) EnsureCatchupLogDestination(catchupEndpoint string) string {
	return h.agent.EnsureCatchupLogDestination(catchupEndpoint)
}

func (h *HostAdapter) CommitPosition() int64 {
	return h.agent.CommitPosition()
}

func (h *HostAdapter) IsLogJoinedForSession(logSessionID int32) bool {
	return h.agent.IsLogJoinedForSession(logSessionID)
}

// OnCatchupPosition starts a leader-side archive catchup replay for the
// follower named in fields. Refuses an unknown follower member id. The agent
// refuses a second start while a replay is active.
func (h *HostAdapter) OnCatchupPosition(fields codecs.CatchupPositionFields) {
	if h.memberByID[fields.FollowerMemberID] == nil {
		return
	}
	h.agent.OnCatchupPosition(fields)
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

// SetLogRecordingID stores the local log recording id.
//
// Production hosts do not overwrite while a cluster log recording is running.
func (h *HostAdapter) SetLogRecordingID(recordingID int64) {
	h.agent.SetLogRecordingID(recordingID)
}

func (h *HostAdapter) TruncateLogEntry(leadershipTermID, logPosition int64) error {
	return h.agent.TruncateLogEntry(leadershipTermID, logPosition)
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
