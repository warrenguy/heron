package election

import (
	"errors"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
)

func (e *Election) handleInit(nowNs int64) int {
	if e.isFirstInit {
		e.isFirstInit = false
		if !e.isNodeStartup {
			e.prepareForNewLeadership(nowNs)
		}
	} else {
		e.prepareForNewLeadership(nowNs)
		e.logSessionID = int32(NullValue)
	}

	e.notifiedCommitPosition = 0
	e.candidateTermID = e.durableCandidateTermID()

	if e.isSingleNodeCluster() {
		e.candidateTermID++
		err := e.deps.NodeState.UpdateCandidateTermID(
			e.candidateTermID,
			e.logPosition,
			e.deps.EpochClock(),
		)
		if err != nil {
			e.HandleError(nowNs, err)
			return 1
		}
		e.leaderMember = e.thisMember
		e.leadershipTermID = e.candidateTermID
		e.state(ElectionStateLeaderLogReplication, nowNs, "single-node cluster leader")
	} else {
		e.state(ElectionStateCanvass, nowNs, "")
	}
	return 1
}

func (e *Election) handleCanvass(nowNs int64) int {
	workCount := 0

	if hasUpdateIntervalExpired(nowNs, e.timeOfLastUpdateNs, e.cfg.ElectionStatusIntervalNs) {
		e.timeOfLastUpdateNs = nowNs
		e.publishCanvassPosition()
		workCount++
	}

	if e.cfg.AppointedLeaderID != int32(NullValue) && e.cfg.AppointedLeaderID != e.thisMember.ID {
		return workCount
	}

	var deadlineNs int64
	if e.isExtendedCanvass {
		deadlineNs = e.timeOfLastStateChangeNs + e.cfg.StartupCanvassTimeoutNs
	} else {
		deadlineNs = e.deps.Host.TimeOfLastLeaderUpdateNs() + e.cfg.LeaderHeartbeatTimeoutNs
	}

	unanimous := IsUnanimousCandidate(e.clusterMembers, e.thisMember, e.gracefulClosedLeaderID)
	quorumAtDeadline := nowNs >= deadlineNs && IsQuorumCandidate(e.clusterMembers, e.thisMember)
	if unanimous || quorumAtDeadline {
		e.nominationDeadlineNs = nowNs + e.nominationDelayNs()
		e.state(ElectionStateNominate, nowNs, "")
		workCount++
	}
	return workCount
}

func (e *Election) handleNominate(nowNs int64) int {
	if nowNs >= e.nominationDeadlineNs {
		proposed, err := e.deps.NodeState.ProposeMaxCandidateTermID(
			e.candidateTermID+1,
			e.logPosition,
			e.deps.EpochClock(),
		)
		if err != nil {
			e.HandleError(nowNs, err)
			return 1
		}
		e.candidateTermID = proposed
		BecomeCandidate(e.clusterMembers, e.candidateTermID, e.thisMember.ID)
		e.state(ElectionStateCandidateBallot, nowNs, "")
		return 1
	}

	if hasUpdateIntervalExpired(nowNs, e.timeOfLastUpdateNs, e.cfg.ElectionStatusIntervalNs) {
		e.timeOfLastUpdateNs = nowNs
		e.publishCanvassPosition()
		return 1
	}
	return 0
}

func (e *Election) handleCandidateBallot(nowNs int64) int {
	workCount := 0

	if IsUnanimousLeader(e.clusterMembers, e.candidateTermID, e.gracefulClosedLeaderID) {
		e.leaderMember = e.thisMember
		e.leadershipTermID = e.candidateTermID
		e.state(ElectionStateLeaderLogReplication, nowNs, "unanimous leader")
		return 1
	}

	if nowNs >= e.timeOfLastStateChangeNs+e.cfg.ElectionTimeoutNs {
		if IsQuorumLeader(e.clusterMembers, e.candidateTermID) {
			e.leaderMember = e.thisMember
			e.leadershipTermID = e.candidateTermID
			e.state(ElectionStateLeaderLogReplication, nowNs, "quorum leader")
		} else {
			e.state(ElectionStateCanvass, nowNs, "")
		}
		return 1
	}

	for _, member := range e.clusterMembers {
		if member.IsBallotSent {
			continue
		}
		workCount++
		member.IsBallotSent = e.deps.Publisher.RequestVote(
			PublicationTarget{MemberID: member.ID},
			e.logLeadershipTermID,
			e.appendPosition,
			e.candidateTermID,
			e.thisMember.ID,
		)
	}
	return workCount
}

func (e *Election) handleFollowerBallot(nowNs int64) int {
	if nowNs >= e.timeOfLastStateChangeNs+e.cfg.ElectionTimeoutNs {
		e.state(ElectionStateCanvass, nowNs, "")
		return 1
	}
	return 0
}

func (e *Election) handleLeaderLogReplication(nowNs int64) int {
	workCount := 0

	if hasUpdateIntervalExpired(nowNs, e.timeOfLastUpdateNs, e.cfg.LeaderHeartbeatIntervalNs) {
		e.timeOfLastUpdateNs = nowNs
		e.publishNewLeadershipTerm(nowNs)
		workCount++
	}

	quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
	if quorum >= e.appendPosition {
		e.state(ElectionStateLeaderReplay, nowNs, "")
		workCount++
	}
	return workCount
}

func (e *Election) handleLeaderReplay(nowNs int64) int {
	if e.logReplay == nil || e.logReplay.IsDone() {
		e.state(ElectionStateLeaderInit, nowNs, "")
		return 1
	}
	return e.logReplay.DoWork(nowNs)
}

func (e *Election) handleLeaderInit(nowNs int64) int {
	e.deps.Handler.JoinLogAsLeader(
		e.leadershipTermID,
		e.logPosition,
		e.logSessionID,
		e.isLeaderStartup,
	)
	e.state(ElectionStateLeaderReady, nowNs, "")
	return 1
}

func (e *Election) handleLeaderReady(int64) int { return 0 }

func (e *Election) handleFollowerLogReplication(int64) int { return 0 }

func (e *Election) handleFollowerReplay(nowNs int64) int {
	e.state(ElectionStateFollowerLogInit, nowNs, "")
	return 1
}

func (e *Election) handleFollowerCatchupInit(int64) int { return 0 }

func (e *Election) handleFollowerCatchupAwait(int64) int { return 0 }

func (e *Election) handleFollowerCatchup(int64) int { return 0 }

func (e *Election) handleFollowerLogInit(nowNs int64) int {
	e.state(ElectionStateFollowerLogAwait, nowNs, "")
	return 1
}

func (e *Election) handleFollowerLogAwait(nowNs int64) int {
	params := consensus.FollowerLogJoinParams{
		LogSessionID:    e.logSessionID,
		LogJoinPosition: e.logPosition,
		IsLeaderStartup: e.isLeaderStartup,
	}
	if e.deps.Handler.JoinLogAsFollower(params, nowNs) {
		e.state(ElectionStateFollowerReady, nowNs, "")
		return 1
	}
	return 0
}

func (e *Election) handleFollowerReady(nowNs int64) int {
	published := false
	if leader := e.leaderMember; leader != nil {
		published = e.deps.Publisher.AppendPosition(
			PublicationTarget{MemberID: leader.ID},
			e.leadershipTermID,
			e.logPosition,
			e.thisMember.ID,
			0,
		)
	}

	if published {
		e.deps.Handler.ElectionComplete(nowNs)
		e.state(ElectionStateClosed, nowNs, "")
	} else if nowNs >= e.timeOfLastStateChangeNs+e.cfg.LeaderHeartbeatTimeoutNs {
		e.HandleError(nowNs, errors.New("election: ready follower failed to notify leader"))
	}
	return 1
}

// HandleConsensusMessage decodes consensus wire and dispatches to On* handlers.
// Returns true when decode succeeds; handlers are skipped in ElectionStateInit.
func (e *Election) HandleConsensusMessage(buf []byte, offset int) bool {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(buf, offset)
	if !ok || schemaID != clusterwire.MessageHeaderSbeSchemaID {
		return false
	}
	accept := e.acceptsConsensusInbound()
	switch templateID {
	case clusterwire.CanvassPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeCanvassPositionAt, e.OnCanvassPosition)
	case clusterwire.RequestVoteSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeRequestVoteAt, e.OnRequestVote)
	case clusterwire.VoteSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeVoteAt, e.OnVote)
	case clusterwire.NewLeadershipTermSbeTemplateID:
		return dispatchWire(
			buf,
			offset,
			accept,
			codecs.TryDecodeNewLeadershipTermAt,
			e.OnNewLeadershipTerm,
		)
	case clusterwire.AppendPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeAppendPositionAt, e.OnAppendPosition)
	case clusterwire.CommitPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeCommitPositionAt, e.OnCommitPosition)
	default:
		return false
	}
}

func (e *Election) acceptsConsensusInbound() bool {
	return e.electionState != ElectionStateInit
}

func dispatchWire[F any](
	buf []byte,
	offset int,
	accept bool,
	decode func([]byte, int) (F, bool),
	on func(F),
) bool {
	fields, ok := decode(buf, offset)
	if !ok {
		return false
	}
	if accept {
		on(fields)
	}
	return true
}

func (e *Election) OnCanvassPosition(fields codecs.CanvassPositionFields) {
	if e.electionState == ElectionStateInit {
		return
	}
	if fields.FollowerMemberID == e.gracefulClosedLeaderID {
		e.gracefulClosedLeaderID = int32(NullValue)
	}

	follower := e.memberByID[fields.FollowerMemberID]
	if follower == nil || follower.ID == e.thisMember.ID {
		return
	}
	e.deps.Host.UpdateMemberLogPosition(follower, fields.LogLeadershipTermID, fields.LogPosition)
}

func (e *Election) OnRequestVote(fields codecs.RequestVoteFields) {
	if e.electionState == ElectionStateInit {
		return
	}
	if fields.CandidateMemberID == e.thisMember.ID {
		return
	}

	if fields.CandidateTermID <= e.candidateTermID {
		e.placeVote(fields.CandidateTermID, fields.CandidateMemberID, false)
		return
	}

	weAreAhead := CompareLog(
		e.logLeadershipTermID,
		e.appendPosition,
		fields.LogLeadershipTermID,
		fields.LogPosition,
	) > 0
	if weAreAhead {
		proposed, err := e.deps.NodeState.ProposeMaxCandidateTermID(
			fields.CandidateTermID,
			fields.LogPosition,
			e.deps.EpochClock(),
		)
		if err != nil {
			e.HandleError(e.deps.NanoClock(), err)
			return
		}
		e.candidateTermID = proposed
		e.placeVote(fields.CandidateTermID, fields.CandidateMemberID, false)

		// A leader-role node also publishes NewLeadershipTerm toward the candidate
		// here; that path is not wired until log replication lands.
		return
	}

	if !e.isBallotState() {
		return
	}
	nowNs := e.deps.NanoClock()
	proposed, err := e.deps.NodeState.ProposeMaxCandidateTermID(
		fields.CandidateTermID,
		fields.LogPosition,
		e.deps.EpochClock(),
	)
	if err != nil {
		e.HandleError(nowNs, err)
		return
	}
	e.candidateTermID = proposed
	e.placeVote(fields.CandidateTermID, fields.CandidateMemberID, true)
	e.state(ElectionStateFollowerBallot, nowNs, "")
}

func (e *Election) OnVote(fields codecs.VoteFields) {
	if e.electionState != ElectionStateCandidateBallot {
		return
	}
	if fields.CandidateTermID != e.candidateTermID || fields.CandidateMemberID != e.thisMember.ID {
		return
	}

	voter := e.memberByID[fields.FollowerMemberID]
	if voter == nil {
		return
	}
	voter.CandidateTermID = fields.CandidateTermID
	voter.Vote = new(fields.Vote == clusterwire.BooleanType_TRUE)
	e.deps.Host.UpdateMemberLogPosition(voter, fields.LogLeadershipTermID, fields.LogPosition)
}

func (e *Election) OnNewLeadershipTerm(fields codecs.NewLeadershipTermFields) {
	accepts := e.electionState == ElectionStateCanvass ||
		e.electionState == ElectionStateFollowerBallot ||
		e.electionState == ElectionStateCandidateBallot
	if !accepts {
		return
	}

	// Ignore a term the node already advanced past; only a term at or above the
	// current ballot can install a new leader.
	if fields.LeadershipTermID < e.candidateTermID {
		return
	}

	leader := e.memberByID[fields.LeaderMemberID]
	if leader == nil {
		return
	}
	e.leaderMember = leader
	e.leadershipTermID = fields.LeadershipTermID
	e.logSessionID = fields.LogSessionID
	e.state(ElectionStateFollowerReplay, e.deps.NanoClock(), "")
}

func (e *Election) OnAppendPosition(fields codecs.AppendPositionFields) {
	if e.electionState == ElectionStateInit {
		return
	}

	follower := e.memberByID[fields.FollowerMemberID]
	if follower == nil {
		return
	}

	if fields.LeadershipTermID <= e.leadershipTermID {
		e.deps.Host.UpdateMemberLogPosition(follower, fields.LeadershipTermID, fields.LogPosition)
		e.deps.Host.TrackCatchupCompletion(follower, fields.LeadershipTermID, fields.Flags)
	}

	if e.electionState == ElectionStateLeaderReady {
		nowNs := e.deps.NanoClock()
		quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
		if quorum >= e.appendPosition {
			e.completeElection(nowNs)
		}
	}
}

func (e *Election) OnCommitPosition(codecs.CommitPositionFields) {}

// OnRecordingSignal handles archive recording signals during log replication.
// Mirrors Election.onRecordingSignal. Stub until logReplication.OnSignal is wired.
func (e *Election) OnRecordingSignal(
	correlationID, recordingID, position int64,
	signal RecordingSignal,
) {
	if !e.acceptsConsensusInbound() {
		return
	}
	if e.logReplication != nil {
		e.logReplication.OnSignal(correlationID, recordingID, position, signal)
		e.deps.Host.LogRecordingID(e.logReplication.RecordingID())
	}
}

// HandleError resets the election to Init regardless of err. Mirrors
// Election.handleError. Stub until error reporting and commit-position
// sync are wired.
//
// A failed candidate-term write lands here with the in-memory term
// untouched; the Init pass re-reads the durable value.
func (e *Election) HandleError(nowNs int64, err error) {
	e.state(ElectionStateInit, nowNs, "")
}
