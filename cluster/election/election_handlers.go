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
		e.logSessionID = NullMemberID
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

	if e.cfg.AppointedLeaderID != NullMemberID && e.cfg.AppointedLeaderID != e.thisMember.ID {
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
	e.thisMember.LogPosition = e.appendPosition
	e.thisMember.TimeOfLastAppendPositionNs = nowNs

	quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
	workCount := e.publishNewLeadershipTermOnInterval(quorum, nowNs)
	workCount += e.publishCommitPositionOnInterval(quorum, nowNs)

	if quorum >= e.appendPosition {
		e.state(ElectionStateLeaderReplay, nowNs, "")
		workCount++
	}
	return workCount
}

func (e *Election) handleLeaderReplay(nowNs int64) int {
	workCount := 0

	if e.logReplay == nil {
		if e.logPosition < e.appendPosition {
			e.logReplay = e.deps.Host.NewLogReplay(e.logPosition, e.appendPosition)
		} else {
			e.state(ElectionStateLeaderInit, nowNs, "")
		}
		workCount++

		// The startup flag reverts to this node's own startup state on every
		// replay start, including the immediate no-replay path: a leader joins
		// its log with its own history, not the previous leader's flag.
		e.isLeaderStartup = e.isNodeStartup
		e.thisMember.LeadershipTermID = e.leadershipTermID
		e.thisMember.LogPosition = e.appendPosition
		e.thisMember.TimeOfLastAppendPositionNs = nowNs
	} else {
		workCount += e.logReplay.DoWork(nowNs)
		if e.logReplay.IsDone() {
			e.logPosition = e.appendPosition
			e.stopReplay()
			e.state(ElectionStateLeaderInit, nowNs, "")
		}
	}

	quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
	workCount += e.publishNewLeadershipTermOnInterval(quorum, nowNs)
	workCount += e.publishCommitPositionOnInterval(quorum, nowNs)
	return workCount
}

func (e *Election) handleLeaderInit(nowNs int64) int {
	e.deps.Handler.JoinLogAsLeader(
		e.leadershipTermID,
		e.logPosition,
		e.logSessionID,
		e.isLeaderStartup,
	)
	if err := e.commitRecordingLogTerm(e.leadershipTermID, e.logPosition, NullValue); err != nil {
		e.HandleError(nowNs, err)
		return 1
	}
	e.state(ElectionStateLeaderReady, nowNs, "")
	return 1
}

func (e *Election) handleLeaderReady(nowNs int64) int {
	quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
	workCount := e.deps.Host.UpdateLeaderPosition(nowNs, e.appendPosition, quorum)
	workCount += e.publishNewLeadershipTermOnInterval(quorum, nowNs)

	hasQuorum := HasQuorumAtPosition(
		e.clusterMembers,
		e.leadershipTermID,
		e.logPosition,
		nowNs,
		e.cfg.LeaderHeartbeatTimeoutNs,
	)
	if hasQuorum && e.completeElection(nowNs) {
		workCount++
	}
	return workCount
}

// handleFollowerLogReplication handles the caught-up and timeout paths only;
// archive log replication is not wired, so no LogReplication is created here.
func (e *Election) handleFollowerLogReplication(nowNs int64) int {
	// A NullPosition stop position means the state was entered without an
	// install (test or simulation injection); fall through to the deadline
	// reset instead of committing a sentinel term.
	caughtUp := e.replicationStopPosition != NullPosition &&
		e.appendPosition >= e.replicationStopPosition
	if caughtUp {
		err := e.commitRecordingLogTerm(
			e.replicationLeadershipTermID,
			e.replicationTermBaseLogPosition,
			e.replicationStopPosition,
		)
		if err != nil {
			e.HandleError(nowNs, err)
			return 1
		}
		e.state(ElectionStateCanvass, nowNs, "")
		return 0
	}
	if nowNs >= e.replicationDeadlineNs {
		e.HandleError(nowNs, errors.New("election: timeout awaiting commit position"))
		return 1
	}
	return 0
}

func (e *Election) handleFollowerReplay(nowNs int64) int {
	workCount := 0

	if e.logReplay == nil {
		if e.logPosition >= e.appendPosition {
			e.state(e.followerJoinState(), nowNs, "nothing to replay")
			return 1
		}
		if e.notifiedCommitPosition == 0 {
			return e.publishFollowerAppendPosition(nowNs)
		}
		if e.logPosition >= e.notifiedCommitPosition {
			e.state(ElectionStateCanvass, nowNs, "log replay rejected")
			return 0
		}
		e.logReplay = e.deps.Host.NewLogReplay(
			e.logPosition,
			min(e.appendPosition, e.notifiedCommitPosition),
		)
		return 1
	}

	workCount += e.logReplay.DoWork(nowNs)
	if e.logReplay.IsDone() {
		e.logPosition = e.logReplay.Position()
		e.stopReplay()
		if e.logPosition == e.appendPosition {
			e.state(e.followerJoinState(), nowNs, "log replay done")
		} else {
			e.state(ElectionStateCanvass, nowNs, "incomplete log replay")
		}
	}
	return workCount
}

func (e *Election) handleFollowerCatchupInit(nowNs int64) int {
	// No archive catchup or replication is wired yet.
	//
	// The install path always sets catchupJoinPosition; the notified-commit
	// fallback covers states entered without an install (test and simulation
	// injection).
	pos := e.catchupJoinPosition
	if pos == NullPosition {
		pos = e.notifiedCommitPosition
	}
	if e.deps.Host.IsCatchupNearLive(pos) {
		e.state(ElectionStateFollowerLogInit, nowNs, "catchup near live")
		return 1
	}
	return 0
}

func (e *Election) handleFollowerCatchupAwait(int64) int { return 0 }

func (e *Election) handleFollowerCatchup(int64) int { return 0 }

func (e *Election) handleFollowerLogInit(nowNs int64) int {
	if e.logSessionID != NullMemberID {
		e.state(ElectionStateFollowerLogAwait, nowNs, "")
	}
	return 1
}

func (e *Election) handleFollowerLogAwait(nowNs int64) int {
	params, ok := e.deps.Host.FollowerLogJoinReady(e.logSessionID)
	if !ok {
		if nowNs >= e.timeOfLastStateChangeNs+e.cfg.LeaderHeartbeatTimeoutNs {
			e.HandleError(nowNs, errors.New("election: failed to join live log"))
			return 1
		}
		return 0
	}
	if params.LogJoinPosition != e.logPosition {
		e.HandleError(nowNs, errors.New("election: log join position mismatch"))
		return 1
	}

	params.IsLeaderStartup = e.isLeaderStartup
	if e.deps.Handler.JoinLogAsFollower(params, nowNs) {
		if err := e.commitRecordingLogTerm(e.leadershipTermID, e.logPosition, NullValue); err != nil {
			e.HandleError(nowNs, err)
			return 1
		}
		e.state(ElectionStateFollowerReady, nowNs, "")
		return 1
	}
	if nowNs >= e.timeOfLastStateChangeNs+e.cfg.LeaderHeartbeatTimeoutNs {
		e.HandleError(nowNs, errors.New("election: failed to join live log as follower"))
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
		e.gracefulClosedLeaderID = NullMemberID
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
	if e.electionState == ElectionStateInit {
		return
	}
	leader := e.memberByID[fields.LeaderMemberID]
	if leader == nil {
		return
	}
	if fields.LeaderMemberID == e.thisMember.ID && fields.LeadershipTermID == e.leadershipTermID {
		return
	}
	if fields.LeaderMemberID == e.gracefulClosedLeaderID {
		e.gracefulClosedLeaderID = NullMemberID
	}
	nowNs := e.deps.NanoClock()

	// A ballot state only accepts the term it is voting on; canvass accepts
	// any term from a known leader.
	inBallotOrCanvass :=
		((e.electionState == ElectionStateFollowerBallot ||
			e.electionState == ElectionStateCandidateBallot) &&
			fields.LeadershipTermID == e.candidateTermID) ||
			e.electionState == ElectionStateCanvass

	if inBallotOrCanvass {
		if fields.LogLeadershipTermID == e.logLeadershipTermID {
			e.installLeadershipTerm(leader, fields, nowNs)
		} else {
			e.state(ElectionStateCanvass, nowNs, "")
		}
	}

	if e.electionState == ElectionStateFollowerLogReplication &&
		fields.LogLeadershipTermID == e.logLeadershipTermID &&
		e.leaderMember != nil && e.leaderMember.ID == fields.LeaderMemberID {
		e.replicationDeadlineNs = nowNs + e.cfg.LeaderHeartbeatTimeoutNs
	}
}

// installLeadershipTerm adopts leader's term and routes to the follower
// replay or log-replication path.
func (e *Election) installLeadershipTerm(
	leader *Member,
	fields codecs.NewLeadershipTermFields,
	nowNs int64,
) {
	truncating := fields.NextTermBaseLogPosition != NullPosition &&
		fields.NextTermBaseLogPosition < e.appendPosition
	if truncating {
		e.deps.Host.TruncateLogEntry(fields.LogLeadershipTermID, fields.NextTermBaseLogPosition)
		e.appendPosition = fields.NextTermBaseLogPosition
		e.HandleError(nowNs, errors.New("election: truncating cluster log"))
		return
	}

	e.leaderMember = leader
	e.isLeaderStartup = fields.IsStartup == clusterwire.BooleanType_TRUE
	e.leadershipTermID = fields.LeadershipTermID
	e.candidateTermID = max(fields.LeadershipTermID, e.candidateTermID)
	e.logSessionID = fields.LogSessionID
	e.leaderRecordingID = fields.LeaderRecordingID
	if e.appendPosition < fields.LogPosition {
		e.catchupJoinPosition = fields.LogPosition
	} else {
		e.catchupJoinPosition = NullPosition
	}
	e.notifiedCommitPosition = max(e.notifiedCommitPosition, fields.CommitPosition)

	if e.appendPosition >= fields.TermBaseLogPosition {
		e.state(ElectionStateFollowerReplay, nowNs, "")
		return
	}
	if fields.NextLeadershipTermID == NullValue {
		e.HandleError(nowNs, errors.New("election: invalid newLeadershipTerm"))
		return
	}
	if e.appendPosition < fields.NextTermBaseLogPosition {
		e.replicationLeadershipTermID = fields.LogLeadershipTermID

		// An open but uncommitted term: its base position is already known, so
		// there is no need to look it up from the recording log.
		e.replicationTermBaseLogPosition = NullValue
		e.replicationStopPosition = fields.NextTermBaseLogPosition
		e.state(ElectionStateFollowerLogReplication, nowNs, "")
		return
	}
	if e.appendPosition == fields.NextTermBaseLogPosition &&
		fields.NextLogPosition != NullPosition {
		e.replicationLeadershipTermID = fields.NextLeadershipTermID
		e.replicationStopPosition = fields.NextLogPosition
		e.replicationTermBaseLogPosition = fields.NextTermBaseLogPosition
		e.state(ElectionStateFollowerLogReplication, nowNs, "")
	}
}

func (e *Election) OnAppendPosition(fields codecs.AppendPositionFields) {
	if e.electionState == ElectionStateInit {
		return
	}
	if fields.LeadershipTermID > e.leadershipTermID {
		return
	}
	follower := e.memberByID[fields.FollowerMemberID]
	if follower == nil {
		return
	}
	e.deps.Host.UpdateMemberLogPosition(follower, fields.LeadershipTermID, fields.LogPosition)
	e.deps.Host.TrackCatchupCompletion(follower, fields.LeadershipTermID, fields.Flags)

	// A follower still on an older term learns the current one from a leader
	// republish; election closure stays with the LEADER_READY duty cycle.
	if fields.LeadershipTermID < e.leadershipTermID &&
		e.deps.Host.Role() == consensus.RoleLeader {
		nowNs := e.deps.NanoClock()
		quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
		e.publishNewLeadershipTermToMember(
			follower,
			fields.LeadershipTermID,
			quorum,
			e.deps.EpochClock(),
		)
	}
}

// OnCommitPosition intentionally skips a leadershipTermID equality check:
// older leaders published a CommitPosition term that did not match their
// NewLeadershipTerm event, so only the leader identity gates the update.
func (e *Election) OnCommitPosition(fields codecs.CommitPositionFields) {
	if e.electionState == ElectionStateInit {
		return
	}

	leader := e.leaderMember
	if leader != nil && leader.ID == fields.LeaderMemberID {
		e.notifiedCommitPosition = max(e.notifiedCommitPosition, fields.LogPosition)
		if e.electionState == ElectionStateFollowerLogReplication {
			e.replicationDeadlineNs = e.deps.NanoClock() + e.cfg.LeaderHeartbeatTimeoutNs
		}
		return
	}

	if fields.LeadershipTermID > e.leadershipTermID &&
		e.electionState == ElectionStateLeaderReady {
		e.HandleError(
			e.deps.NanoClock(),
			errors.New("election: new leader detected from commit position"),
		)
	}
}

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
		e.deps.Host.SetLogRecordingID(e.logReplication.RecordingID())
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
