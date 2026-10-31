package election

import (
	"errors"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

var (
	errNewElectionFromCanvass = errors.New("election: new election detected from canvass position")
	errCommitPositionTimeout  = errors.New("election: timeout awaiting commit position")
)

// handleInit holds in INIT until PrepareForNewLeadership reports prepared.
// The first node-startup init still skips prepare.
func (e *Election) handleInit(nowNs int64) int {
	skipPrepare := false
	if e.isFirstInit {
		e.isFirstInit = false
		skipPrepare = e.isNodeStartup
	} else {
		e.replayTermErr = nil
		if work, stopping := e.stopLogReplication(nowNs); stopping {
			return work
		}
		e.logSessionID = NullMemberID
		e.stopReplay()

		// PrepareForNewLeadership tears down the catchup receive path. The next
		// leadership install derives catchupJoinPosition from its own term.
		e.catchupJoinPosition = NullPosition
	}
	pending := e.deps.Handler.IsPrepareInProgress()
	if !skipPrepare {
		last, prepared, err := e.deps.Handler.PrepareForNewLeadership(
			e.logPosition,
			nowNs,
		)
		if err != nil {
			e.HandleError(nowNs, err)
			return 1
		}
		if !prepared {
			// A re-poll of a pending stop reports zero work so the idle strategy
			// can pace the wait.
			if pending {
				return 0
			}
			return 1
		}
		if last != NullPosition {
			e.appendPosition = last
		}
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
		e.becomeSelfLeader(nowNs, "single-node cluster leader")
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
		e.becomeSelfLeader(nowNs, "unanimous leader")
		return 1
	}

	if nowNs >= e.timeOfLastStateChangeNs+e.cfg.ElectionTimeoutNs {
		if IsQuorumLeader(e.clusterMembers, e.candidateTermID) {
			e.becomeSelfLeader(nowNs, "quorum leader")
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
			codecs.RequestVoteFields{
				LogLeadershipTermID: e.logLeadershipTermID,
				LogPosition:         e.appendPosition,
				CandidateTermID:     e.candidateTermID,
				CandidateMemberID:   e.thisMember.ID,
			},
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
			replayed := e.logReplay.Position()
			e.stopReplay()
			e.logPosition = replayed
			if replayed == e.appendPosition {
				e.state(ElectionStateLeaderInit, nowNs, "")
			} else {
				// Incomplete replay exits LeaderReplay without publishing
				// leadership. Canvass entry resets interval stamps. Fall-through
				// would fan out NewLeadershipTerm and CommitPosition while Role
				// is Follower.
				e.state(ElectionStateCanvass, nowNs, "incomplete log replay")
				return workCount
			}
		}
	}

	quorum := e.deps.Host.QuorumPositionBoundedByLeaderLog(e.appendPosition, nowNs)
	workCount += e.publishNewLeadershipTermOnInterval(quorum, nowNs)
	workCount += e.publishCommitPositionOnInterval(quorum, nowNs)
	return workCount
}

func (e *Election) handleLeaderInit(nowNs int64) int {
	if err := e.deps.Handler.JoinLogAsLeader(
		e.leadershipTermID,
		e.logPosition,
		e.logSessionID,
		e.isLeaderStartup,
	); err != nil {
		e.HandleError(nowNs, err)
		return 1
	}
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

// handleFollowerLogReplication runs the installed LogReplication helper. It
// sends the helper position to the leader each tick.
//
// Behaviour:
//   - no helper: startFollowerLogReplication runs instead
//   - a DoWork error enters Init. The helper stays installed for Init to stop
//   - done, notified commit at or above the append position: the append
//     position takes the helper position. The helper closes. The replicated
//     term commits
//   - done, notified commit below the append position: enters Init at the
//     deadline. Leader NewLeadershipTerm and CommitPosition traffic refresh
//     the deadline while a helper is installed
func (e *Election) handleFollowerLogReplication(nowNs int64) int {
	rep := e.logReplication
	if rep == nil {
		return e.startFollowerLogReplication(nowNs)
	}
	workCount, err := rep.DoWork(nowNs)
	if err != nil {
		e.HandleError(nowNs, err)
		return 1
	}
	done := rep.IsDone()
	position := rep.Position()
	workCount += e.publishFollowerReplicationPosition(position, nowNs)
	if !done {
		return workCount
	}
	if e.notifiedCommitPosition >= e.appendPosition {
		e.appendPosition = position
		e.storeReplicationRecordingID(rep)
		rep.Close()
		e.clearLogReplication()
		return workCount + 1 + e.commitReplicatedTerm(nowNs)
	}
	if nowNs >= e.replicationDeadlineNs {
		e.HandleError(nowNs, errCommitPositionTimeout)
		return 1
	}
	return workCount
}

// startFollowerLogReplication runs the state without a helper.
//
// Behaviour:
//   - append position at a known stop position: commits the replicated term
//   - NullPosition stop position: an injected state. Does not build a helper
//   - otherwise asks the host for a helper. A new helper sets the deadline one
//     heartbeat timeout out. A nil helper, from a nil factory or a missing
//     leader, keeps the install deadline
//   - at the deadline without a helper: enters Init
func (e *Election) startFollowerLogReplication(nowNs int64) int {
	stopPosition := e.replicationStopPosition
	if stopPosition != NullPosition {
		if e.appendPosition >= stopPosition {
			return e.commitReplicatedTerm(nowNs)
		}
		if rep := e.newFollowerLogReplication(nowNs); rep != nil {
			e.logReplication = rep
			e.replicationDeadlineNs = nowNs + e.cfg.LeaderHeartbeatTimeoutNs
			return 1
		}
	}
	if nowNs >= e.replicationDeadlineNs {
		e.HandleError(nowNs, errCommitPositionTimeout)
		return 1
	}
	return 0
}

// commitReplicatedTerm writes the replicated term entry, then enters Canvass.
// Returns 1 when a write error enters Init, else 0.
func (e *Election) commitReplicatedTerm(nowNs int64) int {
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

// newFollowerLogReplication asks the host for a replication helper. Returns nil
// when no leader is set or the host factory returns nil.
func (e *Election) newFollowerLogReplication(nowNs int64) LogReplication {
	leader := e.leaderMember
	if leader == nil {
		return nil
	}
	return e.deps.Host.NewLogReplication(
		leader.ArchiveEndpoint,
		leader.ArchiveResponseEndpoint,
		e.leaderRecordingID,
		e.replicationStopPosition,
		nowNs,
	)
}

// storeReplicationRecordingID passes the helper's destination recording id to
// the host. It skips a NullValue id, which the helper reports before EXTEND.
func (e *Election) storeReplicationRecordingID(rep LogReplication) {
	if id := rep.RecordingID(); id != NullValue {
		e.deps.Host.SetLogRecordingID(id)
	}
}

// stopLogReplication closes the installed helper and drives its stop.
//
// Returns:
//   - 0, false: no helper, or the stop finished on this call.
//     clearLogReplication has run
//   - work, true: the stop is still running. The caller holds its state
//
// HandleError receives a stop error. The error closes the helper, so the same
// call clears it.
func (e *Election) stopLogReplication(nowNs int64) (int, bool) {
	if rep := e.logReplication; rep != nil {
		rep.Close()
		workCount, err := rep.DoWork(nowNs)
		if err != nil {
			e.HandleError(nowNs, err)
		}
		if !rep.IsClosed() {
			return workCount, true
		}
	}
	e.clearLogReplication()
	return 0, false
}

// clearLogReplication drops the helper reference. It resets the replication
// deadline and the last published commit position. It does not close the
// helper.
func (e *Election) clearLogReplication() {
	e.logReplication = nil
	e.replicationDeadlineNs = 0
	e.lastPublishedCommitPosition = 0
}

// refreshReplicationDeadline extends the log-replication timeout on matching
// leader traffic. Extends only while a LogReplication helper is installed. Lag
// under a nil factory still times out at the deadline set on install.
func (e *Election) refreshReplicationDeadline() {
	if e.electionState != ElectionStateFollowerLogReplication || e.logReplication == nil {
		return
	}
	e.replicationDeadlineNs = e.deps.NanoClock() + e.cfg.LeaderHeartbeatTimeoutNs
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

	if err := e.takeReplayTermErr(); err != nil {
		e.HandleError(nowNs, err)
		return workCount + 1
	}
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
	// Near-live short-circuits to LOG_INIT only; it is not catchup complete.
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

	leader := e.leaderMember
	endpoint := e.thisMember.CatchupEndpoint
	if leader == nil || endpoint == "" {
		return e.heartbeatTimeoutWork(
			nowNs,
			"election: no leader or catchup endpoint for catchup position",
		)
	}

	// Open the catchup receive path before the leader starts archive replay.
	// Ensure is idempotent when the destination is already open.
	if e.deps.Host.EnsureCatchupLogDestination(endpoint) == "" {
		return e.heartbeatTimeoutWork(nowNs, "election: failed to open catchup log destination")
	}

	sent := e.deps.Publisher.CatchupPosition(
		PublicationTarget{MemberID: leader.ID},
		codecs.CatchupPositionFields{
			LeadershipTermID: e.leadershipTermID,
			LogPosition:      e.logPosition,
			FollowerMemberID: e.thisMember.ID,
			CatchupEndpoint:  endpoint,
		},
	)
	if sent {
		e.timeOfLastUpdateNs = nowNs
		e.deps.Host.CatchupInitiated(nowNs)
		e.state(ElectionStateFollowerCatchupAwait, nowNs, "")
		return 1
	}
	// Work 1 re-offers the send on the next tick. Back-pressure clears as the
	// publication drains, unlike a failed destination open.
	e.heartbeatTimeoutWork(nowNs, "election: failed to send catchup position")
	return 1
}

// heartbeatTimeoutWork fires HandleError with msg once the leader heartbeat
// deadline has passed since the last state change.
//
// Returns: 1 when HandleError fires, 0 while still waiting. Callers that already
// count the tick may discard the result.
func (e *Election) heartbeatTimeoutWork(nowNs int64, msg string) int {
	if nowNs < e.timeOfLastStateChangeNs+e.cfg.LeaderHeartbeatTimeoutNs {
		return 0
	}
	e.HandleError(nowNs, errors.New(msg))
	return 1
}

func (e *Election) handleFollowerCatchupAwait(nowNs int64) int {
	params, ok := e.deps.Host.FollowerLogJoinReady(e.logSessionID)
	if !ok {
		return e.heartbeatTimeoutWork(nowNs, "election: failed to join catchup log")
	}
	if params.LogJoinPosition != e.logPosition {
		// Wait for an image at the follower log position. After reseed only the
		// catchup destination feeds the subscription. A mismatch means the
		// catchup image is not ready yet.
		return e.heartbeatTimeoutWork(nowNs, "election: log join position mismatch")
	}

	joined, failed := e.joinLogAsFollower(params, nowNs)
	if failed {
		return 1
	}
	if joined {
		e.state(ElectionStateFollowerCatchup, nowNs, "")
		return 1
	}
	return e.heartbeatTimeoutWork(nowNs, "election: failed to join catchup log as follower")
}

// handleFollowerCatchup polls the host for catchup progress up to the notified
// commit bound.
//
// Unset notified commit is 0 (field init), not NullPosition. A zero limit falls
// back to catchupJoinPosition.
//
// During catchup the log subscription carries only the catchup destination until
// the image is near live. Near live attaches the live destination so multicast
// and replay frames merge into one image. StopCatchup removes the catchup
// destination. RemoveCatchupLogDestination re-attaches live when the near-live
// attach has not run.
//
// Completion requires all of:
//   - commit at the join and notified bounds
//   - catchup destination removed
//   - live destination attached
//
// Then advances append and log position and moves to LOG_INIT.
func (e *Election) handleFollowerCatchup(nowNs int64) int {
	host := e.deps.Host
	limit := e.notifiedCommitPosition
	if limit == 0 {
		limit = e.catchupJoinPosition
	}
	workCount, err := host.CatchupPoll(limit, nowNs)
	if stored := e.takeReplayTermErr(); stored != nil {
		err = errors.Join(err, stored)
	}
	if err != nil {
		e.HandleError(nowNs, err)
		return workCount + 1
	}

	// Near live: attach the live destination. Frames merge with the catchup image.
	// Failure leaves LiveLogDestination empty. The next tick retries.
	nearLivePos := max(e.notifiedCommitPosition, e.catchupJoinPosition)
	if host.LiveLogDestination() == "" && host.IsCatchupNearLive(nearLivePos) {
		host.AddLiveLogDestination()
		workCount++
	}

	// Complete only when install set a join bound. Injected CATCHUP without a
	// join position never completes here (tests and sim). A host with an
	// append source fails the poll at the progress timeout instead.
	join := e.catchupJoinPosition
	if join == NullPosition {
		return workCount
	}
	position := host.CommitPosition()
	if position < join || position < e.notifiedCommitPosition {
		return workCount
	}
	if host.CatchupLogDestination() != "" || host.LiveLogDestination() == "" {
		return workCount
	}
	e.appendPosition = position
	e.logPosition = position
	e.state(ElectionStateFollowerLogInit, nowNs, "")
	return workCount + 1
}

// handleFollowerLogInit routes the follower to the live log.
//
// LOG_INIT is reached with the adapter already on the advertised session in
// two cases:
//   - catchup joined the session earlier in this election
//   - a settled re-entry kept the previous bind while prepare stopped the log
//     recording
//
// Bound re-join restarts a stopped recording for the bound image. The re-join
// selects no image and never rebinds the adapter. The bound path does not
// compare join position. The bound image fixed that position in an earlier
// election, and the adapter has consumed past it.
//
// A refused re-join routes to LOG_AWAIT. LOG_AWAIT position checks guard any
// fresh bind.
//
// A successful bound re-join commits the leadership term into the recording
// log before READY. Catchup join starts the REMOTE recording but does not call
// commitRecordingLogTerm. Without that commit the election can close with an
// active recording and an empty term index.
func (e *Election) handleFollowerLogInit(nowNs int64) int {
	if e.logSessionID == NullMemberID {
		return 1
	}
	if !e.deps.Host.IsLogJoinedForSession(e.logSessionID) {
		e.state(ElectionStateFollowerLogAwait, nowNs, "")
		return 1
	}

	params, ok := e.deps.Host.FollowerLogJoinReady(e.logSessionID)
	if !ok {
		e.state(ElectionStateFollowerLogAwait, nowNs, "")
		return 1
	}

	params.IsBoundRejoin = true
	joined, failed := e.joinLogAsFollower(params, nowNs)
	if failed {
		return 1
	}
	if !joined {
		e.state(ElectionStateFollowerLogAwait, nowNs, "")
		return 1
	}
	if err := e.commitRecordingLogTerm(e.leadershipTermID, e.logPosition, NullValue); err != nil {
		e.HandleError(nowNs, err)
		return 1
	}
	e.state(ElectionStateFollowerReady, nowNs, "")
	return 1
}

func (e *Election) handleFollowerLogAwait(nowNs int64) int {
	params, ok := e.deps.Host.FollowerLogJoinReady(e.logSessionID)
	if !ok {
		return e.heartbeatTimeoutWork(nowNs, "election: failed to join live log")
	}
	if params.LogJoinPosition != e.logPosition {
		e.HandleError(nowNs, errors.New("election: log join position mismatch"))
		return 1
	}

	joined, failed := e.joinLogAsFollower(params, nowNs)
	if failed {
		return 1
	}
	if joined {
		if err := e.commitRecordingLogTerm(e.leadershipTermID, e.logPosition, NullValue); err != nil {
			e.HandleError(nowNs, err)
			return 1
		}
		e.state(ElectionStateFollowerReady, nowNs, "")
		return 1
	}
	return e.heartbeatTimeoutWork(nowNs, "election: failed to join live log as follower")
}

func (e *Election) handleFollowerReady(nowNs int64) int {
	leader := e.leaderMember
	published := leader != nil && e.deps.Publisher.AppendPosition(
		PublicationTarget{MemberID: leader.ID},
		codecs.AppendPositionFields{
			LeadershipTermID: e.leadershipTermID,
			LogPosition:      e.logPosition,
			FollowerMemberID: e.thisMember.ID,
		},
	)
	if published {
		e.deps.Handler.ElectionComplete(nowNs)
		e.state(ElectionStateClosed, nowNs, "")
	} else {
		e.heartbeatTimeoutWork(nowNs, "election: ready follower failed to notify leader")
	}
	return 1
}

func (e *Election) becomeSelfLeader(nowNs int64, reason string) {
	e.leaderMember = e.thisMember
	e.leadershipTermID = e.candidateTermID
	e.state(ElectionStateLeaderLogReplication, nowNs, reason)

	// The entry cannot reset itself: state writes the new state after its
	// switch. Reset here so no duty cycle runs with the failed term.
	if err := e.logPublicationErr; err != nil {
		e.logPublicationErr = nil
		e.HandleError(nowNs, err)
	}
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
	case clusterwire.CatchupPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeCatchupPositionAt, e.OnCatchupPosition)
	case clusterwire.StopCatchupSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeStopCatchupAt, e.OnStopCatchup)
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
	if !e.acceptsConsensusInbound() {
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
	switch logTermID := fields.LogLeadershipTermID; {
	case logTermID < e.leadershipTermID:
		e.publishNewLeadershipTermToLaggingMember(follower, logTermID, e.deps.NanoClock())
	case logTermID > e.leadershipTermID && e.isLeaderAwaitingFollowers():
		e.HandleError(e.deps.NanoClock(), errNewElectionFromCanvass)
	}
}

func (e *Election) isLeaderAwaitingFollowers() bool {
	return e.electionState == ElectionStateLeaderLogReplication ||
		e.electionState == ElectionStateLeaderReady
}

func (e *Election) OnRequestVote(fields codecs.RequestVoteFields) {
	if !e.acceptsConsensusInbound() {
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
		nowNs := e.deps.NanoClock()
		if !e.proposeCandidateTermAndVote(fields, false, nowNs) {
			return
		}
		if candidate := e.memberByID[fields.CandidateMemberID]; candidate != nil {
			e.publishNewLeadershipTermToLaggingMember(candidate, fields.LogLeadershipTermID, nowNs)
		}
		return
	}

	if !e.isBallotState() {
		return
	}
	nowNs := e.deps.NanoClock()
	if !e.proposeCandidateTermAndVote(fields, true, nowNs) {
		return
	}
	e.state(ElectionStateFollowerBallot, nowNs, "")
}

func (e *Election) proposeCandidateTermAndVote(
	fields codecs.RequestVoteFields,
	vote bool,
	nowNs int64,
) bool {
	proposed, err := e.deps.NodeState.ProposeMaxCandidateTermID(
		fields.CandidateTermID,
		fields.LogPosition,
		e.deps.EpochClock(),
	)
	if err != nil {
		e.HandleError(nowNs, err)
		return false
	}
	e.candidateTermID = proposed
	e.placeVote(fields.CandidateTermID, fields.CandidateMemberID, vote)
	return true
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
	if !e.acceptsConsensusInbound() {
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

	if e.acceptsLeadershipInstall(fields.LeadershipTermID) {
		if fields.LogLeadershipTermID == e.logLeadershipTermID {
			e.installLeadershipTerm(leader, fields, nowNs)
		} else {
			e.state(ElectionStateCanvass, nowNs, "")
		}
	}

	if fields.LogLeadershipTermID == e.logLeadershipTermID &&
		e.leaderMember != nil && e.leaderMember.ID == fields.LeaderMemberID {
		e.refreshReplicationDeadline()
	}
}

// acceptsLeadershipInstall is true for canvass (any term) and for ballot
// states when termID matches the term under vote.
func (e *Election) acceptsLeadershipInstall(termID int64) bool {
	if e.electionState == ElectionStateCanvass {
		return true
	}
	inBallot := e.electionState == ElectionStateFollowerBallot ||
		e.electionState == ElectionStateCandidateBallot
	return inBallot && termID == e.candidateTermID
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
		e.truncateLog(fields.LogLeadershipTermID, fields.NextTermBaseLogPosition, nowNs)
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

	// Handoff leader id + Agent term for post-election steady notify gates.
	e.deps.Host.SetLeaderMemberID(leader.ID)
	e.deps.Host.ApplyLeadershipTerm(fields.LeadershipTermID)

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
		e.replicationDeadlineNs = nowNs + e.cfg.LeaderHeartbeatTimeoutNs
		e.state(ElectionStateFollowerLogReplication, nowNs, "")
		return
	}
	if e.appendPosition == fields.NextTermBaseLogPosition &&
		fields.NextLogPosition != NullPosition {
		e.replicationLeadershipTermID = fields.NextLeadershipTermID
		e.replicationStopPosition = fields.NextLogPosition
		e.replicationTermBaseLogPosition = fields.NextTermBaseLogPosition
		e.replicationDeadlineNs = nowNs + e.cfg.LeaderHeartbeatTimeoutNs
		e.state(ElectionStateFollowerLogReplication, nowNs, "")
	}
}

// truncateLog lowers the recording log term, drops the divergent recorded tail,
// and returns the election to Init.
//
// Behaviour:
//   - success: lowers append to newPosition; ends logPosition at
//     min(Host.CommitPosition(), newPosition)
//   - half-fail: keeps append high; syncs logPosition only via HandleError
//
// The recording log write runs first. The install path re-enters this branch
// only while NextTermBaseLogPosition is below the tracked append position.
// The success append lowering stops the re-entry. A later prepare adopts the
// control stop from [LogRecordingControl.StopPositionFor]. Archive first would
// make a failed recording-log write permanent: prepare would adopt the control
// stop, close the install path, and leave the term entry at the divergent
// position.
//
// Both halves are idempotent. The half-fail retry converges.
func (e *Election) truncateLog(logLeadershipTermID, newPosition, nowNs int64) {
	if err := e.truncateRecordingLogTerm(logLeadershipTermID, newPosition); err != nil {
		e.HandleError(nowNs, err)
		return
	}
	if err := e.deps.Host.TruncateLogEntry(logLeadershipTermID, newPosition); err != nil {
		e.HandleError(nowNs, err)
		return
	}
	e.appendPosition = newPosition
	e.HandleError(nowNs, errors.New("election: truncating cluster log"))

	// HandleError sets logPosition from host commit. Cap logPosition at newPosition
	// when host commit sits above the truncate end.
	e.logPosition = min(e.logPosition, newPosition)
}

func (e *Election) OnAppendPosition(fields codecs.AppendPositionFields) {
	if !e.acceptsConsensusInbound() {
		return
	}
	if fields.LeadershipTermID > e.leadershipTermID {
		return
	}
	follower := e.memberByID[fields.FollowerMemberID]
	if follower == nil {
		return
	}

	// A catchup-flagged position is a recorded position. It enters the commit
	// quorum like an unflagged one.
	e.deps.Host.UpdateMemberLogPosition(follower, fields.LeadershipTermID, fields.LogPosition)
	e.deps.Host.TrackCatchupCompletion(follower, fields.LogPosition, fields.Flags)

	// A follower on an older term learns the current one from the leader.
	// The LEADER_READY duty cycle still closes the election.
	if fields.LeadershipTermID < e.leadershipTermID {
		e.publishNewLeadershipTermToLaggingMember(
			follower,
			fields.LeadershipTermID,
			e.deps.NanoClock(),
		)
	}
}

// OnStopCatchup detaches the catchup receive destination when the leader
// confirms this follower has caught up.
//
// Behaviour:
//   - refuses on term or follower-member mismatch
//   - remove is idempotent when the catchup destination is already absent
//
// Concurrency: invoker-only.
func (e *Election) OnStopCatchup(fields codecs.StopCatchupFields) {
	if !e.acceptsConsensusInbound() {
		return
	}
	if fields.LeadershipTermID != e.leadershipTermID ||
		fields.FollowerMemberID != e.thisMember.ID {
		return
	}
	e.deps.Host.RemoveCatchupLogDestination()
}

// OnCommitPosition intentionally skips a leadershipTermID equality check:
// older leaders published a CommitPosition term that did not match their
// NewLeadershipTerm event, so only the leader identity gates the update.
func (e *Election) OnCommitPosition(fields codecs.CommitPositionFields) {
	if !e.acceptsConsensusInbound() {
		return
	}

	leader := e.leaderMember
	if leader != nil && leader.ID == fields.LeaderMemberID {
		e.notifiedCommitPosition = max(e.notifiedCommitPosition, fields.LogPosition)
		e.refreshReplicationDeadline()
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

// OnCatchupPosition forwards a follower catchup request to the host. The
// leader starts an archive replay toward the follower catchup endpoint.
func (e *Election) OnCatchupPosition(fields codecs.CatchupPositionFields) {
	if !e.acceptsConsensusInbound() {
		return
	}
	e.deps.Host.OnCatchupPosition(fields)
}

// OnRecordingSignal forwards one archive recording signal to the installed
// LogReplication helper. It then passes the helper's recording id to the host
// when the id is known.
//
// Behaviour: drops the signal in Init or without a helper.
//
// Concurrency: invoker-only. The archive session handler calls it.
func (e *Election) OnRecordingSignal(
	correlationID, recordingID, position int64,
	signal RecordingSignal,
) {
	if !e.acceptsConsensusInbound() {
		return
	}
	rep := e.logReplication
	if rep == nil {
		return
	}
	rep.OnSignal(correlationID, recordingID, position, signal)
	e.storeReplicationRecordingID(rep)
}

// HandleError records err, syncs log position from Host.CommitPosition, and
// enters Init.
//
// Behaviour:
//   - stores err as LastError (nil clears)
//   - calls ErrorHandler when set and err is non-nil
//   - sets logPosition to Host.CommitPosition
//   - transitions to Init with err.Error() as reason when err is non-nil
//
// Does not change role. Does not rewrite appendPosition. Does not read
// NotifiedCommitPosition. Does not close an open prepare episode. The agent
// owns the episode. The next non-first Init pass runs
// [consensus.AgentHandler.PrepareForNewLeadership].
//
// The reason string is reserved. When later consumed, treat the reason as
// untrusted (same remote text and channel URIs as LastError). The ErrorHandler
// contract is documented on [ElectionDeps.ErrorHandler].
//
// A failed candidate-term write lands here with the in-memory term untouched.
// The Init pass re-reads the durable value.
//
// Concurrency: invoker-only.
func (e *Election) HandleError(nowNs int64, err error) {
	e.lastError = err
	if err != nil && e.deps.ErrorHandler != nil {
		e.deps.ErrorHandler(err)
	}
	e.logPosition = e.deps.Host.CommitPosition()
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	e.state(ElectionStateInit, nowNs, reason)
}
