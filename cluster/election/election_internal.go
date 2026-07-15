package election

func hasUpdateIntervalExpired(nowNs, lastUpdateNs, intervalNs int64) bool {
	return nowNs >= lastUpdateNs+intervalNs
}

func (e *Election) nominationDelayNs() int64 {
	r := 0.0
	if e.deps.NominationRand != nil {
		r = e.deps.NominationRand()
	}
	return int64(r * float64(e.cfg.ElectionTimeoutNs>>1))
}

func (e *Election) placeVote(candidateTermID int64, candidateMemberID int32, vote bool) {
	candidate := e.memberByID[candidateMemberID]
	if candidate == nil {
		return
	}
	e.deps.Publisher.PlaceVote(
		PublicationTarget{MemberID: candidateMemberID},
		candidateTermID,
		e.logLeadershipTermID,
		e.appendPosition,
		candidateMemberID,
		e.thisMember.ID,
		vote,
	)
}

func (e *Election) completeElection(nowNs int64) bool {
	if !e.deps.Host.AppendNewLeadershipTermEvent(nowNs) {
		return false
	}
	e.deps.Handler.ElectionComplete(nowNs)
	e.state(ElectionStateClosed, nowNs, "")
	return true
}

func (e *Election) prepareForNewLeadership(nowNs int64) {
	e.appendPosition = e.deps.Handler.PrepareForNewLeadership(e.logPosition, nowNs)
}

func (e *Election) durableCandidateTermID() int64 {
	ct, err := e.deps.NodeState.CandidateTerm()
	if err != nil {
		// A closed or unreadable node-state file cannot raise the candidate term;
		// fall back to the known leadership term so init still makes progress.
		return e.leadershipTermID
	}
	return max(ct.CandidateTermID(), e.leadershipTermID)
}

func (e *Election) isSingleNodeCluster() bool {
	return len(e.clusterMembers) == 1
}

func (e *Election) isBallotState() bool {
	return e.electionState == ElectionStateCanvass ||
		e.electionState == ElectionStateNominate ||
		e.electionState == ElectionStateCandidateBallot ||
		e.electionState == ElectionStateFollowerBallot
}

func (e *Election) stopReplay() {
	e.logReplay = nil
	e.lastPublishedAppendPosition = 0
}

// followerJoinState selects catchup init when the leader's live log is ahead
// of this node's append position, live log init otherwise.
func (e *Election) followerJoinState() ElectionState {
	if e.catchupJoinPosition != NullPosition {
		return ElectionStateFollowerCatchupInit
	}
	return ElectionStateFollowerLogInit
}

func (e *Election) publishCanvassPosition() {
	for _, member := range e.clusterMembers {
		if member.ID == e.thisMember.ID {
			continue
		}
		e.deps.Publisher.CanvassPosition(
			PublicationTarget{MemberID: member.ID},
			e.logLeadershipTermID,
			e.appendPosition,
			e.leadershipTermID,
			e.thisMember.ID,
		)
	}
}
