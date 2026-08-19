package election

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/consensus"
)

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
		codecs.VoteFields{
			CandidateTermID:     candidateTermID,
			LogLeadershipTermID: e.logLeadershipTermID,
			LogPosition:         e.appendPosition,
			CandidateMemberID:   candidateMemberID,
			FollowerMemberID:    e.thisMember.ID,
			Vote:                boolToClusterwire(vote),
		},
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

// joinLogAsFollower stamps the election's isLeaderStartup into params and runs
// the follower log join. A join error resets the election.
//
// Returns:
//   - joined true, failed false: the log is bound
//   - joined false, failed true: the join errored and the election reset
//   - joined false, failed false: the join was refused; the caller routes the
//     next state
func (e *Election) joinLogAsFollower(
	params consensus.FollowerLogJoinParams,
	nowNs int64,
) (
	joined bool,
	failed bool,
) {
	params.IsLeaderStartup = e.isLeaderStartup
	joined, err := e.deps.Handler.JoinLogAsFollower(params, nowNs)
	if err != nil {
		e.HandleError(nowNs, err)
		return false, true
	}
	return joined, false
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
	if replay := e.logReplay; replay != nil {
		replay.Close()
		e.logReplay = nil
	}
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
			codecs.CanvassPositionFields{
				LogLeadershipTermID: e.logLeadershipTermID,
				LogPosition:         e.appendPosition,
				LeadershipTermID:    e.leadershipTermID,
				FollowerMemberID:    e.thisMember.ID,
			},
		)
	}
}
