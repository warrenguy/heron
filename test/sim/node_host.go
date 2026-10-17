package sim

import (
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
)

const simLogSessionID int32 = 777

// simElectionHost supplies role, sim time, and log-session values the ballot
// and leader paths read. Methods left unset fall through to the noop host.
//
// TimeOfLastLeaderUpdateNs stays on the noop (returns 0), so the non-extended
// canvass heartbeat deadline is LeaderHeartbeatTimeoutNs from epoch, not from
// sim start. Append position stamps use h.now().
type simElectionHost struct {
	electiontest.NoopElectionHost

	now func() int64

	role consensus.Role

	quorumSequence             []int64
	quorumCallCount            int
	localLogRecordingID        int64
	followerLogJoinOK          bool
	catchupNearLive            bool
	catchupPollCalls           int
	publishCommitPositionCalls []publishCommitPositionCall
}

// publishCommitPositionCall records one PublishCommitPosition host invocation.
type publishCommitPositionCall struct {
	commitPosition   int64
	leadershipTermID int64
}

func (h *simElectionHost) Role() consensus.Role { return h.role }

func (h *simElectionHost) RoleSet(role consensus.Role) { h.role = role }

func (h *simElectionHost) AddLogPublication(int64) (int32, error) { return simLogSessionID, nil }

func (h *simElectionHost) QuorumPositionBoundedByLeaderLog(int64, int64) int64 {
	if len(h.quorumSequence) == 0 {
		return 0
	}
	idx := min(h.quorumCallCount, len(h.quorumSequence)-1)
	h.quorumCallCount++
	return h.quorumSequence[idx]
}

func (h *simElectionHost) LocalLogRecordingID() int64 { return h.localLogRecordingID }

func (h *simElectionHost) SetLogRecordingID(recordingID int64) {
	h.localLogRecordingID = recordingID
}

func (h *simElectionHost) PublishCommitPosition(commitPosition, leadershipTermID int64) {
	h.publishCommitPositionCalls = append(h.publishCommitPositionCalls, publishCommitPositionCall{
		commitPosition:   commitPosition,
		leadershipTermID: leadershipTermID,
	})
}

func (h *simElectionHost) UpdateMemberLogPosition(
	member *election.Member,
	leadershipTermID, logPosition int64,
) {
	member.LeadershipTermID = leadershipTermID
	member.LogPosition = logPosition
	member.TimeOfLastAppendPositionNs = h.now()
}

func (h *simElectionHost) AppendNewLeadershipTermEvent(int64) bool { return true }

func (h *simElectionHost) CatchupPoll(int64, int64) int {
	h.catchupPollCalls++
	return 0
}

func (h *simElectionHost) IsCatchupNearLive(int64) bool { return h.catchupNearLive }

// FollowerLogJoinReady reports the live log image immediately joinable: sim
// nodes carry no replicated log, so the join position is the harness start
// position zero.
func (h *simElectionHost) FollowerLogJoinReady(
	logSessionID int32,
) (consensus.FollowerLogJoinParams, bool) {
	if !h.followerLogJoinOK {
		return consensus.FollowerLogJoinParams{}, false
	}
	return consensus.FollowerLogJoinParams{
		LogChannel:      "log=sim",
		LogStreamID:     100,
		LogSessionID:    logSessionID,
		LogJoinPosition: 0,
	}, true
}

var _ election.ElectionHost = (*simElectionHost)(nil)

// joinLogCall records one JoinLogAsFollower invocation from the election handler.
type joinLogCall struct {
	params consensus.FollowerLogJoinParams
	nowNs  int64
}

// simAgentHandler records follower log joins and returns joinReturns so
// scenarios can exercise join success and stall paths. Other callbacks fall
// through to the noop handler.
//
// When agentCM is set (replication host path), JoinLogAsLeader seeds the Agent
// term/role. ElectionDeps.Handler stays this type, not *consensus.Agent.
type simAgentHandler struct {
	electiontest.NoopAgentHandler

	prepareCalls       int
	prepareLogPosition int64
	prepareNowNs       int64

	joinLogCalls []joinLogCall
	joinReturns  bool

	agentCM *consensus.Agent
}

func (h *simAgentHandler) PrepareForNewLeadership(
	logPosition int64,
	nowNs int64,
) (
	int64,
	bool,
	error,
) {
	h.prepareCalls++
	h.prepareLogPosition = logPosition
	h.prepareNowNs = nowNs
	return logPosition, true, nil
}

func (h *simAgentHandler) JoinLogAsLeader(
	leadershipTermID, logPosition int64,
	logSessionID int32,
	isStartup bool,
) error {
	if h.agentCM == nil {
		return nil
	}
	return h.agentCM.JoinLogAsLeader(leadershipTermID, logPosition, logSessionID, isStartup)
}

func (h *simAgentHandler) JoinLogAsFollower(
	params consensus.FollowerLogJoinParams,
	nowNs int64,
) (bool, error) {
	h.joinLogCalls = append(h.joinLogCalls, joinLogCall{params: params, nowNs: nowNs})
	return h.joinReturns, nil
}

var _ consensus.AgentHandler = (*simAgentHandler)(nil)
