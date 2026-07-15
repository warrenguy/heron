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
}

func (h *simElectionHost) Role() consensus.Role { return h.role }

func (h *simElectionHost) RoleSet(role consensus.Role) { h.role = role }

func (h *simElectionHost) AddLogPublication(int64) int32 { return simLogSessionID }

func (h *simElectionHost) UpdateMemberLogPosition(
	member *election.Member,
	leadershipTermID, logPosition int64,
) {
	member.LeadershipTermID = leadershipTermID
	member.LogPosition = logPosition
	member.TimeOfLastAppendPositionNs = h.now()
}

func (h *simElectionHost) AppendNewLeadershipTermEvent(int64) bool { return true }

var _ election.ElectionHost = (*simElectionHost)(nil)

// simAgentHandler joins the follower log unconditionally so thin-stub follower
// close paths can run without a real replicated log. Other callbacks fall
// through to the noop handler.
type simAgentHandler struct {
	electiontest.NoopAgentHandler

	prepareCalls       int
	prepareLogPosition int64
	prepareNowNs       int64
}

func (h *simAgentHandler) PrepareForNewLeadership(logPosition, nowNs int64) int64 {
	h.prepareCalls++
	h.prepareLogPosition = logPosition
	h.prepareNowNs = nowNs
	return logPosition
}

func (h *simAgentHandler) JoinLogAsFollower(consensus.FollowerLogJoinParams, int64) bool {
	return true
}

var _ consensus.AgentHandler = (*simAgentHandler)(nil)
