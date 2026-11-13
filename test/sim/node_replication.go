//go:build testhooks

package sim

import (
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
)

// simReplicationHost embeds HostAdapter for production ranking and commit
// fan-out. Override methods keep multi-node elect live under raised append
// without archive or log publisher: session id, refuse election close, near-live
// catchup, and follower log join at harness start.
type simReplicationHost struct {
	*election.HostAdapter

	followerLogJoinOK bool
}

// AddLogPublication returns the sim log session id for NLT control wire.
func (h *simReplicationHost) AddLogPublication(int64) (int32, error) { return simLogSessionID, nil }

// AppendNewLeadershipTermEvent refuses election close so the leader stays in
// LEADER_READY and keeps UpdateLeaderPosition live.
//
// Distinct from control-wire NewLeadershipTerm publish on Publisher.
func (h *simReplicationHost) AppendNewLeadershipTermEvent(int64) bool { return false }

// IsCatchupNearLive always reports near-live so raised leader append does not
// stall followers in CatchupInit without archive catchup.
func (h *simReplicationHost) IsCatchupNearLive(int64) bool { return true }

// FollowerLogJoinReady reports the live log image immediately joinable: sim
// nodes carry no replicated log, so the join position is harness start
// position zero.
func (h *simReplicationHost) FollowerLogJoinReady(
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

var _ election.ElectionHost = (*simReplicationHost)(nil)

// replicationHost returns the replication host when UseReplicationHost, else nil.
func (n *Node) replicationHost() *simReplicationHost {
	h, _ := n.host.(*simReplicationHost)
	return h
}

// HostIsSimElectionHost reports whether n uses the default scripted host.
func HostIsSimElectionHost(n *Node) bool {
	if n == nil {
		return false
	}
	_, ok := n.host.(*simElectionHost)
	return ok
}

// HostIsSimReplicationHost reports whether n uses the HostAdapter replication host.
func HostIsSimReplicationHost(n *Node) bool {
	if n == nil {
		return false
	}
	return n.replicationHost() != nil
}

// SetLeaderAppend raises the synthetic leader append high-water on election.
// The Agent append source reads election live; no counter re-sync needed.
//
// Does not rewrite peer member LogPositions. PrepareForNewLeadership on
// restart overwrites the raised append; re-call if still needed.
func SetLeaderAppend(n *Node, pos int64) {
	if n == nil || n.election == nil {
		return
	}
	election.SetAppendPositionForTest(n.election, pos)
}

// LeaderAppend returns the election append high-water, or 0 when election is nil.
func LeaderAppend(n *Node) int64 {
	if n == nil || n.election == nil {
		return 0
	}
	return n.election.AppendPosition()
}

// AgentCommitPosition returns Agent.CommitPosition when the consensus Agent is
// wired (UseReplicationHost), else 0.
func AgentCommitPosition(n *Node) int64 {
	if n == nil || n.agentCM == nil {
		return 0
	}
	return n.agentCM.CommitPosition()
}
