//go:build testhooks

package sim

import "github.com/warrenguy/heron/cluster/election"

// SetLeaderAppend raises the synthetic leader append high-water on election.
// The Agent append source reads election live; no counter re-sync needed.
//
// Does not rewrite peer member LogPositions. prepareForNewLeadership on
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
