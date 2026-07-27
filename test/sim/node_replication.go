package sim

import (
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
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
func (h *simReplicationHost) AddLogPublication(int64) int32 { return simLogSessionID }

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
