//go:build testhooks

package sim

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
)

// CrashNode marks id dead: snapshots log identity, clears election host
// wiring, keeps nodeState and Network registration.
//
// Already-crashed is a no-op.
func (s *ElectionSim) CrashNode(id int32) error {
	node := s.Node(id)
	if node == nil {
		return fmt.Errorf("sim: crash unknown node %d", id)
	}
	if node.election == nil {
		return nil
	}
	node.crashSnap = &crashSnap{
		leadershipTerm:    node.election.LeadershipTermID(),
		logPosition:       node.election.LogPosition(),
		logLeadershipTerm: election.LogLeadershipTermIDForTest(node.election),
	}
	node.initLoaded = false
	clearLiveHost(node)

	// Continuous commit high-water must not survive crash: restart rebuilds
	// empty Sent and a fresh Agent at 0 under the restored term.
	s.clearCommitSignalsForMember(id)
	s.appendTimelineNow("crash", id, TimelineEntry{
		Detail: fmt.Sprintf("node=%d", id),
		Action: ActionCrash,
	})
	return nil
}

// RestartNode rebuilds Election on a crashed node, restores log leadership
// term, drives through Init, and re-seeds maxCandidateSeen when the node
// leaves Init.
func (s *ElectionSim) RestartNode(id int32) error {
	node := s.Node(id)
	if node == nil {
		return fmt.Errorf("sim: restart unknown node %d", id)
	}
	if node.election != nil {
		return fmt.Errorf("sim: restart live node %d", id)
	}
	snap := node.crashSnap
	if snap == nil {
		return fmt.Errorf("sim: restart node %d missing crash snapshot", id)
	}

	// Timeline before rebuild so stuck-Init failures still record the attempt;
	// rebuild and leave-Init emit later state/tick rows with higher Seq.
	s.appendTimelineNow("restart", id, TimelineEntry{
		Detail: fmt.Sprintf("node=%d log_term=%d", id, snap.logLeadershipTerm),
		Action: ActionRestart,
	})

	if err := s.rebuildElection(node, snap); err != nil {
		return err
	}

	// Stuck Init rolls back to crashed (snap kept for retry).
	now := s.Now()
	s.driveRestartLeaveInit(node, now)
	if node.election.State() == election.ElectionStateInit {
		clearLiveHost(node)
		return fmt.Errorf("sim: restart node %d stuck in Init", id)
	}

	node.crashSnap = nil

	// Focused sims only schedule DoWork ticks for the subject; peers stay inject-only.
	if s.ticksNode(id) {
		s.scheduleEvent(now, EventNodeTick, id, nil)
	}
	return nil
}

// driveRestartLeaveInit runs one DoWork after rebuild.
//
// Tests set restartLeaveInitHook to force stuck-Init without racing a
// package-global.
func (s *ElectionSim) driveRestartLeaveInit(node *Node, nowNs int64) {
	if s.restartLeaveInitHook != nil {
		s.restartLeaveInitHook(node, nowNs)
		return
	}
	node.Tick(nowNs)
}

// rebuildElection attaches a live Election from crashSnap identity.
//
// Always non-startup recovery, even if PerNodeOverrides set IsNodeStartup.
// AppendPosition is not restored: leave-Init PrepareForNewLeadership overwrites
// it with the handler return (logPosition under simAgentHandler).
func (s *ElectionSim) rebuildElection(node *Node, snap *crashSnap) error {
	cfg := perNodeElectionConfig(s.cfg.ElectionConfig, node.id, s.cfg.PerNodeOverrides)
	cfg.IsNodeStartup = false
	cfg.LeadershipTermID = snap.leadershipTerm
	cfg.LogPosition = snap.logPosition

	if err := attachLiveElection(s, node, cfg); err != nil {
		return err
	}
	election.SetLogLeadershipTermIDForTest(node.election, snap.logLeadershipTerm)
	return nil
}
