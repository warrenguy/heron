package sim

import (
	"github.com/warrenguy/heron/cluster/election"
)

// StateObserver records election state transitions for the timeline.
type StateObserver struct {
	sim    *ElectionSim
	nodeID int32
	States []election.ElectionState
}

func (o *StateObserver) OnElectionStateChange(s election.ElectionState) {
	o.States = append(o.States, s)
	o.sim.appendTimelineNow("state", o.nodeID, TimelineEntry{State: s})
}

// StatesCopy implements stateHistory.
func (o *StateObserver) StatesCopy() []election.ElectionState {
	return append([]election.ElectionState(nil), o.States...)
}

// newElectionStateObserver builds the timeline-only observer. Testhooks may
// replace this var in init with the safety-aware observer.
var newElectionStateObserver = func(sim *ElectionSim, node *Node) election.StateObserver {
	return &StateObserver{sim: sim, nodeID: node.id}
}
