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
	if o.sim == nil {
		return
	}
	o.sim.appendTimelineNow("state", o.nodeID, TimelineEntry{State: s})
}
