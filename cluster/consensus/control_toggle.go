package consensus

import (
	"fmt"

	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
)

// clusterActionFlagsStandbySnapshot marks a SNAPSHOT action for a standby
// only. Every member skips the action.
const clusterActionFlagsStandbySnapshot int32 = 1

// ControlToggleState is a cluster control toggle code. Codes match upstream
// ClusterControl.ToggleState.
//
// [ClusterControlToggleCounterTypeID] carries the code.
type ControlToggleState int32

const (
	ControlToggleInactive ControlToggleState = iota
	ControlToggleNeutral
	ControlToggleSuspend
	ControlToggleResume
	ControlToggleSnapshot
	ControlToggleShutdown
	ControlToggleAbort
	ControlToggleStandbySnapshot
)

var controlToggleNames = [...]string{
	ControlToggleInactive:        "INACTIVE",
	ControlToggleNeutral:         "NEUTRAL",
	ControlToggleSuspend:         "SUSPEND",
	ControlToggleResume:          "RESUME",
	ControlToggleSnapshot:        "SNAPSHOT",
	ControlToggleShutdown:        "SHUTDOWN",
	ControlToggleAbort:           "ABORT",
	ControlToggleStandbySnapshot: "STANDBY_SNAPSHOT",
}

func (s ControlToggleState) Code() int32 { return int32(s) }

func (s ControlToggleState) String() string {
	if s < 0 || int(s) >= len(controlToggleNames) {
		return fmt.Sprintf("ControlToggleState(%d)", int32(s))
	}
	return controlToggleNames[s]
}

// Toggle sets counter to s by compare-and-set from NEUTRAL. RESUME on a
// counter that reads SUSPEND compares from SUSPEND instead.
//
// Returns false when counter is nil or the compare-and-set fails.
func (s ControlToggleState) Toggle(counter ToggleCounter) bool {
	if counter == nil {
		return false
	}
	suspend := int64(ControlToggleSuspend)
	if s == ControlToggleResume && counter.Load() == suspend {
		return counter.CompareAndSet(suspend, int64(s))
	}
	return counter.CompareAndSet(int64(ControlToggleNeutral), int64(s))
}

func ControlToggleStateFromCode(code int64) (ControlToggleState, error) {
	if code < 0 || code >= int64(len(controlToggleNames)) {
		return 0, fmt.Errorf("consensus: invalid control toggle value: %d", code)
	}
	return ControlToggleState(code), nil
}

// NodeControlToggleState is a node control toggle code. Codes match upstream
// NodeControl.ToggleState.
//
// [NodeControlToggleCounterTypeID] carries the code.
type NodeControlToggleState int32

const (
	NodeControlToggleInactive NodeControlToggleState = iota
	NodeControlToggleNeutral
	NodeControlToggleReplicateStandbySnapshot
	NodeControlToggleValidateRecordingLog
)

var nodeControlToggleNames = [...]string{
	NodeControlToggleInactive:                 "INACTIVE",
	NodeControlToggleNeutral:                  "NEUTRAL",
	NodeControlToggleReplicateStandbySnapshot: "REPLICATE_STANDBY_SNAPSHOT",
	NodeControlToggleValidateRecordingLog:     "VALIDATE_RECORDING_LOG",
}

func (s NodeControlToggleState) Code() int32 { return int32(s) }

func (s NodeControlToggleState) String() string {
	if s < 0 || int(s) >= len(nodeControlToggleNames) {
		return fmt.Sprintf("NodeControlToggleState(%d)", int32(s))
	}
	return nodeControlToggleNames[s]
}

// Toggle sets counter to s by compare-and-set from NEUTRAL.
//
// Returns false when counter is nil or the compare-and-set fails.
func (s NodeControlToggleState) Toggle(counter ToggleCounter) bool {
	if counter == nil {
		return false
	}
	return counter.CompareAndSet(int64(NodeControlToggleNeutral), int64(s))
}

func NodeControlToggleStateFromCode(code int64) (NodeControlToggleState, error) {
	if code < 0 || code >= int64(len(nodeControlToggleNames)) {
		return 0, fmt.Errorf("consensus: invalid node control toggle value: %d", code)
	}
	return NodeControlToggleState(code), nil
}

// ToggleCounter holds one toggle value. Tools in other processes also write
// the value.
type ToggleCounter interface {
	Load() int64
	CompareAndSet(expect, update int64) bool

	// Store writes value with release order.
	Store(value int64)
}

// SetClusterControlToggle sets or clears the cluster control toggle. The
// setter does not store a value.
//
// Behaviour: [Agent.ElectionComplete] stores NEUTRAL on a leader. The
// leadership teardown stores INACTIVE. A settled leader acts on the value on
// each slow tick.
//
// Concurrency: invoker-only.
func (a *Agent) SetClusterControlToggle(counter ToggleCounter) {
	a.controlToggle = counter
}

// SetNodeControlToggle sets or clears the node control toggle. The setter
// does not store a value.
//
// Behaviour: [Agent.ElectionComplete] stores NEUTRAL on every role. The agent
// does not act on the other node toggle values.
//
// Concurrency: invoker-only.
func (a *Agent) SetNodeControlToggle(counter ToggleCounter) {
	a.nodeControlToggle = counter
}

// clusterControlToggleWork acts on the cluster control toggle on a settled
// leader.
//
// STANDBY_SNAPSHOT and RESUME reset the toggle to NEUTRAL by compare-and-set,
// so a tool write during the append survives. Every other toggle stays set
// after it acts. A refusal or a failed append also keeps the toggle, and the
// next slow tick retries.
//
// Returns 1 after an append or an ABORT start, else 0.
func (a *Agent) clusterControlToggleWork(nowNs int64) int {
	if a.controlToggle == nil {
		return 0
	}
	toggle := a.controlToggle.Load()
	if a.moduleState == ModuleStateSuspended && toggle == int64(ControlToggleResume) {
		if !a.appendModuleStateAction(clusterwire.ClusterAction_RESUME, nowNs) {
			return 0
		}
		a.setModuleState(ModuleStateActive, moduleStateReasonControlResume)
		a.resetControlToggleFrom(ControlToggleResume)
		return 1
	}
	if a.moduleState != ModuleStateActive {
		return 0
	}
	switch toggle {
	case int64(ControlToggleSuspend):
		if !a.appendModuleStateAction(clusterwire.ClusterAction_SUSPEND, nowNs) {
			return 0
		}
		a.setModuleState(ModuleStateSuspended, moduleStateReasonControlSuspend)
		return 1
	case int64(ControlToggleSnapshot):
		if a.RequestSnapshot(nowNs) != nil {
			return 0
		}
		return 1
	case int64(ControlToggleStandbySnapshot):
		_, ok := a.logPublisher.AppendClusterAction(
			a.leadershipTermID,
			nowNs,
			clusterwire.ClusterAction_SNAPSHOT,
			clusterActionFlagsStandbySnapshot,
		)
		if !ok {
			return 0
		}
		a.resetControlToggleFrom(ControlToggleStandbySnapshot)
		return 1

	case int64(ControlToggleShutdown):
		if a.RequestSnapshot(nowNs) != nil {
			return 0
		}

		a.startClusterTermination()
		return 1

	case int64(ControlToggleAbort):
		if !a.startClusterTermination() {
			return 0
		}

		if a.termination == nil {
			a.terminateServices(moduleStateReasonControlAbort, nowNs)
		}

		return 1
	}
	return 0
}

func (a *Agent) resetControlToggleFrom(from ControlToggleState) {
	if a.controlToggle == nil {
		return
	}
	a.controlToggle.CompareAndSet(int64(from), int64(ControlToggleNeutral))
}
