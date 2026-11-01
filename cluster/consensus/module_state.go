package consensus

import "fmt"

// ModuleState is the consensus module state. Codes match upstream
// ConsensusModule.State.
//
// [ClusterConsensusModuleStateCounterTypeID] carries the code.
type ModuleState int32

const (
	ModuleStateInit        ModuleState = iota // INIT, code 0
	ModuleStateActive                         // ACTIVE, code 1
	ModuleStateSuspended                      // SUSPENDED, code 2
	ModuleStateSnapshot                       // SNAPSHOT, code 3
	ModuleStateQuitting                       // QUITTING, code 4
	ModuleStateTerminating                    // TERMINATING, code 5
	ModuleStateClosed                         // CLOSED, code 6
)

var moduleStateNames = [...]string{
	"INIT",
	"ACTIVE",
	"SUSPENDED",
	"SNAPSHOT",
	"QUITTING",
	"TERMINATING",
	"CLOSED",
}

const (
	moduleStateReasonSnapshotAction      = "snapshot action"
	moduleStateReasonSnapshotComplete    = "snapshot complete"
	moduleStateReasonSnapshotCancelled   = "snapshot cancelled"
	moduleStateReasonSnapshotWaitRetired = "snapshot wait retired"
	moduleStateReasonServiceAckFailure   = "service ack failure"
	moduleStateReasonClosed              = "closed"
)

func (s ModuleState) String() string {
	if s < 0 || int(s) >= len(moduleStateNames) {
		return fmt.Sprintf("ModuleState(%d)", int32(s))
	}
	return moduleStateNames[s]
}

func (s ModuleState) Code() int32 { return int32(s) }

// Callers must not use the result when err != nil.
func ModuleStateFromCode(code int64) (ModuleState, error) {
	if code < 0 || code >= int64(len(moduleStateNames)) {
		return 0, fmt.Errorf("consensus: invalid module state counter code: %d", code)
	}
	return ModuleState(code), nil
}

// ModuleStateCounter stores the module state for counter readers.
//
// Concurrency: invoker-only.
type ModuleStateCounter interface {
	// Stores the code with release order.
	StoreModuleState(state ModuleState)
}

func (a *Agent) ModuleState() ModuleState { return a.moduleState }

// SetModuleStateCounter sets or clears the module state counter.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetModuleStateCounter(counter ModuleStateCounter) {
	a.moduleStateCounter = counter
	if counter != nil {
		counter.StoreModuleState(a.moduleState)
	}
}

// MarkClosed moves the module to CLOSED. Idempotent.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) MarkClosed() {
	a.setModuleState(ModuleStateClosed, moduleStateReasonClosed)
}

func (a *Agent) setModuleState(newState ModuleState, reason string) {
	if newState == a.moduleState || a.moduleState == ModuleStateClosed {
		return
	}
	noteModuleStateChange(a.moduleState, newState, reason)
	a.moduleState = newState
	if a.moduleStateCounter != nil {
		a.moduleStateCounter.StoreModuleState(newState)
	}
}

// isSnapshotInProgress reports a take in flight, or a queued snapshot wait
// that can still pop. A wait can pop only while serviceAckFailure is nil.
func (a *Agent) isSnapshotInProgress() bool {
	if a.snapshotTake != nil {
		return true
	}
	return a.snapshotWaitPosition != 0 && a.serviceAckFailure == nil
}

func (a *Agent) endSnapshotState(reason string) {
	if a.moduleState != ModuleStateSnapshot || a.isSnapshotInProgress() {
		return
	}
	a.setModuleState(ModuleStateActive, reason)
}

func (a *Agent) isLeaderHeld() bool {
	return a.role == RoleLeader && a.moduleState != ModuleStateActive
}

func (a *Agent) isActiveOrSuspended() bool {
	return a.moduleState == ModuleStateActive || a.moduleState == ModuleStateSuspended
}
