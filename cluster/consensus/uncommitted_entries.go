package consensus

import (
	"slices"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// uncommittedTimer is one TimerEvent the leader appended above commit.
type uncommittedTimer struct {
	// appendPosition is the claim end position of the TimerEvent.
	appendPosition int64
	correlationID  int64
}

// uncommittedModuleState holds the module state before one SUSPEND or RESUME
// action the leader appended above commit.
type uncommittedModuleState struct {
	// appendPosition is the frame end of the action.
	appendPosition int64
	previous       ModuleState
}

func (a *Agent) appendModuleStateAction(action clusterwire.ClusterAction, nowNs int64) bool {
	pos, ok := a.logPublisher.AppendClusterAction(a.leadershipTermID, nowNs, action, 0)
	if ok {
		a.uncommittedModuleStates = append(a.uncommittedModuleStates, uncommittedModuleState{
			appendPosition: pos,
			previous:       a.moduleState,
		})
	}
	return ok
}

// sweepUncommittedEntriesTo drops each tracked entry at or below
// quorumPosition. UpdateLeaderPosition calls it on each gate pass, after
// ProposeMaxCommit.
//
// Each list holds entries in append order, so each sweep drops a prefix.
//
// Concurrency: invoker-only.
func (a *Agent) sweepUncommittedEntriesTo(quorumPosition int64) {
	n := 0
	for n < len(a.uncommittedTimers) && a.uncommittedTimers[n].appendPosition <= quorumPosition {
		n++
	}
	a.uncommittedTimers = slices.Delete(a.uncommittedTimers, 0, n)
	a.sessions.sweepUncommittedCloses(quorumPosition)
	a.uncommittedModuleStates = slices.Delete(
		a.uncommittedModuleStates,
		0,
		a.uncommittedModuleStatesThrough(quorumPosition),
	)
}

func (a *Agent) uncommittedModuleStatesThrough(logPosition int64) int {
	states := a.uncommittedModuleStates
	n := 0
	for n < len(states) && states[n].appendPosition <= logPosition {
		n++
	}
	return n
}

// restoreUncommittedEntries rolls back each tracked entry above logPosition.
// Then empties each list.
//
// Behaviour:
//   - Schedules each timer again at deadline 0. The schedule replaces a live
//     timer for the id. A refused schedule marks timerFailure once. Leaves
//     the expired timer counts.
//   - Re-adds each closed session as closing (restoreUncommittedCloses).
//     clearSessionsAfter then drops the ones opened above logPosition.
//   - Restores the module state from before the first module state action
//     above logPosition. A CLOSED module stays CLOSED.
//
// A later log read of a surviving frame cancels the timer or removes the
// session. When the frame is lost and this member leads next, timerWork
// fires the timer again and sessionCloseWork appends the close. Under another
// leader, that leader's own frame settles the entry.
//
// Concurrency: invoker-only.
func (a *Agent) restoreUncommittedEntries(logPosition int64) {
	for _, timer := range a.uncommittedTimers {
		if timer.appendPosition > logPosition {
			a.scheduleTimerOrMarkFailure(timer.correlationID, 0)
		}
	}
	a.uncommittedTimers = a.uncommittedTimers[:0]
	a.sessions.restoreUncommittedCloses(logPosition)
	rolledBack := a.uncommittedModuleStates[a.uncommittedModuleStatesThrough(logPosition):]
	if len(rolledBack) > 0 {
		a.setModuleState(rolledBack[0].previous, moduleStateReasonRollback)
	}
	a.uncommittedModuleStates = a.uncommittedModuleStates[:0]
}
