package consensus

import "slices"

// uncommittedTimer is one TimerEvent the leader appended above commit.
type uncommittedTimer struct {
	// appendPosition is the claim end position of the TimerEvent.
	appendPosition int64
	correlationID  int64
}

// sweepUncommittedEntriesTo drops each tracked TimerEvent and
// SessionCloseEvent at or below quorumPosition. UpdateLeaderPosition calls it
// on each gate pass, after ProposeMaxCommit.
//
// Both lists hold entries in append order. The sweeps delete a prefix in
// place and keep the capacity.
//
// Concurrency: invoker-only.
func (a *Agent) sweepUncommittedEntriesTo(quorumPosition int64) {
	n := 0
	for n < len(a.uncommittedTimers) && a.uncommittedTimers[n].appendPosition <= quorumPosition {
		n++
	}
	a.uncommittedTimers = slices.Delete(a.uncommittedTimers, 0, n)
	a.sessions.sweepUncommittedCloses(quorumPosition)
}

// restoreUncommittedEntries rolls back each tracked TimerEvent and
// SessionCloseEvent above logPosition. Then empties both lists.
//
// Behaviour:
//   - Schedules each timer again at deadline 0. The schedule replaces a live
//     timer for the id. A refused schedule marks timerFailure once. Leaves
//     the expired timer counts.
//   - Re-adds each closed session as closing (restoreUncommittedCloses).
//     clearSessionsAfter then drops the ones opened above logPosition.
//   - Both lists keep their capacity.
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
}
