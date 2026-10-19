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

// clearUncommittedEntries empties both lists. teardownForNewLeadership calls
// it, so no entry of a lost term outlives the prepare.
//
// Both clears keep the capacity. The close clear nils the vacated slots, so
// the held sessions can be freed.
//
// Concurrency: invoker-only.
func (a *Agent) clearUncommittedEntries() {
	a.uncommittedTimers = a.uncommittedTimers[:0]
	a.sessions.clearUncommittedCloses()
}
