package recordinglog

import "slices"

func compareEntries(a, b Entry) int {
	if less(a, b) {
		return -1
	}
	if less(b, a) {
		return 1
	}
	return 0
}

func sortEntries(entries []Entry) {
	slices.SortStableFunc(entries, compareEntries)
}

func less(a, b Entry) bool {
	if a.LeadershipTermID != b.LeadershipTermID {
		return a.LeadershipTermID < b.LeadershipTermID
	}
	if a.TermBaseLogPosition != b.TermBaseLogPosition {
		return a.TermBaseLogPosition < b.TermBaseLogPosition
	}

	aIsTerm := a.Type == EntryTypeTerm
	bIsTerm := b.Type == EntryTypeTerm
	if aIsTerm != bIsTerm {
		return aIsTerm
	}
	if aIsTerm {
		return a.Index < b.Index
	}

	// Snapshots at one log position sort together. The standby snapshot
	// sorts first, then service IDs in descending order.
	if a.LogPosition != b.LogPosition {
		return a.LogPosition < b.LogPosition
	}
	if a.Type != b.Type {
		return a.Type > b.Type
	}
	return a.ServiceID > b.ServiceID
}
