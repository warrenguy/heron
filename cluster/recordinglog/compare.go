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

	aIsSnapshot := a.Type == EntryTypeSnapshot
	bIsSnapshot := b.Type == EntryTypeSnapshot
	if aIsSnapshot != bIsSnapshot {
		return !aIsSnapshot
	}
	if a.Type != b.Type {
		return a.Type < b.Type
	}

	if a.Type == EntryTypeTerm {
		return a.Index < b.Index
	}

	if a.LogPosition != b.LogPosition {
		return a.LogPosition < b.LogPosition
	}
	return a.ServiceID > b.ServiceID
}
