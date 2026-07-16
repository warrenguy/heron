//go:build testhooks

package election

import "github.com/warrenguy/heron/cluster/consensus"

// Cross-package test hooks. Values mirror export_test.go but live here because
// export_test.go helpers are not visible to external test packages.

var (
	InitialiseOnCreate          = initialiseOnCreate
	ScanForTemplateOffset       = scanForTemplateOffset
	EncodeFramedCandidateTerm   = encodeFramedCandidateTerm
	EncodeFramedNodeStateFooter = encodeFramedNodeStateFooter
	AlignRecordOffset           = alignRecordOffset
)

// SetStateForTest injects election state directly without invoking StateObserver.
//
// Use only to seed handler preconditions; observed transitions come from Election paths.
func SetStateForTest(e *Election, s ElectionState) { e.electionState = s }

func SetLogReplicationForTest(e *Election, lr LogReplication) { e.logReplication = lr }

// SetLogSessionIDForTest seeds the log session id without running AddLogPublication.
//
// Use only to preset leader replication preconditions; observed session ids come
// from Election paths.
func SetLogSessionIDForTest(e *Election, id int32) { e.logSessionID = id }

// IsLeaderStartupForTest exposes the leader startup flag for startup-flag
// oracle parity tests. Test-only; production paths set this from NewLeadershipTerm.
func IsLeaderStartupForTest(e *Election) bool { return e.isLeaderStartup }

func ElectionStateNameCount() int { return len(electionStateNames) }

func TimeOfLastUpdateNsForTest(e *Election) int64 { return e.timeOfLastUpdateNs }

func TimeOfLastCommitPositionUpdateNsForTest(e *Election) int64 {
	return e.timeOfLastCommitPositionUpdateNs
}

func MembersForTest(e *Election) []*Member { return e.clusterMembers }

func MemberForTest(e *Election, id int32) *Member { return e.memberByID[id] }

func NominationDeadlineNsForTest(e *Election) int64 { return e.nominationDeadlineNs }

func SetNominationDeadlineNsForTest(e *Election, ns int64) { e.nominationDeadlineNs = ns }

func CandidateTermIDForTest(e *Election) int64 { return e.candidateTermID }

func IsExtendedCanvassForTest(e *Election) bool { return e.isExtendedCanvass }

func LogLeadershipTermIDForTest(e *Election) int64 { return e.logLeadershipTermID }

// SetLogLeadershipTermIDForTest seeds the log leadership term without running
// installLeadershipTerm.
//
// Use only to restore canvass/vote log identity after a mid-run NewElection when
// cfg.LeadershipTermID alone is insufficient.
func SetLogLeadershipTermIDForTest(e *Election, id int64) { e.logLeadershipTermID = id }

func ReplicationForTest(e *Election) (leadershipTermID, stopPosition, termBaseLogPosition int64) {
	return e.replicationLeadershipTermID, e.replicationStopPosition, e.replicationTermBaseLogPosition
}

// ElectionSnapshot captures handler-visible state for drift and parity tests.
type ElectionSnapshot struct {
	State            ElectionState
	Role             consensus.Role
	CandidateTermID  int64
	LeadershipTermID int64
	MemberSnapshots  []MemberSnapshot
	PublishedCount   int
}

// MemberSnapshot captures per-member ballot state for parity comparison.
type MemberSnapshot struct {
	ID               int32
	LogPosition      int64
	LeadershipTermID int64
	CandidateTermID  int64
	Vote             *bool
	IsBallotSent     bool
}

func SnapshotForTest(e *Election, hostRole consensus.Role, pub *SbePublisher) ElectionSnapshot {
	members := make([]MemberSnapshot, 0, len(e.clusterMembers))
	for _, m := range e.clusterMembers {
		members = append(members, MemberSnapshot{
			ID:               m.ID,
			LogPosition:      m.LogPosition,
			LeadershipTermID: m.LeadershipTermID,
			CandidateTermID:  m.CandidateTermID,
			Vote:             m.Vote,
			IsBallotSent:     m.IsBallotSent,
		})
	}
	published := 0
	if pub != nil {
		published = len(pub.Sent)
	}
	return ElectionSnapshot{
		State:            e.electionState,
		Role:             hostRole,
		CandidateTermID:  e.candidateTermID,
		LeadershipTermID: e.leadershipTermID,
		MemberSnapshots:  members,
		PublishedCount:   published,
	}
}
