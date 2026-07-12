//go:build !testhooks

package election

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

func ElectionStateNameCount() int { return len(electionStateNames) }

func TimeOfLastUpdateNsForTest(e *Election) int64 { return e.timeOfLastUpdateNs }

func TimeOfLastCommitPositionUpdateNsForTest(e *Election) int64 {
	return e.timeOfLastCommitPositionUpdateNs
}
