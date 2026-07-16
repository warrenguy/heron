package election

import (
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// StateObserver receives each applied election state transition. Production
// wiring updates the CLUSTER_ELECTION_STATE_TYPE_ID counter; tests record
// transitions.
type StateObserver interface {
	OnElectionStateChange(ElectionState)
}

type ElectionDeps struct {
	Handler   consensus.AgentHandler
	Host      ElectionHost
	Publisher Publisher

	NanoClock  func() int64
	EpochClock func() int64

	// NominationRand seeds the randomized nomination back-off. Callers must
	// return a value in [0, 1) (e.g. rand.Float64); it scales half the election
	// timeout. Nil yields a zero delay, which keeps unit and sim scenarios
	// deterministic.
	NominationRand func() float64

	NodeState  CandidateTermStore
	Members    []*Member
	ThisMember *Member

	// RecordingLog and RecordingLogLookup are the recording-log seams for term
	// coherency and successor-term lookup. Both may be nil when the agent
	// omits a recording log: nil RecordingLog skips writes; nil
	// RecordingLogLookup publishes current-term defaults.
	//
	// Production and tests should set both to the same *recordinglog.Log.
	// RecordingLogFileSyncLevel is zero (FileSyncNone) by default; production
	// agents must set FileSyncData or FileSyncFull.
	RecordingLog              RecordingLogCoherency
	RecordingLogLookup        RecordingLogLookup
	RecordingLogFileSyncLevel recordinglog.FileSyncLevel

	StateObserver StateObserver
}
