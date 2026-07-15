package election

import "github.com/warrenguy/heron/cluster/recordinglog"

// RecordingLogCoherency is the recording-log seam for term-adoption coherency.
// Nil is tolerated until the consensus module wires the real recording log.
type RecordingLogCoherency interface {
	EnsureCoherent(recordinglog.EnsureCoherentParams) error
}

// RecordingLogLookup resolves the successor term entry for NewLeadershipTerm
// publishes. Nil (or a nil entry) selects the current-term defaults.
type RecordingLogLookup interface {
	FindTermEntry(logLeadershipTermID int64) *recordinglog.Entry
}

// EnsureRecordingLogCoherent brings the recording log up to
// params.LeadershipTermID.
//
// A NullValue RecordingID means log replication never created a local
// recording for the adopted term, so there is nothing to make coherent
// regardless of LogPosition. A nil log is likewise a no-op. A NullValue
// TermBaseLogPosition falls back to InitialTermBaseLogPosition.
func EnsureRecordingLogCoherent(
	log RecordingLogCoherency,
	params recordinglog.EnsureCoherentParams,
) error {
	if params.RecordingID == NullValue || log == nil {
		return nil
	}
	if params.TermBaseLogPosition == NullValue {
		params.TermBaseLogPosition = params.InitialTermBaseLogPosition
	}
	return log.EnsureCoherent(params)
}

// commitRecordingLogTerm commits the term to the recording log, then advances
// the tracked log leadership term. Log join passes a NullValue logPosition for
// the still-open term; replication passes the replication stop position for
// the committed one.
func (e *Election) commitRecordingLogTerm(
	leadershipTermID, termBaseLogPosition, logPosition int64,
) error {
	err := EnsureRecordingLogCoherent(e.deps.RecordingLog, recordinglog.EnsureCoherentParams{
		RecordingID:                e.deps.Host.LocalLogRecordingID(),
		InitialLogLeadershipTermID: e.initialLogLeadershipTermID,
		InitialTermBaseLogPosition: e.initialTermBaseLogPosition,
		LeadershipTermID:           leadershipTermID,
		TermBaseLogPosition:        termBaseLogPosition,
		LogPosition:                logPosition,
		Timestamp:                  e.deps.EpochClock(),
	})
	if err != nil {
		return err
	}
	e.logLeadershipTermID = leadershipTermID
	return nil
}
