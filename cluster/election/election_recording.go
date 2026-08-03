package election

import "github.com/warrenguy/heron/cluster/recordinglog"

// RecordingLogCoherency is the recording-log write surface the election owns.
// Nil skips every write when the agent omits a recording log.
type RecordingLogCoherency interface {
	EnsureCoherent(recordinglog.EnsureCoherentParams) error
	CommitLogPosition(leadershipTermID, logPosition int64) error
	Force(recordinglog.FileSyncLevel) error
}

// RecordingLogLookup resolves the successor term entry for NewLeadershipTerm
// publishes. Nil (or a nil entry) selects the current-term defaults.
type RecordingLogLookup interface {
	FindTermEntry(logLeadershipTermID int64) *recordinglog.Entry
}

var (
	_ RecordingLogCoherency = (*recordinglog.Log)(nil)
	_ RecordingLogLookup    = (*recordinglog.Log)(nil)
)

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
		FileSyncLevel:              e.deps.RecordingLogFileSyncLevel,
	})
	if err != nil {
		return err
	}
	e.logLeadershipTermID = leadershipTermID
	return nil
}

// truncateRecordingLogTerm lowers the stored logPosition of leadershipTermID to
// logPosition and forces the write.
//
// Behaviour: writes through CommitLogPosition, which updates one field in place
// and skips the write when the stored position already equals logPosition.
// Appends no term. Does not advance the tracked log leadership term.
//
// A nil recording log is a no-op, matching EnsureRecordingLogCoherent. A
// NullValue leadershipTermID is a no-op, matching the upstream guard.
//
// Returns an error when the term is unknown (ErrUnknownLeadershipTermID), when
// the field write is short, or when the force fails.
func (e *Election) truncateRecordingLogTerm(leadershipTermID, logPosition int64) error {
	if leadershipTermID == NullValue || e.deps.RecordingLog == nil {
		return nil
	}
	if err := e.deps.RecordingLog.CommitLogPosition(leadershipTermID, logPosition); err != nil {
		return err
	}
	return e.deps.RecordingLog.Force(e.deps.RecordingLogFileSyncLevel)
}
