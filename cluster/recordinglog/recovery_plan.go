package recordinglog

import (
	"errors"
	"fmt"
	"slices"
)

// NullRecordingID is the sentinel for "no recording id" on caller-facing APIs.
// Same value as unexported nullValue and Aeron NULL_VALUE (-1).
const NullRecordingID int64 = nullValue

var (
	// ErrUnknownRecordingID reports a listRecording lookup for an unknown recording id.
	ErrUnknownRecordingID = errors.New("recordinglog: unknown recording id")

	// ErrSnapshotMissingForService reports selectRecoveryAnchor index walk failure.
	ErrSnapshotMissingForService = errors.New("recordinglog: snapshot missing for service")

	// ErrInvalidServiceCount reports a negative RecoveryPlanConfig.ServiceCount.
	ErrInvalidServiceCount = errors.New("recordinglog: service count must be non-negative")

	// ErrSnapshotServiceIDOutOfRange reports a snapshot group entry whose ServiceID is
	// outside [0, serviceCount).
	ErrSnapshotServiceIDOutOfRange = errors.New("recordinglog: snapshot service id out of range")
)

// Snapshot is one snapshot step in a RecoveryPlan output. It is a plan-facing
// DTO - not a second wire type. Populated only via snapshotFromEntry.
type Snapshot struct {
	RecordingID         int64
	LeadershipTermID    int64
	TermBaseLogPosition int64
	LogPosition         int64
	Timestamp           int64
	ServiceID           int32
}

// TermRef is the recording.log term entry subset carried in a recovery plan.
type TermRef struct {
	RecordingID         int64
	LeadershipTermID    int64
	TermBaseLogPosition int64
	LogPosition         int64 // NullPosition if uncommitted
}

// LogRecordingExtents is Archive listRecording geometry for log replay.
//
// Subset of RecordingDescriptorFields; not a full descriptor DTO.
//
// Fields consumed from listRecording: StartPosition, StopPosition, InitialTermID,
// TermBufferLength, MtuLength, SessionID.
//
// StopPosition may be NullPosition (-1) while the recording is still active.
type LogRecordingExtents struct {
	StartPosition    int64
	StopPosition     int64
	InitialTermID    int32
	TermBufferLength int32
	MtuLength        int32
	SessionID        int32
}

// RecoveryPlanLog joins index term metadata with Archive replay extents.
//
// Mirrors Java RecordingLog.Log fields; composed instead of one flat struct.
type RecoveryPlanLog struct {
	Term   TermRef
	Extent LogRecordingExtents // StartPosition may be overridden when snapshots exist
}

// RecoveryPlan is the cluster recovery state computed at startup.
type RecoveryPlan struct {
	// LastLeadershipTermID is the leadership term id from the selected term or
	// consensus-module snapshot.
	LastLeadershipTermID int64

	// LastTermBaseLogPosition is the term base log position from the selected term or
	// consensus-module snapshot.
	LastTermBaseLogPosition int64

	// AppendedLogPosition is the end of the recoverable log (snapshot logPosition or term
	// stop position).
	//
	// NullPosition (-1) when the term recording is still active.
	AppendedLogPosition int64

	// CommittedLogPosition is the last committed log position (snapshot or term logPosition).
	CommittedLogPosition int64

	// Snapshots is nil when empty, not []Snapshot{}.
	Snapshots []Snapshot

	// Log is nil when no recoverable term exists and no replicated recording id is set.
	//
	// An empty log with ReplicatedRecordingID still populates Log.
	Log *RecoveryPlanLog
}

// IsEmpty reports whether the plan holds no log term and no snapshots.
//
// Do not key emptiness on LastLeadershipTermID == NullRecordingID: computed
// empty plans carry that sentinel, but a zero-value RecoveryPlan has
// LastLeadershipTermID == 0 and is still empty.
func (p RecoveryPlan) IsEmpty() bool {
	return p.Log == nil && len(p.Snapshots) == 0
}

// TermRecordingID returns the log term recording id, or NullRecordingID when
// the plan has no log term.
func (p RecoveryPlan) TermRecordingID() int64 {
	if p.Log == nil {
		return NullRecordingID
	}
	return p.Log.Term.RecordingID
}

// SeedLogPosition returns the log position a restart replays from.
func (p RecoveryPlan) SeedLogPosition() int64 {
	if len(p.Snapshots) > 0 {
		return p.Snapshots[0].LogPosition
	}
	return 0
}

// AppendedOrCommittedPosition returns the appended log position, or the
// committed log position when the appended position is NullPosition.
//
// NullPosition means the term recording is still active: the append stop is
// unknown, so the committed position is the append floor.
func (p RecoveryPlan) AppendedOrCommittedPosition() int64 {
	if p.AppendedLogPosition != NullPosition {
		return p.AppendedLogPosition
	}
	return p.CommittedLogPosition
}

// RecoveryPlanConfig holds inputs for PlanRecovery.
type RecoveryPlanConfig struct {
	// ServiceCount is the number of clustered services (excluding the consensus module).
	ServiceCount int

	// ReplicatedRecordingID is the leader log recording when entries are empty but the
	// node is joining via replication.
	//
	// Use NullRecordingID when unused. The int64 zero value is recording id 0, not unused;
	// prefer NewRecoveryPlanConfig.
	ReplicatedRecordingID int64
}

// NewRecoveryPlanConfig returns cfg with ReplicatedRecordingID set to NullRecordingID.
func NewRecoveryPlanConfig(serviceCount int) RecoveryPlanConfig {
	return RecoveryPlanConfig{
		ServiceCount:          serviceCount,
		ReplicatedRecordingID: NullRecordingID,
	}
}

// ArchiveLister looks up recording extents.
//
// Production adapters use archive.NewLister, which wraps archive.Client.ListRecording;
// unit tests use map-backed fakes.
type ArchiveLister interface {
	ListRecording(recordingID int64) (LogRecordingExtents, error)
}

// MissingServiceSnapshotError carries structured context for selectRecoveryAnchor failures.
//
// Use errors.As for ServiceIndex, LeadershipTermID, and LogPosition; errors.Is for the sentinel.
type MissingServiceSnapshotError struct {
	// ServiceIndex is the 1-based comparator-sorted cache index (i+1) of the
	// consensus-module snapshot when serviceCount > i in selectRecoveryAnchor.
	//
	// Not Entry.Index (file-order ordinal), walkSteps, or the missing ServiceID.
	ServiceIndex     int
	LeadershipTermID int64
	LogPosition      int64
}

func recoveryAnchorErrorSuffix(leadershipTermID, logPosition int64) string {
	return fmt.Sprintf("(leadershipTermId=%d, logPosition=%d)", leadershipTermID, logPosition)
}

func (e *MissingServiceSnapshotError) Error() string {
	return fmt.Sprintf(
		"recordinglog: snapshot missing for service at index %d %s",
		e.ServiceIndex,
		recoveryAnchorErrorSuffix(e.LeadershipTermID, e.LogPosition),
	)
}

func (e *MissingServiceSnapshotError) Is(target error) bool {
	return target == ErrSnapshotMissingForService
}

// SnapshotServiceIDOutOfRangeError carries structured context when a snapshot group
// entry has ServiceID outside [0, serviceCount).
//
// Use errors.As for fields; errors.Is for ErrSnapshotServiceIDOutOfRange.
type SnapshotServiceIDOutOfRangeError struct {
	ServiceID        int32
	ServiceCount     int
	LeadershipTermID int64
	LogPosition      int64
}

func (e *SnapshotServiceIDOutOfRangeError) Error() string {
	return fmt.Sprintf(
		"recordinglog: snapshot service id %d out of range for serviceCount %d %s",
		e.ServiceID,
		e.ServiceCount,
		recoveryAnchorErrorSuffix(e.LeadershipTermID, e.LogPosition),
	)
}

func (e *SnapshotServiceIDOutOfRangeError) Is(target error) bool {
	return target == ErrSnapshotServiceIDOutOfRange
}

func snapshotFromEntry(e Entry) Snapshot {
	return Snapshot{
		RecordingID:         e.RecordingID,
		LeadershipTermID:    e.LeadershipTermID,
		TermBaseLogPosition: e.TermBaseLogPosition,
		LogPosition:         e.LogPosition,
		Timestamp:           e.Timestamp,
		ServiceID:           e.ServiceID,
	}
}

func termRefFromEntry(e Entry) TermRef {
	return TermRef{
		RecordingID:         e.RecordingID,
		LeadershipTermID:    e.LeadershipTermID,
		TermBaseLogPosition: e.TermBaseLogPosition,
		LogPosition:         e.LogPosition,
	}
}

// PlanRecovery builds a RecoveryPlan from the log cache and Archive extents.
//
// cfg.ServiceCount must be non-negative; ErrInvalidServiceCount otherwise.
//
// The ArchiveLister argument must be non-nil when the plan needs a ListRecording lookup: empty
// log with ReplicatedRecordingID set, or when recovery selects a term entry.
//
// ArchiveLister.ListRecording errors propagate; ErrUnknownRecordingID when the
// recording id is absent from the lister. selectRecoveryAnchor may return
// *MissingServiceSnapshotError (errors.Is ErrSnapshotMissingForService) or
// *SnapshotServiceIDOutOfRangeError (errors.Is ErrSnapshotServiceIDOutOfRange).
func (l *Log) PlanRecovery(
	archive ArchiveLister,
	cfg RecoveryPlanConfig,
) (RecoveryPlan, error) {
	if cfg.ServiceCount < 0 {
		return RecoveryPlan{}, ErrInvalidServiceCount
	}

	entries := l.entries
	var logRef *RecoveryPlanLog

	if len(entries) == 0 {
		if cfg.ReplicatedRecordingID != NullRecordingID {
			extent, err := archive.ListRecording(cfg.ReplicatedRecordingID)
			if err != nil {
				return RecoveryPlan{}, err
			}
			logRef = &RecoveryPlanLog{
				Term: TermRef{
					RecordingID:         cfg.ReplicatedRecordingID,
					LeadershipTermID:    NullRecordingID,
					TermBaseLogPosition: 0,
					LogPosition:         0,
				},
				Extent: extent,
			}
		}
		return recoveryPlanFromParts(nil, logRef), nil
	}

	sel, err := selectRecoveryAnchor(entries, cfg.ServiceCount)
	if err != nil {
		return RecoveryPlan{}, err
	}

	snapshots := sel.snapshots

	if sel.hasLog {
		entry := entries[sel.logIndex]
		extent, err := archive.ListRecording(entry.RecordingID)
		if err != nil {
			return RecoveryPlan{}, err
		}

		if len(snapshots) > 0 {
			extent.StartPosition = snapshots[0].LogPosition
		}

		logRef = &RecoveryPlanLog{
			Term:   termRefFromEntry(entry),
			Extent: extent,
		}
	}

	return recoveryPlanFromParts(snapshots, logRef), nil
}

type recoverySelection struct {
	snapshots     []Snapshot
	snapshotIndex int
	logIndex      int
	hasSnapshot   bool
	hasLog        bool
}

func selectRecoveryAnchor(entries []Entry, serviceCount int) (recoverySelection, error) {
	var sel recoverySelection
	var cm Snapshot
	serviceByID := make([]Snapshot, serviceCount)
	serviceFilled := make([]bool, serviceCount)
	walkSteps := 0
	walkDone := serviceCount == 0

	for i, entry := range slices.Backward(entries) {
		if !sel.hasLog && isValidTerm(entry) && entry.RecordingID != NullRecordingID {
			sel.logIndex, sel.hasLog = i, true
		}

		if !sel.hasSnapshot && isValidConsensusModuleSnapshot(entry) {
			cm = snapshotFromEntry(entry)
			sel.snapshotIndex, sel.hasSnapshot = i, true
			if serviceCount > i {
				return recoverySelection{}, &MissingServiceSnapshotError{
					ServiceIndex:     i + 1,
					LeadershipTermID: cm.LeadershipTermID,
					LogPosition:      cm.LogPosition,
				}
			}
		} else if sel.hasSnapshot && !walkDone {
			walkSteps++
			if walkSteps > serviceCount {
				walkDone = true
			} else if matchesConsensusModuleSnapshotGroup(entry, cm) {
				sid := int(entry.ServiceID)
				if sid < 0 || sid >= serviceCount {
					return recoverySelection{}, &SnapshotServiceIDOutOfRangeError{
						ServiceID:        entry.ServiceID,
						ServiceCount:     serviceCount,
						LeadershipTermID: cm.LeadershipTermID,
						LogPosition:      cm.LogPosition,
					}
				}
				serviceByID[sid] = snapshotFromEntry(entry)
				serviceFilled[sid] = true
			}
		}

		if sel.hasSnapshot && sel.hasLog && walkDone {
			break
		}
	}

	if !sel.hasSnapshot {
		return sel, nil
	}

	out := make([]Snapshot, 1, 1+serviceCount)
	out[0] = cm
	for sid := range serviceCount {
		if serviceFilled[sid] {
			out = append(out, serviceByID[sid])
		}
	}
	sel.snapshots = out
	return sel, nil
}

// matchesConsensusModuleSnapshotGroup reports whether entry belongs to the consensus-
// module snapshot group walk in selectRecoveryAnchor.
//
// RecordingLog.addSnapshots matches raw ENTRY_TYPE_SNAPSHOT without re-checking
// Valid; do not use isValidSnapshot here.
func matchesConsensusModuleSnapshotGroup(entry Entry, cm Snapshot) bool {
	return entry.Type == EntryTypeSnapshot &&
		entry.LeadershipTermID == cm.LeadershipTermID &&
		entry.LogPosition == cm.LogPosition
}

func recoveryPlanFromParts(snapshots []Snapshot, logRef *RecoveryPlanLog) RecoveryPlan {
	plan := RecoveryPlan{
		LastLeadershipTermID:    NullRecordingID,
		LastTermBaseLogPosition: 0,
		AppendedLogPosition:     0,
		CommittedLogPosition:    0,
		Snapshots:               snapshots,
		Log:                     logRef,
	}

	if logRef != nil {
		plan.LastLeadershipTermID = logRef.Term.LeadershipTermID
		plan.LastTermBaseLogPosition = logRef.Term.TermBaseLogPosition
		plan.AppendedLogPosition = logRef.Extent.StopPosition
		switch {
		case logRef.Term.LogPosition != NullPosition:
			plan.CommittedLogPosition = logRef.Term.LogPosition
		case len(snapshots) > 0:
			plan.CommittedLogPosition = snapshots[0].LogPosition
		}
	} else if len(snapshots) > 0 {
		s := snapshots[0]
		plan.LastLeadershipTermID = s.LeadershipTermID
		plan.LastTermBaseLogPosition = s.TermBaseLogPosition
		plan.AppendedLogPosition = s.LogPosition
		plan.CommittedLogPosition = s.LogPosition
	}

	return plan
}
