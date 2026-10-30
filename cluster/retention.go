package cluster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

var (
	// ErrNoSnapshotToRetain reports that the recording log does not hold a
	// snapshot group to keep.
	ErrNoSnapshotToRetain = errors.New(
		"cluster: recording log does not hold a snapshot group to keep",
	)

	// ErrIncompleteSnapshotGroup reports a kept snapshot group that recovery
	// cannot load:
	//   - the plan holds the group without a log term
	//   - the group has fewer rows than the service count requires
	//   - a row in the group is invalid
	//   - a kept recording is unknown or empty
	ErrIncompleteSnapshotGroup = errors.New("cluster: snapshot group is incomplete")

	// ErrSnapshotRecordingActive reports an active snapshot recording. Stop
	// the recording before a later RetainLatestSnapshot call.
	ErrSnapshotRecordingActive = errors.New(
		"cluster: snapshot recording is still active",
	)
)

// SnapshotRetentionArchive is the archive API [RetainLatestSnapshot] uses.
// [*archive.Client] satisfies the interface.
type SnapshotRetentionArchive interface {
	archive.RecordingLister
	TruncateRecording(
		ctx context.Context,
		recordingID int64,
		position int64,
	) (int64, error)
}

var _ SnapshotRetentionArchive = (*archive.Client)(nil)

// SnapshotRetentionConfig holds inputs for [RetainLatestSnapshot].
type SnapshotRetentionConfig struct {
	// ClusterDir is the member cluster directory. The directory must exist.
	ClusterDir string

	// ServiceCount is the member service count. A negative count returns an
	// error.
	ServiceCount int

	// RecordingLogFileSyncLevel selects the Force level after the snapshot
	// marks. Zero selects [recordinglog.FileSyncData]. The other allowed
	// value is [recordinglog.FileSyncFull]. Any other value returns an error.
	RecordingLogFileSyncLevel recordinglog.FileSyncLevel

	// Archive is the client for the term recording and the snapshot
	// recordings. The caller owns Archive.
	//
	// Each kept snapshot recording must be in the archive.
	Archive SnapshotRetentionArchive
}

// SnapshotRetentionResult reports one [RetainLatestSnapshot] call.
//
// LeadershipTermID and LogPosition name the kept group after the group
// checks pass, even when the call does not change a file. Both fields stay
// zero on a refusal before the checks. Counts and id lists cover only this
// call.
type SnapshotRetentionResult struct {
	// LeadershipTermID is the kept group's term.
	LeadershipTermID int64

	// LogPosition is the kept group's log position.
	LogPosition int64

	// InvalidatedEntries is the count of entries this call marked.
	InvalidatedEntries int

	// DroppedEntries is the count of invalid entries Compact dropped.
	DroppedEntries int

	// TruncatedRecordingIDs lists recordings this call truncated to the
	// start position.
	TruncatedRecordingIDs []int64

	// SkippedRecordingIDs lists candidates this call did not truncate:
	//   - unknown recording
	//   - already empty
	//   - different stream
	SkippedRecordingIDs []int64
}

// RetainLatestSnapshot keeps the newest valid snapshot group in a stopped
// member's recording log. It truncates each superseded snapshot recording
// to its start position. Truncation skips the term recording.
//
// A live [OpenRecovery] holds cluster.lock, so the call refuses that
// member. The lock open creates cluster.lock when the file is absent.
// The cluster directory and recording.log must already exist.
//
// The call marks every valid SNAPSHOT row whose term or log position
// differs from the kept group. TERM and STANDBY_SNAPSHOT rows stay valid.
// A snapshot service id outside -1 to ServiceCount-1 returns
// [recordinglog.ErrSnapshotServiceIDOutOfRange]. A refusal before the mark
// does not change recording.log. After Force succeeds, a later error leaves
// the marks in the file. Compact does not run after that error.
//
// ctx bounds each archive list and each truncate. The lock wait and the
// file writes do not read ctx.
//
// Concurrency: blocks the calling goroutine. cluster.lock keeps the
// recording log single-writer.
//
// Ownership: does not own cfg.Archive. Owns the lock and the recording log
// until return.
func RetainLatestSnapshot(
	ctx context.Context,
	cfg SnapshotRetentionConfig,
) (_ SnapshotRetentionResult, err error) {
	level, err := validateSnapshotRetentionConfig(cfg)
	if err != nil {
		return SnapshotRetentionResult{}, err
	}

	lockFile, err := openClusterLock(cfg.ClusterDir)
	if err != nil {
		return SnapshotRetentionResult{}, err
	}
	defer func() { _ = releaseClusterLock(lockFile) }()

	log, err := recordinglog.Open(cfg.ClusterDir, false)
	if err != nil {
		return SnapshotRetentionResult{}, fmt.Errorf(
			"cluster: open recording log in %s: %w",
			cfg.ClusterDir,
			err,
		)
	}

	// Close before the unlock. A later defer runs first, so a panic in
	// retainLatestSnapshot still closes the log before the unlock.
	defer func() {
		closeErr := log.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf(
				"cluster: close recording log in %s: %w",
				cfg.ClusterDir,
				closeErr,
			)
		}
	}()

	return retainLatestSnapshot(ctx, cfg, log, level)
}

func validateSnapshotRetentionConfig(
	cfg SnapshotRetentionConfig,
) (recordinglog.FileSyncLevel, error) {
	if cfg.ClusterDir == "" {
		return 0, errors.New("cluster: retain latest snapshot: ClusterDir is required")
	}
	if cfg.ServiceCount < 0 {
		return 0, errors.New(
			"cluster: retain latest snapshot: ServiceCount must not be negative",
		)
	}
	if cfg.Archive == nil {
		return 0, errors.New("cluster: retain latest snapshot: Archive is required")
	}

	// FileSyncNone is zero. validateFileSyncLevel rejects it, so an unset
	// level becomes FileSyncData before the check.
	level := cfg.RecordingLogFileSyncLevel
	if level == recordinglog.FileSyncNone {
		level = recordinglog.FileSyncData
	}
	if err := validateFileSyncLevel(level, "RecordingLogFileSyncLevel"); err != nil {
		return 0, err
	}
	return level, nil
}

func retainLatestSnapshot(
	ctx context.Context,
	cfg SnapshotRetentionConfig,
	log *recordinglog.Log,
	level recordinglog.FileSyncLevel,
) (SnapshotRetentionResult, error) {
	var result SnapshotRetentionResult
	plan, err := log.PlanRecovery(
		archive.NewLister(ctx, cfg.Archive),
		recordinglog.NewRecoveryPlanConfig(cfg.ServiceCount),
	)
	if err != nil {
		return result, fmt.Errorf(
			"cluster: plan recovery from %s: %w",
			cfg.ClusterDir,
			err,
		)
	}
	if err := checkRetainedGroup(cfg, log.Entries(), plan); err != nil {
		return result, err
	}

	head := plan.Snapshots[0]
	result.LeadershipTermID = head.LeadershipTermID
	result.LogPosition = head.LogPosition

	streams, err := listKeptRecordings(ctx, cfg.Archive, plan)
	if err != nil {
		return result, err
	}

	marked, err := log.InvalidateEntriesBefore(math.MaxInt64)
	result.InvalidatedEntries = marked
	if err != nil {
		return result, fmt.Errorf(
			"cluster: invalidate snapshot entries in %s: %w",
			cfg.ClusterDir,
			err,
		)
	}
	if err := log.Force(level); err != nil {
		return result, fmt.Errorf(
			"cluster: force recording log in %s: %w",
			cfg.ClusterDir,
			err,
		)
	}

	if err := truncateSuperseded(
		ctx,
		cfg.Archive,
		collectCandidates(log.Entries(), log.TermRecordingID()),
		streams,
		&result,
	); err != nil {
		return result, err
	}

	dropped, err := log.Compact()
	result.DroppedEntries = dropped
	if err != nil {
		return result, fmt.Errorf(
			"cluster: compact recording log in %s: %w",
			cfg.ClusterDir,
			err,
		)
	}
	return result, nil
}

func checkRetainedGroup(
	cfg SnapshotRetentionConfig,
	entries []recordinglog.Entry,
	plan recordinglog.RecoveryPlan,
) error {
	if len(plan.Snapshots) == 0 {
		return ErrNoSnapshotToRetain
	}
	head := plan.Snapshots[0]
	if plan.Log == nil {
		return fmt.Errorf(
			"%w: no log term for the group at term %d position %d",
			ErrIncompleteSnapshotGroup,
			head.LeadershipTermID,
			head.LogPosition,
		)
	}
	if ids := missingServiceSnapshots(plan, cfg.ServiceCount); len(ids) > 0 {
		return errIncompleteSnapshotGroup(head.LeadershipTermID, head.LogPosition, ids)
	}
	if ids := unmatchedSnapshotServiceIDs(entries, plan.Snapshots); len(ids) > 0 {
		return errIncompleteSnapshotGroup(head.LeadershipTermID, head.LogPosition, ids)
	}
	entry, outOfRange := outOfRangeSnapshot(entries, cfg.ServiceCount)
	if !outOfRange {
		return nil
	}
	return fmt.Errorf(
		"cluster: snapshot row out of range in %s: %w",
		cfg.ClusterDir,
		&recordinglog.SnapshotServiceIDOutOfRangeError{
			ServiceID:        entry.ServiceID,
			ServiceCount:     cfg.ServiceCount,
			LeadershipTermID: entry.LeadershipTermID,
			LogPosition:      entry.LogPosition,
		},
	)
}

func unmatchedSnapshotServiceIDs(
	entries []recordinglog.Entry,
	snapshots []recordinglog.Snapshot,
) []int32 {
	var ids []int32
	for _, snap := range snapshots {
		if !validSnapshotMatch(entries, snap) {
			ids = append(ids, snap.ServiceID)
		}
	}
	return ids
}

func validSnapshotMatch(entries []recordinglog.Entry, snap recordinglog.Snapshot) bool {
	for _, entry := range entries {
		if entry.Valid &&
			entry.Type == recordinglog.EntryTypeSnapshot &&
			entry.RecordingID == snap.RecordingID &&
			entry.LeadershipTermID == snap.LeadershipTermID &&
			entry.LogPosition == snap.LogPosition &&
			entry.ServiceID == snap.ServiceID {
			return true
		}
	}
	return false
}

func outOfRangeSnapshot(
	entries []recordinglog.Entry,
	serviceCount int,
) (recordinglog.Entry, bool) {
	for _, entry := range entries {
		if entry.Type != recordinglog.EntryTypeSnapshot {
			continue
		}
		sid := int(entry.ServiceID)
		if sid < int(recordinglog.ConsensusModuleServiceID) || sid > serviceCount-1 {
			return entry, true
		}
	}
	return recordinglog.Entry{}, false
}

func listKeptRecordings(
	ctx context.Context,
	client SnapshotRetentionArchive,
	plan recordinglog.RecoveryPlan,
) (map[int32]int32, error) {
	streams := make(map[int32]int32, len(plan.Snapshots))
	for _, snap := range plan.Snapshots {
		fields, err := listSnapshotRecording(ctx, client, snap.RecordingID)
		if err != nil {
			if unknownRecording(err) {
				return nil, errKeptRecording(snap.RecordingID, "unknown recording")
			}
			return nil, err
		}
		if fields.StopPosition == recordinglog.NullPosition {
			return nil, errActiveRecordings([]int64{snap.RecordingID})
		}
		if fields.StopPosition == fields.StartPosition {
			return nil, errKeptRecording(snap.RecordingID, "empty recording")
		}
		streams[snap.ServiceID] = fields.StreamID
	}
	return streams, nil
}

type snapshotCandidate struct {
	recordingID int64
	serviceID   int32
}

// collectCandidates lists invalid snapshot rows in recording id order. It
// skips a recording that a valid entry or the term names. The kept group
// rows stay valid, so the skip covers each kept recording.
func collectCandidates(
	entries []recordinglog.Entry,
	termRecordingID int64,
) []snapshotCandidate {
	keep := map[int64]struct{}{termRecordingID: {}}
	for _, entry := range entries {
		if entry.Valid {
			keep[entry.RecordingID] = struct{}{}
		}
	}

	var candidates []snapshotCandidate
	for _, entry := range entries {
		if entry.Valid || entry.Type != recordinglog.EntryTypeSnapshot {
			continue
		}
		if _, ok := keep[entry.RecordingID]; ok {
			continue
		}
		keep[entry.RecordingID] = struct{}{}
		candidates = append(candidates, snapshotCandidate{
			recordingID: entry.RecordingID,
			serviceID:   entry.ServiceID,
		})
	}
	slices.SortFunc(candidates, func(a, b snapshotCandidate) int {
		return cmp.Compare(a.recordingID, b.recordingID)
	})
	return candidates
}

func truncateSuperseded(
	ctx context.Context,
	client SnapshotRetentionArchive,
	candidates []snapshotCandidate,
	streams map[int32]int32,
	result *SnapshotRetentionResult,
) error {
	var active []int64
	for _, candidate := range candidates {
		skip := func() {
			result.SkippedRecordingIDs = append(
				result.SkippedRecordingIDs,
				candidate.recordingID,
			)
		}
		fields, err := listSnapshotRecording(ctx, client, candidate.recordingID)
		if err != nil {
			if unknownRecording(err) {
				skip()
				continue
			}
			return err
		}
		keptStream, ok := streams[candidate.serviceID]
		if !ok || fields.StreamID != keptStream {
			skip()
			continue
		}
		if fields.StopPosition == recordinglog.NullPosition {
			active = append(active, candidate.recordingID)
			continue
		}
		if fields.StopPosition == fields.StartPosition {
			skip()
			continue
		}
		if _, err := client.TruncateRecording(
			ctx,
			candidate.recordingID,
			fields.StartPosition,
		); err != nil {
			return fmt.Errorf(
				"cluster: truncate snapshot recording %d: %w",
				candidate.recordingID,
				err,
			)
		}
		result.TruncatedRecordingIDs = append(
			result.TruncatedRecordingIDs,
			candidate.recordingID,
		)
	}
	if len(active) > 0 {
		return errActiveRecordings(active)
	}
	return nil
}

func listSnapshotRecording(
	ctx context.Context,
	client SnapshotRetentionArchive,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	fields, err := client.ListRecording(ctx, recordingID)
	if err == nil || unknownRecording(err) {
		return fields, err
	}
	return codecs.RecordingDescriptorFields{}, fmt.Errorf(
		"cluster: list snapshot recording %d: %w",
		recordingID,
		err,
	)
}

func unknownRecording(err error) bool {
	code, ok := archive.ExceptionCode(err)
	return ok && code == archive.ErrorUnknownRecording
}

func errIncompleteSnapshotGroup(termID, logPosition int64, serviceIDs []int32) error {
	label := "services"
	if len(serviceIDs) == 1 {
		label = "service"
	}
	return fmt.Errorf(
		"%w: %s %s at term %d position %d",
		ErrIncompleteSnapshotGroup,
		label,
		formatIDs(serviceIDs),
		termID,
		logPosition,
	)
}

func errKeptRecording(id int64, reason string) error {
	return fmt.Errorf("%w: recording %d: %s", ErrIncompleteSnapshotGroup, id, reason)
}

func errActiveRecordings(ids []int64) error {
	return fmt.Errorf("%w: %s", ErrSnapshotRecordingActive, formatIDs(ids))
}

func formatIDs[T ~int32 | ~int64](ids []T) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(int64(id), 10)
	}
	return strings.Join(parts, ", ")
}
