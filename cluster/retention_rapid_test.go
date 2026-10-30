//go:build rapid

package cluster_test

import (
	"context"
	"errors"
	"maps"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestRetainLatestSnapshot_model_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		serviceCount := rapid.IntRange(0, 2).Draw(rt, "serviceCount")
		dir, archive := newRetentionDir(t)
		log := createLog(t, dir)
		const termRecordingID int64 = 10
		serviceByID := drawRetentionHistory(rt, log, serviceCount, termRecordingID)
		if rapid.Bool().Draw(rt, "invalidate") {
			_, err := log.InvalidateLatestSnapshot()
			require.NoError(rt, err)
		}
		entries := slices.Clone(log.Entries())
		require.NoError(rt, log.Close())

		applyRetentionStates(rt, archive, entries, serviceByID)
		before := readRecordingLog(rt, dir)
		planBefore, planErr := openRetentionPlan(rt, dir, archive, serviceCount)

		fault := rapid.SampledFrom([]retentionFault{
			faultNone,
			faultTruncate,
			faultContext,
		}).Draw(rt, "fault")
		faultCall := rapid.IntRange(0, 8).Draw(rt, "faultCall")
		runRetentionFault(
			rt,
			dir,
			archive,
			serviceCount,
			termRecordingID,
			fault,
			faultCall,
			before,
			planBefore,
			planErr,
		)

		archive.truncateErr = nil
		stopActiveRetentionRecordings(archive)
		planNow, openErr := openRetentionPlan(rt, dir, archive, serviceCount)
		refusal := retentionRefusal(rt, dir, archive, serviceCount, planNow, openErr)
		if planErr == nil {
			require.NoError(rt, openErr)
			require.Equal(
				rt,
				planWithTermStop(planBefore, archive, termRecordingID),
				planNow,
			)
		}

		truncatesBefore := len(archive.truncates)
		recorded := maps.Clone(archive.recordings)
		entriesNow := reloadEntries(rt, dir)

		// sameStreamSuperseded reads the pre-run entries. A completed
		// RetainLatestSnapshot drops invalid rows from the log.
		var superseded []int64
		if refusal == nil {
			require.NoError(rt, planErr)
			superseded = sameStreamSuperseded(
				entries,
				planBefore,
				termRecordingID,
				archive,
			)
		}

		run2, err := retain(rt, dir, serviceCount, archive)
		assertRetentionRun(
			rt,
			dir,
			archive,
			serviceCount,
			termRecordingID,
			truncatesBefore,
			entriesNow,
			planBefore,
			planErr,
			before,
			refusal,
			err,
		)
		run2Truncates := slices.Clone(archive.truncates[truncatesBefore:])

		truncatesBefore = len(archive.truncates)
		if refusal != nil {
			_, err = retain(rt, dir, serviceCount, archive)
			assertRetentionRefused(
				rt,
				dir,
				archive,
				truncatesBefore,
				before,
				refusal,
				err,
			)
			require.Equal(rt, recorded, archive.recordings)
			return
		}

		require.NoError(rt, err)
		want := slices.DeleteFunc(slices.Clone(superseded), func(id int64) bool {
			return recorded[id].StopPosition == recorded[id].StartPosition
		})
		require.ElementsMatch(rt, want, run2Truncates)
		require.ElementsMatch(rt, want, run2.TruncatedRecordingIDs)
		for _, id := range superseded {
			fields := archive.recording(rt, id)
			require.Equal(rt, fields.StartPosition, fields.StopPosition, "recording %d", id)
		}
		for _, entry := range reloadEntries(rt, dir) {
			require.True(rt, entry.Valid)
		}
		kept := planBefore.Snapshots[0]
		logBefore := readRecordingLog(rt, dir)
		bakBefore := readOptional(rt, backupPath(dir))
		result, err := retain(rt, dir, serviceCount, archive)
		require.NoError(rt, err)
		require.Equal(rt, kept.LeadershipTermID, result.LeadershipTermID)
		require.Equal(rt, kept.LogPosition, result.LogPosition)
		require.Zero(rt, result.InvalidatedEntries)
		require.Zero(rt, result.DroppedEntries)
		require.Empty(rt, result.TruncatedRecordingIDs)
		require.Empty(rt, result.SkippedRecordingIDs)
		require.Equal(rt, logBefore, readRecordingLog(rt, dir))
		require.Equal(rt, bakBefore, readOptional(rt, backupPath(dir)))
		assertProtectedTruncates(
			rt,
			archive,
			truncatesBefore,
			entriesNow,
			planBefore,
			termRecordingID,
		)
	})
}

type retentionFault int

const (
	faultNone retentionFault = iota
	faultTruncate
	faultContext
)

const (
	retentionStateStopped = iota
	retentionStateActive
	retentionStateMissing
	retentionStateEmpty
	retentionStateOtherStream
)

const retentionOtherStreamBase int32 = 700

type retentionKey struct {
	term int64
	base int64
	pos  int64
}

type retentionTerm struct {
	id   int64
	base int64
}

func drawRetentionHistory(
	rt *rapid.T,
	log *recordinglog.Log,
	serviceCount int,
	termRecordingID int64,
) map[int64]int32 {
	rt.Helper()
	require.NoError(rt, log.AppendTerm(termRecordingID, 0, 0, 1000))
	terms := []retentionTerm{{id: 0, base: 0}}
	nextTerm := int64(1)
	nextPos := int64(128)
	nextID := int64(1000)
	stamp := int64(2000)
	seq := 0
	name := func(prefix string) string {
		seq++
		return prefix + strconv.Itoa(seq)
	}
	var keys []retentionKey
	serviceByID := map[int64]int32{}

	note := func(id int64, serviceID int32) {
		if _, ok := serviceByID[id]; ok {
			return
		}
		serviceByID[id] = serviceID
	}
	appendServices := func(key retentionKey, services []int32) {
		rt.Helper()
		for _, serviceID := range services {
			id := nextID
			nextID++
			if rapid.IntRange(0, 9).Draw(rt, name("reuse")) == 0 {
				id = termRecordingID
			}
			require.NoError(rt, log.AppendSnapshot(
				id,
				key.term,
				key.base,
				key.pos,
				stamp,
				serviceID,
			))
			stamp++
			note(id, serviceID)
		}
	}
	freshKey := func() retentionKey {
		rt.Helper()
		term := rapid.SampledFrom(terms).Draw(rt, name("term"))
		nextPos += rapid.Int64Range(1, 64).Draw(rt, name("step"))
		return retentionKey{term: term.id, base: term.base, pos: nextPos}
	}
	addKey := func() retentionKey {
		rt.Helper()
		key := freshKey()
		keys = append(keys, key)
		return key
	}
	pickKey := func(reuse bool) retentionKey {
		rt.Helper()
		if reuse && len(keys) > 0 && rapid.IntRange(0, 3).Draw(rt, name("reuseKey")) == 0 {
			return rapid.SampledFrom(keys).Draw(rt, name("old"))
		}
		return addKey()
	}

	ops := rapid.IntRange(1, 5).Draw(rt, "ops")
	for range ops {
		kind := rapid.SampledFrom([]string{
			"term",
			"complete",
			"partial",
			"duplicate",
		}).Draw(rt, name("op"))
		switch kind {
		case "term":
			base := nextTerm * 4096
			require.NoError(rt, log.AppendTerm(termRecordingID, nextTerm, base, 1000+nextTerm))
			terms = append(terms, retentionTerm{id: nextTerm, base: base})
			nextTerm++
		case "complete":
			key := pickKey(true)
			appendServices(key, fullServiceIDs(serviceCount))
		case "partial":
			appendServices(addKey(), drawPartialServices(rt, name, serviceCount))
		case "duplicate":
			if len(keys) == 0 {
				appendServices(addKey(), fullServiceIDs(serviceCount))
				continue
			}
			key := rapid.SampledFrom(keys).Draw(rt, name("dup"))
			appendServices(key, drawDuplicateServices(rt, name, serviceCount))
			if rapid.Bool().Draw(rt, name("retake")) {
				appendServices(key, fullServiceIDs(serviceCount))
			}
		}
	}
	note(termRecordingID, recordinglog.ConsensusModuleServiceID)
	return serviceByID
}

func drawPartialServices(
	rt *rapid.T,
	name func(string) string,
	serviceCount int,
) []int32 {
	rt.Helper()
	var services []int32
	if rapid.IntRange(0, 7).Draw(rt, name("oor")) == 0 {
		services = append(services, int32(serviceCount))
	}
	n := rapid.IntRange(0, serviceCount).Draw(rt, name("rows"))
	services = append(services, fullServiceIDs(serviceCount)[:n]...)
	if rapid.Bool().Draw(rt, name("module")) {
		services = append(services, recordinglog.ConsensusModuleServiceID)
	}
	return services
}

func drawDuplicateServices(
	rt *rapid.T,
	name func(string) string,
	serviceCount int,
) []int32 {
	rt.Helper()
	if serviceCount == 0 {
		return []int32{recordinglog.ConsensusModuleServiceID}
	}
	n := 1
	if serviceCount > 1 {
		n = rapid.IntRange(1, serviceCount).Draw(rt, name("dupRows"))
	}
	return fullServiceIDs(serviceCount)[:n]
}

func applyRetentionStates(
	rt *rapid.T,
	archive *retentionArchive,
	entries []recordinglog.Entry,
	serviceByID map[int64]int32,
) {
	rt.Helper()
	seen := map[int64]struct{}{}
	for _, entry := range entries {
		if _, ok := seen[entry.RecordingID]; ok {
			continue
		}
		seen[entry.RecordingID] = struct{}{}
		state := rapid.SampledFrom([]int{
			retentionStateStopped,
			retentionStateStopped,
			retentionStateStopped,
			retentionStateStopped,
			retentionStateStopped,
			retentionStateActive,
			retentionStateMissing,
			retentionStateEmpty,
			retentionStateOtherStream,
		}).Draw(rt, "state"+strconv.FormatInt(entry.RecordingID, 10))
		if state == retentionStateMissing {
			continue
		}
		sid := recordinglog.ConsensusModuleServiceID
		if drawn, ok := serviceByID[entry.RecordingID]; ok {
			sid = drawn
		}
		stream := streamForService(sid)
		if state == retentionStateOtherStream {
			stream = retentionOtherStreamBase + sid
		}
		stop := retentionStop
		switch state {
		case retentionStateActive:
			stop = recordinglog.NullPosition
		case retentionStateEmpty:
			stop = retentionStart
		}
		archive.put(entry.RecordingID, stream, retentionStart, stop)
	}
}

func stopActiveRetentionRecordings(archive *retentionArchive) {
	for id, fields := range archive.recordings {
		if fields.StopPosition == recordinglog.NullPosition {
			archive.setStop(id, retentionStop)
		}
	}
}

func predictRetentionRefusal(
	rt *rapid.T,
	dir string,
	archive *retentionArchive,
	serviceCount int,
) error {
	rt.Helper()
	plan, err := openRetentionPlan(rt, dir, archive, serviceCount)
	return retentionRefusal(rt, dir, archive, serviceCount, plan, err)
}

func retentionRefusal(
	rt *rapid.T,
	dir string,
	archive *retentionArchive,
	serviceCount int,
	plan recordinglog.RecoveryPlan,
	err error,
) error {
	rt.Helper()
	if err != nil {
		return retentionPlanError(err)
	}
	if len(plan.Snapshots) == 0 {
		return cluster.ErrNoSnapshotToRetain
	}
	if len(plan.Snapshots) < serviceCount+1 {
		return cluster.ErrIncompleteSnapshotGroup
	}
	entries := reloadEntries(rt, dir)
	if unmatchedRetentionSnapshots(entries, plan.Snapshots) {
		return cluster.ErrIncompleteSnapshotGroup
	}
	if outOfRangeRetentionSnapshot(entries, serviceCount) {
		return recordinglog.ErrSnapshotServiceIDOutOfRange
	}
	for _, snap := range plan.Snapshots {
		fields, listErr := archive.ListRecording(context.Background(), snap.RecordingID)
		if listErr != nil {
			if retentionUnknown(listErr) {
				return cluster.ErrIncompleteSnapshotGroup
			}
			return listErr
		}
		if fields.StopPosition == recordinglog.NullPosition {
			return cluster.ErrSnapshotRecordingActive
		}
		if fields.StopPosition == fields.StartPosition {
			return cluster.ErrIncompleteSnapshotGroup
		}
	}
	return nil
}

// PlanRecovery returns a new error on each call. ErrorIs does not match
// the error from one call to the error from the next call.
func retentionPlanError(err error) error {
	targets := [...]error{
		recordinglog.ErrUnknownRecordingID,
		recordinglog.ErrSnapshotServiceIDOutOfRange,
		recordinglog.ErrSnapshotMissingForService,
	}
	for _, target := range targets {
		if errors.Is(err, target) {
			return target
		}
	}
	return err
}

func unmatchedRetentionSnapshots(
	entries []recordinglog.Entry,
	snapshots []recordinglog.Snapshot,
) bool {
	for _, snap := range snapshots {
		if !validRetentionSnapshot(entries, snap) {
			return true
		}
	}
	return false
}

func validRetentionSnapshot(entries []recordinglog.Entry, snap recordinglog.Snapshot) bool {
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

func outOfRangeRetentionSnapshot(entries []recordinglog.Entry, serviceCount int) bool {
	for _, entry := range entries {
		if entry.Type != recordinglog.EntryTypeSnapshot {
			continue
		}
		sid := int(entry.ServiceID)
		if sid < int(recordinglog.ConsensusModuleServiceID) || sid > serviceCount-1 {
			return true
		}
	}
	return false
}

func retentionUnknown(err error) bool {
	code, ok := archive.ExceptionCode(err)
	return ok && code == archive.ErrorUnknownRecording
}

// sameStreamSuperseded returns same-stream recording ids that truncation
// cuts to the start position once stopped. An invalid snapshot row matches.
// A valid row matches when InvalidateEntriesBefore(math.MaxInt64) would
// mark it. retentionKeepIDs excludes an id that a surviving row still holds.
func sameStreamSuperseded(
	entries []recordinglog.Entry,
	plan recordinglog.RecoveryPlan,
	termRecordingID int64,
	archive *retentionArchive,
) []int64 {
	keep := retentionKeepIDs(entries, plan, termRecordingID)
	streams := make(map[int32]int32, len(plan.Snapshots))
	for _, snap := range plan.Snapshots {
		fields, ok := archive.recordings[snap.RecordingID]
		if ok {
			streams[snap.ServiceID] = fields.StreamID
		}
	}
	protectedTerm, protectedPosition, hasProtected := protectedRetentionKey(entries)
	seen := map[int64]struct{}{}
	var ids []int64
	for _, entry := range entries {
		if !retentionSupersededRow(entry, protectedTerm, protectedPosition, hasProtected) {
			continue
		}
		if _, ok := keep[entry.RecordingID]; ok {
			continue
		}
		if _, ok := seen[entry.RecordingID]; ok {
			continue
		}
		seen[entry.RecordingID] = struct{}{}
		keptStream, ok := streams[entry.ServiceID]
		if !ok {
			continue
		}
		fields, ok := archive.recordings[entry.RecordingID]
		if !ok || fields.StreamID != keptStream {
			continue
		}
		ids = append(ids, entry.RecordingID)
	}
	return ids
}

func retentionSupersededRow(
	entry recordinglog.Entry,
	protectedTerm, protectedPosition int64,
	hasProtected bool,
) bool {
	if entry.Type != recordinglog.EntryTypeSnapshot {
		return false
	}
	if !entry.Valid {
		return true
	}
	if entry.LogPosition == math.MaxInt64 {
		return false
	}
	return !retentionAtProtectedKey(entry, protectedTerm, protectedPosition, hasProtected)
}

func retentionAtProtectedKey(
	entry recordinglog.Entry,
	termID, logPosition int64,
	hasProtected bool,
) bool {
	return hasProtected &&
		entry.LeadershipTermID == termID &&
		entry.LogPosition == logPosition
}

func runRetentionFault(
	rt *rapid.T,
	dir string,
	archive *retentionArchive,
	serviceCount int,
	termRecordingID int64,
	fault retentionFault,
	faultCall int,
	before []byte,
	planBefore recordinglog.RecoveryPlan,
	planErr error,
) {
	rt.Helper()
	refusal := predictRetentionRefusal(rt, dir, archive, serviceCount)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if fault != faultNone {
		archive.truncateErr = func(attempt int) error {
			if attempt != faultCall {
				return nil
			}
			if fault == faultContext {
				cancel()
				return context.Canceled
			}
			return errRetentionTruncate
		}
	}
	truncatesBefore := len(archive.truncates)
	attemptsBefore := archive.truncateAttempts
	entries := reloadEntries(rt, dir)
	var active []int64
	if refusal == nil {
		superseded := sameStreamSuperseded(entries, planBefore, termRecordingID, archive)
		active = slices.DeleteFunc(superseded, func(id int64) bool {
			return archive.recordings[id].StopPosition != recordinglog.NullPosition
		})
	}
	_, err := cluster.RetainLatestSnapshot(ctx, cluster.SnapshotRetentionConfig{
		ClusterDir:   dir,
		ServiceCount: serviceCount,
		Archive:      archive,
	})
	assertRetentionRun(
		rt,
		dir,
		archive,
		serviceCount,
		termRecordingID,
		truncatesBefore,
		entries,
		planBefore,
		planErr,
		before,
		refusal,
		err,
	)
	for _, id := range active {
		require.NotContains(rt, archive.truncates[truncatesBefore:], id)
	}
	fired := fault != faultNone && archive.truncateAttempts > attemptsBefore+faultCall
	switch {
	case fired:
		want := errRetentionTruncate
		if fault == faultContext {
			want = context.Canceled
		}
		require.ErrorIs(rt, err, want)
		require.Nil(rt, readOptional(rt, backupPath(dir)))
	case refusal == nil && len(active) > 0:
		require.ErrorIs(rt, err, cluster.ErrSnapshotRecordingActive)
		require.Nil(rt, readOptional(rt, backupPath(dir)))
		require.Len(rt, reloadEntries(rt, dir), len(entries))
	case refusal == nil:
		require.NoError(rt, err)
	}
	if err == nil {
		require.NoError(rt, predictRetentionRefusal(rt, dir, archive, serviceCount))
	}
}

func assertRetentionRun(
	rt *rapid.T,
	dir string,
	archive *retentionArchive,
	serviceCount int,
	termRecordingID int64,
	truncatesBefore int,
	entries []recordinglog.Entry,
	planBefore recordinglog.RecoveryPlan,
	planErr error,
	before []byte,
	refusal error,
	err error,
) {
	rt.Helper()
	assertProtectedTruncates(rt, archive, truncatesBefore, entries, planBefore, termRecordingID)
	if refusal != nil {
		assertRetentionRefused(rt, dir, archive, truncatesBefore, before, refusal, err)
		return
	}
	require.NoError(rt, planErr)
	require.Equal(
		rt,
		planWithTermStop(planBefore, archive, termRecordingID),
		planRetention(rt, dir, archive, serviceCount),
	)
}

func assertRetentionRefused(
	rt *rapid.T,
	dir string,
	archive *retentionArchive,
	truncatesBefore int,
	before []byte,
	refusal error,
	err error,
) {
	rt.Helper()
	require.ErrorIs(rt, err, refusal)
	require.Len(rt, archive.truncates, truncatesBefore)
	require.Equal(rt, before, readRecordingLog(rt, dir))
}

// planWithTermStop copies the plan. The copy takes Log.Extent.StopPosition
// and AppendedLogPosition from the archive. Stopping an active term
// recording changes both positions. RetainLatestSnapshot does not write
// them. Snapshot rows and the term identity stay on the copy.
func planWithTermStop(
	plan recordinglog.RecoveryPlan,
	archive *retentionArchive,
	termRecordingID int64,
) recordinglog.RecoveryPlan {
	if plan.Log == nil {
		return plan
	}
	fields, ok := archive.recordings[termRecordingID]
	if !ok {
		return plan
	}
	logCopy := *plan.Log
	logCopy.Extent.StopPosition = fields.StopPosition
	plan.Log = &logCopy
	plan.AppendedLogPosition = fields.StopPosition
	return plan
}

func retentionKeepIDs(
	entries []recordinglog.Entry,
	plan recordinglog.RecoveryPlan,
	termRecordingID int64,
) map[int64]struct{} {
	keep := map[int64]struct{}{termRecordingID: {}}
	for _, snap := range plan.Snapshots {
		keep[snap.RecordingID] = struct{}{}
	}
	protectedTerm, protectedPosition, hasProtected := protectedRetentionKey(entries)
	for _, entry := range entries {
		if !entry.Valid {
			continue
		}
		if entry.Type == recordinglog.EntryTypeSnapshot &&
			!retentionAtProtectedKey(entry, protectedTerm, protectedPosition, hasProtected) {
			continue
		}
		keep[entry.RecordingID] = struct{}{}
	}
	return keep
}

func protectedRetentionKey(entries []recordinglog.Entry) (int64, int64, bool) {
	for _, entry := range slices.Backward(entries) {
		if entry.Valid &&
			entry.Type == recordinglog.EntryTypeSnapshot &&
			entry.ServiceID == recordinglog.ConsensusModuleServiceID {
			return entry.LeadershipTermID, entry.LogPosition, true
		}
	}
	return 0, 0, false
}

func assertProtectedTruncates(
	rt *rapid.T,
	archive *retentionArchive,
	from int,
	entries []recordinglog.Entry,
	plan recordinglog.RecoveryPlan,
	termRecordingID int64,
) {
	rt.Helper()
	keep := retentionKeepIDs(entries, plan, termRecordingID)
	for _, id := range archive.truncates[from:] {
		_, protected := keep[id]
		require.False(rt, protected, "truncated recording %d", id)
	}
}
