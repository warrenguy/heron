package recordinglog

import (
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/warrenguy/heron/atomicbuffer"
)

const recordingLogFileName = "recording.log"

// Log is a mutable handle over a recording.log file.
//
// It owns the open file, an in-memory cache of decoded entries in comparator
// order, and the index maps the write paths consult. Reads of the cache go
// through Entries; durability is an explicit Force step.
//
// Log is single-writer: one handle owns the file and there is no internal
// locking. Concurrent use is a caller bug. Close releases the file handle;
// further operations fail.
type Log struct {
	file                    *os.File
	entries                 []Entry
	indexByLeadershipTermID map[int64]int32
	invalidSnapshots        []int32
	nextEntryIndex          int32
	writeOffset             int64
	termRecordingID         int64
	closed                  bool
}

// Open opens the recording.log under dir.
//
// When the file is absent and createNew is true, it is created and the parent
// directory is fsynced so the new directory entry is durable. When the file
// exists, its records are decoded into the in-memory cache and the index maps
// are rebuilt; nextEntryIndex is seeded from the decoded next entry ordinal so
// it matches the file-order index of the next appended entry.
//
// Returns the os.OpenFile error when the file cannot be opened, or any decode
// error from reloading existing content.
func Open(dir string, createNew bool) (*Log, error) {
	path := filepath.Join(dir, recordingLogFileName)
	_, statErr := os.Stat(path)
	missingFile := os.IsNotExist(statErr)

	flags := os.O_RDWR
	if createNew {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}

	l := &Log{
		file:                    file,
		indexByLeadershipTermID: make(map[int64]int32),
		termRecordingID:         nullValue,
	}

	if missingFile {
		SyncDirectory(dir)
		return l, nil
	}

	if err := l.reload(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return l, nil
}

func (l *Log) reload() error {
	buf, err := io.ReadAll(l.file)
	if err != nil {
		return err
	}

	entries, nextOrdinal, termRecordingID, err := decodeEntriesAndCount(buf)
	if err != nil {
		return err
	}
	sortEntries(entries)

	l.entries = entries
	l.nextEntryIndex = nextOrdinal
	// writeOffset seeds the next append from aligned file length, matching the
	// reference writer rather than decoder consumed bytes. A torn tail ignored on
	// read may be zero-padded into a full record on append; reload can then fail
	// decode validation on truncated files.
	l.writeOffset = int64(atomicbuffer.Align(len(buf), recordAlignment))
	l.termRecordingID = termRecordingID

	for i := range entries {
		entry := entries[i]
		switch {
		case isValidTerm(entry):
			l.indexByLeadershipTermID[entry.LeadershipTermID] = int32(i)
		case !entry.Valid && isSnapshotType(entry.Type):
			l.invalidSnapshots = append(l.invalidSnapshots, int32(i))
		}
	}

	return nil
}

// Close releases the file handle. Subsequent operations on the Log fail.
//
// Close is idempotent: a second call returns nil without touching the file.
// A failed Close still marks the handle closed; do not retry Close to recover.
func (l *Log) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	return l.file.Close()
}

// Entries returns the cache in comparator order. The slice aliases internal
// state; callers must not mutate it and must copy to retain it across further
// operations.
func (l *Log) Entries() []Entry {
	return l.entries
}

// NextEntryIndex returns the file-order ordinal the next appended entry takes.
func (l *Log) NextEntryIndex() int32 {
	return l.nextEntryIndex
}

// AppendTerm records a leadership term and persists it.
//
// Term entries may use any leadershipTermID order; duplicates return
// ErrDuplicateTerm. The cache stays in comparator order via insertion, not
// strict append sequence. When the cache is non-empty, the prior term
// (leadershipTermID-1) is committed to termBaseLogPosition and the new
// term's logPosition is seeded from the next term's base when one exists.
//
// recordingID must be consistent across all terms; the first stores the shared
// value. Returns ErrTermRecordingIDInvalid on NULL_VALUE,
// ErrTermRecordingIDMismatch on a differing recordingID, ErrShortWrite on a
// truncated write, or the underlying WriteAt error.
func (l *Log) AppendTerm(
	recordingID int64,
	leadershipTermID int64,
	termBaseLogPosition int64,
	timestamp int64,
) error {
	if err := l.validateTermRecordingID(recordingID); err != nil {
		return err
	}

	logPosition := NullPosition
	if len(l.entries) > 0 {
		if _, ok := l.indexByLeadershipTermID[leadershipTermID]; ok {
			return ErrDuplicateTerm
		}
		if priorIndex, ok := l.indexByLeadershipTermID[leadershipTermID-1]; ok {
			if err := l.commitLogPositionAt(priorIndex, termBaseLogPosition); err != nil {
				return err
			}
		}
		if nextTerm := l.FindTermEntry(leadershipTermID + 1); nextTerm != nil {
			logPosition = nextTerm.TermBaseLogPosition
		}
	}

	entry := Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		Timestamp:           timestamp,
		ServiceID:           int32(nullValue),
		Type:                EntryTypeTerm,
		Valid:               true,
	}
	insertionIndex, err := l.append(entry)
	if err != nil {
		return err
	}
	if l.termRecordingID == nullValue {
		l.termRecordingID = recordingID
	}
	l.indexByLeadershipTermID[leadershipTermID] = insertionIndex
	return nil
}

// AppendSnapshot records a consensus-module or service snapshot for the current
// term and persists it.
//
// recordingID must not be NULL_VALUE; ErrInvalidRecordingID otherwise. Returns
// ErrShortWrite on a truncated write or the underlying WriteAt error.
//
// A matching invalidated snapshot (same leadershipTermId, logPosition,
// serviceId) is re-validated in place; otherwise the snapshot appends as a fresh
// entry.
func (l *Log) AppendSnapshot(
	recordingID int64,
	leadershipTermID int64,
	termBaseLogPosition int64,
	logPosition int64,
	timestamp int64,
	serviceID int32,
) error {
	if err := validateRecordingID(recordingID); err != nil {
		return err
	}
	return l.appendOrRestore(Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		Timestamp:           timestamp,
		ServiceID:           serviceID,
		Type:                EntryTypeSnapshot,
		Valid:               true,
	})
}

// AppendStandbySnapshot records a remote standby snapshot for the current term
// and persists it.
//
// archiveEndpoint must be non-empty and within the maximum length;
// ErrStandbyEndpointEmpty or ErrEndpointTooLong otherwise. recordingID must not
// be NULL_VALUE; ErrInvalidRecordingID otherwise. Returns ErrShortWrite on a
// truncated write or the underlying WriteAt error.
//
// A matching invalidated standby snapshot (same leadershipTermId, logPosition,
// serviceId) is re-validated in place; otherwise the snapshot appends as a fresh
// entry.
func (l *Log) AppendStandbySnapshot(
	recordingID int64,
	leadershipTermID int64,
	termBaseLogPosition int64,
	logPosition int64,
	timestamp int64,
	serviceID int32,
	archiveEndpoint string,
) error {
	if err := validateRecordingID(recordingID); err != nil {
		return err
	}
	if archiveEndpoint == "" {
		return ErrStandbyEndpointEmpty
	}
	if len(archiveEndpoint) > maxEndpointLen {
		return ErrEndpointTooLong
	}
	return l.appendOrRestore(Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		Timestamp:           timestamp,
		ServiceID:           serviceID,
		Type:                EntryTypeStandbySnapshot,
		Valid:               true,
		ArchiveEndpoint:     archiveEndpoint,
	})
}

// appendOrRestore re-validates a matching invalidated snapshot in place when one
// exists, otherwise appends entry as a fresh record.
func (l *Log) appendOrRestore(entry Entry) error {
	if len(l.invalidSnapshots) == 0 {
		_, err := l.append(entry)
		return err
	}
	restored, err := l.restoreInvalidSnapshot(entry)
	if err != nil {
		return err
	}
	if restored {
		return nil
	}
	_, err = l.append(entry)
	return err
}

// append encodes entry, writes it at the aligned end of the file, then inserts
// it into the comparator-sorted cache at its ordered position, shifting the
// index map and invalidSnapshots indices that move. It returns the cache slot
// the entry landed in.
//
// The write happens before any cache mutation so a failed WriteAt leaves the
// cache untouched.
func (l *Log) append(entry Entry) (int32, error) {
	entry.Index = l.nextEntryIndex
	writeOffset := l.writeOffset
	entry.Position = writeOffset

	encoded := encodeEntry(entry)
	if err := l.writeAt(encoded, writeOffset); err != nil {
		return 0, err
	}

	l.writeOffset = writeOffset + int64(len(encoded))
	l.nextEntryIndex++

	originalLen := int32(len(l.entries))
	insertionIndex := originalLen
	for insertionIndex > 0 && compareEntries(entry, l.entries[insertionIndex-1]) < 0 {
		insertionIndex--
	}
	l.entries = slices.Insert(l.entries, int(insertionIndex), entry)
	if insertionIndex < originalLen {
		l.shiftIndicesAt(insertionIndex)
	}

	return insertionIndex, nil
}

// commitLogPositionAt writes logPosition in place at the cache slot index and
// updates the cached entry. The write is skipped when the position is unchanged
// so no redundant bytes hit storage.
func (l *Log) commitLogPositionAt(index int32, logPosition int64) error {
	entry := l.entries[index]
	if entry.LogPosition == logPosition {
		return nil
	}
	if err := l.writeInt64FieldAt(entry.Position, logPositionOffset, logPosition); err != nil {
		return err
	}
	entry.LogPosition = logPosition
	l.entries[index] = entry
	return nil
}

// writeFieldAt writes exactly len(b) bytes at the absolute file offset
// position+offset, the single-field path the in-place mutators use.
func (l *Log) writeFieldAt(position int64, offset int, b []byte) error {
	return l.writeAt(b, position+int64(offset))
}

// writeAt writes b at the absolute file offset position and returns
// ErrShortWrite when fewer than len(b) bytes persist.
func (l *Log) writeAt(b []byte, position int64) error {
	n, err := l.file.WriteAt(b, position)
	if err != nil {
		return err
	}
	if n != len(b) {
		return ErrShortWrite
	}
	return nil
}

// consensusModuleServiceID is the service id the consensus module writes its own
// snapshots under. InvalidateLatestSnapshot keys the newest snapshot group on it
// and walks the per-service entries that precede it. It shares the value -1 with
// the NULL_VALUE sentinel; the meaning differs, so it keeps its own name.
const consensusModuleServiceID int32 = -1

// CommitLogPosition records logPosition for the term leadershipTermID in place.
//
// Returns ErrUnknownLeadershipTermID when no term carries leadershipTermID. The
// write is skipped when the stored position already equals logPosition.
// Propagates ErrShortWrite or the underlying WriteAt error.
func (l *Log) CommitLogPosition(leadershipTermID, logPosition int64) error {
	index, ok := l.indexByLeadershipTermID[leadershipTermID]
	if !ok {
		return ErrUnknownLeadershipTermID
	}
	return l.commitLogPositionAt(index, logPosition)
}

// InvalidateLatestSnapshot marks the newest consensus-module snapshot group
// invalid in place and reports whether a group was found.
//
// The group is collected by a tail-to-head scan while consensus-module
// logPosition does not decrease; each service's contiguous snapshot run at that
// position is invalidated. Standby snapshots in the collected group participate;
// standbys outside the scan window stay valid because comparator-sorted
// placement keys leadershipTermID and termBaseLogPosition before logPosition,
// defers EntryTypeSnapshot after non-snapshot types at the same key, then
// breaks ties by logPosition and serviceID, not global logPosition alone.
// Each entry's type word gains
// the invalid flag and its cache slot is recorded in invalidSnapshots so a later
// matching append re-validates it.
//
// Returns ErrNoMatchingTermForSnapshot when any consensus-module snapshot at
// the highest logPosition has a leadershipTermId with no term entry; this term
// check completes before any write, so a missing term aborts with the cache
// untouched. A writeFieldAt failure mid-group still persists the earlier
// members, matching the reference.
// Propagates ErrShortWrite or the underlying WriteAt error.
func (l *Log) InvalidateLatestSnapshot() (bool, error) {
	var indices []int32
	highLogPosition := NullPosition
	for idx := int32(len(l.entries)) - 1; idx >= 0; idx-- {
		entry := l.entries[idx]
		if !isLiveSnapshot(entry, consensusModuleServiceID) {
			continue
		}
		if entry.LogPosition < highLogPosition {
			break
		}
		if l.FindTermEntry(entry.LeadershipTermID) == nil {
			return false, ErrNoMatchingTermForSnapshot
		}
		if entry.LogPosition > highLogPosition {
			indices = indices[:0]
			highLogPosition = entry.LogPosition
		}
		indices = append(indices, idx)
	}

	found := len(indices) > 0
	for _, idx := range slices.Backward(indices) {
		serviceID := consensusModuleServiceID
		for ; idx >= 0; idx-- {
			entry := l.entries[idx]
			if !isLiveSnapshot(entry, serviceID) {
				break
			}
			if err := l.invalidateEntry(idx); err != nil {
				return found, err
			}
			serviceID++
		}
	}
	return found, nil
}

// invalidateEntry sets the invalid flag on the entry at cache slot index and
// records the slot for later restore or, for a term, drops its index-map entry.
//
// The on-disk type word becomes entryTypeInvalidFlag | entry.Type: the stripped
// type the cache holds OR the high bit, the same word encodeEntry writes for an
// invalid entry.
func (l *Log) invalidateEntry(index int32) error {
	entry := l.entries[index]
	typeWord := entryTypeInvalidFlag | entry.Type
	if err := l.writeInt32FieldAt(entry.Position, entryTypeOffset, typeWord); err != nil {
		return err
	}
	entry.Valid = false
	l.entries[index] = entry
	if entry.Type == EntryTypeTerm {
		delete(l.indexByLeadershipTermID, entry.LeadershipTermID)
	} else {
		l.invalidSnapshots = append(l.invalidSnapshots, index)
	}
	return nil
}

// restoreInvalidSnapshot re-validates a previously invalidated snapshot in place
// when one matches entry on type, leadershipTermId, termBaseLogPosition,
// logPosition, and serviceId.
//
// The match key carries every comparator sort key (compare.go), so the in-place
// rewrite never moves the slot's ordered position; a differing termBase falls
// through to a fresh append, matching the reference matchesEntry.
//
// It rewrites the whole record at the slot's existing position with entry's
// recordingId and timestamp and clears the invalid flag, then drops the slot
// from invalidSnapshots. The restored record reuses the existing slot's
// endpoint so its span never grows: the new endpoint is not part of the match
// key, and a longer encode would overrun the contiguous next record. Returns
// false when no invalid snapshot matches, leaving the caller to append a fresh
// entry.
//
// invalidSnapshots is scanned newest-first so duplicate match keys restore the
// most-recently-invalidated slot, matching the reference scan order.
func (l *Log) restoreInvalidSnapshot(entry Entry) (bool, error) {
	for i, idx := range slices.Backward(l.invalidSnapshots) {
		existing := l.entries[idx]
		if compareEntries(existing, entry) != 0 {
			continue
		}

		entry.Position = existing.Position
		entry.Index = existing.Index
		entry.Valid = true
		entry.ArchiveEndpoint = existing.ArchiveEndpoint
		encoded := encodeEntry(entry)
		if err := l.writeAt(encoded, existing.Position); err != nil {
			return false, err
		}

		l.entries[idx] = entry
		l.invalidSnapshots = slices.Delete(l.invalidSnapshots, i, i+1)
		return true, nil
	}
	return false, nil
}

func (l *Log) validateTermRecordingID(recordingID int64) error {
	if recordingID == nullValue {
		return ErrTermRecordingIDInvalid
	}
	if l.termRecordingID != nullValue && recordingID != l.termRecordingID {
		return ErrTermRecordingIDMismatch
	}
	return nil
}

func validateRecordingID(recordingID int64) error {
	if recordingID == nullValue {
		return ErrInvalidRecordingID
	}
	return nil
}

const noTermIDCeiling int64 = -1

// FileSyncLevel controls Force durability. Values match Aeron fileSyncLevel.
type FileSyncLevel int

const (
	FileSyncNone FileSyncLevel = 0
	FileSyncData FileSyncLevel = 1
	FileSyncFull FileSyncLevel = 2
)

// EnsureCoherentParams groups EnsureCoherent inputs.
type EnsureCoherentParams struct {
	RecordingID                int64
	InitialLogLeadershipTermID int64
	InitialTermBaseLogPosition int64
	LeadershipTermID           int64
	TermBaseLogPosition        int64
	LogPosition                int64
	Timestamp                  int64
	FileSyncLevel              FileSyncLevel
}

// FindTermEntry returns the cached valid TERM entry for leadershipTermID, or nil
// when no such term exists. The entry aliases internal state; callers must not
// mutate it and must copy to retain it across further operations.
func (l *Log) FindTermEntry(leadershipTermID int64) *Entry {
	index, ok := l.indexByLeadershipTermID[leadershipTermID]
	if !ok {
		return nil
	}
	return &l.entries[index]
}

// maxIndexedTermID returns the greatest leadershipTermID in the term index. When
// ceiling is non-negative, only ids strictly less than ceiling are considered.
func (l *Log) maxIndexedTermID(ceiling int64) (int64, bool) {
	var maxID int64
	found := false
	for termID := range l.indexByLeadershipTermID {
		if ceiling >= 0 && termID >= ceiling {
			continue
		}
		if !found || termID > maxID {
			maxID = termID
			found = true
		}
	}
	return maxID, found
}

// findLastTerm returns the highest-leadershipTermId valid TERM entry, the last
// in comparator order, or nil when the log holds no valid term.
func (l *Log) findLastTerm() *Entry {
	maxID, ok := l.maxIndexedTermID(noTermIDCeiling)
	if !ok {
		return nil
	}
	return l.FindTermEntry(maxID)
}

// findGreatestTermBelow returns the highest valid TERM entry whose
// leadershipTermID is strictly less than leadershipTermID, or nil when none
// exist below it.
func (l *Log) findGreatestTermBelow(leadershipTermID int64) *Entry {
	maxBelow, ok := l.maxIndexedTermID(leadershipTermID)
	if !ok {
		return nil
	}
	return l.FindTermEntry(maxBelow)
}

func (l *Log) appendInitialTerms(p EnsureCoherentParams, skipExisting bool) error {
	for termID := max(int64(0), p.InitialLogLeadershipTermID); termID < p.LeadershipTermID; termID++ {
		if skipExisting {
			if _, ok := l.indexByLeadershipTermID[termID]; ok {
				continue
			}
		}
		if err := l.AppendTerm(
			p.RecordingID, termID, p.InitialTermBaseLogPosition, p.Timestamp,
		); err != nil {
			return err
		}
	}
	if skipExisting {
		if _, ok := l.indexByLeadershipTermID[p.LeadershipTermID]; ok {
			return nil
		}
	}
	return l.AppendTerm(
		p.RecordingID, p.LeadershipTermID, p.TermBaseLogPosition, p.Timestamp,
	)
}

func (l *Log) backfillTermsToTarget(p EnsureCoherentParams, anchor *Entry) error {
	if anchor.LogPosition == NullPosition {
		if p.TermBaseLogPosition == NullPosition {
			return ErrPriorTermNotCommitted
		}
		if err := l.CommitLogPosition(anchor.LeadershipTermID, p.TermBaseLogPosition); err != nil {
			return err
		}
		anchor = l.FindTermEntry(anchor.LeadershipTermID)
	}
	backfillBase := anchor.LogPosition
	for termID := anchor.LeadershipTermID + 1; termID < p.LeadershipTermID; termID++ {
		if _, ok := l.indexByLeadershipTermID[termID]; ok {
			continue
		}
		if err := l.AppendTerm(p.RecordingID, termID, backfillBase, p.Timestamp); err != nil {
			return err
		}
	}
	if _, ok := l.indexByLeadershipTermID[p.LeadershipTermID]; ok {
		return nil
	}
	return l.AppendTerm(p.RecordingID, p.LeadershipTermID, backfillBase, p.Timestamp)
}

// EnsureCoherent brings the log up to leadershipTermID and commits the reached
// position, then forces the file to storage.
//
// On an empty log it appends a term for every id in
// [max(0, initialLogLeadershipTermID), leadershipTermID) at
// initialTermBaseLogPosition, then the target term at termBaseLogPosition.
//
// On a log whose last term precedes leadershipTermID it backfills the gap. When
// that last term is uncommitted it first commits it to termBaseLogPosition; a
// missing termBaseLogPosition (NullPosition) instead returns
// ErrPriorTermNotCommitted before any write. The backfilled and target terms
// take the last term's committed logPosition as their base.
//
// When the highest term exceeds leadershipTermID but that term is missing, it
// backfills from the greatest term below leadershipTermID, or from
// initialLogLeadershipTermID when none exist below the target.
//
// Go divergence from the reference Java ensureCoherent: the reference skips
// that backfill and either returns without appending the target when
// logPosition is NullPosition or fails commit with ErrUnknownLeadershipTermID.
// Heron backfills so the local index reaches leadershipTermID. Appended rows
// use the same TERM wire layout; only this node's recording.log changes.
// recording.log is per-node and is not sent on the cluster wire, so mixed
// Go and Java members are not cross-affected. A Java peer in the same abnormal
// gappy state would not self-heal its own index. Normal election-driven calls
// advance leadershipTermID forward and follow the same path as the reference.
//
// On a log already at or beyond leadershipTermID it only commits the reached
// position.
//
// A logPosition of NullPosition leaves the target term uncommitted. Propagates
// any AppendTerm, CommitLogPosition, or Force error. Every non-error path ends
// with Force.
func (l *Log) EnsureCoherent(p EnsureCoherentParams) error {
	if l.FindTermEntry(p.LeadershipTermID) == nil {
		if err := l.backfillMissingTargetTerm(p); err != nil {
			return err
		}
	}
	if p.LogPosition != NullPosition {
		if err := l.CommitLogPosition(p.LeadershipTermID, p.LogPosition); err != nil {
			return err
		}
	}
	return l.Force(p.FileSyncLevel)
}

func (l *Log) backfillMissingTargetTerm(p EnsureCoherentParams) error {
	lastTerm := l.findLastTerm()
	switch {
	case lastTerm == nil:
		return l.appendInitialTerms(p, false)
	case lastTerm.LeadershipTermID < p.LeadershipTermID:
		return l.backfillTermsToTarget(p, lastTerm)
	default:
		anchor := l.findGreatestTermBelow(p.LeadershipTermID)
		if anchor == nil {
			return l.appendInitialTerms(p, true)
		}
		return l.backfillTermsToTarget(p, anchor)
	}
}

// Force flushes file data to durable storage when level is greater than
// FileSyncNone. It is a no-op at FileSyncNone.
//
// Go's os.File.Sync maps to fsync, so FileSyncData and FileSyncFull both fully
// sync; FileSyncData over-syncs relative to Aeron's data-only force but stays
// wire-compatible.
func (l *Log) Force(level FileSyncLevel) error {
	if level > FileSyncNone {
		return l.file.Sync()
	}
	return nil
}

// SyncDirectory fsyncs dir so a newly created file's directory entry is durable.
// Errors are ignored: the file open already succeeded.
func SyncDirectory(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

func (l *Log) shiftIndicesAt(insertionIndex int32) {
	for termID, idx := range l.indexByLeadershipTermID {
		if idx >= insertionIndex {
			l.indexByLeadershipTermID[termID] = idx + 1
		}
	}
	for i, snapshotIndex := range l.invalidSnapshots {
		if snapshotIndex >= insertionIndex {
			l.invalidSnapshots[i] = snapshotIndex + 1
		}
	}
}

func (l *Log) writeInt64FieldAt(position int64, offset int, value int64) error {
	var field [8]byte
	atomicbuffer.Wrap(field[:]).PutInt64(0, value)
	return l.writeFieldAt(position, offset, field[:])
}

func (l *Log) writeInt32FieldAt(position int64, offset int, value int32) error {
	var field [4]byte
	atomicbuffer.Wrap(field[:]).PutInt32(0, value)
	return l.writeFieldAt(position, offset, field[:])
}

func isLiveSnapshot(e Entry, serviceID int32) bool {
	return e.Valid && isSnapshotType(e.Type) && e.ServiceID == serviceID
}
