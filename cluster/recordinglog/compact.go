package recordinglog

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	recordingLogBackupFileName = recordingLogFileName + ".bak"
	recordingLogTempFileName   = recordingLogFileName + ".tmp"
)

// Compact rewrites recording.log so the file holds only valid entries.
//
// Behaviour:
//   - writes valid entries in comparator order to recording.log.tmp
//   - drops invalid entries and removed slots
//   - keeps LogPosition and every field other than Index and Position
//   - returns 0 without a write when every entry is valid and the file
//     does not hold a removed slot
//
// recording.log.bak stores the pre-compact bytes. Compact writes that
// backup before the rename. A no-op leaves an existing backup in place.
//
// Concurrency: single-writer; see [Log]. Compact replaces the open file.
// Another handle on the same recording.log keeps the old file.
//
// Returns the count of invalid entries dropped. A closed log returns an
// error matching os.ErrClosed before the no-op check. When the rename has
// not succeeded, a failed Compact leaves recording.log, the handle, and
// the cache unchanged. After reload fails, Compact closes the log and
// returns 0. The cache then holds the entries from before the call.
func (l *Log) Compact() (int, error) {
	if l.closed {
		return 0, fmt.Errorf("recordinglog: compact: closed: %w", os.ErrClosed)
	}
	dropped := l.invalidEntryCount()
	hasRemovedSlot := int(l.nextEntryIndex) != len(l.entries)
	if dropped == 0 && !hasRemovedSlot {
		return 0, nil
	}

	buf, err := l.readWholeFile()
	if err != nil {
		return 0, fmt.Errorf("recordinglog: compact: read: %w", err)
	}
	encoded := encodeValidEntries(l.entries)

	backupPath := filepath.Join(l.dir, recordingLogBackupFileName)
	if err := writeSyncFile(backupPath, buf); err != nil {
		return 0, fmt.Errorf("recordinglog: compact: backup: %w", err)
	}

	tempPath := filepath.Join(l.dir, recordingLogTempFileName)
	temp, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, fmt.Errorf("recordinglog: compact: temporary file: %w", err)
	}
	if err := writeAndSync(temp, encoded); err != nil {
		closeAndRemove(temp, tempPath)
		return 0, fmt.Errorf("recordinglog: compact: temporary file: %w", err)
	}

	SyncDirectory(l.dir)

	logPath := filepath.Join(l.dir, recordingLogFileName)
	if err := os.Rename(tempPath, logPath); err != nil {
		closeAndRemove(temp, tempPath)
		return 0, fmt.Errorf("recordinglog: compact: rename: %w", err)
	}

	SyncDirectory(l.dir)

	// os.Rename leaves temp on the recording.log inode. Compact stores temp
	// and closes the previous handle.
	old := l.file
	l.file = temp
	_ = old.Close()

	if err := l.reload(); err != nil {
		_ = l.Close()
		return 0, fmt.Errorf("recordinglog: compact: reload: %w", err)
	}
	return dropped, nil
}

func (l *Log) invalidEntryCount() int {
	count := 0
	for _, entry := range l.entries {
		if !entry.Valid {
			count++
		}
	}
	return count
}

func encodeValidEntries(entries []Entry) []byte {
	var encoded []byte
	for _, entry := range entries {
		if entry.Valid {
			encoded = append(encoded, encodeEntry(entry)...)
		}
	}
	return encoded
}

func writeSyncFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeAndSync(file, data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func writeAndSync(file *os.File, data []byte) error {
	if err := writeAt(file, data, 0); err != nil {
		return err
	}
	return file.Sync()
}

func closeAndRemove(file *os.File, path string) {
	_ = file.Close()
	_ = os.Remove(path)
}
