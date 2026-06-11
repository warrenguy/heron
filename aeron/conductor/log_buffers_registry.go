package conductor

import (
	"sync/atomic"
	"time"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

func timeNowNs() int64 {
	return time.Now().UnixNano()
}

func normalizeLingerNs(lingerNs int64) int64 {
	if lingerNs < 0 {
		return 0
	}
	return lingerNs
}

type logBufferEntry struct {
	buffers          *logbuffer.LogBuffers
	refCount         int32
	lingerDeadlineNs int64
}

// LogBuffersRegistry is the sole conductor log-buffer map: dedup mmap by absolute
// path, refCount, and lingerDeadlineNs. Not thread-safe; mutation must run under
// Conductor.resourceMu write lock during mutation scopes or close drain.
type LogBuffersRegistry struct {
	entries                map[string]*logBufferEntry
	mediaDriverDir         string
	resolvedMediaDriverDir string
	// lingerScheduled counts entries awaiting linger sweep; updated with atomic.Add
	// under resourceMu write lock. HasScheduledLinger uses atomic.Load for lock-free
	// reads from service() while other goroutines may drain under write lock.
	lingerScheduled atomic.Int32
}

func NewLogBuffersRegistry(mediaDriverDir string) *LogBuffersRegistry {
	registry := &LogBuffersRegistry{
		entries:        make(map[string]*logBufferEntry),
		mediaDriverDir: mediaDriverDir,
	}
	if mediaDriverDir != "" {
		if resolved, err := resolveLogFileMediaDriverDir(mediaDriverDir); err == nil {
			registry.resolvedMediaDriverDir = resolved
		}
	}
	return registry
}

func (registry *LogBuffersRegistry) Acquire(logFileName string) (*logbuffer.LogBuffers, error) {
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return nil, err
	}
	return registry.acquireByKey(key)
}

func (registry *LogBuffersRegistry) resolveLogFileKey(logFileName string) (string, error) {
	if registry.resolvedMediaDriverDir != "" {
		return resolveLogFileKeyWithResolvedMediaDriverDir(
			logFileName,
			registry.resolvedMediaDriverDir,
		)
	}
	return resolveLogFileKey(logFileName, registry.mediaDriverDir)
}

func (registry *LogBuffersRegistry) acquireByKey(key string) (*logbuffer.LogBuffers, error) {
	if key == "" {
		return nil, ErrLogFileNameEmpty
	}

	entry := registry.entries[key]
	if entry != nil {
		registry.decrementLingerScheduled(entry)
		entry.refCount++
		entry.lingerDeadlineNs = 0
		return entry.buffers, nil
	}

	mapped, err := logbuffer.MapLogFile(key)
	if err != nil {
		return nil, ErrLogBufferMapFailed
	}

	buffers, err := logbuffer.NewLogBuffers(mapped)
	if err != nil {
		_ = mapped.Close()
		return nil, err
	}

	registry.entries[key] = &logBufferEntry{
		buffers:  buffers,
		refCount: 1,
	}
	return buffers, nil
}

// Release decrements the refcount for logFileName. When the count reaches zero
// the mapped log is closed immediately when lingerNs is zero. A positive lingerNs
// schedules unmap after the deadline via SweepLingering. Releasing an unknown or
// unresolvable path is an intentional no-op.
func (registry *LogBuffersRegistry) Release(logFileName string, lingerNs int64) {
	nowNs := int64(0)
	if lingerNs > 0 {
		nowNs = timeNowNs()
	}
	registry.releaseAt(logFileName, nowNs, lingerNs)
}

func (registry *LogBuffersRegistry) releaseAt(
	logFileName string,
	nowNs int64,
	lingerNs int64,
) {
	if registry == nil || logFileName == "" {
		return
	}

	// Unknown or unresolvable path is an intentional no-op.
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return
	}
	registry.releaseResolvedKeyAt(key, nowNs, lingerNs)
}

// releaseResolvedKey: lifecycle must use the acquire-time resolved key, not
// Release, because re-resolve can fail after acquire (e.g. EvalSymlinks).
func (registry *LogBuffersRegistry) releaseResolvedKey(key string, lingerNs int64) {
	nowNs := int64(0)
	if lingerNs > 0 {
		nowNs = timeNowNs()
	}
	registry.releaseResolvedKeyAt(key, nowNs, lingerNs)
}

func (registry *LogBuffersRegistry) releaseResolvedKeyAt(
	key string,
	nowNs int64,
	lingerNs int64,
) {
	if registry == nil || key == "" {
		return
	}

	entry := registry.entries[key]
	if entry == nil {
		return
	}

	lingerNs = normalizeLingerNs(lingerNs)
	// refCount <= 0 panic is intentional fail-fast during linger, not idempotent.
	if entry.refCount <= 0 {
		panic("conductor: log buffer release underflow")
	}

	entry.refCount--
	// lingerNs applies only when refCount hits zero; intermediate releases ignore it.
	if entry.refCount > 0 {
		return
	}

	if lingerNs == 0 {
		registry.deleteEntry(key, entry)
		return
	}

	entry.lingerDeadlineNs = nowNs + lingerNs
	registry.lingerScheduled.Add(1)
}

func (registry *LogBuffersRegistry) deleteEntry(key string, entry *logBufferEntry) {
	registry.decrementLingerScheduled(entry)
	delete(registry.entries, key)
	if mapped := entry.buffers.MappedLogBuffer(); mapped != nil {
		_ = mapped.Close()
	}
}

// SweepLingering closes and removes entries whose linger deadline has passed.
func (registry *LogBuffersRegistry) SweepLingering(nowNs int64) int {
	if registry == nil {
		return 0
	}

	workCount := 0
	for key, entry := range registry.entries {
		if entry.refCount != 0 ||
			entry.lingerDeadlineNs == 0 ||
			nowNs < entry.lingerDeadlineNs {
			continue
		}
		registry.deleteEntry(key, entry)
		workCount++
	}
	return workCount
}

func (registry *LogBuffersRegistry) lingerDeadlineNs(logFileName string) int64 {
	_, entry, ok := registry.lookupLogFile(logFileName)
	if !ok || entry == nil {
		return 0
	}
	return entry.lingerDeadlineNs
}

func (registry *LogBuffersRegistry) lookupLogFile(
	logFileName string,
) (
	key string,
	entry *logBufferEntry,
	ok bool,
) {
	if registry == nil {
		return "", nil, false
	}

	var err error
	key, err = registry.resolveLogFileKey(logFileName)
	if err != nil {
		return "", nil, false
	}

	entry, ok = registry.entries[key]
	return key, entry, ok
}

func (registry *LogBuffersRegistry) refCount(logFileName string) int32 {
	_, entry, ok := registry.lookupLogFile(logFileName)
	if !ok || entry == nil {
		return 0
	}
	return entry.refCount
}

func (registry *LogBuffersRegistry) contains(logFileName string) bool {
	_, _, ok := registry.lookupLogFile(logFileName)
	return ok
}

func (registry *LogBuffersRegistry) decrementLingerScheduled(entry *logBufferEntry) {
	if entry.lingerDeadlineNs != 0 {
		registry.lingerScheduled.Add(-1)
	}
}

// HasScheduledLinger reports whether any entry is awaiting linger sweep.
func (registry *LogBuffersRegistry) HasScheduledLinger() bool {
	return registry != nil && registry.lingerScheduled.Load() > 0
}

func (registry *LogBuffersRegistry) lingerScheduledCount() int {
	if registry == nil {
		return 0
	}
	return int(registry.lingerScheduled.Load())
}

func (registry *LogBuffersRegistry) countScheduledLingers() int {
	if registry == nil {
		return 0
	}

	count := 0
	for _, entry := range registry.entries {
		if entry.refCount == 0 && entry.lingerDeadlineNs != 0 {
			count++
		}
	}
	return count
}
