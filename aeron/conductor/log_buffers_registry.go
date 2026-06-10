package conductor

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
)

type logBufferEntry struct {
	buffers  *logbuffer.LogBuffers
	refCount int32
}

// LogBuffersRegistry refcount-maps log files so IPC publications and images
// can share one mmap when MediaDriverDir is configured. Not thread-safe; the
// client must not call DoWork concurrently with resource mutation.
type LogBuffersRegistry struct {
	entries                map[string]*logBufferEntry
	mediaDriverDir         string
	resolvedMediaDriverDir string
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
		entry.refCount++
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
// the mapped log is closed immediately when lingerNs is zero. Releasing an
// unknown or unresolvable path is an intentional no-op.
func (registry *LogBuffersRegistry) Release(logFileName string, lingerNs int64) {
	if registry == nil || logFileName == "" {
		return
	}
	if lingerNs != 0 {
		panic("conductor: log buffer linger is not implemented")
	}

	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return
	}
	if registry.entries[key] == nil {
		return
	}
	registry.releaseResolvedKey(key, lingerNs)
}

// releaseResolvedKey decrements refcount for an acquire-time resolved map key.
// Unknown keys are a no-op. Lifecycle paths use this instead of Release to avoid
// re-resolving a stored key that may fail EvalSymlinks after acquire.
func (registry *LogBuffersRegistry) releaseResolvedKey(key string, lingerNs int64) {
	if registry == nil || key == "" {
		return
	}
	if lingerNs != 0 {
		panic("conductor: log buffer linger is not implemented")
	}
	if registry.entries[key] == nil {
		return
	}
	registry.releaseByKey(key, lingerNs)
}

func (registry *LogBuffersRegistry) releaseByKey(key string, lingerNs int64) {
	_ = lingerNs
	entry := registry.entries[key]
	if entry.refCount <= 0 {
		panic("conductor: log buffer release underflow")
	}

	entry.refCount--
	if entry.refCount > 0 {
		return
	}

	delete(registry.entries, key)
	if mapped := entry.buffers.MappedLogBuffer(); mapped != nil {
		_ = mapped.Close()
	}
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
