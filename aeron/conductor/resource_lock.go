package conductor

// underResourceWriteLock acquires resourceMu for exclusive map/registry mutation.
// It is not reentrant on the same goroutine unless resourceMu was suspended first
// (image handlers only); nested calls deadlock. Hooks and tests must run off-agent
// or from image handlers after suspendResourceWriteLock unlocks resourceMu.
// resourceWriteLockDepth is a held flag (0 or 1) for suspend/resume bookkeeping,
// not a nested-scope counter; suspendResourceWriteLock saves it and unlocks,
// resumeResourceWriteLock restores it for the outer defer.
func (conductor *Conductor) underResourceWriteLock(work func()) {
	conductor.resourceMu.Lock()
	conductor.resourceWriteLockDepth++
	defer func() {
		conductor.resourceWriteLockDepth--
		if conductor.resourceWriteLockDepth == 0 {
			conductor.resourceMu.Unlock()
		}
	}()
	work()
}

func underResourceWriteLockResult[T any](conductor *Conductor, work func() T) T {
	var result T
	conductor.underResourceWriteLock(func() { result = work() })
	return result
}

func (conductor *Conductor) suspendResourceWriteLock() int32 {
	savedDepth := conductor.resourceWriteLockDepth
	assertInvokeImageHandlerUnderWriteLock(savedDepth)
	if savedDepth > 0 {
		conductor.resourceWriteLockDepth = 0
		conductor.resourceMu.Unlock()
	}
	return savedDepth
}

func (conductor *Conductor) resumeResourceWriteLock(savedDepth int32) {
	if savedDepth > 0 {
		conductor.resourceMu.Lock()
		conductor.resourceWriteLockDepth = savedDepth
	}
}
