package conductor

// underResourceWriteLock acquires resourceMu for exclusive map/registry access.
// It is not reentrant on the same goroutine during non-callback DoWork; hooks
// and tests must run off-agent or from image callbacks after the write lock is
// suspended. resourceWriteLockDepth counts outer DoWork scopes; suspendResourceWriteLock
// swaps depth to zero and unlocks, then resumeResourceWriteLock restores on return.
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
	assertInvokeImageCallbackUnderWriteLock(savedDepth)
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
