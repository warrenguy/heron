//go:build !testhooks

package conductor

// suspendResourceWriteLock calls this before unlocking for image handlers.
// test_hooks.go replaces this stub under the testhooks tag and panics when
// depth<=0, catching invokeImageCallback off the mutation-scope write-lock path.
func assertInvokeImageCallbackUnderWriteLock(savedDepth int32) {}

// service calls this before linger sweep under resourceMu write lock.
// hook_access.go invokes Conductor.lingerSweepBlocker when set under testhooks.
func invokeLingerSweepBlockerForTest(conductor *Conductor) {}
