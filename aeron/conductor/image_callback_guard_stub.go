//go:build !testhooks

package conductor

// suspendResourceWriteLock calls this before unlocking for image handlers.
// test_hooks.go replaces this stub under the testhooks tag and panics when
// depth<=0, catching invokeImageCallback off the DoWork write-lock path.
func assertInvokeImageCallbackUnderWriteLock(savedDepth int32) {}
