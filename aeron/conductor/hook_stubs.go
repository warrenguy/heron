//go:build !testhooks

package conductor

// suspendResourceWriteLock calls this before unlocking for image handlers.
// test_hooks.go replaces this stub under the testhooks tag and panics when
// depth<=0, catching invokeImageHandler off the mutation-scope write-lock path.
func assertInvokeImageHandlerUnderWriteLock(savedDepth int32) {}

// service calls this before linger sweep under resourceMu write lock.
// hook_access.go invokes linger sweep blocker side table under testhooks.
func invokeLingerSweepBlocker(conductor *Conductor) {}

// pollAndCopy calls this after ReceiveNext and before the lap check.
// hook_access.go invokes the receive next hook side table under testhooks.
func invokeReceiveNextHook(copyReceiver *CopyBroadcastReceiver) {}

// pollAndCopy calls this after the copy and before Validate.
// hook_access.go invokes the receive copied hook side table under testhooks.
func invokeReceiveCopiedHook(copyReceiver *CopyBroadcastReceiver) {}

// deliverCopied calls this after Validate and before the handler.
// hook_access.go invokes the receive validated hook side table under testhooks.
func invokeReceiveValidatedHook(copyReceiver *CopyBroadcastReceiver) {}
