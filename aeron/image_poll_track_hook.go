//go:build testhooks

package aeron

// pollAfterPaddingAdvanceHook is unsynchronized; tests must not mutate it
// concurrently without serializing set/clear with poll execution.
var pollAfterPaddingAdvanceHook func()

func trackPollAfterPaddingAdvance() {
	if pollAfterPaddingAdvanceHook != nil {
		pollAfterPaddingAdvanceHook()
	}
}

func setPollAfterPaddingAdvanceHook(hook func()) {
	pollAfterPaddingAdvanceHook = hook
}
