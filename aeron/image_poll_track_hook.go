//go:build testhooks

package aeron

import "github.com/warrenguy/heron/internal/testhook"

// pollAfterPaddingAdvanceHooks is keyed by *Image. Concurrent Poll peers must
// not observe another test's hook.
var pollAfterPaddingAdvanceHooks = testhook.NewRegistry[*Image, func()]()

func trackPollAfterPaddingAdvance(image *Image) {
	if hook := pollAfterPaddingAdvanceHooks.Load(image); hook != nil {
		hook()
	}
}

func setPollAfterPaddingAdvanceHook(image *Image, hook func()) {
	if hook == nil {
		pollAfterPaddingAdvanceHooks.Delete(image)
		return
	}
	pollAfterPaddingAdvanceHooks.Store(image, hook)
}
