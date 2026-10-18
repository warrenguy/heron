//go:build !arm64

package atomicbuffer

import "sync/atomic"

var fenceWord atomic.Uint32

// AcquireFence orders loads before the call ahead of loads and stores after
// the call, like Java VarHandle.acquireFence. Holds on amd64 and arm64.
func AcquireFence() {
	fenceWord.Load()
}

// ReleaseFence orders loads and stores before the call ahead of stores after
// the call, like Java VarHandle.releaseFence. Holds on amd64 and arm64.
func ReleaseFence() {
	fenceWord.Store(0)
}
