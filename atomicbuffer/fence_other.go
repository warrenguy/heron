//go:build !arm64

package atomicbuffer

// AcquireFence orders loads before the call ahead of loads and stores after
// the call, like Java VarHandle.acquireFence. Holds on amd64 and arm64.
func AcquireFence() {
	fencePace.Load()
}
