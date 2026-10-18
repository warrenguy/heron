//go:build !arm64 && (!amd64 || race)

package atomicbuffer

// ReleaseFence orders loads and stores before the call ahead of stores after
// the call, like Java VarHandle.releaseFence. Holds on amd64 and arm64.
func ReleaseFence() {
	fenceWord.Store(0)
}
