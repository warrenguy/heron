//go:build race

package atomicbuffer

import "sync/atomic"

// StoreInt64ReleaseStoreFence stores value at ptr with release order, then
// keeps later stores behind it. It matches Agrona putLongRelease followed by
// Java VarHandle.storeStoreFence. Get ptr from Int64Pointer, or from a slice
// whose length the caller checked. ptr must be 8-byte aligned.
func StoreInt64ReleaseStoreFence(ptr *int64, value int64) {
	atomic.StoreInt64(ptr, value)
	ReleaseFence()
}
