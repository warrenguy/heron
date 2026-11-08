//go:build !race

package atomicbuffer

import "sync/atomic"

// fenceWord is the target of the load-acquire in StoreInt64ReleaseStoreFence.
var fenceWord atomic.Uint32

// StoreInt64ReleaseStoreFence stores value at ptr with release order, then
// keeps later stores behind it. It matches Agrona putLongRelease followed by
// Java VarHandle.storeStoreFence. Get ptr from Int64Pointer, or from a slice
// whose length the caller checked. ptr must be 8-byte aligned.
//
// The store compiles to STLR. The load-acquire of fenceWord compiles to LDAR.
// The sequence does not run DMB. The Barrier-ordered-before rules give the
// order (Arm Architecture Reference Manual, DDI 0487, chapter B2):
//   - A store-release is ordered before a later load-acquire in program order.
//   - A load-acquire is ordered before every later load and store in program
//     order.
func StoreInt64ReleaseStoreFence(ptr *int64, value int64) {
	atomic.StoreInt64(ptr, value)
	fenceWord.Load()
}
