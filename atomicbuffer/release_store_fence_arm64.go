//go:build !race

package atomicbuffer

import (
	"sync/atomic"
	"unsafe"
)

// fenceWord is the target of the load-acquire in PutInt64ReleaseStoreFenceAt.
var fenceWord atomic.Uint32

// PutInt64ReleaseStoreFenceAt stores value at offset with release order, then
// keeps later stores behind it. It matches Agrona putLongRelease followed by
// Java VarHandle.storeStoreFence. It skips boundsCheck, like the other *At
// accessors. Unlike them, it does not reject an empty buffer. The caller proves
// offset+SizeInt64 <= Capacity.
//
// The store compiles to STLR. A load-acquire of fenceWord follows as LDAR. No
// DMB runs. The Barrier-ordered-before rules of the Arm memory model give the
// order (Arm Architecture Reference Manual for A-profile, DDI 0487, chapter
// B2, The AArch64 Application Level Memory Model):
//   - A store-release is ordered before a later load-acquire in program order.
//   - A load-acquire is ordered before every later load and store in program
//     order.
func (b AtomicBuffer) PutInt64ReleaseStoreFenceAt(offset int, value int64) {
	ptr := (*int64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b.buf)), offset))
	atomic.StoreInt64(ptr, value)
	fenceWord.Load()
}
