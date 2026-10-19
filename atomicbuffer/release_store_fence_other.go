//go:build race

package atomicbuffer

import (
	"sync/atomic"
	"unsafe"
)

// PutInt64ReleaseStoreFenceAt stores value at offset with release order, then
// keeps later stores behind it. It matches Agrona putLongRelease followed by
// Java VarHandle.storeStoreFence. It skips boundsCheck, like the other *At
// accessors. Unlike them, it does not reject an empty buffer. The caller proves
// offset+SizeInt64 <= Capacity.
func (b AtomicBuffer) PutInt64ReleaseStoreFenceAt(offset int, value int64) {
	ptr := (*int64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b.buf)), offset))
	atomic.StoreInt64(ptr, value)
	ReleaseFence()
}
