//go:build !amd64 || race

package atomicbuffer

import "sync/atomic"

// PutInt32Release stores value with release order, like Agrona putIntRelease.
func (b AtomicBuffer) PutInt32Release(offset int, value int32) {
	atomic.StoreInt32((*int32)(addressAt(b.buf, offset, SizeInt32)), value)
}

// PutInt64Release stores value with release order, like Agrona putLongRelease.
func (b AtomicBuffer) PutInt64Release(offset int, value int64) {
	atomic.StoreInt64((*int64)(addressAt(b.buf, offset, SizeInt64)), value)
}

// StoreInt64Release stores value at ptr with release order, like Agrona
// putLongRelease. Get ptr from Int64Pointer.
func StoreInt64Release(ptr *int64, value int64) {
	atomic.StoreInt64(ptr, value)
}
