//go:build !race

package atomicbuffer

import _ "unsafe"

// PutInt32Release stores value with release order, like Agrona putIntRelease.
func (b AtomicBuffer) PutInt32Release(offset int, value int32) {
	storeRelease32((*int32)(addressAt(b.buf, offset, SizeInt32)), value)
}

// PutInt64Release stores value with release order, like Agrona putLongRelease.
func (b AtomicBuffer) PutInt64Release(offset int, value int64) {
	storeRelease64((*int64)(addressAt(b.buf, offset, SizeInt64)), value)
}

// ReleaseFence orders loads and stores before the call ahead of stores after
// the call, like Java VarHandle.releaseFence. Holds on amd64 and arm64. The
// x86 total store order gives the release order. The atomic load keeps
// compiler order and inlines to one MOVL.
func ReleaseFence() {
	fenceWord.Load()
}

//go:linkname storeRelease32 atomicbuffer_storeRelease32
//go:noescape
func storeRelease32(ptr *int32, value int32)

//go:linkname storeRelease64 atomicbuffer_storeRelease64
//go:noescape
func storeRelease64(ptr *int64, value int64)
