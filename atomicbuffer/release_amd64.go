//go:build !race

package atomicbuffer

// PutInt32Release stores value with release order, like Agrona putIntRelease.
//
// The store compiles to one plain MOVL:
//   - ReleaseFence keeps the compiler from moving an earlier load or store past
//     the store. Its atomic load sits on the memory chain ahead of the store.
//   - The x86 total store order keeps the store behind every earlier load and
//     store in hardware.
func (b AtomicBuffer) PutInt32Release(offset int, value int32) {
	ptr := (*int32)(addressAt(b.buf, offset, SizeInt32))
	ReleaseFence()
	*ptr = value
}

// PutInt64Release stores value with release order, like Agrona putLongRelease.
//
// The store compiles to one plain MOVQ. PutInt32Release states the ordering
// argument.
func (b AtomicBuffer) PutInt64Release(offset int, value int64) {
	ptr := (*int64)(addressAt(b.buf, offset, SizeInt64))
	ReleaseFence()
	*ptr = value
}

// StoreInt64Release stores value at ptr with release order, like Agrona
// putLongRelease. Get ptr from Int64Pointer. PutInt32Release states the
// ordering argument.
func StoreInt64Release(ptr *int64, value int64) {
	ReleaseFence()
	*ptr = value
}

// StoreInt64ReleaseStoreFence stores value at ptr with release order, then
// keeps later stores behind it. It matches Agrona putLongRelease followed by
// Java VarHandle.storeStoreFence. Get ptr from Int64Pointer, or from a slice
// whose length the caller checked. ptr must be 8-byte aligned.
//
// The store compiles to one plain MOVQ between two fenceWord loads:
//   - The leading load gives release order, as in PutInt32Release.
//   - The trailing load keeps the compiler from moving a later store ahead of
//     this one. The x86 total store order keeps store order in hardware.
//
// The loads read fenceWord directly. A ReleaseFence call costs more inline
// budget. HeaderWriter.WriteHeader must stay inlinable.
func StoreInt64ReleaseStoreFence(ptr *int64, value int64) {
	fenceWord.Load()
	*ptr = value
	fenceWord.Load()
}

// ReleaseFence orders loads and stores before the call ahead of stores after
// the call, like Java VarHandle.releaseFence. Holds on amd64 and arm64. The
// x86 total store order gives the release order. The atomic load keeps
// compiler order and inlines to one MOVL.
func ReleaseFence() {
	fenceWord.Load()
}
