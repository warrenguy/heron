// Command fenceprobe links the atomicbuffer fences and release stores for
// objdump. Each release store sits in its own non-inlined wrapper.
package main

import "github.com/warrenguy/heron/atomicbuffer"

//go:noinline
func putInt32Release(buf atomicbuffer.AtomicBuffer, value int32) {
	buf.PutInt32Release(0, value)
}

//go:noinline
func putInt64Release(buf atomicbuffer.AtomicBuffer, value int64) {
	buf.PutInt64Release(8, value)
}

//go:noinline
func storeInt64Release(ptr *int64, value int64) {
	atomicbuffer.StoreInt64Release(ptr, value)
}

//go:noinline
func putInt64ReleaseStoreFenceAt(buf atomicbuffer.AtomicBuffer, value int64) {
	buf.PutInt64ReleaseStoreFenceAt(8, value)
}

func main() {
	buf := atomicbuffer.Wrap(make([]byte, 16))
	putInt32Release(buf, -5)
	atomicbuffer.ReleaseFence()
	putInt64Release(buf, -7)
	storeInt64Release(buf.Int64Pointer(8), -9)
	putInt64ReleaseStoreFenceAt(buf, -11)
	atomicbuffer.AcquireFence()
}
