package atomic

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

type AtomicBuffer struct {
	buf []byte
}

// Caller must keep buf reachable for hot-path use (e.g. hold the mmap Mapping).
func Wrap(buf []byte) AtomicBuffer {
	return AtomicBuffer{buf: buf}
}

func (b AtomicBuffer) Capacity() int {
	return len(b.buf)
}

func (b AtomicBuffer) Bytes() []byte {
	return b.buf
}

func (b AtomicBuffer) Slice(offset, length int) AtomicBuffer {
	boundsCheck(b.buf, offset, length)
	return AtomicBuffer{buf: b.buf[offset : offset+length]}
}

func boundsCheck(buf []byte, offset, size int) {
	if offset < 0 || size < 0 || offset > len(buf)-size {
		panic("atomic: offset out of bounds")
	}
}

func addressAt(buf []byte, offset, size int) unsafe.Pointer {
	boundsCheck(buf, offset, size)
	return unsafe.Pointer(uintptr(unsafe.Pointer(&buf[0])) + uintptr(offset))
}

func (b AtomicBuffer) GetInt32(offset int) int32 {
	return int32(binary.LittleEndian.Uint32((*[4]byte)(addressAt(b.buf, offset, 4))[:]))
}

func (b AtomicBuffer) PutInt32(offset int, value int32) {
	binary.LittleEndian.PutUint32((*[4]byte)(addressAt(b.buf, offset, 4))[:], uint32(value))
}

func (b AtomicBuffer) GetInt32Volatile(offset int) int32 {
	return atomic.LoadInt32((*int32)(addressAt(b.buf, offset, 4)))
}

func (b AtomicBuffer) PutInt32Release(offset int, value int32) {
	atomic.StoreInt32((*int32)(addressAt(b.buf, offset, 4)), value)
}

func (b AtomicBuffer) GetInt64(offset int) int64 {
	return int64(binary.LittleEndian.Uint64((*[8]byte)(addressAt(b.buf, offset, 8))[:]))
}

func (b AtomicBuffer) PutInt64(offset int, value int64) {
	binary.LittleEndian.PutUint64((*[8]byte)(addressAt(b.buf, offset, 8))[:], uint64(value))
}

func (b AtomicBuffer) GetInt64Volatile(offset int) int64 {
	return atomic.LoadInt64((*int64)(addressAt(b.buf, offset, 8)))
}

func (b AtomicBuffer) PutInt64Release(offset int, value int64) {
	atomic.StoreInt64((*int64)(addressAt(b.buf, offset, 8)), value)
}

func (b AtomicBuffer) CompareAndSwapInt64(offset int, expect, update int64) bool {
	return atomic.CompareAndSwapInt64((*int64)(addressAt(b.buf, offset, 8)), expect, update)
}

func (b AtomicBuffer) AddInt64(offset int, delta int64) int64 {
	return atomic.AddInt64((*int64)(addressAt(b.buf, offset, 8)), delta)
}
