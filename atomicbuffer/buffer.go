// Package atomicbuffer provides typed little-endian and atomic access to a byte
// slice.
//
// AtomicBuffer follows the Agrona AtomicBuffer accessors. The package also
// holds Agrona bit helpers and the Agrona ASCII string layout.
package atomicbuffer

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"unsafe"
)

const (
	SizeByte   = int(unsafe.Sizeof(byte(0)))
	SizeInt32  = int(unsafe.Sizeof(int32(0)))
	SizeInt64  = int(unsafe.Sizeof(int64(0)))
	SizeUint16 = int(unsafe.Sizeof(uint16(0)))
)

// AtomicBuffer provides typed access to a byte slice, including atomic
// operations for shared mmap regions.
//
// Plain GetInt32/PutInt32 (and int64, byte) are non-atomic loads/stores. Hot paths in
// ringbuffer that publish record headers across goroutines should use
// GetInt32Volatile/PutInt32Release. Broadcast pairs plain record writes with
// ReleaseFence and volatile tail counters (BroadcastReceiver ordering); migrating
// broadcast to volatile int32 accessors would be a separate change.
//
// *At accessors skip boundsCheck. Application code must use Get*. Internal tree
// packages may call *At only after bind-time or header bounds proof.
type AtomicBuffer struct {
	buf []byte
}

// Caller must keep buf reachable for hot-path use (e.g. hold the mmap Mapping).
func Wrap(buf []byte) AtomicBuffer {
	return AtomicBuffer{buf: buf}
}

// ValidateBackingSlice reports whether buf is non-empty and its base address is aligned.
func ValidateBackingSlice(buf []byte, alignment int) error {
	if len(buf) == 0 {
		return errors.New("atomicbuffer: empty buffer")
	}
	if uintptr(unsafe.Pointer(&buf[0]))%uintptr(alignment) != 0 {
		return errors.New("atomicbuffer: buffer base address is not aligned")
	}
	return nil
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

func sliceRangeInBounds(capacity, offset, length int) bool {
	return offset >= 0 && length >= 0 && offset <= capacity-length
}

func boundsCheck(buf []byte, offset, size int) {
	if !sliceRangeInBounds(len(buf), offset, size) {
		panic("atomicbuffer: offset out of bounds")
	}
}

func addressAtUnchecked(buf []byte, offset int) unsafe.Pointer {
	return unsafe.Add(unsafe.Pointer(&buf[0]), offset)
}

func addressAt(buf []byte, offset, size int) unsafe.Pointer {
	boundsCheck(buf, offset, size)
	return addressAtUnchecked(buf, offset)
}

func (b AtomicBuffer) GetInt32(offset int) int32 {
	boundsCheck(b.buf, offset, SizeInt32)
	return b.Int32At(offset)
}

func (b AtomicBuffer) Int32At(offset int) int32 {
	//nolint:gosec // G115: reads the two's complement bit pattern
	return int32(binary.LittleEndian.Uint32(b.buf[offset : offset+SizeInt32]))
}

func (b AtomicBuffer) PutInt32(offset int, value int32) {
	boundsCheck(b.buf, offset, SizeInt32)
	//nolint:gosec // G115: writes the two's complement bit pattern
	binary.LittleEndian.PutUint32(b.buf[offset:offset+SizeInt32], uint32(value))
}

func (b AtomicBuffer) GetInt32Volatile(offset int) int32 {
	boundsCheck(b.buf, offset, SizeInt32)
	return b.VolatileInt32At(offset)
}

func (b AtomicBuffer) VolatileInt32At(offset int) int32 {
	return atomic.LoadInt32((*int32)(addressAtUnchecked(b.buf, offset)))
}

func (b AtomicBuffer) PutInt32Release(offset int, value int32) {
	atomic.StoreInt32((*int32)(addressAt(b.buf, offset, SizeInt32)), value)
}

func (b AtomicBuffer) GetInt64(offset int) int64 {
	boundsCheck(b.buf, offset, SizeInt64)
	return b.Int64At(offset)
}

func (b AtomicBuffer) Int64At(offset int) int64 {
	//nolint:gosec // G115: reads the two's complement bit pattern
	return int64(binary.LittleEndian.Uint64(b.buf[offset : offset+SizeInt64]))
}

func (b AtomicBuffer) PutInt64(offset int, value int64) {
	boundsCheck(b.buf, offset, SizeInt64)
	//nolint:gosec // G115: writes the two's complement bit pattern
	binary.LittleEndian.PutUint64(b.buf[offset:offset+SizeInt64], uint64(value))
}

func (b AtomicBuffer) GetInt64Volatile(offset int) int64 {
	boundsCheck(b.buf, offset, SizeInt64)
	return b.VolatileInt64At(offset)
}

func (b AtomicBuffer) VolatileInt64At(offset int) int64 {
	return atomic.LoadInt64((*int64)(addressAtUnchecked(b.buf, offset)))
}

func (b AtomicBuffer) PutInt64Release(offset int, value int64) {
	atomic.StoreInt64((*int64)(addressAt(b.buf, offset, SizeInt64)), value)
}

func (b AtomicBuffer) GetByte(offset int) byte {
	boundsCheck(b.buf, offset, SizeByte)
	return b.ByteAt(offset)
}

func (b AtomicBuffer) PutByte(offset int, value byte) {
	boundsCheck(b.buf, offset, SizeByte)
	b.buf[offset] = value
}

func (b AtomicBuffer) ByteAt(offset int) byte {
	return b.buf[offset]
}

func (b AtomicBuffer) CompareAndSwapInt64(offset int, expect, update int64) bool {
	return atomic.CompareAndSwapInt64((*int64)(addressAt(b.buf, offset, SizeInt64)), expect, update)
}

func (b AtomicBuffer) CompareAndSwapInt32(offset int, expect, update int32) bool {
	return atomic.CompareAndSwapInt32((*int32)(addressAt(b.buf, offset, SizeInt32)), expect, update)
}

func (b AtomicBuffer) AddInt64(offset int, delta int64) int64 {
	return atomic.AddInt64((*int64)(addressAt(b.buf, offset, SizeInt64)), delta)
}

func copyBytes(dst []byte, dstOffset int, src []byte, srcOffset, length int) {
	boundsCheck(dst, dstOffset, length)
	boundsCheck(src, srcOffset, length)
	if length == 0 {
		return
	}
	copy(dst[dstOffset:dstOffset+length], src[srcOffset:srcOffset+length])
}

func (b AtomicBuffer) PutBytes(offset int, src []byte) {
	copyBytes(b.buf, offset, src, 0, len(src))
}

func (b AtomicBuffer) PutStringBytes(offset int, value string) {
	n := len(value)
	if n == 0 {
		return
	}
	copyBytes(b.buf, offset, unsafe.Slice(unsafe.StringData(value), n), 0, n)
}

func (b AtomicBuffer) PutBytesFrom(dstOffset int, src AtomicBuffer, srcOffset, length int) {
	copyBytes(b.buf, dstOffset, src.buf, srcOffset, length)
}

func (b AtomicBuffer) SetBytes(offset, length int, value byte) {
	if length == 0 {
		return
	}
	boundsCheck(b.buf, offset, length)
	seg := b.buf[offset : offset+length]
	if value == 0 {
		clear(seg)
		return
	}
	for i := range seg {
		seg[i] = value
	}
}

func (b AtomicBuffer) GetAndAddInt64(offset int, delta int64) int64 {
	return b.AddInt64(offset, delta) - delta
}

func (b AtomicBuffer) GetInt64Acquire(offset int) int64 {
	AcquireFence()
	return b.GetInt64Volatile(offset)
}

func (b AtomicBuffer) VerifyAlignment(offset, fieldLength int) {
	if fieldLength <= 0 || !IsPowerOfTwo(fieldLength) {
		panic("atomicbuffer: field length must be a positive power of two")
	}
	if Align(offset, fieldLength) != offset {
		panic("atomicbuffer: index is not aligned")
	}
}
