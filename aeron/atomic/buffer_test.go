package atomic_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestAtomicBuffer_Int32Roundtrip(t *testing.T) {
	buf := make([]byte, 64)
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt32(0, 0x01020304)
	if got := atomicBuffer.GetInt32(0); got != 0x01020304 {
		t.Fatalf("got %x", got)
	}
}

func TestAtomicBuffer_Int64VolatileCAS(t *testing.T) {
	buf := make([]byte, 64)
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt64Release(8, 42)
	if got := atomicBuffer.GetInt64Volatile(8); got != 42 {
		t.Fatalf("got %d", got)
	}
	if !atomicBuffer.CompareAndSwapInt64(8, 42, 99) {
		t.Fatal("CAS failed")
	}
	if atomicBuffer.GetInt64Volatile(8) != 99 {
		t.Fatal("CAS did not update")
	}
}

func TestAtomicBuffer_LoadInt64Volatile_ZeroAllocs(t *testing.T) {
	buf := make([]byte, 64)
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt64Release(0, 1)
	if allocs := testing.AllocsPerRun(1000, func() {
		_ = atomicBuffer.GetInt64Volatile(0)
	}); allocs != 0 {
		t.Fatalf("GetInt64Volatile allocs = %v, want 0", allocs)
	}
}

func TestAtomicBuffer_PutInt64Release_ZeroAllocs(t *testing.T) {
	buf := make([]byte, 64)
	atomicBuffer := atomic.Wrap(buf)
	var value int64
	if allocs := testing.AllocsPerRun(1000, func() {
		atomicBuffer.PutInt64Release(0, value)
		value++
	}); allocs != 0 {
		t.Fatalf("PutInt64Release allocs = %v, want 0", allocs)
	}
}

func TestAtomicBuffer_EmptyPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on empty buffer")
		}
	}()
	atomic.Wrap(nil).GetInt32Volatile(0)
}

func TestAtomicBuffer_SliceBounds(t *testing.T) {
	buf := make([]byte, 16)
	atomicBuffer := atomic.Wrap(buf)

	sub := atomicBuffer.Slice(4, 8)
	if sub.Capacity() != 8 {
		t.Fatalf("capacity %d want 8", sub.Capacity())
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on out-of-bounds slice")
		}
	}()
	atomicBuffer.Slice(10, 8)
}
