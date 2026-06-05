package atomic_test

import (
	"bytes"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
)

const testAtomicBufferSize = 64

var (
	testPayload4 = []byte{1, 2, 3, 4}
	testPayload2 = []byte{9, 10}
)

func newTestAtomicBuffer(tb testing.TB, size int) atomic.AtomicBuffer {
	tb.Helper()
	return atomic.Wrap(make([]byte, size))
}

func TestAtomicBuffer_Int32Roundtrip(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt32(0, 0x01020304)
	if got := atomicBuffer.GetInt32(0); got != 0x01020304 {
		t.Fatalf("got %x", got)
	}
}

func TestAtomicBuffer_Int64VolatileCAS(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
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

func TestAtomicBuffer_ZeroAllocs(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt64Release(0, 1)
	atomicBuffer.PutInt32Release(0, 1)

	var i64 int64
	var i32 int32
	src := testPayload4
	srcBuf := atomic.Wrap(src)

	cases := []struct {
		name string
		fn   func()
	}{
		{"GetInt64Volatile", func() { _ = atomicBuffer.GetInt64Volatile(0) }},
		{"GetInt64Acquire", func() { _ = atomicBuffer.GetInt64Acquire(0) }},
		{"PutInt64Release", func() {
			atomicBuffer.PutInt64Release(0, i64)
			i64++
		}},
		{"CompareAndSwapInt64", func() {
			expect := atomicBuffer.GetInt64Volatile(0)
			_ = atomicBuffer.CompareAndSwapInt64(0, expect, expect+1)
		}},
		{"PutInt32Release", func() {
			atomicBuffer.PutInt32Release(0, i32)
			i32++
		}},
		{"GetInt32Volatile", func() { _ = atomicBuffer.GetInt32Volatile(0) }},
		{"PutBytes", func() { atomicBuffer.PutBytes(0, src) }},
		{"PutBytesFrom", func() { atomicBuffer.PutBytesFrom(8, srcBuf, 0, 4) }},
		{"GetAndAddInt64", func() { _ = atomicBuffer.GetAndAddInt64(0, 1) }},
		{"SetBytes", func() { atomicBuffer.SetBytes(0, 8, 0) }},
		{"SetBytes_zeroLength", func() { atomicBuffer.SetBytes(0, 0, 0) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if allocs := testing.AllocsPerRun(1000, tc.fn); allocs != 0 {
				t.Fatalf("%s allocs = %v, want 0", tc.name, allocs)
			}
		})
	}
}

func TestAtomicBuffer_EmptyPanics(t *testing.T) {
	expectPanic(t, "GetInt32Volatile on nil buffer", func() {
		atomic.Wrap(nil).GetInt32Volatile(0)
	})
}

func TestAtomicBuffer_GetAndAddInt64ReturnsPrevious(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	if prev := atomicBuffer.GetAndAddInt64(0, 5); prev != 0 {
		t.Fatalf("first prev=%d want 0", prev)
	}
	if prev := atomicBuffer.GetAndAddInt64(0, 3); prev != 5 {
		t.Fatalf("second prev=%d want 5", prev)
	}
	if got := atomicBuffer.GetInt64Volatile(0); got != 8 {
		t.Fatalf("value=%d want 8", got)
	}
}

func TestAtomicBuffer_PutBytesSetBytes(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, 16)
	buf := atomicBuffer.Bytes()
	atomicBuffer.PutBytes(0, testPayload4)
	atomicBuffer.SetBytes(4, 4, 0xff)
	atomicBuffer.PutBytesFrom(8, atomic.Wrap(testPayload2), 0, 2)
	want := []byte{1, 2, 3, 4, 0xff, 0xff, 0xff, 0xff, 9, 10}
	if !bytes.Equal(buf[:len(want)], want) {
		t.Fatalf("buf=%v want %v", buf[:len(want)], want)
	}
}

func TestAtomicBuffer_SetBytes_zeroLength(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.SetBytes(0, 0, 0xff)
}

func TestAtomicBuffer_VerifyAlignment(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.VerifyAlignment(0, 8)
	atomicBuffer.VerifyAlignment(8, 8)
	expectPanic(t, "misaligned offset", func() {
		atomicBuffer.VerifyAlignment(4, 8)
	})
	expectPanic(t, "non-power-of-two field length", func() {
		atomicBuffer.VerifyAlignment(0, 6)
	})
}

func TestAtomicBuffer_SliceBounds(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, 16)

	sub := atomicBuffer.Slice(4, 8)
	if sub.Capacity() != 8 {
		t.Fatalf("capacity %d want 8", sub.Capacity())
	}

	expectPanic(t, "out-of-bounds slice", func() {
		atomicBuffer.Slice(10, 8)
	})
}

func expectPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic: %s", what)
		}
	}()
	fn()
}
