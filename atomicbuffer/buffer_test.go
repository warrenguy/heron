package atomicbuffer_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

const testAtomicBufferSize = 64

var (
	testPayload4 = []byte{1, 2, 3, 4}
	testPayload2 = []byte{9, 10}
)

func newTestAtomicBuffer(tb testing.TB, size int) atomicbuffer.AtomicBuffer {
	tb.Helper()
	return atomicbuffer.Wrap(make([]byte, size))
}

func TestAtomicBuffer_ByteRoundtrip(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutByte(0, 0xab)
	require.Equal(t, byte(0xab), atomicBuffer.GetByte(0))
}

func TestAtomicBuffer_Int32Roundtrip(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt32(0, 0x01020304)
	require.Equal(t, int32(0x01020304), atomicBuffer.GetInt32(0))
}

func TestAtomicBuffer_Int64VolatileCAS(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt64Release(8, 42)
	require.Equal(t, int64(42), atomicBuffer.GetInt64Volatile(8))
	require.True(t, atomicBuffer.CompareAndSwapInt64(8, 42, 99))
	require.Equal(t, int64(99), atomicBuffer.GetInt64Volatile(8))
	atomicBuffer.PutInt64Volatile(8, -7)
	require.Equal(t, int64(-7), atomicBuffer.GetInt64Volatile(8))
}

func TestAtomicBuffer_Int32VolatileCAS(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt32Release(0, 42)
	require.Equal(t, int32(42), atomicBuffer.GetInt32Volatile(0))
	require.True(t, atomicBuffer.CompareAndSwapInt32(0, 42, 99))
	require.Equal(t, int32(99), atomicBuffer.GetInt32Volatile(0))
	require.False(t, atomicBuffer.CompareAndSwapInt32(0, 42, 100))
	require.Equal(t, int32(99), atomicBuffer.GetInt32Volatile(0))
}

func TestAtomicBuffer_ZeroAllocs(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt64Release(0, 1)
	atomicBuffer.PutInt32Release(0, 1)

	var i64 int64
	var i32 int32
	src := testPayload4
	srcBuf := atomicbuffer.Wrap(src)

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
		{"PutInt64Release_stackBuffer", func() {
			var stack [16]byte
			atomicbuffer.Wrap(stack[:]).PutInt64Release(8, i64)
			i64++
		}},
		{"PutInt32Release_stackBuffer", func() {
			var stack [16]byte
			atomicbuffer.Wrap(stack[:]).PutInt32Release(4, i32)
			i32++
		}},
		{"PutInt64ReleaseStoreFenceAt", func() {
			atomicBuffer.PutInt64ReleaseStoreFenceAt(8, i64)
			i64++
		}},
		{"StoreInt64Release", func() {
			atomicbuffer.StoreInt64Release(atomicBuffer.Int64Pointer(8), i64)
			i64++
		}},
		{"PutInt64Volatile", func() {
			atomicBuffer.PutInt64Volatile(0, i64)
			i64++
		}},
		{"GetInt32Volatile", func() { _ = atomicBuffer.GetInt32Volatile(0) }},
		{"CompareAndSwapInt32", func() {
			expect := atomicBuffer.GetInt32Volatile(0)
			_ = atomicBuffer.CompareAndSwapInt32(0, expect, expect+1)
		}},
		{"PutBytes", func() { atomicBuffer.PutBytes(0, src) }},
		{"PutBytesFrom", func() { atomicBuffer.PutBytesFrom(8, srcBuf, 0, 4) }},
		{"GetAndAddInt64", func() { _ = atomicBuffer.GetAndAddInt64(0, 1) }},
		{"SetBytes", func() { atomicBuffer.SetBytes(0, 8, 0) }},
		{"SetBytes_zeroLength", func() { atomicBuffer.SetBytes(0, 0, 0) }},
		{"GetByte", func() { _ = atomicBuffer.GetByte(0) }},
		{"PutByte", func() { atomicBuffer.PutByte(0, 0x42) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testutil.AssertZeroAllocs(t, tc.name, 1000, tc.fn)
		})
	}
}

func TestAtomicBuffer_EmptyPanics(t *testing.T) {
	require.Panics(t, func() {
		atomicbuffer.Wrap(nil).GetInt32Volatile(0)
	}, "GetInt32Volatile on nil buffer")
}

func TestAtomicBuffer_TrustedAt_matchesBoundedGetters(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.PutInt32(0, 0x01020304)
	atomicBuffer.PutInt64Release(8, 0x0A0B0C0D0E0F1011)
	atomicBuffer.PutByte(4, 0x55)

	cases := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"Int32At", func(t *testing.T) {
			require.Equal(t, atomicBuffer.GetInt32(0), atomicBuffer.Int32At(0))
		}},
		{"Int64At", func(t *testing.T) {
			require.Equal(t, atomicBuffer.GetInt64(8), atomicBuffer.Int64At(8))
		}},
		{"VolatileInt32At", func(t *testing.T) {
			require.Equal(t, atomicBuffer.GetInt32Volatile(0), atomicBuffer.VolatileInt32At(0))
		}},
		{"VolatileInt64At", func(t *testing.T) {
			require.Equal(t, atomicBuffer.GetInt64Volatile(8), atomicBuffer.VolatileInt64At(8))
		}},
		{"ByteAt", func(t *testing.T) {
			require.Equal(t, atomicBuffer.GetByte(4), atomicBuffer.ByteAt(4))
		}},
		{"Int32Pointer", func(t *testing.T) {
			ptr := atomicBuffer.Int32Pointer(0)
			require.Equal(t, atomicBuffer.GetInt32Volatile(0), atomic.LoadInt32(ptr))
		}},
		{"Int64Pointer", func(t *testing.T) {
			ptr := atomicBuffer.Int64Pointer(8)
			require.Equal(t, atomicBuffer.GetInt64Volatile(8), atomic.LoadInt64(ptr))
		}},
		{"StoreInt64Release", func(t *testing.T) {
			const value = int64(-0x0102030405060708)
			released := newTestAtomicBuffer(t, testAtomicBufferSize)
			released.PutInt64Release(16, value)

			stored := newTestAtomicBuffer(t, testAtomicBufferSize)
			atomicbuffer.StoreInt64Release(stored.Int64Pointer(16), value)

			require.Equal(t, released.Bytes(), stored.Bytes())
			require.Equal(t, value, stored.GetInt64(16))
		}},
		{"PutInt64ReleaseStoreFenceAt", func(t *testing.T) {
			const value = int64(-0x0A0B0C0D0E0F1011)
			released := newTestAtomicBuffer(t, testAtomicBufferSize)
			released.PutInt64Release(8, value)

			fenced := newTestAtomicBuffer(t, testAtomicBufferSize)
			fenced.PutInt64ReleaseStoreFenceAt(8, value)

			require.Equal(t, released.Bytes(), fenced.Bytes())
			require.Equal(t, value, fenced.GetInt64(8))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.fn)
	}
}

func TestAtomicBuffer_TrustedAt_outOfBoundsPanics(t *testing.T) {
	int32Buffer := newTestAtomicBuffer(t, atomicbuffer.SizeInt32)
	int64Buffer := newTestAtomicBuffer(t, atomicbuffer.SizeInt64)
	byteBuffer := newTestAtomicBuffer(t, atomicbuffer.SizeByte)
	emptyBuffer := atomicbuffer.Wrap(nil)

	cases := []struct {
		name string
		fn   func()
	}{
		{"GetInt32 out of bounds", func() { int32Buffer.GetInt32(atomicbuffer.SizeInt32) }},
		{
			"GetInt32Volatile out of bounds",
			func() { int32Buffer.GetInt32Volatile(atomicbuffer.SizeInt32) },
		},
		{"GetInt64 out of bounds", func() { int64Buffer.GetInt64(atomicbuffer.SizeInt64) }},
		{
			"GetInt64Volatile out of bounds",
			func() { int64Buffer.GetInt64Volatile(atomicbuffer.SizeInt64) },
		},
		{"GetByte at capacity", func() { byteBuffer.GetByte(byteBuffer.Capacity()) }},
		{"GetByte negative offset", func() { byteBuffer.GetByte(-1) }},
		{"PutByte at capacity", func() { byteBuffer.PutByte(byteBuffer.Capacity(), 0x01) }},
		{"PutByte negative offset", func() { byteBuffer.PutByte(-1, 0x01) }},
		{"GetByte on nil buffer", func() { emptyBuffer.GetByte(0) }},
		{"PutByte on nil buffer", func() { emptyBuffer.PutByte(0, 0x01) }},
		{"VolatileInt32At on nil buffer", func() { emptyBuffer.VolatileInt32At(0) }},
		{"VolatileInt64At on nil buffer", func() { emptyBuffer.VolatileInt64At(0) }},
		{"ByteAt on nil buffer", func() { emptyBuffer.ByteAt(0) }},
		{"Int32Pointer out of bounds", func() { int32Buffer.Int32Pointer(1) }},
		{"Int64Pointer out of bounds", func() { int64Buffer.Int64Pointer(1) }},
		{"Int64Pointer negative offset", func() { int64Buffer.Int64Pointer(-1) }},
		{"Int64Pointer on nil buffer", func() { emptyBuffer.Int64Pointer(0) }},
	}
	for _, tc := range cases {
		require.Panics(t, tc.fn, tc.name)
	}
}

func TestAtomicBuffer_GetAndAddInt64ReturnsPrevious(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	require.Equal(t, int64(0), atomicBuffer.GetAndAddInt64(0, 5))
	require.Equal(t, int64(5), atomicBuffer.GetAndAddInt64(0, 3))
	require.Equal(t, int64(8), atomicBuffer.GetInt64Volatile(0))
}

func TestAtomicBuffer_PutBytesSetBytes(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, 16)
	buf := atomicBuffer.Bytes()
	atomicBuffer.PutBytes(0, testPayload4)
	atomicBuffer.SetBytes(4, 4, 0xff)
	atomicBuffer.PutBytesFrom(8, atomicbuffer.Wrap(testPayload2), 0, 2)
	want := []byte{1, 2, 3, 4, 0xff, 0xff, 0xff, 0xff, 9, 10}
	require.Equal(t, want, buf[:len(want)])
}

func TestAtomicBuffer_SetBytes_zeroLength(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.SetBytes(0, 0, 0xff)
}

func TestAtomicBuffer_VerifyAlignment(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, testAtomicBufferSize)
	atomicBuffer.VerifyAlignment(0, 8)
	atomicBuffer.VerifyAlignment(8, 8)
	require.Panics(t, func() {
		atomicBuffer.VerifyAlignment(4, 8)
	}, "misaligned offset")
	require.Panics(t, func() {
		atomicBuffer.VerifyAlignment(0, 6)
	}, "non-power-of-two field length")
}

func TestAtomicPrimitiveSizes(t *testing.T) {
	require.Equal(t, 1, atomicbuffer.SizeByte)
	require.Equal(t, 4, atomicbuffer.SizeInt32)
	require.Equal(t, 8, atomicbuffer.SizeInt64)
	require.Equal(t, 2, atomicbuffer.SizeUint16)
}

func TestAtomicBuffer_SliceBounds(t *testing.T) {
	atomicBuffer := newTestAtomicBuffer(t, 16)

	sub := atomicBuffer.Slice(4, 8)
	require.Equal(t, 8, sub.Capacity())

	require.Panics(t, func() {
		atomicBuffer.Slice(10, 8)
	}, "out-of-bounds slice")
}
