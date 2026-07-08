package ringbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/ringbuffer"
)

var (
	ringSink      any
	testPayload64 = make([]byte, 64)
)

func seedRingOneMessage(tb testing.TB, ring *ringbuffer.ManyToOneRingBuffer, payload []byte) {
	tb.Helper()
	src := atomicbuffer.Wrap(payload)
	ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
	require.NoError(tb, err)
	require.True(tb, ok)
}

func benchmarkRingNoopHandler(int, atomicbuffer.AtomicBuffer, int, int) {}

func benchmarkControlledContinue(
	int,
	atomicbuffer.AtomicBuffer,
	int,
	int,
) ringbuffer.ControlledAction {
	return ringbuffer.ControlledContinue
}

func setupEmptyRing(t *testing.T) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer) {
	t.Helper()
	return newTestRingBuffer(t, testDataCapacity)
}

func setupRingOneMessage(
	t *testing.T,
) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer) {
	t.Helper()
	ring, buffer := setupEmptyRing(t)
	seedRingOneMessage(t, ring, testPayload64)
	return ring, buffer
}

func setupRingCaughtUp(t *testing.T) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer) {
	t.Helper()
	ring, buffer := setupRingOneMessage(t)
	ring.Read(benchmarkRingNoopHandler)
	return ring, buffer
}

func setupUnblockEmptyRing(
	t *testing.T,
) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer) {
	t.Helper()
	ring, buffer := setupEmptyRing(t)
	emptyPos := int64(unblockMessageLength)
	seedUnblockCase(buffer, testDataCapacity, unblockCase{
		head: emptyPos,
		tail: emptyPos,
	})
	return ring, buffer
}

func benchmarkDrainRing(b *testing.B, ring *ringbuffer.ManyToOneRingBuffer) {
	b.Helper()
	for ring.Read(benchmarkRingNoopHandler) > 0 {
	}
}

func benchmarkPublishBatchSize(payloadLen int) int {
	perRecord := alignedRecordLength(payloadLen)
	if perRecord <= 0 {
		return 1
	}
	batch := testDataCapacity / perRecord / 2
	if batch < 1 {
		return 1
	}
	return batch
}

func benchmarkWriteMessage(
	b *testing.B,
	ring *ringbuffer.ManyToOneRingBuffer,
	src atomicbuffer.AtomicBuffer,
	payloadLen int,
) {
	b.Helper()
	for {
		ok, err := ring.Write(testMsgTypeID, src, 0, payloadLen)
		if err != nil {
			b.Fatal(err)
		}
		if ok {
			return
		}
	}
}

func benchmarkTryClaimMessage(
	b *testing.B,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomicbuffer.AtomicBuffer,
	payload []byte,
) {
	b.Helper()
	for {
		index, err := ring.TryClaim(testMsgTypeID, len(payload))
		if err != nil {
			b.Fatal(err)
		}
		if index >= 0 {
			buffer.PutBytes(index, payload)
			if err := ring.Commit(index); err != nil {
				b.Fatal(err)
			}
			return
		}
	}
}

func BenchmarkManyToOneRingBuffer_WritePublishBatch(b *testing.B) {
	ring, _ := newTestRingBuffer(b, testDataCapacity)
	src := atomicbuffer.Wrap(testPayload64)
	batch := benchmarkPublishBatchSize(len(testPayload64))

	b.ReportAllocs()
	b.SetBytes(int64(len(testPayload64)) * int64(batch))
	b.ResetTimer()
	for range b.N {
		for range batch {
			benchmarkWriteMessage(b, ring, src, len(testPayload64))
		}
		benchmarkDrainRing(b, ring)
	}
}

func BenchmarkManyToOneRingBuffer_WriteRoundTrip(b *testing.B) {
	ring, _ := newTestRingBuffer(b, testDataCapacity)
	src := atomicbuffer.Wrap(testPayload64)

	b.ReportAllocs()
	b.SetBytes(int64(len(testPayload64)))
	for b.Loop() {
		benchmarkWriteMessage(b, ring, src, len(testPayload64))
		ring.Read(benchmarkRingNoopHandler)
	}
}

func BenchmarkManyToOneRingBuffer_TryClaimPublishBatch(b *testing.B) {
	ring, buffer := newTestRingBuffer(b, testDataCapacity)
	batch := benchmarkPublishBatchSize(len(testPayload64))

	b.ReportAllocs()
	b.SetBytes(int64(len(testPayload64)) * int64(batch))
	b.ResetTimer()
	for range b.N {
		for range batch {
			benchmarkTryClaimMessage(b, ring, buffer, testPayload64)
		}
		benchmarkDrainRing(b, ring)
	}
}

func BenchmarkManyToOneRingBuffer_TryClaimCommitRoundTrip(b *testing.B) {
	ring, buffer := newTestRingBuffer(b, testDataCapacity)

	b.ReportAllocs()
	b.SetBytes(int64(len(testPayload64)))
	for b.Loop() {
		benchmarkTryClaimMessage(b, ring, buffer, testPayload64)
		ring.Read(benchmarkRingNoopHandler)
	}
}

func BenchmarkManyToOneRingBuffer_Read(b *testing.B) {
	benchmarkManyToOneRead(b, false)
}

func BenchmarkManyToOneRingBuffer_ControlledRead(b *testing.B) {
	benchmarkManyToOneRead(b, true)
}

func benchmarkManyToOneRead(b *testing.B, controlled bool) {
	b.Helper()
	ring, _ := newTestRingBuffer(b, testDataCapacity)
	src := atomicbuffer.Wrap(testPayload64)

	b.ReportAllocs()
	b.SetBytes(int64(len(testPayload64)))
	for b.Loop() {
		benchmarkWriteMessage(b, ring, src, len(testPayload64))
		if controlled {
			ringSink = ring.ControlledRead(benchmarkControlledContinue)
			continue
		}
		ringSink = ring.Read(benchmarkRingNoopHandler)
	}
}
