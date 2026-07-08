package ringbuffer_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
	"github.com/warrenguy/heron/ringbuffer"
)

const (
	testMsgTypeID    = 7
	testDataCapacity = 4096
)

func newRingBufferBuffer(
	dataCapacity int,
) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer, error) {
	buf := make([]byte, ringbuffer.TotalBufferLength(dataCapacity))
	atomicBuffer := atomicbuffer.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(atomicBuffer)
	if err != nil {
		return nil, atomicbuffer.AtomicBuffer{}, err
	}
	return ring, atomicBuffer, nil
}

func seedTrailerPositions(
	buffer atomicbuffer.AtomicBuffer,
	dataCapacity int,
	head, tail int64,
) {
	buffer.PutInt64Release(ringbuffer.HeadIndex(dataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(dataCapacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(dataCapacity), head)
}

func alignedRecordLength(payloadLen int) int {
	return record.AlignRecordLength(payloadLen + ringbuffer.HeaderLength)
}

func clampMaxMsgLen(maxLen, floor int) int {
	if maxLen < floor {
		return floor
	}
	return maxLen
}

func seedCommitAtEndCase(
	buffer atomicbuffer.AtomicBuffer,
	dataCapacity, msgLen int,
) {
	recordAligned := alignedRecordLength(msgLen)
	head := int64(dataCapacity - recordAligned)
	tail := head + int64(recordAligned)
	seedTrailerPositions(buffer, dataCapacity, head, tail)
	buffer.PutInt32Release(record.LengthOffset(int(head)), int32(recordAligned))
	buffer.PutInt32(record.TypeOffset(int(head)), int32(testMsgTypeID))
}

type testReporter interface {
	Helper()
	Errorf(string, ...any)
	FailNow()
}

func assertConsumerCaughtUp(tb testReporter, ring *ringbuffer.ManyToOneRingBuffer) {
	tb.Helper()
	require.Equal(tb, ring.ProducerPosition(), ring.ConsumerPosition())
}

func drainReadMessages(ring *ringbuffer.ManyToOneRingBuffer) [][]byte {
	var got [][]byte
	for {
		n := ring.Read(func(_ int, buffer atomicbuffer.AtomicBuffer, index, length int) {
			got = append(got, readPayload(buffer, index, length))
		})
		if n == 0 {
			break
		}
	}
	return got
}

func readPayload(buffer atomicbuffer.AtomicBuffer, index, length int) []byte {
	b := buffer.Bytes()
	out := make([]byte, length)
	copy(out, b[index:index+length])
	return out
}

func assertConsumerUnblockedPadding(
	tb testReporter,
	buffer atomicbuffer.AtomicBuffer,
	consumerIndex, wantPaddingLength int,
) {
	tb.Helper()
	require.Equal(
		tb, int32(ringbuffer.PaddingMsgTypeID), buffer.GetInt32(record.TypeOffset(consumerIndex)),
	)
	require.Equal(tb, int32(wantPaddingLength), buffer.GetInt32(record.LengthOffset(consumerIndex)))
}

type unblockCase struct {
	name          string
	head, tail    int64
	lengthIndex   int
	lengthValue   int32
	wantUnblock   bool
	paddingIndex  int
	paddingLength int
}

const unblockMessageLength = ringbuffer.Alignment * 4

func unblockGapWithZerosCase(messageLength int) unblockCase {
	head := int64(messageLength)
	return unblockCase{
		name:          "GapWithZeros",
		head:          head,
		tail:          head * 3,
		lengthIndex:   messageLength * 2,
		lengthValue:   int32(messageLength),
		wantUnblock:   true,
		paddingIndex:  messageLength,
		paddingLength: messageLength,
	}
}

func seedUnblockCase(
	buffer atomicbuffer.AtomicBuffer,
	dataCapacity int,
	tc unblockCase,
) {
	seedTrailerPositions(buffer, dataCapacity, tc.head, tc.tail)
	if tc.lengthIndex != 0 || tc.lengthValue != 0 {
		buffer.PutInt32(tc.lengthIndex, tc.lengthValue)
	}
}

func assertUnblockResult(
	tb testReporter,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomicbuffer.AtomicBuffer,
	tc unblockCase,
) {
	tb.Helper()
	require.Equal(tb, tc.wantUnblock, ring.Unblock())
	if tc.paddingLength > 0 {
		assertConsumerUnblockedPadding(tb, buffer, tc.paddingIndex, tc.paddingLength)
	}
}

func newTestRingBuffer(
	tb testReporter,
	dataCapacity int,
) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer) {
	tb.Helper()
	ring, atomicBuffer, err := newRingBufferBuffer(dataCapacity)
	require.NoError(tb, err)
	return ring, atomicBuffer
}

func TestNewManyToOneRingBuffer_Capacity(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	require.Equal(t, testDataCapacity, ring.Capacity())
}

func TestNewManyToOneRingBuffer_InvalidCapacity(t *testing.T) {
	cases := []struct {
		name         string
		dataCapacity int
		bufferLen    int // 0 -> TotalBufferLength(dataCapacity)
	}{
		{"below min", ringbuffer.MinCapacity - 1, 0},
		{"not power of two", 777, 777 + ringbuffer.TrailerLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			total := tc.bufferLen
			if total == 0 {
				total = ringbuffer.TotalBufferLength(tc.dataCapacity)
			}
			_, err := ringbuffer.NewManyToOneRingBuffer(atomicbuffer.Wrap(make([]byte, total)))
			require.Error(t, err)
		})
	}
}

func TestManyToOneRingBuffer_MaxMsgLength(t *testing.T) {
	cases := []struct {
		dataCapacity int
		want         int
	}{
		{ringbuffer.MinCapacity, 0},
		{16, 8},
		{1024, 128},
		{testDataCapacity, 512},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("capacity%d", tc.dataCapacity), func(t *testing.T) {
			ring, _ := newTestRingBuffer(t, tc.dataCapacity)
			require.Equal(t, tc.want, ring.MaxMsgLength())
		})
	}
}

func TestManyToOneRingBuffer_WriteRead(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := []byte("abcdefgh")
	src := atomicbuffer.Wrap(payload)

	ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
	require.NoError(t, err)
	require.True(t, ok)

	var gotType int
	var gotPayload []byte
	n := ring.Read(func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
		gotType = msgTypeID
		gotPayload = readPayload(buffer, index, length)
	})
	require.Equal(t, 1, n)
	require.Equal(t, testMsgTypeID, gotType)
	require.Equal(t, payload, gotPayload)
	assertConsumerCaughtUp(t, ring)
}

func TestManyToOneRingBuffer_RejectWrite(t *testing.T) {
	cases := []struct {
		name   string
		length int
		tail   int64
	}{
		{
			name:   "insufficient space",
			length: 200,
			tail:   int64(testDataCapacity - record.AlignRecordLength(200-ringbuffer.Alignment)),
		},
		{
			name:   "when full",
			length: 8,
			tail:   int64(testDataCapacity),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, buffer := newTestRingBuffer(t, testDataCapacity)
			seedTrailerPositions(buffer, testDataCapacity, 0, tc.tail)
			src := atomicbuffer.Wrap(make([]byte, tc.length))
			ok, err := ring.Write(testMsgTypeID, src, 0, tc.length)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func TestManyToOneRingBuffer_WriteWrapPadding(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 200
	recordAligned := alignedRecordLength(length)
	tail := int64(testDataCapacity - ringbuffer.HeaderLength)
	head := tail

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	payload := bytes.Repeat([]byte{0xAB}, length)
	src := atomicbuffer.Wrap(payload)
	ok, err := ring.Write(testMsgTypeID, src, 0, length)
	require.NoError(t, err)
	require.True(t, ok)

	paddingIndex := int(tail)
	require.Equal(
		t, int32(ringbuffer.HeaderLength), buffer.GetInt32(record.LengthOffset(paddingIndex)),
	)
	require.Equal(
		t, int32(ringbuffer.PaddingMsgTypeID), buffer.GetInt32(record.TypeOffset(paddingIndex)),
	)

	var got []byte
	ring.Read(func(_ int, buf atomicbuffer.AtomicBuffer, index, msgLen int) {})
	ring.Read(func(_ int, buf atomicbuffer.AtomicBuffer, index, msgLen int) {
		got = readPayload(buf, index, msgLen)
	})
	require.Equal(t, payload, got)
	require.Equal(t, tail+int64(recordAligned)+int64(ringbuffer.HeaderLength), ring.ProducerPosition())
}

func TestManyToOneRingBuffer_WriteWrapHeadEqualsTail(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 200
	tail := int64(testDataCapacity - ringbuffer.HeaderLength)
	head := tail

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	payload := bytes.Repeat([]byte{0xCD}, length)
	ok, err := ring.Write(testMsgTypeID, atomicbuffer.Wrap(payload), 0, length)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int32(ringbuffer.PaddingMsgTypeID), buffer.GetInt32(record.TypeOffset(int(tail))))
}

func TestManyToOneRingBuffer_ReadEmpty(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	n := ring.Read(func(int, atomicbuffer.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	require.Equal(t, 0, n)
}

func TestManyToOneRingBuffer_ReadSkipsInFlight(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	recordLength := ringbuffer.HeaderLength + 16
	buffer.PutInt32Release(record.LengthOffset(0), int32(-recordLength))

	n := ring.Read(func(int, atomicbuffer.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	require.Equal(t, 0, n)
	require.Zero(t, ring.ConsumerPosition())
}

func TestManyToOneRingBuffer_ReadTwoMessages(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	msgLen := 16
	payload := bytes.Repeat([]byte{1}, msgLen)

	for range 2 {
		ok, err := ring.Write(testMsgTypeID, atomicbuffer.Wrap(payload), 0, msgLen)
		require.NoError(t, err)
		require.True(t, ok)
	}

	count := 0
	n := ring.Read(func(int, atomicbuffer.AtomicBuffer, int, int) {
		count++
	})
	require.Equal(t, 2, n)
	require.Equal(t, 2, count)
}

func TestManyToOneRingBuffer_ReadWithLimit_ZeroAndNegative(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := bytes.Repeat([]byte{2}, 16)

	_, err := ring.Write(testMsgTypeID, atomicbuffer.Wrap(payload), 0, len(payload))
	require.NoError(t, err)

	cases := []struct {
		name  string
		limit int
	}{
		{"zero", 0},
		{"negative", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			n := ring.ReadWithLimit(func(int, atomicbuffer.AtomicBuffer, int, int) {
				called = true
			}, tc.limit)
			require.Equal(t, 0, n)
			require.False(t, called)
			require.Zero(t, ring.ConsumerPosition())
		})
	}
}

func TestManyToOneRingBuffer_ReadWithLimit(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	msgLen := 16
	payload := bytes.Repeat([]byte{2}, msgLen)

	for range 2 {
		_, err := ring.Write(testMsgTypeID, atomicbuffer.Wrap(payload), 0, msgLen)
		require.NoError(t, err)
	}

	count := 0
	n := ring.ReadWithLimit(func(int, atomicbuffer.AtomicBuffer, int, int) {
		count++
	}, 1)
	require.Equal(t, 1, n)
	require.Equal(t, 1, count)
	require.NotEqual(t, ring.ProducerPosition(), ring.ConsumerPosition())
}

func TestManyToOneRingBuffer_TryClaimCommitRead(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 10
	payload := bytes.Repeat([]byte{3}, length)

	index, err := ring.TryClaim(testMsgTypeID, length)
	require.NoError(t, err)
	require.Equal(t, ringbuffer.EncodedMsgOffset(0), index)

	buffer.PutBytes(index, payload)
	require.NoError(t, ring.Commit(index))

	var got []byte
	ring.Read(func(_ int, buf atomicbuffer.AtomicBuffer, off, msgLen int) {
		got = readPayload(buf, off, msgLen)
	})
	require.Equal(t, payload, got)
}

func TestManyToOneRingBuffer_TryClaimAbortSkippedOnRead(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	length := 10

	index, err := ring.TryClaim(testMsgTypeID, length)
	require.NoError(t, err)
	require.NoError(t, ring.Abort(index))

	called := false
	n := ring.Read(func(int, atomicbuffer.AtomicBuffer, int, int) {
		called = true
	})
	require.Equal(t, 0, n)
	require.False(t, called)
}

func TestManyToOneRingBuffer_ClaimCapacityStaleHeadCache(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	const headCache = int64(0)
	head := int64(testDataCapacity * 3)
	tail := head

	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(testDataCapacity), headCache)
	buffer.PutInt64Release(ringbuffer.HeadIndex(testDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(testDataCapacity), tail)

	wantIndex := ringbuffer.EncodedMsgOffset(int(tail) & (testDataCapacity - 1))
	index, err := ring.TryClaim(testMsgTypeID, 8)
	require.NoError(t, err)
	require.NotEqual(t, ringbuffer.InsufficientCapacity, index)
	require.Equal(t, wantIndex, index)
	require.NoError(t, ring.Abort(index))

	src := atomicbuffer.Wrap([]byte("abcdefgh"))
	ok, err := ring.Write(testMsgTypeID, src, 0, 8)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestManyToOneRingBuffer_TryClaimInsufficientCapacity(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	head := int64(0)
	tail := int64(testDataCapacity)

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	index, err := ring.TryClaim(testMsgTypeID, 8)
	require.NoError(t, err)
	require.Equal(t, ringbuffer.InsufficientCapacity, index)
}

func TestManyToOneRingBuffer_TryClaimWrapPadding(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 10
	recordAligned := alignedRecordLength(length)
	head := int64(248)
	padding := 22
	tail := int64(testDataCapacity - padding)

	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(testDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(testDataCapacity), tail)

	index, err := ring.TryClaim(testMsgTypeID, length)
	require.NoError(t, err)
	require.Equal(t, ringbuffer.EncodedMsgOffset(0), index)

	paddingIndex := int(tail)
	require.Equal(
		t, int32(ringbuffer.PaddingMsgTypeID), buffer.GetInt32(record.TypeOffset(paddingIndex)),
	)
	require.Equal(t, tail+int64(recordAligned)+int64(padding), ring.ProducerPosition())
}

func TestManyToOneRingBuffer_InvalidTypeAndLength(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	src := atomicbuffer.Wrap(make([]byte, 8))

	_, err := ring.Write(0, src, 0, 8)
	require.Error(t, err)
	index, err := ring.TryClaim(-1, 8)
	require.Error(t, err)
	require.Equal(t, -1, index)
	_, err = ring.Write(testMsgTypeID, src, 0, -1)
	require.Error(t, err)
	_, err = ring.Write(testMsgTypeID, src, 0, ring.MaxMsgLength()+1)
	require.Error(t, err)
}

func TestManyToOneRingBuffer_CommitZeroLengthNotInFlight(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	index, err := ring.TryClaim(testMsgTypeID, 8)
	require.NoError(t, err)
	recordIndex := index - ringbuffer.HeaderLength
	buffer.PutInt32(record.LengthOffset(recordIndex), 0)

	err = ring.Commit(index)
	require.ErrorContains(t, err, "ringbuffer: claimed space not in flight")
}

func TestManyToOneRingBuffer_CommitAbortStateErrors(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	index, err := ring.TryClaim(testMsgTypeID, 8)
	require.NoError(t, err)
	require.NoError(t, ring.Commit(index))
	err = ring.Commit(index)
	require.Error(t, err)

	index2, err := ring.TryClaim(testMsgTypeID, 8)
	require.NoError(t, err)
	require.NoError(t, ring.Abort(index2))
	err = ring.Abort(index2)
	require.Error(t, err)
}

func TestManyToOneRingBuffer_Size(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	require.Equal(t, 0, ring.Size())

	msgLen := 8
	ok, err := ring.Write(testMsgTypeID, atomicbuffer.Wrap(make([]byte, msgLen)), 0, msgLen)
	require.NoError(t, err)
	require.True(t, ok)

	want := alignedRecordLength(msgLen)
	require.Equal(t, want, ring.Size())
}

func writeHelloWorld(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) []byte {
	t.Helper()
	msg := []byte("Hello World")
	seedRingOneMessage(t, ring, msg)
	return msg
}

func TestManyToOneRingBuffer_ControlledRead(t *testing.T) {
	type (
		controlledActionFn func(
			invocation int,
			ring *ringbuffer.ManyToOneRingBuffer,
		) ringbuffer.ControlledAction
		controlledSetupFn func(
			t *testing.T,
			ring *ringbuffer.ManyToOneRingBuffer,
			buffer atomicbuffer.AtomicBuffer,
		)
	)

	type controlledCase struct {
		name         string
		writeCount   int
		messageLimit int
		action       controlledActionFn
		wantMessages int
		wantHandler  int
		checkPayload bool
		setup        controlledSetupFn
		assertAfter  func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer)
	}

	cases := []controlledCase{
		{
			name:         "Continue",
			writeCount:   1,
			messageLimit: 1,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledContinue
			},
			wantMessages: 1,
			wantHandler:  1,
			checkPayload: true,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				assertConsumerCaughtUp(t, ring)
			},
		},
		{
			name:         "Abort",
			writeCount:   1,
			messageLimit: 1,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledAbort
			},
			wantMessages: 0,
			wantHandler:  1,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				require.Zero(t, ring.ConsumerPosition())
			},
		},
		{
			name:       "AbortSecondMessage",
			writeCount: 2,
			action: func(inv int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				if inv == 1 {
					return ringbuffer.ControlledContinue
				}
				return ringbuffer.ControlledAbort
			},
			wantMessages: 1,
			wantHandler:  2,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				require.NotEqual(t, ring.ProducerPosition(), ring.ConsumerPosition())
			},
		},
		{
			name:       "CommitAtBufferEndWrapsHeadIndex",
			writeCount: 1,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledCommit
			},
			wantMessages: 1,
			wantHandler:  1,
			setup: func(t *testing.T, _ *ringbuffer.ManyToOneRingBuffer, buffer atomicbuffer.AtomicBuffer) {
				t.Helper()
				seedCommitAtEndCase(buffer, 64, 8)
			},
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				t.Helper()
				assertConsumerCaughtUp(t, ring)
				n := ring.Read(func(int, atomicbuffer.AtomicBuffer, int, int) {
					t.Fatal("unexpected message after commit at buffer end")
				})
				require.Equal(t, 0, n)
			},
		},
		{
			name:       "CommitEachMessage",
			writeCount: 2,
			action: func(inv int, ring *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				if inv == 1 {
					require.Zero(t, ring.ConsumerPosition())
				} else {
					require.Equal(t, ring.ProducerPosition()/2, ring.ConsumerPosition())
				}
				return ringbuffer.ControlledCommit
			},
			wantMessages: 2,
			wantHandler:  2,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				assertConsumerCaughtUp(t, ring)
			},
		},
		{
			name:       "Break",
			writeCount: 2,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledBreak
			},
			wantMessages: 1,
			wantHandler:  1,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				require.NotEqual(t, ring.ProducerPosition(), ring.ConsumerPosition())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataCapacity := testDataCapacity
			if tc.setup != nil {
				dataCapacity = 64
			}
			ring, buffer := newTestRingBuffer(t, dataCapacity)
			var msg []byte
			if tc.setup != nil {
				tc.setup(t, ring, buffer)
			} else {
				for range tc.writeCount {
					msg = writeHelloWorld(t, ring)
				}
			}

			var (
				gotType, gotIndex, gotLen int
				gotPayload                []byte
				invocations               int
			)
			handler := func(
				msgTypeID int,
				buffer atomicbuffer.AtomicBuffer,
				index int,
				length int,
			) ringbuffer.ControlledAction {
				invocations++
				action := tc.action(invocations, ring)
				if tc.checkPayload && invocations == 1 {
					gotType = msgTypeID
					gotIndex = index
					gotLen = length
					gotPayload = readPayload(buffer, index, length)
				}
				return action
			}

			var messages int
			if tc.messageLimit > 0 {
				messages = ring.ControlledReadWithLimit(handler, tc.messageLimit)
			} else {
				messages = ring.ControlledRead(handler)
			}

			require.Equal(t, tc.wantMessages, messages)
			require.Equal(t, tc.wantHandler, invocations)
			if tc.checkPayload {
				wantIndex := ringbuffer.EncodedMsgOffset(0)
				require.Equal(t, testMsgTypeID, gotType)
				require.Equal(t, wantIndex, gotIndex)
				require.Equal(t, len(msg), gotLen)
				require.Equal(t, msg, gotPayload)
			}
			if tc.assertAfter != nil {
				tc.assertAfter(t, ring)
			}
		})
	}
}

func TestManyToOneRingBuffer_Unblock(t *testing.T) {
	messageLength := unblockMessageLength
	head := int64(messageLength)
	emptyPos := int64(messageLength)

	cases := []unblockCase{
		{
			name:        "Empty",
			head:        emptyPos,
			tail:        emptyPos,
			wantUnblock: false,
		},
		{
			name:          "StuckNegativeLength",
			head:          head,
			tail:          head * 2,
			lengthIndex:   messageLength,
			lengthValue:   int32(-messageLength),
			wantUnblock:   true,
			paddingIndex:  messageLength,
			paddingLength: messageLength,
		},
		{
			name:          "WhenFullWithHeader",
			head:          head,
			tail:          head + int64(testDataCapacity),
			lengthIndex:   messageLength,
			lengthValue:   int32(-messageLength),
			wantUnblock:   true,
			paddingIndex:  messageLength,
			paddingLength: messageLength,
		},
		{
			name:          "WhenFullWithoutHeader",
			head:          head,
			tail:          head + int64(testDataCapacity),
			lengthIndex:   messageLength * 2,
			lengthValue:   int32(messageLength),
			wantUnblock:   true,
			paddingIndex:  messageLength,
			paddingLength: messageLength,
		},
		unblockGapWithZerosCase(messageLength),
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, buffer := newTestRingBuffer(t, testDataCapacity)
			seedUnblockCase(buffer, testDataCapacity, tc)
			assertUnblockResult(t, ring, buffer, tc)
		})
	}
}

func TestManyToOneRingBuffer_NextCorrelationId(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	require.Zero(t, ring.NextCorrelationID())
	require.Equal(t, int64(1), ring.NextCorrelationID())
	require.Equal(t, int64(2), ring.NextCorrelationID())
}

func TestManyToOneRingBuffer_ConsumerHeartbeatTime(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	require.Zero(t, ring.ConsumerHeartbeatTime())
	want := time.Now().UnixMilli()
	ring.SetConsumerHeartbeatTime(want)
	require.Equal(t, want, ring.ConsumerHeartbeatTime())
}
