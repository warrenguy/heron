package conductor_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
)

const fuzzCopyBroadcastReceiveCalls = 8

func FuzzCopyBroadcastReceiver_receive(f *testing.F) {
	f.Add(fuzzCopyBroadcastValidRun(f))
	f.Add(fuzzCopyBroadcastPastBufferEnd(f))
	f.Add(fuzzCopyBroadcastFirstRecordLength(f, aboveMaxRecordLength))
	f.Add(fuzzCopyBroadcastFirstRecordLength(f, 0))

	maxMsgLength := broadcast.MaxMsgLength(copyBroadcastTestCapacity)
	f.Fuzz(func(t *testing.T, input []byte) {
		buf := newCopyBroadcastBuffer(t)
		copy(buf.Bytes(), input)
		alignFuzzTrailerPositions(buf)
		rx, err := broadcast.NewBroadcastReceiver(buf)
		require.NoError(t, err)
		copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

		for range fuzzCopyBroadcastReceiveCalls {
			_, err := copyReceiver.Receive(func(_ int, _ atomicbuffer.AtomicBuffer, _, length int) {
				require.GreaterOrEqual(t, length, 0)
				require.LessOrEqual(t, length, maxMsgLength)
			})
			// The caller must stop receiving after ErrBroadcastMessageTooLong.
			if errors.Is(err, conductor.ErrBroadcastMessageTooLong) {
				return
			}
			if err != nil {
				require.ErrorIs(t, err, conductor.ErrBroadcastLapped)
			}
		}
	})
}

// alignFuzzTrailerPositions rounds the trailer positions down to Alignment.
// A driver writes only aligned positions. An unaligned record offset faults
// an atomic load on some arm64 hosts.
func alignFuzzTrailerPositions(buf atomicbuffer.AtomicBuffer) {
	for _, index := range []int{
		broadcast.TailIntentIndex(copyBroadcastTestCapacity),
		broadcast.TailIndex(copyBroadcastTestCapacity),
		broadcast.LatestIndex(copyBroadcastTestCapacity),
	} {
		buf.PutInt64(index, buf.GetInt64(index)&^int64(broadcast.Alignment-1))
	}
}

// fuzzCopyBroadcastValidRun returns a buffer that holds a run of records from
// offset 0.
func fuzzCopyBroadcastValidRun(tb testing.TB) []byte {
	tb.Helper()

	buf := newCopyBroadcastBuffer(tb)
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)
	for sequence := range copyBroadcastTestCapacity / 24 {
		require.NoError(tb, tx.Transmit(testBroadcastMsgTypeID, make([]byte, 8+sequence%9)))
	}
	buf.PutInt64Release(broadcast.LatestIndex(copyBroadcastTestCapacity), 0)
	return buf.Bytes()
}

// fuzzCopyBroadcastPastBufferEnd returns a buffer whose next record has a
// length that runs past the buffer end.
func fuzzCopyBroadcastPastBufferEnd(tb testing.TB) []byte {
	tb.Helper()

	buf := newCopyBroadcastBuffer(tb)
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)
	tearLastRecordPastBufferEnd(tb, tx, buf)
	return buf.Bytes()
}

// fuzzCopyBroadcastFirstRecordLength returns a buffer whose first record
// carries rawLength in place of its real length. A length above MaxMsgLength
// still fits the buffer. A zero length puts the next record on the same slot.
func fuzzCopyBroadcastFirstRecordLength(tb testing.TB, rawLength int32) []byte {
	tb.Helper()

	buf := newCopyBroadcastBuffer(tb)
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)
	transmitThenTearFirstLength(tb, tx, buf, rawLength)
	return buf.Bytes()
}
