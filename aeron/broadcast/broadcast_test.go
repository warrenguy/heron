package broadcast_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/record"
)

const (
	testMsgTypeID       = 7
	testCapacity        = 1024
	invalidDataCapacity = 777 // deliberately not a power of two
)

var testPayload8 = []byte{1, 2, 3, 4, 5, 6, 7, 8}

func newBufferWithDataCapacity(dataCapacity int) atomic.AtomicBuffer {
	return atomic.Wrap(make([]byte, broadcast.TotalBufferLength(dataCapacity)))
}

func newTestBuffer(tb testing.TB) atomic.AtomicBuffer {
	tb.Helper()
	return newBufferWithDataCapacity(testCapacity)
}

func invalidCapacityBuffer() atomic.AtomicBuffer {
	return atomic.Wrap(make([]byte, invalidDataCapacity+broadcast.TrailerLength))
}

func newBroadcastTXRX(
	buf atomic.AtomicBuffer,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver, error) {
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	if err != nil {
		return nil, nil, err
	}
	rx, err := broadcast.NewBroadcastReceiver(buf)
	if err != nil {
		return nil, nil, err
	}
	return tx, rx, nil
}

func alignedRecordLength(payloadLen int) int {
	return record.AlignRecordLength(payloadLen + broadcast.HeaderLength)
}

func writesToLap(dataCapacity, payloadLen, slack int) int {
	return dataCapacity/alignedRecordLength(payloadLen) + slack
}

func messagesBeforeWrap(dataCapacity, payloadLen int) int {
	aligned := alignedRecordLength(payloadLen)
	return (dataCapacity - 2*aligned) / aligned
}

func copyPayload(rx *broadcast.BroadcastReceiver) []byte {
	length := rx.Length()
	if length <= 0 {
		return nil
	}
	buf := rx.Buffer().Bytes()
	off := rx.Offset()
	out := make([]byte, length)
	copy(out, buf[off:off+length])
	return out
}

func newTransmitter(tb testing.TB, buf atomic.AtomicBuffer) *broadcast.BroadcastTransmitter {
	tb.Helper()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)
	return tx
}

func newReceiver(tb testing.TB, buf atomic.AtomicBuffer) *broadcast.BroadcastReceiver {
	tb.Helper()
	rx, err := broadcast.NewBroadcastReceiver(buf)
	require.NoError(tb, err)
	return rx
}

func newTestBroadcastPair(tb testing.TB) (
	*broadcast.BroadcastTransmitter,
	*broadcast.BroadcastReceiver,
	atomic.AtomicBuffer,
) {
	tb.Helper()
	ab := newBufferWithDataCapacity(testCapacity)
	tx, rx, err := newBroadcastTXRX(ab)
	require.NoError(tb, err)
	return tx, rx, ab
}

func warmTransmit(tb testing.TB, tx *broadcast.BroadcastTransmitter) {
	tb.Helper()
	if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
		tb.Fatal(err)
	}
}

func warmTransmitReceive(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	rx *broadcast.BroadcastReceiver,
) {
	tb.Helper()
	warmTransmit(tb, tx)
	if !rx.ReceiveNext() {
		tb.Fatal("warm-up receive failed")
	}
}

func warmTransmitOnly(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	_ *broadcast.BroadcastReceiver,
) {
	warmTransmit(tb, tx)
}
