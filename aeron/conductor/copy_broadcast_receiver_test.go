package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/conductor"
)

const copyBroadcastTestCapacity = 1024

func TestCopyBroadcastReceiver_receiveMessage(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	payload := []byte{10, 11, 12, 13, 14, 15, 16, 17}
	err := tx.Transmit(testBroadcastMsgTypeID, payload)
	require.NoError(t, err)

	var (
		gotTypeID int
		gotBytes  []byte
	)
	count, err := copyReceiver.Receive(func(
		msgTypeID int,
		buffer aeronatomic.AtomicBuffer,
		offset int,
		length int,
	) {
		gotTypeID = msgTypeID
		gotBytes = make([]byte, length)
		copy(gotBytes, buffer.Bytes()[offset:offset+length])
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, testBroadcastMsgTypeID, gotTypeID)
	require.Equal(t, payload, gotBytes)
}

func TestCopyBroadcastReceiver_emptyBuffer(t *testing.T) {
	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	count, err := copyReceiver.Receive(func(int, aeronatomic.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestCopyBroadcastReceiver_lappedDuringReceive(t *testing.T) {
	buf := newCopyBroadcastBuffer(t)
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(t, err)

	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	fillBroadcastUntilLap(t, tx, testBroadcastMsgTypeID, payload, copyBroadcastTestCapacity)

	actualLatest := buf.GetInt64Volatile(broadcast.LatestIndex(copyBroadcastTestCapacity))
	buf.PutInt64Release(broadcast.LatestIndex(copyBroadcastTestCapacity), 0)

	rx, err := broadcast.NewBroadcastReceiver(buf)
	require.NoError(t, err)
	buf.PutInt64Release(broadcast.LatestIndex(copyBroadcastTestCapacity), actualLatest)

	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	_, err = copyReceiver.Receive(func(int, aeronatomic.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	require.ErrorIs(t, err, conductor.ErrBroadcastLapped)
}

func TestCopyBroadcastReceiver_tailIntentCorruptionBeforeReceive(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	payload := []byte{1, 2, 3, 4}
	err := tx.Transmit(testBroadcastMsgTypeID, payload)
	require.NoError(t, err)

	buf := rx.Buffer()
	buf.PutInt64Release(
		broadcast.TailIntentIndex(copyBroadcastTestCapacity),
		int64(copyBroadcastTestCapacity),
	)

	handlerCalled := false
	count, err := copyReceiver.Receive(func(int, aeronatomic.AtomicBuffer, int, int) {
		handlerCalled = true
	})
	require.ErrorIs(t, err, conductor.ErrBroadcastLapped)
	require.Zero(t, count)
	require.False(t, handlerCalled)
}

func TestCopyBroadcastReceiver_messageTooLong(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiverWithScratchLength(rx, 8)

	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	err := tx.Transmit(testBroadcastMsgTypeID, payload)
	require.NoError(t, err)

	count, err := copyReceiver.Receive(func(int, aeronatomic.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	require.ErrorIs(t, err, conductor.ErrBroadcastMessageTooLong)
	require.Zero(t, count)
}

const testBroadcastMsgTypeID = 7

func newCopyBroadcastBuffer(tb testing.TB) aeronatomic.AtomicBuffer {
	tb.Helper()
	return aeronatomic.Wrap(make([]byte, broadcast.TotalBufferLength(copyBroadcastTestCapacity)))
}

func newCopyBroadcastPair(
	tb testing.TB,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	tb.Helper()

	buf := newCopyBroadcastBuffer(tb)
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)
	rx, err := broadcast.NewBroadcastReceiver(buf)
	require.NoError(tb, err)
	return tx, rx
}
