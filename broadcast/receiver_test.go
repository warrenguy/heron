package broadcast_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/record"
)

func TestBroadcastReceiverCapacity(t *testing.T) {
	rx := newReceiver(t, newTestBuffer(t))
	require.Equal(t, testCapacity, rx.Capacity())
}

func TestBroadcastReceiverInvalidCapacity(t *testing.T) {
	_, err := broadcast.NewBroadcastReceiver(invalidCapacityBuffer())
	require.Error(t, err)
}

func TestBroadcastReceiverNotLappedBeforeReception(t *testing.T) {
	rx := newReceiver(t, newTestBuffer(t))
	require.Zero(t, rx.LappedCount())
}

func TestBroadcastReceiverEmptyBuffer(t *testing.T) {
	rx := newReceiver(t, newTestBuffer(t))
	require.False(t, rx.ReceiveNext())
}

func TestBroadcastReceiverFirstMessage(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	const length = 8
	src := []byte{10, 11, 12, 13, 14, 15, 16, 17}
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	rx := newReceiver(t, buf)
	require.True(t, rx.ReceiveNext())
	require.Equal(t, testMsgTypeID, rx.TypeID())
	require.Equal(t, broadcast.MsgOffset(0), rx.Offset())
	require.Equal(t, length, rx.Length())
	require.True(t, rx.Validate())
	got := copyPayload(rx)
	require.Equal(t, src, got)
}

func TestBroadcastReceiverTwoMessages(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)
	rx := newReceiver(t, buf)

	src := testPayload8
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)
	err = tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	require.True(t, rx.ReceiveNext())
	require.Equal(t, broadcast.MsgOffset(0), rx.Offset())
	require.True(t, rx.Validate())

	require.True(t, rx.ReceiveNext())
	recordOffsetTwo := alignedRecordLength(len(src))
	require.Equal(t, broadcast.MsgOffset(recordOffsetTwo), rx.Offset())
	require.True(t, rx.Validate())
}

func TestBroadcastReceiverLateJoinTransmission(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	for range 3*testCapacity/16 + 1 {
		err := tx.Transmit(testMsgTypeID, src)
		require.NoError(t, err)
	}

	actualLatest := buf.GetInt64Volatile(broadcast.LatestIndex(testCapacity))
	buf.PutInt64Release(broadcast.LatestIndex(testCapacity), 0)

	rx, err := broadcast.NewBroadcastReceiver(buf)
	require.NoError(t, err)
	buf.PutInt64Release(broadcast.LatestIndex(testCapacity), actualLatest)

	require.True(t, rx.ReceiveNext())
	require.Positive(t, rx.LappedCount())
	require.True(t, rx.Validate())
}

func TestBroadcastReceiverPaddingAndWrap(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)
	rx := newReceiver(t, buf)

	small := testPayload8
	beforeWrap := messagesBeforeWrap(testCapacity, len(small))
	for range beforeWrap {
		err := tx.Transmit(testMsgTypeID, small)
		require.NoError(t, err)
		require.True(t, rx.ReceiveNext())
	}

	const length = 120
	src := make([]byte, length)
	for i := range src {
		src[i] = byte(i)
	}
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	require.True(t, rx.ReceiveNext())
	require.Equal(t, testMsgTypeID, rx.TypeID())
	require.Equal(t, length, rx.Length())
	require.True(t, rx.Validate())
}

func TestBroadcastReceiverLengthDefensive(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	rx := newReceiver(t, buf)
	require.True(t, rx.ReceiveNext())
	require.Equal(t, len(src), rx.Length())

	buf.PutInt32(record.LengthOffset(0), int32(broadcast.HeaderLength-1))
	require.Equal(t, 0, rx.Length())
}

func TestBroadcastReceiverRecordInvalidatedByOverwrite(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	rx := newReceiver(t, buf)
	require.True(t, rx.ReceiveNext())
	require.Equal(t, testMsgTypeID, rx.TypeID())

	recordLengthAligned := alignedRecordLength(len(src))
	// Advance tailIntent past cursor+capacity without receiver catching up.
	buf.PutInt64Release(
		broadcast.TailIntentIndex(testCapacity),
		int64(recordLengthAligned)+int64(testCapacity-recordLengthAligned),
	)

	require.False(t, rx.Validate())
}

func TestBroadcastReceiver_PendingMessageForTestAtPaddingBoundary(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)
	rx := newReceiver(t, buf)

	small := testPayload8
	beforeWrap := messagesBeforeWrap(testCapacity, len(small))
	for range beforeWrap {
		err := tx.Transmit(testMsgTypeID, small)
		require.NoError(t, err)
		require.True(t, rx.ReceiveNext())
	}

	const length = 120
	src := make([]byte, length)
	for i := range src {
		src[i] = byte(i)
	}
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	pendingTypeID, pendingPayload, ok := rx.PendingMessageForTest()
	require.True(t, ok)
	require.Equal(t, testMsgTypeID, pendingTypeID)
	require.Len(t, pendingPayload, length)
	require.Equal(t, src, pendingPayload)
}

func TestBroadcastReceiver_PendingMessageForTest(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)
	rx := newReceiver(t, buf)

	require.NoError(t, tx.Transmit(testMsgTypeID, []byte{0x01}))
	require.NoError(t, tx.Transmit(testMsgTypeID+1, []byte{0x02}))

	require.True(t, rx.ReceiveNext())
	require.True(t, rx.Validate())

	pendingTypeID, pendingPayload, ok := rx.PendingMessageForTest()
	require.True(t, ok)
	require.Equal(t, testMsgTypeID+1, pendingTypeID)
	require.Equal(t, []byte{0x02}, pendingPayload)
}
