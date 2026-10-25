package conductor_test

import (
	"encoding/binary"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/internal/testutil"
	"github.com/warrenguy/heron/record"
)

const (
	copyBroadcastTestCapacity = 1024
	lapMsgTypeID              = 9
	tornRecordLength          = 1 << 20
	firstRecordOffset         = 0
	// eightByteRecordLength is the aligned length of a record with an 8-byte
	// body.
	eightByteRecordLength = 16
	recordsBeforeEnd      = 62
	lastRecordOffset      = recordsBeforeEnd * eightByteRecordLength
	// pastEndRecordLength fits MaxMsgLength but runs past the buffer end from
	// lastRecordOffset.
	pastEndRecordLength  = 108
	aboveMaxRecordLength = 300
)

func TestCopyBroadcastReceiver_receiveMessage(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	payload := []byte{10, 11, 12, 13, 14, 15, 16, 17}
	err := tx.Transmit(testBroadcastMsgTypeID, payload)
	require.NoError(t, err)

	gotTypeID, gotBytes := receiveOneCopied(t, copyReceiver)
	require.Equal(t, testBroadcastMsgTypeID, gotTypeID)
	require.Equal(t, payload, gotBytes)
}

func TestCopyBroadcastReceiver_typeIDReadBeforeCopy(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	require.NoError(t, tx.Transmit(testBroadcastMsgTypeID, payload))

	// The hook laps the ring over the first header.
	lapPayload := []byte{90, 91, 92, 93, 94, 95, 96, 97}
	copyReceiver.SetReceiveValidatedHookForTest(t, func() {
		fillBroadcastUntilLap(t, tx, lapMsgTypeID, lapPayload, copyBroadcastTestCapacity)
	})

	gotTypeID, gotBytes := receiveOneCopied(t, copyReceiver)
	require.Equal(t, testBroadcastMsgTypeID, gotTypeID)
	require.Equal(t, payload, gotBytes)
}

func TestCopyBroadcastReceiver_emptyBuffer(t *testing.T) {
	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	count, err := copyReceiver.Receive(func(int, atomicbuffer.AtomicBuffer, int, int) {
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
	receiveError(t, copyReceiver, conductor.ErrBroadcastLapped)

	// Phase: the next Receive delivers the latest record.
	gotTypeID, gotBytes := receiveOneCopied(t, copyReceiver)
	require.Equal(t, testBroadcastMsgTypeID, gotTypeID)
	require.Equal(t, payload, gotBytes)

	// Phase: a later transmit follows it.
	nextPayload := []byte{11, 12, 13, 14, 15, 16, 17, 18}
	require.NoError(t, tx.Transmit(testBroadcastMsgTypeID+1, nextPayload))
	gotTypeID, gotBytes = receiveOneCopied(t, copyReceiver)
	require.Equal(t, testBroadcastMsgTypeID+1, gotTypeID)
	require.Equal(t, nextPayload, gotBytes)
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
	count, err := copyReceiver.Receive(func(int, atomicbuffer.AtomicBuffer, int, int) {
		handlerCalled = true
	})
	require.ErrorIs(t, err, conductor.ErrBroadcastLapped)
	require.Zero(t, count)
	require.False(t, handlerCalled)
}

func TestCopyBroadcastReceiver_tornLengthReportsLap(t *testing.T) {
	const lapRecordCount = 63

	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	require.NoError(t, tx.Transmit(testBroadcastMsgTypeID, []byte{1, 2, 3, 4, 5, 6, 7, 8}))

	// The hook laps the first slot, then tears its length.
	copyReceiver.SetReceiveNextHookForTest(t, sync.OnceFunc(func() {
		transmitSequencedRecords(t, tx, 1, lapRecordCount)
		rx.Buffer().PutInt32(record.LengthOffset(firstRecordOffset), tornRecordLength)
	}))

	// Phase: the torn length reports a lap.
	receiveLapped(t, copyReceiver, rx)

	// Phase: the receiver resumes at the latest record.
	receiveSequenced(t, copyReceiver, lapRecordCount)
	transmitSequencedRecords(t, tx, lapRecordCount+1, 1)
	receiveSequenced(t, copyReceiver, lapRecordCount+1)
}

func TestCopyBroadcastReceiver_tornNextRecordResyncs(t *testing.T) {
	const lapRecordCount = 64

	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	// ReceiveNext loads the torn length after its slot check, so the next
	// record is off a boundary. The hook then laps the slot.
	transmitThenTearFirstLength(t, tx, rx.Buffer(), tornRecordLength)
	copyReceiver.SetReceiveNextHookForTest(t, sync.OnceFunc(func() {
		transmitSequencedRecords(t, tx, 1, lapRecordCount)
	}))

	// Phase: the torn length fails the bounds check and Validate, so the
	// receiver resyncs.
	receiveLapped(t, copyReceiver, rx)

	// Phase: the receiver resumes at the latest record.
	receiveSequenced(t, copyReceiver, lapRecordCount)
}

func TestCopyBroadcastReceiver_tornLatestRecordResyncs(t *testing.T) {
	const (
		lapRecordCount     = 64
		relapRecordCount   = 63
		latestRecordOffset = 1008
	)

	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	// The transmits lap the receiver. ReceiveNext jumps to the latest record,
	// whose length is torn.
	transmitSequencedRecords(t, tx, 1, lapRecordCount)
	rx.Buffer().PutInt32(record.LengthOffset(latestRecordOffset), tornRecordLength)
	copyReceiver.SetReceiveNextHookForTest(t, sync.OnceFunc(func() {
		transmitSequencedRecords(t, tx, lapRecordCount+1, relapRecordCount)
	}))

	// Phase: the hook laps the latest slot, so Validate fails and the
	// receiver resyncs. The resync adds no second lap count.
	receiveLapped(t, copyReceiver, rx)

	// Phase: the receiver resumes at the new latest record.
	receiveSequenced(t, copyReceiver, lapRecordCount+relapRecordCount)
}

func TestCopyBroadcastReceiver_growsScratch(t *testing.T) {
	var (
		gotBytes         []byte
		gotScratchLength int
	)
	captureHandler := func(_ int, buffer atomicbuffer.AtomicBuffer, offset, length int) {
		gotBytes = slices.Clone(buffer.Bytes()[offset : offset+length])
		gotScratchLength = buffer.Capacity()
	}

	maxMsgLength := broadcast.MaxMsgLength(copyBroadcastTestCapacity)
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiverWithScratchLength(rx, 8)

	// Phase: the first longer record grows the scratch to MaxMsgLength.
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	require.NoError(t, tx.Transmit(testBroadcastMsgTypeID, payload))

	count, err := copyReceiver.Receive(captureHandler)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, payload, gotBytes)
	require.Equal(t, maxMsgLength, gotScratchLength)

	// Phase: a record at MaxMsgLength reuses the scratch.
	payload = make([]byte, maxMsgLength)
	failures := 0
	reuseHandler := func(_ int, buffer atomicbuffer.AtomicBuffer, _, length int) {
		if buffer.Capacity() != maxMsgLength || length != maxMsgLength {
			failures++
		}
	}
	testutil.AssertZeroAllocs(t, "Transmit and Receive", 100, func() {
		transmitErr := tx.Transmit(testBroadcastMsgTypeID, payload)
		count, receiveErr := copyReceiver.Receive(reuseHandler)
		if transmitErr != nil || receiveErr != nil || count != 1 {
			failures++
		}
	})
	require.Zero(t, failures)
}

func TestCopyBroadcastReceiver_deliversRecordAboveInitialScratch(t *testing.T) {
	const (
		capacity   = 64 * 1024
		bodyLength = 5000
	)

	buf := atomicbuffer.Wrap(make([]byte, broadcast.TotalBufferLength(capacity)))
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(t, err)
	rx, err := broadcast.NewBroadcastReceiver(buf)
	require.NoError(t, err)
	require.Equal(t, 8192, broadcast.MaxMsgLength(capacity))
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)

	payload := make([]byte, bodyLength)
	for i := range payload {
		payload[i] = byte(i)
	}
	require.NoError(t, tx.Transmit(testBroadcastMsgTypeID, payload))

	gotTypeID, gotBytes := receiveOneCopied(t, copyReceiver)
	require.Equal(t, testBroadcastMsgTypeID, gotTypeID)
	require.Equal(t, payload, gotBytes)
}

func TestCopyBroadcastReceiver_recordPastBufferEnd(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	tearLastRecordPastBufferEnd(t, tx, rx.Buffer())

	// Phase: the records before the last slot copy as usual.
	for range recordsBeforeEnd {
		receiveOneCopied(t, copyReceiver)
	}

	// Phase: the body fits MaxMsgLength but runs past the buffer end. The
	// record stays valid.
	receiveError(t, copyReceiver, conductor.ErrBroadcastMessageTooLong)
}

func TestCopyBroadcastReceiver_recordAboveMaxMsgLength(t *testing.T) {
	tx, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	transmitThenTearFirstLength(t, tx, rx.Buffer(), aboveMaxRecordLength)

	// Phase: the body fits the buffer, but the length exceeds MaxMsgLength.
	receiveError(t, copyReceiver, conductor.ErrBroadcastMessageTooLong)
}

func TestCopyBroadcastReceiver_recordShorterThanHeader(t *testing.T) {
	for _, rawLength := range []int32{0, 4, -16} {
		tx, rx := newCopyBroadcastPair(t)
		copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
		transmitThenTearFirstLength(t, tx, rx.Buffer(), rawLength)

		// Phase: the record stays valid, but its length cannot hold a header.
		receiveError(t, copyReceiver, conductor.ErrBroadcastMessageTooLong)
		require.Zero(t, rx.LappedCount(), "raw length %d", rawLength)
	}
}

const testBroadcastMsgTypeID = 7

// receiveOneCopied runs one Receive. Returns the delivered type id and a copy
// of the body.
func receiveOneCopied(
	tb testing.TB,
	copyReceiver *conductor.CopyBroadcastReceiver,
) (int, []byte) {
	tb.Helper()

	var (
		gotTypeID int
		gotBytes  []byte
	)
	count, err := copyReceiver.Receive(func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
	) {
		gotTypeID = msgTypeID
		gotBytes = slices.Clone(buffer.Bytes()[offset : offset+length])
	})
	require.NoError(tb, err)
	require.Equal(tb, 1, count)
	return gotTypeID, gotBytes
}

// receiveError runs one Receive that must report wantErr without running the
// handler.
func receiveError(
	tb testing.TB,
	copyReceiver *conductor.CopyBroadcastReceiver,
	wantErr error,
) {
	tb.Helper()

	count, err := copyReceiver.Receive(func(int, atomicbuffer.AtomicBuffer, int, int) {
		tb.Fatal("handler should not run")
	})
	require.ErrorIs(tb, err, wantErr)
	require.Zero(tb, count)
}

// receiveLapped runs one Receive that must report ErrBroadcastLapped and
// count one new lap.
func receiveLapped(
	tb testing.TB,
	copyReceiver *conductor.CopyBroadcastReceiver,
	rx *broadcast.BroadcastReceiver,
) {
	tb.Helper()

	lappedCount := rx.LappedCount()
	receiveError(tb, copyReceiver, conductor.ErrBroadcastLapped)
	require.Equal(tb, lappedCount+1, rx.LappedCount())
}

// receiveSequenced receives one record of lapMsgTypeID whose body holds
// sequence.
func receiveSequenced(
	tb testing.TB,
	copyReceiver *conductor.CopyBroadcastReceiver,
	sequence int,
) {
	tb.Helper()

	gotTypeID, gotBytes := receiveOneCopied(tb, copyReceiver)
	require.Equal(tb, lapMsgTypeID, gotTypeID)
	require.Equal(tb, sequencedBody(sequence), gotBytes)
}

// tearLastRecordPastBufferEnd fills the buffer with 8-byte records up to the
// last slot, then tears that slot's length so the body runs past the end.
func tearLastRecordPastBufferEnd(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	buf atomicbuffer.AtomicBuffer,
) {
	tb.Helper()

	for range recordsBeforeEnd + 1 {
		require.NoError(tb, tx.Transmit(testBroadcastMsgTypeID, make([]byte, 8)))
	}
	buf.PutInt32(record.LengthOffset(lastRecordOffset), pastEndRecordLength)
}

// transmitThenTearFirstLength transmits one 8-byte record, then overwrites
// the first record's length with rawLength.
func transmitThenTearFirstLength(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	buf atomicbuffer.AtomicBuffer,
	rawLength int32,
) {
	tb.Helper()

	require.NoError(tb, tx.Transmit(testBroadcastMsgTypeID, make([]byte, 8)))
	buf.PutInt32(record.LengthOffset(firstRecordOffset), rawLength)
}

// sequencedBody returns an 8-byte body that holds sequence.
func sequencedBody(sequence int) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(sequence))
}

// transmitSequencedRecords transmits count records of lapMsgTypeID. Each
// body holds its sequence number, from firstSequence up.
func transmitSequencedRecords(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	firstSequence int,
	count int,
) {
	tb.Helper()

	for sequence := firstSequence; sequence < firstSequence+count; sequence++ {
		require.NoError(tb, tx.Transmit(lapMsgTypeID, sequencedBody(sequence)))
	}
}

func newCopyBroadcastBuffer(tb testing.TB) atomicbuffer.AtomicBuffer {
	tb.Helper()
	return atomicbuffer.Wrap(make([]byte, broadcast.TotalBufferLength(copyBroadcastTestCapacity)))
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
