//go:build rapid

package conductor_test

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
)

const (
	pairsRapidMinBodyLength = 8
	pairsRapidMinRecordSpan = 16
)

const (
	pairsRapidStepReceiveNext = iota
	pairsRapidStepReceiveCopied
	pairsRapidStepReceiveValidated
)

// pairsRapidRecord is one transmitted record in the reference model.
type pairsRapidRecord struct {
	msgTypeID int
	body      []byte
}

// pairsRapidModel transmits sequenced records and keeps each by sequence.
type pairsRapidModel struct {
	tx           *broadcast.BroadcastTransmitter
	maxMsgLength int
	records      []pairsRapidRecord
}

// transmit sends one record. The body holds the sequence number and filler.
func (model *pairsRapidModel) transmit(rt *rapid.T) {
	rt.Helper()

	msgTypeID := rapid.IntRange(1, 65535).Draw(rt, "msgTypeID")
	bodyLength := rapid.IntRange(pairsRapidMinBodyLength, model.maxMsgLength).Draw(rt, "bodyLength")
	sequence := len(model.records)
	body := make([]byte, bodyLength)
	binary.LittleEndian.PutUint64(body, uint64(sequence))
	for i := pairsRapidMinBodyLength; i < bodyLength; i++ {
		body[i] = byte(sequence + i)
	}
	require.NoError(rt, model.tx.Transmit(msgTypeID, body))
	model.records = append(model.records, pairsRapidRecord{msgTypeID: msgTypeID, body: body})
}

func (model *pairsRapidModel) transmitMany(rt *rapid.T, count int) {
	rt.Helper()

	for range count {
		model.transmit(rt)
	}
}

// pairsRapidReceiver tracks what the copy receiver delivered.
type pairsRapidReceiver struct {
	copyReceiver         *conductor.CopyBroadcastReceiver
	model                *pairsRapidModel
	lastSequence         int
	lappedSinceDelivered bool
}

// receive runs one Receive. Requires a delivered record to match the model.
// Reports whether Receive found no pending record.
func (receiver *pairsRapidReceiver) receive(rt *rapid.T) bool {
	rt.Helper()

	handlerCalls := 0
	count, err := receiver.copyReceiver.Receive(func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
	) {
		handlerCalls++
		require.GreaterOrEqual(rt, length, pairsRapidMinBodyLength)
		body := slices.Clone(buffer.Bytes()[offset : offset+length])
		sequence := int(binary.LittleEndian.Uint64(body))
		require.Less(rt, sequence, len(receiver.model.records))
		want := receiver.model.records[sequence]
		require.Equal(rt, want.msgTypeID, msgTypeID)
		require.Equal(rt, want.body, body)
		require.Greater(rt, sequence, receiver.lastSequence)
		if sequence != receiver.lastSequence+1 {
			require.True(rt, receiver.lappedSinceDelivered, "gap without a lap report")
		}
		receiver.lastSequence = sequence
		receiver.lappedSinceDelivered = false
	})
	switch {
	case err == nil && count == 1:
		require.Equal(rt, 1, handlerCalls)
	case err == nil && count == 0:
		require.Zero(rt, handlerCalls)
	default:
		require.ErrorIs(rt, err, conductor.ErrBroadcastLapped)
		require.Zero(rt, count)
		require.Zero(rt, handlerCalls)
		receiver.lappedSinceDelivered = true
	}
	return count == 0 && err == nil
}

func TestCopyBroadcastReceiver_transmittedPairs_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.SampledFrom([]int{128, 256, 512, 1024}).Draw(rt, "capacity")
		scratchLength := rapid.IntRange(8, 64).Draw(rt, "scratchLength")

		buf := atomicbuffer.Wrap(make([]byte, broadcast.TotalBufferLength(capacity)))
		tx, err := broadcast.NewBroadcastTransmitter(buf)
		require.NoError(rt, err)
		rx, err := broadcast.NewBroadcastReceiver(buf)
		require.NoError(rt, err)
		model := &pairsRapidModel{tx: tx, maxMsgLength: tx.MaxMsgLength()}
		receiver := &pairsRapidReceiver{
			copyReceiver: conductor.NewCopyBroadcastReceiverWithScratchLength(rx, scratchLength),
			model:        model,
			lastSequence: -1,
		}

		// Each hook transmits once at its step. The hooks register on t, so each
		// iteration clears them on exit to free the model.
		hookStep := -1
		hookCount := 0
		hookAt := func(step int) func() {
			return func() {
				if hookStep != step {
					return
				}
				hookStep = -1
				model.transmitMany(rt, hookCount)
			}
		}
		defer receiver.copyReceiver.ClearReceiveHooksForTest()
		receiver.copyReceiver.SetReceiveNextHookForTest(t, hookAt(pairsRapidStepReceiveNext))
		receiver.copyReceiver.SetReceiveCopiedHookForTest(t, hookAt(pairsRapidStepReceiveCopied))
		receiver.copyReceiver.SetReceiveValidatedHookForTest(
			t,
			hookAt(pairsRapidStepReceiveValidated),
		)

		maxTransmitCount := 2 * capacity / pairsRapidMinRecordSpan
		operationCount := rapid.IntRange(1, 60).Draw(rt, "operationCount")
		for range operationCount {
			switch rapid.IntRange(0, 2).Draw(rt, "operation") {
			case 0:
				model.transmitMany(rt, rapid.IntRange(1, maxTransmitCount).Draw(rt, "transmitCount"))
			case 1:
				receiver.receive(rt)
			default:
				hookStep = rapid.IntRange(
					pairsRapidStepReceiveNext,
					pairsRapidStepReceiveValidated,
				).Draw(rt, "hookStep")
				hookCount = rapid.IntRange(1, maxTransmitCount).Draw(rt, "hookCount")
				receiver.receive(rt)
				hookStep = -1
			}
		}

		// Phase: a final drain reaches an empty receive.
		drained := false
		for range capacity/pairsRapidMinRecordSpan + 2 {
			if receiver.receive(rt) {
				drained = true
				break
			}
		}
		require.True(rt, drained, "drain did not reach an empty receive")
		require.False(rt, rx.ReceiveNext())
	})
}
