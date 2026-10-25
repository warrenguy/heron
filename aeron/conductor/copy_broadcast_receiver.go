package conductor

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
)

// ScratchBufferLength is the initial scratch length of a CopyBroadcastReceiver.
// The first longer record grows the scratch to the broadcast MaxMsgLength.
const ScratchBufferLength = 4096

var (
	// ErrBroadcastLapped reports a lap or a torn read on the to-clients
	// broadcast. The driver events in the lapped range are lost, so the
	// client's view of its resources may be stale.
	ErrBroadcastLapped = errors.New("conductor: unable to keep up with broadcast")
	// ErrBroadcastMessageTooLong reports a record that no conforming driver
	// writes.
	ErrBroadcastMessageTooLong = errors.New(
		"conductor: broadcast message exceeds buffer bounds",
	)
)

type EventMessageHandler func(
	msgTypeID int,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
)

type CopyBroadcastReceiver struct {
	receiver     *broadcast.BroadcastReceiver
	scratch      atomicbuffer.AtomicBuffer
	maxMsgLength int
}

func NewCopyBroadcastReceiver(receiver *broadcast.BroadcastReceiver) *CopyBroadcastReceiver {
	return newCopyBroadcastReceiver(receiver, ScratchBufferLength)
}

func newCopyBroadcastReceiver(
	receiver *broadcast.BroadcastReceiver,
	scratchLength int,
) *CopyBroadcastReceiver {
	return &CopyBroadcastReceiver{
		receiver:     receiver,
		scratch:      atomicbuffer.Wrap(make([]byte, scratchLength)),
		maxMsgLength: broadcast.MaxMsgLength(receiver.Capacity()),
	}
}

func (copyReceiver *CopyBroadcastReceiver) BroadcastReceiver() *broadcast.BroadcastReceiver {
	return copyReceiver.receiver
}

// Receive copies the next broadcast record to scratch. Receive passes the copy
// to handler.
//
// Returns:
//   - 1 and nil after handler ran.
//   - 0 and nil when no record is pending.
//   - 0 and ErrBroadcastLapped after a lap or a torn read. The receiver
//     resumes at a record boundary. The caller may keep receiving.
//   - 0 and ErrBroadcastMessageTooLong for a record longer than
//     MaxMsgLength, one that runs past the buffer end, or one shorter than
//     the record header. The caller must stop receiving.
//
// Concurrency: invoker-only.
func (copyReceiver *CopyBroadcastReceiver) Receive(handler EventMessageHandler) (int, error) {
	msgTypeID, length, received, err := copyReceiver.pollAndCopy()
	if err != nil || !received {
		return 0, err
	}
	return copyReceiver.deliverCopied(handler, msgTypeID, length)
}

func (copyReceiver *CopyBroadcastReceiver) pollAndCopy() (
	msgTypeID, length int,
	received bool,
	err error,
) {
	receiver := copyReceiver.receiver
	lastSeenLappedCount := receiver.LappedCount()

	if !receiver.ReceiveNext() {
		return 0, 0, false, nil
	}
	invokeReceiveNextHook(copyReceiver)

	// After a lap, ReceiveNext consumed the latest record and set the next
	// record from an unchecked length. Resync moves back to the latest record
	// without a second count, so the next Receive delivers it.
	if lastSeenLappedCount != receiver.LappedCount() {
		receiver.Resync()
		return 0, 0, false, ErrBroadcastLapped
	}

	// A negative length means a record shorter than its header. Its next
	// record sits on the same slot or inside the record, so it never passes.
	length = receiver.Length()
	recordFits := length >= 0 &&
		length <= copyReceiver.maxMsgLength &&
		receiver.Offset()+length <= receiver.Capacity()
	if !recordFits {
		// A valid record outside the bounds means the buffer is not a
		// conforming broadcast. An invalid one means a lap tore the length.
		return 0, 0, false, copyReceiver.resyncIfInvalid(ErrBroadcastMessageTooLong)
	}
	copyReceiver.growScratch(length)

	// Validate covers only reads that precede it. Read the type id with the body.
	msgTypeID = receiver.TypeID()
	copyReceiver.scratch.PutBytesFrom(0, receiver.Buffer(), receiver.Offset(), length)
	invokeReceiveCopiedHook(copyReceiver)
	return msgTypeID, length, true, nil
}

// resyncIfInvalid calls Resync when Validate reports false and returns
// ErrBroadcastLapped. Otherwise it returns valid.
func (copyReceiver *CopyBroadcastReceiver) resyncIfInvalid(valid error) error {
	if copyReceiver.receiver.Validate() {
		return valid
	}
	copyReceiver.receiver.Resync()
	return ErrBroadcastLapped
}

// growScratch grows the scratch to maxMsgLength once length exceeds it. No
// later record needs more, so the scratch grows at most once.
func (copyReceiver *CopyBroadcastReceiver) growScratch(length int) {
	if length <= copyReceiver.scratch.Capacity() {
		return
	}
	copyReceiver.scratch = atomicbuffer.Wrap(make([]byte, copyReceiver.maxMsgLength))
}

func (copyReceiver *CopyBroadcastReceiver) deliverCopied(
	handler EventMessageHandler,
	msgTypeID int,
	length int,
) (int, error) {
	if err := copyReceiver.resyncIfInvalid(nil); err != nil {
		return 0, err
	}
	invokeReceiveValidatedHook(copyReceiver)

	handler(msgTypeID, copyReceiver.scratch, 0, length)
	return 1, nil
}
