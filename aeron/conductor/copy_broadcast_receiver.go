package conductor

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
)

const ScratchBufferLength = 4096

var (
	ErrBroadcastLapped         = errors.New("conductor: unable to keep up with broadcast")
	ErrBroadcastMessageTooLong = errors.New(
		"conductor: broadcast message exceeds scratch buffer",
	)
)

type EventMessageHandler func(
	msgTypeID int,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
)

type CopyBroadcastReceiver struct {
	receiver      *broadcast.BroadcastReceiver
	scratch       atomicbuffer.AtomicBuffer
	scratchLength int
}

func NewCopyBroadcastReceiver(receiver *broadcast.BroadcastReceiver) *CopyBroadcastReceiver {
	return newCopyBroadcastReceiver(receiver, ScratchBufferLength)
}

func newCopyBroadcastReceiver(
	receiver *broadcast.BroadcastReceiver,
	scratchLength int,
) *CopyBroadcastReceiver {
	return &CopyBroadcastReceiver{
		receiver:      receiver,
		scratch:       atomicbuffer.Wrap(make([]byte, scratchLength)),
		scratchLength: scratchLength,
	}
}

func (copyReceiver *CopyBroadcastReceiver) BroadcastReceiver() *broadcast.BroadcastReceiver {
	return copyReceiver.receiver
}

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

	if lastSeenLappedCount != receiver.LappedCount() {
		return 0, 0, false, ErrBroadcastLapped
	}

	length = receiver.Length()
	if length > copyReceiver.scratchLength {
		return 0, 0, false, ErrBroadcastMessageTooLong
	}

	// Validate covers only reads that precede it. Read the type id with the body.
	msgTypeID = receiver.TypeID()
	copyReceiver.scratch.PutBytesFrom(0, receiver.Buffer(), receiver.Offset(), length)
	return msgTypeID, length, true, nil
}

func (copyReceiver *CopyBroadcastReceiver) deliverCopied(
	handler EventMessageHandler,
	msgTypeID int,
	length int,
) (int, error) {
	if !copyReceiver.receiver.Validate() {
		return 0, ErrBroadcastLapped
	}
	invokeReceiveValidatedHook(copyReceiver)

	handler(msgTypeID, copyReceiver.scratch, 0, length)
	return 1, nil
}
