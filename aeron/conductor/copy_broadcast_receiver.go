package conductor

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
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
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
)

type CopyBroadcastReceiver struct {
	receiver      *broadcast.BroadcastReceiver
	scratch       atomic.AtomicBuffer
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
		scratch:       atomic.Wrap(make([]byte, scratchLength)),
		scratchLength: scratchLength,
	}
}

func (copyReceiver *CopyBroadcastReceiver) BroadcastReceiver() *broadcast.BroadcastReceiver {
	return copyReceiver.receiver
}

func (copyReceiver *CopyBroadcastReceiver) Receive(handler EventMessageHandler) (int, error) {
	length, received, err := copyReceiver.pollAndCopy()
	if err != nil || !received {
		return 0, err
	}
	return copyReceiver.deliverCopied(handler, length)
}

func (copyReceiver *CopyBroadcastReceiver) pollAndCopy() (length int, received bool, err error) {
	receiver := copyReceiver.receiver
	lastSeenLappedCount := receiver.LappedCount()

	if !receiver.ReceiveNext() {
		return 0, false, nil
	}

	if lastSeenLappedCount != receiver.LappedCount() {
		return 0, false, ErrBroadcastLapped
	}

	length = receiver.Length()
	if length > copyReceiver.scratchLength {
		return 0, false, ErrBroadcastMessageTooLong
	}

	copyReceiver.scratch.PutBytesFrom(0, receiver.Buffer(), receiver.Offset(), length)
	return length, true, nil
}

func (copyReceiver *CopyBroadcastReceiver) deliverCopied(
	handler EventMessageHandler,
	length int,
) (int, error) {
	if !copyReceiver.receiver.Validate() {
		return 0, ErrBroadcastLapped
	}

	msgTypeID := copyReceiver.receiver.TypeID()
	handler(msgTypeID, copyReceiver.scratch, 0, length)
	return 1, nil
}
