package broadcast

import (
	"github.com/warrenguy/heron/record"
)

func (r *BroadcastReceiver) PendingMessageForTest() (
	msgTypeID int,
	payload []byte,
	ok bool,
) {
	recordOffset, recordLength, msgTypeID, ok := r.pendingRecordAt(r.nextRecord)
	if !ok {
		return 0, nil, false
	}

	bodyLength := recordLength - HeaderLength
	payloadOffset := record.EncodedMsgOffset(recordOffset)
	payload = r.buffer.Bytes()[payloadOffset : payloadOffset+bodyLength]
	return msgTypeID, payload, true
}

// NextRecordForTest returns the position of the record the next ReceiveNext
// loads.
func (r *BroadcastReceiver) NextRecordForTest() int64 {
	return r.nextRecord
}
