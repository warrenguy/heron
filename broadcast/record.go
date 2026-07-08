package broadcast

import (
	"github.com/warrenguy/heron/record"
)

const (
	HeaderLength     = record.HeaderLength
	Alignment        = record.Alignment
	PaddingMsgTypeID = record.PaddingMsgTypeID
)

// MsgOffset is the byte offset of the encoded message (alias for record.EncodedMsgOffset).
var MsgOffset = record.EncodedMsgOffset

func MaxMsgLength(capacity int) int {
	max := capacity / HeaderLength
	for max > 0 && record.AlignRecordLength(HeaderLength+max) > capacity {
		max--
	}
	return max
}
