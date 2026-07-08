package ringbuffer

import (
	"github.com/warrenguy/heron/record"
)

const (
	HeaderLength         = record.HeaderLength
	Alignment            = record.Alignment
	PaddingMsgTypeID     = record.PaddingMsgTypeID
	InsufficientCapacity = record.InsufficientCapacity
	MinCapacity          = record.HeaderLength
)

// EncodedMsgOffset is the byte offset of the encoded message (alias for record.EncodedMsgOffset).
var EncodedMsgOffset = record.EncodedMsgOffset

func MaxMsgLength(dataCapacity int) int {
	if dataCapacity == MinCapacity {
		return 0
	}
	return max(HeaderLength, dataCapacity>>3)
}
