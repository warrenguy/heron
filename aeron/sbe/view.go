package sbe

import (
	"encoding/binary"
	"errors"
)

// ErrVarSliceOutOfBounds is returned by ReadVarSlice when the length prefix or payload
// extends past the end of buf.
var ErrVarSliceOutOfBounds = errors.New("sbe: var slice out of buffer bounds")

// ErrVarSliceTooLarge is returned by ReadVarSlice when the decoded length exceeds
// the non-negative maxLen bound.
var ErrVarSliceTooLarge = errors.New("sbe: var slice length exceeds max")

const VarLengthPrefixSize = 4

// VarSlice is a zero-alloc view into a fragment buffer for varAscii/varData fields.
type VarSlice struct {
	Data []byte
}

// ReadVarSlice reads a little-endian uint32 length prefix at offset and returns a slice
// view of the following bytes. nextOffset is the index immediately after the payload.
// When maxLen is non-negative, lengths above maxLen are rejected.
func ReadVarSlice(buf []byte, offset, maxLen int) (VarSlice, int, error) {
	if offset < 0 || offset > len(buf) {
		return VarSlice{}, offset, ErrVarSliceOutOfBounds
	}

	prefixEnd := offset + VarLengthPrefixSize
	if prefixEnd < offset || prefixEnd > len(buf) {
		return VarSlice{}, offset, ErrVarSliceOutOfBounds
	}

	length := binary.LittleEndian.Uint32(buf[offset:prefixEnd])
	remaining := len(buf) - prefixEnd
	if length > uint32(remaining) {
		return VarSlice{}, offset, ErrVarSliceOutOfBounds
	}
	if maxLen >= 0 && int(length) > maxLen {
		return VarSlice{}, offset, ErrVarSliceTooLarge
	}

	end := prefixEnd + int(length)
	return VarSlice{Data: buf[prefixEnd:end]}, end, nil
}
