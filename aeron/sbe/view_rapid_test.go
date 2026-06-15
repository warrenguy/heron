//go:build rapid

package sbe_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/sbe"
)

const varLengthPrefixSize = 4

func TestReadVarSlice_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		bufLen := rapid.IntRange(0, 256).Draw(rt, "bufLen")
		buf := make([]byte, bufLen)

		offset := rapid.OneOf(
			rapid.IntRange(-16, bufLen+16),
			rapid.Just(math.MaxInt),
		).Draw(rt, "offset")

		writePrefix := rapid.Bool().Draw(rt, "writePrefix")
		prefixEnd := offset + varLengthPrefixSize
		canWritePrefix := writePrefix &&
			offset >= 0 &&
			prefixEnd >= offset &&
			prefixEnd <= bufLen
		if canWritePrefix {
			length := rapid.Uint32().Draw(rt, "length")
			binary.LittleEndian.PutUint32(buf[offset:], length)
		}

		maxLen := rapid.OneOf(
			rapid.Just(-1),
			rapid.IntRange(0, 64),
		).Draw(rt, "maxLen")

		wantValid, wantData, wantNext, wantErr := readVarSliceOracle(buf, offset, maxLen)

		var got sbe.VarSlice
		var next int
		var err error
		require.NotPanics(rt, func() {
			got, next, err = sbe.ReadVarSlice(buf, offset, maxLen)
		})

		if !wantValid {
			require.Error(rt, err)
			require.ErrorIs(rt, err, wantErr)
			return
		}

		require.NoError(rt, err)
		require.Equal(rt, wantData, got.Data)
		require.Equal(rt, wantNext, next)
	})
}

func readVarSliceOracle(
	buf []byte,
	offset, maxLen int,
) (valid bool, data []byte, nextOffset int, wantErr error) {
	if offset < 0 || offset > len(buf) {
		return false, nil, offset, sbe.ErrVarSliceOutOfBounds
	}

	prefixEnd := offset + varLengthPrefixSize
	if prefixEnd < offset || prefixEnd > len(buf) {
		return false, nil, offset, sbe.ErrVarSliceOutOfBounds
	}

	length := binary.LittleEndian.Uint32(buf[offset:prefixEnd])
	remaining := len(buf) - prefixEnd
	if length > uint32(remaining) {
		return false, nil, offset, sbe.ErrVarSliceOutOfBounds
	}
	if maxLen >= 0 && int(length) > maxLen {
		return false, nil, offset, sbe.ErrVarSliceTooLarge
	}

	end := prefixEnd + int(length)
	return true, buf[prefixEnd:end], end, nil
}
