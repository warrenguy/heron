//go:build rapid

package codecs_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive/codecs"
)

func TestReadVarSlice_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		bufLen := rapid.IntRange(0, 256).Draw(rt, "bufLen")
		buf := make([]byte, bufLen)

		offset := rapidWireOffset(rt, bufLen)

		writePrefix := rapid.Bool().Draw(rt, "writePrefix")
		prefixEnd := offset + codecs.VarLengthPrefixSize
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

		var got codecs.VarSlice
		var next int
		var err error
		require.NotPanics(rt, func() {
			got, next, err = codecs.ReadVarSlice(buf, offset, maxLen)
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
		return false, nil, offset, codecs.ErrVarSliceOutOfBounds
	}

	prefixEnd := offset + codecs.VarLengthPrefixSize
	if prefixEnd < offset || prefixEnd > len(buf) {
		return false, nil, offset, codecs.ErrVarSliceOutOfBounds
	}

	length := binary.LittleEndian.Uint32(buf[offset:prefixEnd])
	remaining := len(buf) - prefixEnd
	if length > uint32(remaining) {
		return false, nil, offset, codecs.ErrVarSliceOutOfBounds
	}
	if maxLen >= 0 && int(length) > maxLen {
		return false, nil, offset, codecs.ErrVarSliceTooLarge
	}

	end := prefixEnd + int(length)
	return true, buf[prefixEnd:end], end, nil
}
