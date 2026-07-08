package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
)

func TestDecodeStringAsciiInBody_empty(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, 8))
	buffer.PutInt32(0, 0)

	value, nextOffset, err := decodeStringASCIIInBody(buffer, 0, 4, 0, ErrEventBodyTooShort)
	require.NoError(t, err)
	require.Empty(t, value)
	require.Equal(t, 4, nextOffset)
}

func TestDecodeStringAsciiInBody_nonEmpty(t *testing.T) {
	const value = "ipc"
	buffer := atomicbuffer.Wrap(make([]byte, 16))
	_, err := atomicbuffer.PutStringASCII(buffer, 0, value)
	require.NoError(t, err)

	got, nextOffset, err := decodeStringASCIIInBody(buffer, 0, 7, 0, ErrEventBodyTooShort)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, 7, nextOffset)
}

func TestDecodeStringAsciiInBody_truncatedBody(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, 6))
	buffer.PutInt32(0, 4)

	_, _, err := decodeStringASCIIInBody(buffer, 0, 4, 0, ErrEventBodyTooShort)
	require.ErrorIs(t, err, atomicbuffer.ErrStringASCIIOutOfBounds)
}

func TestDecodeStringAsciiInBody_overCapacity(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, 8))
	buffer.PutInt32(0, 8)

	_, _, err := decodeStringASCIIInBody(buffer, 0, 4, 0, ErrEventBodyTooShort)
	require.ErrorIs(t, err, atomicbuffer.ErrStringASCIIOutOfBounds)
}
