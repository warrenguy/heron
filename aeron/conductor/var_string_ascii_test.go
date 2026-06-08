package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestDecodeStringAsciiInBody_empty(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 8))
	buffer.PutInt32(0, 0)

	value, nextOffset, err := decodeStringASCIIInBody(buffer, 0, 4, 0, ErrEventBodyTooShort)
	require.NoError(t, err)
	require.Empty(t, value)
	require.Equal(t, 4, nextOffset)
}

func TestDecodeStringAsciiInBody_nonEmpty(t *testing.T) {
	const value = "ipc"
	buffer := atomic.Wrap(make([]byte, 16))
	_, err := atomic.PutStringASCII(buffer, 0, value)
	require.NoError(t, err)

	got, nextOffset, err := decodeStringASCIIInBody(buffer, 0, 7, 0, ErrEventBodyTooShort)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, 7, nextOffset)
}

func TestDecodeStringAsciiInBody_truncatedBody(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 6))
	buffer.PutInt32(0, 4)

	_, _, err := decodeStringASCIIInBody(buffer, 0, 4, 0, ErrEventBodyTooShort)
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}

func TestDecodeStringAsciiInBody_overCapacity(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 8))
	buffer.PutInt32(0, 8)

	_, _, err := decodeStringASCIIInBody(buffer, 0, 4, 0, ErrEventBodyTooShort)
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}
