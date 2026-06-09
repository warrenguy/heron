package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestDataHeaderLength(t *testing.T) {
	require.Equal(t, 32, logbuffer.DataHeaderLength)
}

func TestWriteReadDataHeader(t *testing.T) {
	buf := make([]byte, logbuffer.DataHeaderLength)
	buffer := atomic.Wrap(buf)

	const (
		sessionID  = int32(11)
		streamID   = int32(22)
		termID     = int32(7)
		termOffset = int32(128)
		flags      = uint8(0x01)
	)
	require.NoError(t, logbuffer.WriteDataHeaderUnfragmented(
		buffer,
		0,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
	))
	logbuffer.PutFrameLengthOrdered(buffer, 0, 64)

	require.Equal(t, int32(64), logbuffer.FrameLength(buffer, 0))
	require.Equal(t, int8(logbuffer.FrameHeaderVersion), logbuffer.FrameVersion(buffer, 0))
	require.Equal(t, flags|logbuffer.Unfragmented, logbuffer.FrameFlags(buffer, 0))
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(buffer, 0))
	require.Equal(t, sessionID, logbuffer.FrameSessionID(buffer, 0))
	require.Equal(t, streamID, logbuffer.FrameStreamID(buffer, 0))
	require.Equal(t, termID, logbuffer.FrameTermID(buffer, 0))
	require.Equal(t, termOffset, logbuffer.FrameTermOffset(buffer, 0))
	require.Equal(t, int64(0), logbuffer.FrameReservedValue(buffer, 0))
}

func TestDataHeader_goldenBytes(t *testing.T) {
	buf := make([]byte, logbuffer.DataHeaderLength)
	buffer := atomic.Wrap(buf)
	require.NoError(t, logbuffer.WriteDataHeaderUnfragmented(buffer, 0, 11, 22, 7, 128, 0x01))
	logbuffer.PutFrameLengthOrdered(buffer, 0, 64)

	want := []byte{
		0x40, 0x00, 0x00, 0x00, // frame length 64
		0x00,       // version
		0xC1,       // flags 0x01 | 0xC0
		0x01, 0x00, // type DATA
		0x80, 0x00, 0x00, 0x00, // term offset 128
		0x0B, 0x00, 0x00, 0x00, // session 11
		0x16, 0x00, 0x00, 0x00, // stream 22
		0x07, 0x00, 0x00, 0x00, // term id 7
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
	}
	require.Equal(t, want, buf)
}

func TestIsPaddingFrame(t *testing.T) {
	buf := make([]byte, logbuffer.DataHeaderLength)
	buffer := atomic.Wrap(buf)
	require.NoError(t, logbuffer.WritePaddingFrame(buffer, 0, 7, 65520))
	require.True(t, logbuffer.IsPaddingFrame(buffer, 0))

	require.NoError(t, logbuffer.WriteDataHeaderUnfragmented(buffer, 0, 1, 2, 3, 4, 0))
	require.False(t, logbuffer.IsPaddingFrame(buffer, 0))
}

func TestIsPaddingFrame_lowByteOnlyMismatch(t *testing.T) {
	buf := make([]byte, logbuffer.DataHeaderLength)
	buffer := atomic.Wrap(buf)
	buf[logbuffer.OffsetFrameType] = 0
	buf[logbuffer.OffsetFrameType+1] = 0x01

	require.NotEqual(t, int16(logbuffer.HdrTypePad), logbuffer.FrameType(buffer, 0))
	require.False(t, logbuffer.IsPaddingFrame(buffer, 0))
}

func TestAlignFrameLength(t *testing.T) {
	require.Equal(t, 64, logbuffer.AlignFrameLength(33))
	require.Equal(t, 32, logbuffer.AlignFrameLength(32))
}

func TestFrameLengthVolatile(t *testing.T) {
	buf := make([]byte, logbuffer.DataHeaderLength)
	buffer := atomic.Wrap(buf)
	logbuffer.PutFrameLengthRelease(buffer, 0, -64)
	require.Equal(t, int32(-64), logbuffer.FrameLengthVolatile(buffer, 0))
}

func TestPutFrameMutators_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, logbuffer.DataHeaderLength-1))
	require.NotPanics(t, func() {
		logbuffer.PutFrameLengthOrdered(buffer, 0, 64)
		logbuffer.PutFrameLengthRelease(buffer, 0, -64)
		logbuffer.PutFrameType(buffer, 0, logbuffer.HdrTypeData)
		logbuffer.PutFrameFlags(buffer, 0, logbuffer.BeginFlag)
		logbuffer.PutFrameSessionID(buffer, 0, 1)
	})
	require.Equal(t, int32(0), logbuffer.FrameLength(buffer, 0))
	require.Equal(t, int16(0), logbuffer.FrameType(buffer, 0))
}

func TestFrameRead_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, logbuffer.DataHeaderLength-1))
	require.Equal(t, int32(0), logbuffer.FrameLength(buffer, 0))
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(buffer, 0))
	require.Equal(t, int8(0), logbuffer.FrameVersion(buffer, 0))
	require.Equal(t, uint8(0), logbuffer.FrameFlags(buffer, 0))
	require.Equal(t, int16(0), logbuffer.FrameType(buffer, 0))
	require.Equal(t, int32(0), logbuffer.FrameTermOffset(buffer, 0))
	require.Equal(t, int32(0), logbuffer.FrameSessionID(buffer, 0))
	require.Equal(t, int32(0), logbuffer.FrameStreamID(buffer, 0))
	require.Equal(t, int32(0), logbuffer.FrameTermID(buffer, 0))
	require.Equal(t, int64(0), logbuffer.FrameReservedValue(buffer, 0))
	require.False(t, logbuffer.IsPaddingFrame(buffer, 0))
}

func TestWriteDataHeaderUnfragmented_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, logbuffer.DataHeaderLength-1))
	err := logbuffer.WriteDataHeaderUnfragmented(buffer, 0, 1, 2, 3, 4, 0)
	require.EqualError(t, err, "logbuffer: frame header out of bounds")
}

func TestWritePaddingFrame_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, logbuffer.DataHeaderLength-1))
	err := logbuffer.WritePaddingFrame(buffer, 0, 1, 2)
	require.EqualError(t, err, "logbuffer: frame header out of bounds")
}
