package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestHeaderWriter_Write_commitProtocol(t *testing.T) {
	metaBuf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(metaBuf)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	require.NoError(t, logbuffer.WriteDataHeaderUnfragmented(
		meta,
		logbuffer.OffsetDefaultFrameHeader,
		11,
		22,
		7,
		0,
		logbuffer.Unfragmented,
	))

	writer := logbuffer.NewHeaderWriter(
		logbuffer.DefaultFrameHeaderBuffer(meta),
		11,
		22,
	)

	termBuf := make([]byte, 256)
	term := atomic.Wrap(termBuf)
	const (
		termOffset  = 64
		frameLength = int32(96)
		termID      = int32(7)
	)
	writer.Write(term, termOffset, frameLength, termID)

	require.Equal(t, -frameLength, logbuffer.FrameLengthVolatile(term, termOffset))
	require.Equal(t, int8(logbuffer.FrameHeaderVersion), logbuffer.FrameVersion(term, termOffset))
	require.Equal(t, uint8(logbuffer.Unfragmented), logbuffer.FrameFlags(term, termOffset))
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(term, termOffset))
	require.Equal(t, int32(11), logbuffer.FrameSessionID(term, termOffset))
	require.Equal(t, int32(22), logbuffer.FrameStreamID(term, termOffset))
	require.Equal(t, termID, logbuffer.FrameTermID(term, termOffset))
	require.Equal(t, int32(termOffset), logbuffer.FrameTermOffset(term, termOffset))

	logbuffer.PutFrameLengthOrdered(term, termOffset, frameLength)
	require.Equal(t, frameLength, logbuffer.FrameLengthVolatile(term, termOffset))
}

func TestHeaderWriter_NewHeaderWriter_clampsShortTemplate(t *testing.T) {
	shortTemplate := atomic.Wrap(make([]byte, 16))
	shortTemplate.Bytes()[logbuffer.OffsetVersion] = byte(logbuffer.FrameHeaderVersion)

	writer := logbuffer.NewHeaderWriter(shortTemplate, 99, 100)

	termBuf := make([]byte, logbuffer.DataHeaderLength)
	term := atomic.Wrap(termBuf)
	writer.Write(term, 0, 32, 1)

	require.Equal(t, int8(logbuffer.FrameHeaderVersion), logbuffer.FrameVersion(term, 0))
	require.Equal(t, int32(99), logbuffer.FrameSessionID(term, 0))
	require.Equal(t, int32(100), logbuffer.FrameStreamID(term, 0))
}
