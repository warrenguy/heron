//go:build rapid

package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// byteHeaderWriter is the oracle: the field-by-field writer that copied the
// template into each header. It writes the reserved value from the template.
type byteHeaderWriter struct {
	header [logbuffer.DataHeaderLength]byte
}

func newByteHeaderWriter(
	template atomicbuffer.AtomicBuffer,
	sessionID int32,
	streamID int32,
) byteHeaderWriter {
	var writer byteHeaderWriter

	copyLength := min(template.Capacity(), logbuffer.DataHeaderLength)
	if copyLength < logbuffer.DataHeaderLength {
		writer.header[logbuffer.OffsetVersion] = byte(logbuffer.FrameHeaderVersion)
	}
	copy(writer.header[:], template.Bytes()[:copyLength])

	header := atomicbuffer.Wrap(writer.header[:])
	logbuffer.PutFrameSessionID(header, 0, sessionID)
	logbuffer.PutFrameStreamID(header, 0, streamID)

	return writer
}

func (writer *byteHeaderWriter) write(
	termBuf atomicbuffer.AtomicBuffer,
	termOffset int,
	frameLength int32,
	termID int32,
	flags uint8,
) {
	header := writer.header[:]

	logbuffer.PutFrameLengthRelease(termBuf, termOffset, -frameLength)

	termBuf.PutBytes(
		termOffset+logbuffer.OffsetVersion,
		header[logbuffer.OffsetVersion:logbuffer.OffsetFlags],
	)
	logbuffer.PutFrameFlags(termBuf, termOffset, flags)
	termBuf.PutBytes(
		termOffset+logbuffer.OffsetFrameType,
		header[logbuffer.OffsetFrameType:logbuffer.OffsetTermOffset],
	)
	termBuf.PutBytes(
		termOffset+logbuffer.OffsetSessionID,
		header[logbuffer.OffsetSessionID:logbuffer.OffsetTermID],
	)
	termBuf.PutBytes(
		termOffset+logbuffer.OffsetReservedValue,
		header[logbuffer.OffsetReservedValue:],
	)

	logbuffer.PutFrameTermID(termBuf, termOffset, termID)
	logbuffer.PutFrameTermOffset(termBuf, termOffset, int32(termOffset))
}

func TestHeaderWriter_matchesByteWriter_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		templateLength := rapid.IntRange(0, logbuffer.DataHeaderLength+8).Draw(rt, "templateLength")
		templateBytes := rapid.SliceOfN(rapid.Byte(), templateLength, templateLength).
			Draw(rt, "template")
		template := atomicbuffer.Wrap(templateBytes)

		sessionID := rapid.Int32().Draw(rt, "sessionID")
		streamID := rapid.Int32().Draw(rt, "streamID")
		termID := rapid.Int32().Draw(rt, "termID")
		frameLength := rapid.Int32().Draw(rt, "frameLength")

		frameCount := rapid.IntRange(1, 8).Draw(rt, "frameCount")
		frameIndex := rapid.IntRange(0, frameCount-1).Draw(rt, "frameIndex")
		termOffset := frameIndex * logbuffer.FrameAlignment

		useTemplateFlags := rapid.Bool().Draw(rt, "useTemplateFlags")
		flags := rapid.Byte().Draw(rt, "flags")

		termLength := frameCount * logbuffer.FrameAlignment
		stale := rapid.SliceOfN(rapid.Byte(), termLength, termLength).Draw(rt, "stale")
		want := append([]byte(nil), stale...)
		got := append([]byte(nil), stale...)

		oracle := newByteHeaderWriter(template, sessionID, streamID)
		writer := logbuffer.NewHeaderWriter(template, sessionID, streamID)
		if useTemplateFlags {
			flags = oracle.header[logbuffer.OffsetFlags]
			writer.Write(atomicbuffer.Wrap(got), termOffset, frameLength, termID)
		} else {
			writer.WriteWithFlags(atomicbuffer.Wrap(got), termOffset, frameLength, termID, flags)
		}

		oracle.write(atomicbuffer.Wrap(want), termOffset, frameLength, termID, flags)

		reservedStart := termOffset + logbuffer.OffsetReservedValue
		headerEnd := termOffset + logbuffer.DataHeaderLength

		require.Equal(rt, want[termOffset:reservedStart], got[termOffset:reservedStart], "header words")
		require.Equal(rt, stale[reservedStart:headerEnd], got[reservedStart:headerEnd], "reserved value")
		require.Equal(rt, stale[:termOffset], got[:termOffset], "bytes before the header")
		require.Equal(rt, stale[headerEnd:], got[headerEnd:], "bytes after the header")
	})
}

func TestHeaderWriter_WriteHeader_fieldsAtAnyTermOffset_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		sessionID := rapid.Int32().Draw(rt, "sessionID")
		streamID := rapid.Int32().Draw(rt, "streamID")
		termID := rapid.Int32().Draw(rt, "termID")
		frameLength := rapid.Int32().Draw(rt, "frameLength")

		frameCount := logbuffer.TermMaxLength / logbuffer.FrameAlignment
		termOffset := rapid.IntRange(0, frameCount-1).Draw(rt, "frameIndex") * logbuffer.FrameAlignment

		template := atomicbuffer.Wrap(make([]byte, logbuffer.DataHeaderLength))
		writer := logbuffer.NewHeaderWriter(template, sessionID, streamID)

		frame := make([]byte, logbuffer.DataHeaderLength)
		writer.WriteHeader(frame, termOffset, frameLength, termID)

		header := atomicbuffer.Wrap(frame)
		require.Equal(rt, -frameLength, logbuffer.FrameLength(header, 0))
		require.Equal(rt, int32(termOffset), logbuffer.FrameTermOffset(header, 0))
		require.Equal(rt, sessionID, logbuffer.FrameSessionID(header, 0))
		require.Equal(rt, streamID, logbuffer.FrameStreamID(header, 0))
		require.Equal(rt, termID, logbuffer.FrameTermID(header, 0))
	})
}
