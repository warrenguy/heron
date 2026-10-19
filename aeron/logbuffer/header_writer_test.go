package logbuffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestHeaderWriter_Write_commitProtocol(t *testing.T) {
	metaBuf := make([]byte, logbuffer.MetaDataLength)
	meta := atomicbuffer.Wrap(metaBuf)
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
	term := atomicbuffer.Wrap(termBuf)
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
	cases := []struct {
		name           string
		templateLength int
		version        byte
		wantVersion    int8
	}{
		// A template that holds the version byte keeps it.
		{name: "versionInTemplate", templateLength: 16, version: 7, wantVersion: 7},

		// A template too short for the version byte gets FrameHeaderVersion.
		{
			name:           "versionPastTemplate",
			templateLength: logbuffer.OffsetVersion,
			wantVersion:    int8(logbuffer.FrameHeaderVersion),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			template := make([]byte, tc.templateLength)
			if tc.templateLength > logbuffer.OffsetVersion {
				template[logbuffer.OffsetVersion] = tc.version
			}

			writer := logbuffer.NewHeaderWriter(atomicbuffer.Wrap(template), 99, 100)

			term := atomicbuffer.Wrap(make([]byte, logbuffer.DataHeaderLength))
			writer.Write(term, 0, 32, 1)

			require.Equal(t, tc.wantVersion, logbuffer.FrameVersion(term, 0))
			require.Equal(t, uint8(0), logbuffer.FrameFlags(term, 0))
			require.Equal(t, int32(99), logbuffer.FrameSessionID(term, 0))
			require.Equal(t, int32(100), logbuffer.FrameStreamID(term, 0))
		})
	}
}

func newTestHeaderWriter(t *testing.T) logbuffer.HeaderWriter {
	t.Helper()
	template := atomicbuffer.Wrap(make([]byte, logbuffer.DataHeaderLength))
	require.NoError(t, logbuffer.WriteDataHeaderUnfragmented(
		template,
		0,
		11,
		22,
		7,
		0,
		logbuffer.Unfragmented,
	))

	return logbuffer.NewHeaderWriter(template, 11, 22)
}

func TestHeaderWriter_WriteWithFlags_replacesTemplateFlags(t *testing.T) {
	writer := newTestHeaderWriter(t)

	const (
		termOffset  = 32
		frameLength = int32(64)
		termID      = int32(9)
	)

	for _, flags := range []uint8{0, logbuffer.BeginFlag, logbuffer.EndFlag, 0xFF} {
		t.Run(fmt.Sprintf("flags=%#x", flags), func(t *testing.T) {
			term := atomicbuffer.Wrap(make([]byte, 128))
			writer.WriteWithFlags(term, termOffset, frameLength, termID, flags)

			require.Equal(t, -frameLength, logbuffer.FrameLengthVolatile(term, termOffset))
			require.Equal(t, int8(logbuffer.FrameHeaderVersion), logbuffer.FrameVersion(term, termOffset))
			require.Equal(t, flags, logbuffer.FrameFlags(term, termOffset))
			require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(term, termOffset))
			require.Equal(t, int32(termOffset), logbuffer.FrameTermOffset(term, termOffset))
			require.Equal(t, int32(11), logbuffer.FrameSessionID(term, termOffset))
			require.Equal(t, int32(22), logbuffer.FrameStreamID(term, termOffset))
			require.Equal(t, termID, logbuffer.FrameTermID(term, termOffset))
		})
	}

	// WriteWithFlags leaves the template flags in place for Write.
	term := atomicbuffer.Wrap(make([]byte, 128))
	writer.Write(term, termOffset, frameLength, termID)

	require.Equal(t, uint8(logbuffer.Unfragmented), logbuffer.FrameFlags(term, termOffset))
}

func TestHeaderWriter_WriteHeader_matchesWrite(t *testing.T) {
	writer := newTestHeaderWriter(t)

	const (
		termOffset  = 64
		frameLength = int32(40)
		termID      = int32(-3)
	)

	written := make([]byte, 256)
	writer.Write(atomicbuffer.Wrap(written), termOffset, frameLength, termID)

	direct := make([]byte, 256)
	writer.WriteHeader(direct[termOffset:termOffset+frameLength], termOffset, frameLength, termID)

	require.Equal(t, written, direct)
	require.Equal(t, -frameLength, logbuffer.FrameLength(atomicbuffer.Wrap(direct), termOffset))
}

func TestHeaderWriter_Write_keepsReservedValue(t *testing.T) {
	writer := newTestHeaderWriter(t)

	const stale = int64(0x0102030405060708)
	term := atomicbuffer.Wrap(make([]byte, 64))
	logbuffer.PutFrameReservedValue(term, 0, stale)

	writer.Write(term, 0, 48, 1)

	require.Equal(t, stale, logbuffer.FrameReservedValue(term, 0))
	require.Equal(t, int32(-48), logbuffer.FrameLength(term, 0))
}

func TestHeaderWriter_headerOutOfBoundsPanics(t *testing.T) {
	writer := newTestHeaderWriter(t)
	term := atomicbuffer.Wrap(make([]byte, 64))

	cases := []struct {
		name string
		fn   func()
	}{
		{"Write past term end", func() { writer.Write(term, 40, 32, 1) }},
		{"Write negative offset", func() { writer.Write(term, -32, 32, 1) }},
		{"WriteWithFlags past term end", func() {
			writer.WriteWithFlags(term, 64, 32, 1, logbuffer.BeginFlag)
		}},
		{"WriteHeader short frame", func() {
			writer.WriteHeader(make([]byte, logbuffer.DataHeaderLength-1), 0, 32, 1)
		}},
	}
	for _, tc := range cases {
		require.Panics(t, tc.fn, tc.name)
	}

	require.Equal(t, make([]byte, 64), term.Bytes(), "no store before the bounds check")
}
