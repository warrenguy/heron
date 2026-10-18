package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

const fuzzMaxLogLength = 524288

// fuzzLogLengthSeed returns the logLength input that maps to a log of length bytes.
func fuzzLogLengthSeed(length int) uint32 {
	return uint32(length - logbuffer.MetaDataLength)
}

// addFuzzTailSeed seeds the fuzz with the length and metadata tail of a synthetic log.
func addFuzzTailSeed(f *testing.F, pageSize int32, logLength int) {
	f.Helper()
	buf := syntheticLog(f, logbuffer.TestTermLength, pageSize, logLength)
	f.Add(fuzzLogLengthSeed(len(buf)), buf[logbuffer.LogMetaDataOffset(len(buf)):])
}

func FuzzMapLogBuffer(f *testing.F) {
	addFuzzTailSeed(f, 4096, 0)
	addFuzzTailSeed(f, 8192, 0)
	addFuzzTailSeed(f, 65536, 0)
	f.Add(fuzzLogLengthSeed(262144), []byte{})
	addFuzzTailSeed(f, 8192, logbuffer.TestComputedLength)

	f.Fuzz(func(t *testing.T, logLength uint32, meta []byte) {
		span := uint32(fuzzMaxLogLength - logbuffer.MetaDataLength + 1)
		length := logbuffer.MetaDataLength + int(logLength%span)
		buf := make([]byte, length)
		tail := logbuffer.LogMetaDataOffset(length)
		copy(buf[tail:], meta)

		mapped, err := logbuffer.MapLogBuffer(buf)
		if err != nil {
			require.Nil(t, mapped)
			return
		}
		defer func() { _ = mapped.Close() }()

		require.Equal(t, length, mapped.ComputedLength())
		computed, err := logbuffer.ComputeLogLength(mapped.TermLength(), mapped.PageSize())
		require.NoError(t, err)
		require.Equal(t, length, computed)
		require.LessOrEqual(
			t,
			logbuffer.PartitionCount*mapped.TermLength()+logbuffer.MetaDataLength,
			length,
		)
		metaBuffer, err := mapped.MetaDataBuffer()
		require.NoError(t, err)
		require.Equal(t, buf[tail:], metaBuffer.Bytes())
	})
}
