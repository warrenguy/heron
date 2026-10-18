//go:build rapid

package logbuffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestMapLogBuffer_layoutRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		termLength := 1 << rapid.IntRange(16, 20).Draw(t, "termShift")
		pageSize := 1 << rapid.IntRange(12, 20).Draw(t, "pageShift")
		rawLength := logbuffer.PartitionCount*termLength + logbuffer.MetaDataLength
		logLength := (rawLength + pageSize - 1) / pageSize * pageSize
		tail := logLength - logbuffer.MetaDataLength

		cfg := logbuffer.DefaultSyntheticMetadataConfig()
		cfg.TermLength = int32(termLength)
		cfg.PageSize = int32(pageSize)
		buf := make([]byte, logLength)
		logbuffer.WriteSyntheticMetadata(buf, cfg)
		for i := range logbuffer.PartitionCount {
			putTermMarker(buf, i, termLength)
		}

		mapped, err := logbuffer.MapLogBuffer(buf)
		require.NoError(t, err)
		require.Equal(t, termLength, mapped.TermLength())
		require.Equal(t, pageSize, mapped.PageSize())
		require.Equal(t, logLength, mapped.ComputedLength())
		require.Equal(t, tail, logbuffer.LogMetaDataOffset(logLength))
		for i := range logbuffer.PartitionCount {
			term, err := mapped.TermBuffer(i)
			require.NoError(t, err)
			require.Equal(t, termLength, term.Capacity())
			require.Equal(t, termMarkerValue(i), term.GetInt32(0))
		}
		meta, err := mapped.MetaDataBuffer()
		require.NoError(t, err)
		require.Equal(t, buf[tail:], meta.Bytes())
		require.NoError(t, mapped.Close())

		minDelta := max(-pageSize, logbuffer.MetaDataLength-logLength)
		delta := rapid.IntRange(minDelta, pageSize-1).Draw(t, "delta")
		if delta >= 0 {
			delta++
		}
		shifted := make([]byte, logLength+delta)
		copy(shifted[logbuffer.LogMetaDataOffset(len(shifted)):], buf[tail:])
		_, err = logbuffer.MapLogBuffer(shifted)
		require.EqualError(t, err, fmt.Sprintf(
			"logbuffer: log length %d != computed length %d (term length %d, page size %d)",
			len(shifted),
			logLength,
			termLength,
			pageSize,
		))

		metaAtTerms := logbuffer.PartitionCount * termLength
		if tail != metaAtTerms {
			headOnly := make([]byte, logLength)
			logbuffer.WriteSyntheticMetadata(headOnly[:metaAtTerms+logbuffer.MetaDataLength], cfg)
			_, err = logbuffer.MapLogBuffer(headOnly)
			require.EqualError(t, err, "logbuffer: term length below minimum: 0 < 65536")
		}
	})
}

func termMarkerValue(index int) int32 {
	return int32(0x7E570000 + index)
}

func putTermMarker(buf []byte, index, termLength int) {
	offset := logbuffer.SectionTermOffset(index, termLength)
	atomicbuffer.Wrap(buf[offset:offset+atomicbuffer.SizeInt32]).PutInt32(0, termMarkerValue(index))
}
