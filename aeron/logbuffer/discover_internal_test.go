package logbuffer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
)

func TestDiscoverDimensionsFromBuffer_shortMetaSpan(t *testing.T) {
	const termLength = 65536
	metaStart := MetaDataOffset(termLength)
	shortLen := metaStart + OffsetPageSize + 8
	require.Less(t, shortLen, metaStart+MetaDataLength)

	buf := make([]byte, shortLen)
	meta := atomicbuffer.Wrap(buf[metaStart:])
	meta.PutInt32(OffsetTermLength, termLength)
	meta.PutInt32(OffsetPageSize, TestPageSize)

	require.NotPanics(t, func() {
		_, err := discoverDimensionsFromBuffer(buf)
		require.EqualError(
			t,
			err,
			"logbuffer: could not discover log layout in span 196896",
		)
	})
}

func TestValidateMetadataDimensions_drift(t *testing.T) {
	err := validateMetadataDimensions(65536, 4096, 65536, 8192)
	require.EqualError(
		t,
		err,
		"logbuffer: metadata layout drift: header term 65536 page 4096 != live term 65536 page 8192",
	)
}
