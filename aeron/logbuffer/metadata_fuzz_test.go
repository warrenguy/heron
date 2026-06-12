package logbuffer_test

import (
	"sync"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

type metadataFuzzKind uint8

const (
	fuzzParseMetadataView metadataFuzzKind = iota
	fuzzDefaultFrameHeaderBuffer
	metadataFuzzKindCount
)

var (
	goldenSyntheticMetadataOnce sync.Once
	goldenSyntheticMetadata     []byte
)

func goldenSyntheticMetadataBytes() []byte {
	goldenSyntheticMetadataOnce.Do(func() {
		buf := make([]byte, logbuffer.TestComputedLength)
		cfg := logbuffer.DefaultSyntheticMetadataConfig()
		logbuffer.WriteSyntheticMetadata(buf, cfg)

		metaStart := logbuffer.MetaDataOffset(logbuffer.TestTermLength)
		metaEnd := metaStart + logbuffer.MetaDataLength
		goldenSyntheticMetadata = append([]byte(nil), buf[metaStart:metaEnd]...)
	})
	return goldenSyntheticMetadata
}

func fuzzMetadataBuffer(data []byte, padToFull bool) atomic.AtomicBuffer {
	if len(data) > logbuffer.MetaDataLength {
		data = data[:logbuffer.MetaDataLength]
	}
	size := len(data)
	if padToFull && size < logbuffer.MetaDataLength {
		size = logbuffer.MetaDataLength
	}
	buf := make([]byte, size)
	copy(buf, data)
	return atomic.Wrap(buf)
}

func FuzzMetadata(f *testing.F) {
	golden := goldenSyntheticMetadataBytes()
	f.Add(uint8(fuzzParseMetadataView), golden)
	f.Add(uint8(fuzzDefaultFrameHeaderBuffer), golden)

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if len(data) == 0 || len(data) > logbuffer.MetaDataLength {
			return
		}
		switch metadataFuzzKind(kind % uint8(metadataFuzzKindCount)) {
		case fuzzParseMetadataView:
			_, _ = logbuffer.ParseMetadataView(fuzzMetadataBuffer(data, false))
		case fuzzDefaultFrameHeaderBuffer:
			_ = logbuffer.DefaultFrameHeaderBuffer(fuzzMetadataBuffer(data, true)).Capacity()
		}
	})
}
