package cnc_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/agrona/atomic"
)

var (
	seedReadyMetadata = func() []byte {
		buf := make([]byte, cnc.MetaDataLength)
		cnc.WriteMetadataFieldsToBuffer(buf, cnc.ReadyMetadata())
		return buf
	}()
	seedNotReadyMetadata     = make([]byte, cnc.MetaDataLength)
	seedIncompatibleMetadata = func() []byte {
		buf := make([]byte, cnc.MetaDataLength)
		cnc.WriteMetadataFieldsToBuffer(buf, cnc.ReadyMetadata())
		atomic.Wrap(buf).PutInt32Release(
			cnc.OffsetCncVersion,
			cnc.ComposeSemanticVersion(99, 0, 0),
		)
		return buf
	}()
	seedGarbageMetadata = func() []byte {
		buf := make([]byte, cnc.MetaDataLength)
		for i := range buf {
			buf[i] = byte(i * 37)
		}
		return buf
	}()
)

func FuzzParseMetadata(f *testing.F) {
	f.Add(seedReadyMetadata)
	f.Add(seedNotReadyMetadata)
	f.Add(seedIncompatibleMetadata)
	f.Add(seedGarbageMetadata)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > cnc.MetaDataLength {
			return
		}
		_, _ = cnc.ParseMetadata(data)
	})
}
