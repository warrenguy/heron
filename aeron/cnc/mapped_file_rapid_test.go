package cnc_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"pgregory.net/rapid"
)

func TestMapCncFile_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		meta := cnc.DrawRapidBufferLengths(rt, cnc.TestRapidMaxField)

		path := filepath.Join(t.TempDir(), cnc.FileName)
		writeCncFile(t, path, meta)

		mapped, err := cnc.MapCncFile(path)
		require.NoError(rt, err, "MapCncFile")
		defer func() { _ = mapped.Close() }()

		cnctest.AssertMappedBufferLengths(rt, mapped, meta)

		if meta.ToDriverBufferLength >= 4 {
			toDriver := mapped.ToDriverBuffer()
			toDriverAtomic := mapped.ToDriverAtomic()
			toDriverAtomic.PutInt32(0, 7)
			require.Equal(
				rt, int32(7), atomic.Wrap(toDriver).GetInt32(0),
				"atomic write not visible in mapped to-driver buffer",
			)
		}
	})
}

func TestMapCncFile_hugeMetadataShortFile_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		meta := cnc.DrawOverflowBufferLengths(rt)

		path := filepath.Join(t.TempDir(), cnc.FileName)
		writeOverflowCncFile(t, path, meta)

		_, err := cnc.MapCncFile(path)
		require.ErrorContains(rt, err, "insufficient for metadata")
	})
}
