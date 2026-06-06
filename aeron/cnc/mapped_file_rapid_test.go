package cnc_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"pgregory.net/rapid"
)

func TestMapCncFile_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		meta := cnc.DrawRapidBufferLengths(rt, cnc.TestRapidMaxField)

		path := filepath.Join(t.TempDir(), cnc.FileName)
		writeCncFile(t, path, meta)

		mapped, err := cnc.MapCncFile(path)
		if err != nil {
			rt.Fatalf("MapCncFile: %v", err)
		}
		defer func() { _ = mapped.Close() }()

		cnc.AssertMappedBufferLengths(rt, mapped, meta)

		if meta.ToDriverBufferLength >= 4 {
			toDriver := mapped.ToDriverBuffer()
			toDriverAtomic := mapped.ToDriverAtomic()
			toDriverAtomic.PutInt32(0, 7)
			if atomic.Wrap(toDriver).GetInt32(0) != 7 {
				rt.Fatal("atomic write not visible in mapped to-driver buffer")
			}
		}
	})
}

func TestMapCncFile_hugeMetadataShortFile_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		meta := cnc.DrawOverflowBufferLengths(rt)

		path := filepath.Join(t.TempDir(), cnc.FileName)
		writeOverflowCncFile(t, path, meta)

		_, err := cnc.MapCncFile(path)
		if err == nil {
			rt.Fatal("MapCncFile should reject short on-disk file for huge metadata")
		}
		if !strings.Contains(err.Error(), "insufficient for metadata") {
			rt.Fatalf("MapCncFile: got %v", err)
		}
	})
}
