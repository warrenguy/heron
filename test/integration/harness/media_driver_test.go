package harness_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/ringbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestMediaDriver(t *testing.T) {
	md := harness.Start(t)

	t.Run("StartStop", func(t *testing.T) {
		if md.Dir == "" {
			t.Fatal("expected AERON_DIR")
		}
	})

	t.Run("ParseLiveCncMetadata", func(t *testing.T) {
		meta, err := cnc.ReadMetadataFile(md.CncPath())
		if err != nil {
			t.Fatalf("read metadata: %v", err)
		}
		if meta.ToDriverBufferLength <= 0 {
			t.Fatalf("to-driver length %d", meta.ToDriverBufferLength)
		}
		if meta.ToClientsBufferLength <= 0 {
			t.Fatalf("to-clients length %d", meta.ToClientsBufferLength)
		}
		if meta.PID <= 0 {
			t.Fatalf("pid %d", meta.PID)
		}
		if meta.FilePageSize <= 0 || !atomic.IsPowerOfTwo(int(meta.FilePageSize)) {
			t.Fatalf("file page size %d", meta.FilePageSize)
		}
		if meta.ClientLivenessTimeoutNs <= 0 {
			t.Fatalf("client liveness timeout %d", meta.ClientLivenessTimeoutNs)
		}
	})

	t.Run("MapLiveCncFile", func(t *testing.T) {
		mapped, err := cnc.MapCncFile(md.CncPath())
		if err != nil {
			t.Fatalf("map cnc file: %v", err)
		}
		defer func() { _ = mapped.Close() }()

		meta := mapped.Metadata()
		readMeta, err := cnc.ReadMetadataFile(md.CncPath())
		if err != nil {
			t.Fatalf("read metadata: %v", err)
		}
		if meta != readMeta {
			t.Fatalf("mapped metadata %+v != read metadata %+v", meta, readMeta)
		}
		if _, err := ringbuffer.CheckCapacity(
			int(meta.ToDriverBufferLength),
			ringbuffer.MinCapacity,
		); err != nil {
			t.Fatalf("to-driver CheckCapacity: %v", err)
		}
		if _, err := broadcast.CheckCapacity(int(meta.ToClientsBufferLength)); err != nil {
			t.Fatalf("to-clients CheckCapacity: %v", err)
		}
		if err := mapped.Close(); err != nil {
			t.Fatalf("close mapped cnc file: %v", err)
		}
		if _, err := cnc.ReadMetadataFile(md.CncPath()); err != nil {
			t.Fatalf("read metadata after close: %v", err)
		}
		mappedAgain, err := cnc.MapCncFile(md.CncPath())
		if err != nil {
			t.Fatalf("remap cnc file after close: %v", err)
		}
		defer func() {
			if err := mappedAgain.Close(); err != nil {
				t.Errorf("close remapped cnc file: %v", err)
			}
		}()
	})
}
