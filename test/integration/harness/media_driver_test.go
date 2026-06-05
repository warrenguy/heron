package harness_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
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
}
