package harness_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestMediaDriver_StartStop(t *testing.T) {
	md := harness.Start(t)
	if md.Dir == "" {
		t.Fatal("expected AERON_DIR")
	}

	info, err := os.Stat(md.CncPath())
	if err != nil {
		t.Fatalf("stat cnc: %v", err)
	}
	if info.Size() <= int64(cnc.MetaDataLength) {
		t.Fatalf("cnc file too small: %d", info.Size())
	}
}
