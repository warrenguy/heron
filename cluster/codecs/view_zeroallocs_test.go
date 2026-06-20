package codecs_test

import (
	"encoding/binary"
	"testing"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestReadVarSlice_ZeroAllocs(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[2:], 4)
	copy(buf[6:], []byte("data"))

	testutil.AssertZeroAllocs(t, "ReadVarSlice", 1000, func() {
		_, _, _ = codecs.ReadVarSlice(buf, 2, 8)
	})
}
