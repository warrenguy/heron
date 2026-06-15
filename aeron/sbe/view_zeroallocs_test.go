package sbe_test

import (
	"encoding/binary"
	"testing"

	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/sbe"
)

func TestReadVarSlice_ZeroAllocs(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[2:], 4)
	copy(buf[6:], []byte("data"))

	testutil.AssertZeroAllocs(t, "ReadVarSlice", 1000, func() {
		_, _, _ = sbe.ReadVarSlice(buf, 2, 8)
	})
}
