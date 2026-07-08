package testutil

import (
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
)

func DrawASCIIString(t *rapid.T, name string, maxLen int) string {
	return DrawASCIIStringLenRange(t, name, 0, maxLen)
}

func DrawASCIIStringLenRange(t *rapid.T, name string, minLen, maxLen int) string {
	n := rapid.IntRange(minLen, maxLen).Draw(t, name+"Len")
	raw := rapid.SliceOfN(rapid.Byte(), n, n).Draw(t, name)
	b := make([]byte, n)
	for i, v := range raw {
		b[i] = v & 127
	}
	return string(b)
}

func DrawNonASCIIString(t *rapid.T, name string, maxLen int) string {
	n := rapid.IntRange(1, maxLen).Draw(t, name+"Len")
	b := rapid.SliceOfN(rapid.Byte(), n, n).Draw(t, name)
	hi := rapid.IntRange(0, n-1).Draw(t, name+"HiIdx")
	b[hi] = byte(rapid.IntRange(128, 255).Draw(t, name+"HiByte"))
	return string(b)
}

func SnapshotBufferPrefix(buffer atomicbuffer.AtomicBuffer, offset int) []byte {
	return append([]byte(nil), buffer.Bytes()[:offset]...)
}
