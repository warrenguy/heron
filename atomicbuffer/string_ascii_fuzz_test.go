package atomicbuffer_test

import (
	"strings"
	"testing"

	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	fuzzStringASCIIMaxLen = 4096
	fuzzStringASCIIMaxOff = 8
)

func encodedStringASCIISeed(offset int, value string) []byte {
	buf := make([]byte, offset+atomicbuffer.StringASCIIEncodedLength(len(value)))
	buffer := atomicbuffer.Wrap(buf)
	_, _ = atomicbuffer.PutStringASCII(buffer, offset, value)
	return buffer.Bytes()
}

func FuzzGetStringASCII(f *testing.F) {
	f.Add(uint8(0), encodedStringASCIISeed(0, "a"))
	f.Add(uint8(0), encodedStringASCIISeed(0, strings.Repeat("x", 64)))
	f.Add(uint8(fuzzStringASCIIMaxOff), encodedStringASCIISeed(fuzzStringASCIIMaxOff, "aeron:ipc"))

	f.Fuzz(func(t *testing.T, offset uint8, data []byte) {
		if len(data) == 0 || len(data) > fuzzStringASCIIMaxLen {
			return
		}
		_, _, _ = atomicbuffer.GetStringASCII(
			atomicbuffer.Wrap(data),
			int(offset%(fuzzStringASCIIMaxOff+1)),
		)
	})
}
