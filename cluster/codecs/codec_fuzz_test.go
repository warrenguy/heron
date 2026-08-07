package codecs_test

import (
	"testing"
)

func FuzzClusterCodecDecode(f *testing.F) {
	for _, seed := range codecFuzzSeedBuffers() {
		f.Add(seed, uint8(0))
	}
	offsetProbeWire := []byte{0x10, 0x00, 0x01, 0x00, 0x6f, 0x00, 0x10, 0x00}
	for offset := uint8(1); offset <= codecFuzzMaxOffset; offset++ {
		f.Add(offsetProbeWire, offset)
	}

	f.Fuzz(func(t *testing.T, data []byte, offset uint8) {
		if len(data) == 0 || len(data) > fuzzClusterCodecMaxLen {
			return
		}
		off := int(offset % codecFuzzOffsetSpan)
		// Registered typed TryDecode*At helpers must match boxed
		// TryDecodeByTemplate on the same bytes. Each parity helper drives both
		// decoders, so the fuzz covers the boxed path on every input.
		for _, requireParity := range fastSlowDecodeParities {
			requireParity(t, data, off)
		}
	})
}
