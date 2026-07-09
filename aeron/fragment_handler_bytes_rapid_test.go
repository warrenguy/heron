//go:build rapid

package aeron

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
)

const fragmentPayloadRapidMaxBufLen = 256

var fragmentPayloadRapidAdapters = []struct {
	name  string
	adapt func(FragmentHandlerBytes) FragmentHandler
}{
	{name: "view", adapt: AdaptFragmentHandlerBytes},
	{name: "retain", adapt: AdaptFragmentHandlerBytesRetain},
}

func TestAdaptFragmentHandlerBytes_rapid_payloadBounds(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		bufLen := rapid.IntRange(0, fragmentPayloadRapidMaxBufLen).Draw(rt, "bufLen")
		raw := rapid.SliceOfN(rapid.Byte(), bufLen, bufLen).Draw(rt, "raw")
		offset := rapid.IntRange(-fragmentPayloadRapidMaxBufLen, fragmentPayloadRapidMaxBufLen+bufLen).
			Draw(rt, "offset")
		length := rapid.IntRange(-fragmentPayloadRapidMaxBufLen, fragmentPayloadRapidMaxBufLen+bufLen).
			Draw(rt, "length")

		buf := atomicbuffer.Wrap(raw)
		want := fragmentPayloadView(buf, offset, length)
		var wantFrozen []byte
		if want != nil {
			wantFrozen = slices.Clone(want)
		}

		var gotView []byte
		var gotRetain []byte
		for _, adapter := range fragmentPayloadRapidAdapters {
			var got []byte
			called := false
			handler := adapter.adapt(func(payload []byte, _ *Header) {
				called = true
				got = payload
			})

			require.NotPanics(rt, func() {
				handler(buf, offset, length, nil)
			})
			require.True(rt, called)
			require.Equal(rt, wantFrozen, got)

			switch adapter.name {
			case "view":
				gotView = got
			case "retain":
				gotRetain = got
			}
		}

		if len(wantFrozen) == 0 {
			return
		}

		raw[offset] ^= 0xff
		require.Equal(rt, fragmentPayloadView(buf, offset, length), gotView)
		require.Equal(rt, wantFrozen, gotRetain)
	})
}
