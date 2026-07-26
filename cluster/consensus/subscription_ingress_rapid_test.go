//go:build rapid

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

// TestSubscriptionConsensusIngress_spanTranslation_rapid checks delivered
// payloads against a reference model: unfragmented frames in the prefix the
// poller actually hands over (min(limit, frameCount)), in order. Poll work
// counts every frame handed over, including skips.
func TestSubscriptionConsensusIngress_spanTranslation_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		frameCount := rapid.IntRange(0, 8).Draw(rt, "frameCount")
		flagChoices := []byte{
			logbuffer.Unfragmented,
			logbuffer.BeginFlag,
			logbuffer.EndFlag,
			0,
		}
		frames := make([]fakeFragment, frameCount)
		payloads := make([][]byte, frameCount)
		flags := make([]byte, frameCount)
		for i := range frameCount {
			payload := rapid.SliceOfN(rapid.Byte(), 1, 200).Draw(rt, "payload")
			flag := rapid.SampledFrom(flagChoices).Draw(rt, "flags")
			payloads[i] = payload
			flags[i] = flag
			frames[i] = buildFakeFragment(rt, payload, flag)
		}
		limit := rapid.IntRange(1, 12).Draw(rt, "fragmentLimit")

		wantWork := min(limit, frameCount)
		var wantDelivered [][]byte
		for i := range wantWork {
			if flags[i]&logbuffer.Unfragmented != logbuffer.Unfragmented {
				continue
			}
			cp := make([]byte, len(payloads[i]))
			copy(cp, payloads[i])
			wantDelivered = append(wantDelivered, cp)
		}

		fake := &fakeFragmentPoller{frames: frames}
		s := NewSubscriptionConsensusIngress(fake)
		var got [][]byte
		var offsets []int
		work := s.Poll(limit, func(buf []byte, offset int) {
			cp := make([]byte, len(buf))
			copy(cp, buf)
			got = append(got, cp)
			offsets = append(offsets, offset)
		})

		require.Equal(rt, wantWork, work)
		require.Len(rt, got, len(wantDelivered))
		for i := range wantDelivered {
			require.Equal(rt, wantDelivered[i], got[i], "frame %d", i)
			require.Equal(rt, 0, offsets[i], "frame %d", i)
		}
	})
}

// TestSubscriptionConsensusIngress_boundsGuards_rapid checks that out-of-range
// spans deliver nothing and in-range spans deliver exactly raw[offset:offset+length].
func TestSubscriptionConsensusIngress_boundsGuards_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rawLen := rapid.IntRange(logbuffer.DataHeaderLength, 256).Draw(rt, "rawLen")
		// Draw offset and length over a range that includes in- and out-of-bounds.
		offset := rapid.IntRange(-8, rawLen+16).Draw(rt, "offset")
		length := rapid.IntRange(-4, rawLen+16).Draw(rt, "length")

		fr := buildFakeFragmentAt(rt, rawLen, offset, length, logbuffer.Unfragmented)
		raw := fr.buffer.Bytes()
		inRange := length > 0 && offset >= 0 && offset <= len(raw)-length

		fake := &fakeFragmentPoller{frames: []fakeFragment{fr}}
		s := NewSubscriptionConsensusIngress(fake)
		var got [][]byte
		work := s.Poll(1, func(buf []byte, off int) {
			require.Equal(rt, 0, off)
			cp := make([]byte, len(buf))
			copy(cp, buf)
			got = append(got, cp)
		})
		require.Equal(rt, 1, work)
		if !inRange {
			require.Empty(rt, got)
			return
		}
		require.Len(rt, got, 1)
		require.Equal(rt, raw[offset:offset+length], got[0])
	})
}
