//go:build rapid && testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

// TestLogAdapter_Poll_boundIsolation_rapid checks Poll delivery against a
// reference model over random frame layouts and a non-decreasing bound
// sequence: frames whose start is strictly before the bound deliver in log
// order, at most fragmentLimit per Poll, exactly once; Position is the end of
// the last delivered frame, so it passes the bound only by finishing a frame
// that straddles it.
func TestLogAdapter_Poll_boundIsolation_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		limit := rapid.IntRange(1, 6).Draw(rt, "fragmentLimit")
		a, image := boundLogAdapterWithLimit(t, limit)
		spy := &logAdapterHandlerSpy{}
		a.SetHandler(spy)

		// Contiguous unfragmented frames from position 0. Session id is the
		// 1-based frame index; trailing filler varies span alignment.
		frameCount := rapid.IntRange(1, 10).Draw(rt, "frameCount")
		starts := make([]int64, frameCount)
		ends := make([]int64, frameCount)
		offset := 0
		for i := range frameCount {
			payload := encodeLogAdapterSessionMessagePayload(t, codecs.SessionMessageHeaderFields{
				ClusterSessionID: int64(i + 1),
				Timestamp:        int64(i),
			})
			filler := rapid.IntRange(0, 96).Draw(rt, "filler")
			payload = append(payload, make([]byte, filler)...)
			writeLogAdapterUnfragmentedFrame(t, image, offset, payload)
			starts[i] = int64(offset)
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
			ends[i] = int64(offset)
		}
		totalEnd := int64(offset)

		// Reference model: next undelivered frame, expected position, totals.
		next := 0
		pos := int64(0)
		delivered := 0

		polls := rapid.IntRange(1, 12).Draw(rt, "polls")
		bound := int64(0)
		for range polls {
			// Non-decreasing bound: exact frame start, arbitrary, or held.
			var proposal int64
			switch rapid.IntRange(0, 2).Draw(rt, "boundMode") {
			case 0:
				proposal = starts[rapid.IntRange(0, frameCount-1).Draw(rt, "boundFrame")]
			case 1:
				proposal = rapid.Int64Range(0, totalEnd+64).Draw(rt, "boundFree")
			default:
				proposal = bound
			}
			bound = max(bound, proposal)

			want := 0
			wantLast := int64(0)
			for next < frameCount && want < limit && starts[next] < bound {
				pos = ends[next]
				wantLast = int64(next + 1)
				next++
				want++
			}

			require.Equal(rt, want, a.Poll(bound), "bound=%d", bound)
			require.Equal(rt, pos, a.Position(), "bound=%d", bound)
			delivered += want
			require.Equal(rt, delivered, spy.sessionMessages)
			if want > 0 {
				require.Equal(rt, wantLast, spy.lastClusterSessionID)
			}
			require.Zero(rt, spy.nltEvents)
		}

		// Drain past the end: every frame delivers exactly once, in order.
		for next < frameCount {
			want := min(limit, frameCount-next)
			require.Equal(rt, want, a.Poll(totalEnd+1))
			next += want
			delivered += want
		}
		require.Equal(rt, totalEnd, a.Position())
		require.Equal(rt, frameCount, spy.sessionMessages)
		require.Equal(rt, int64(frameCount), spy.lastClusterSessionID)
	})
}
