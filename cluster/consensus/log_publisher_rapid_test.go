//go:build rapid && testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// TestLogPublisher_AppendSessionOpen_lengthBounds_rapid asserts AppendSessionOpen
// accepts totals within MaxMessageLength and rejects oversize with
// UnsupportedMessageLength before Offer.
//
// Draws stratify into reject, single-frame accept, and mid-band accept with fixed
// mass each. Fresh publisher per draw keeps term capacity out of the oracle.
// Field roundtrip runs only when total fits MaxPayloadLength.
func TestLogPublisher_AppendSessionOpen_lengthBounds_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		p, pub := boundLogPublisher(t)
		maxPayload := pub.MaxPayloadLength()
		maxMessage := pub.MaxMessageLength()

		base, ok := codecs.ClusterWireMessageLen(
			clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
		)
		require.True(rt, ok)
		require.Less(rt, base, maxPayload)

		// Band:
		//   - 0: reject (total over MaxMessageLength)
		//   - 1: single-frame accept (total within MaxPayloadLength)
		//   - 2: mid-band accept; falls back to single-frame when MaxMessageLength
		//     equals MaxPayloadLength
		band := rapid.IntRange(0, 2).Draw(rt, "band")
		var varBudget int
		switch band {
		case 0:
			// Reject band: base plus varBudget exceeds maxMessage. Extra is at most 32.
			varBudget = maxMessage - base + rapid.IntRange(1, 32).Draw(rt, "rejectExtra")
		case 1:
			varBudget = rapid.IntRange(0, maxPayload-base).Draw(rt, "singleFrameBudget")
		default:
			if maxMessage <= maxPayload {
				varBudget = rapid.IntRange(0, maxPayload-base).Draw(rt, "singleFrameBudget")
			} else {
				varBudget = rapid.IntRange(maxPayload-base+1, maxMessage-base).
					Draw(rt, "midBandBudget")
			}
		}
		chLen := rapid.IntRange(0, varBudget).Draw(rt, "chLen")
		prinLen := varBudget - chLen

		// Zero length draws nil slices. Decode roundtrip matches empty wire vars.
		var responseChannel, encodedPrincipal []byte
		if chLen > 0 {
			responseChannel = rapid.SliceOfN(rapid.Byte(), chLen, chLen).Draw(rt, "responseChannel")
		}
		if prinLen > 0 {
			encodedPrincipal = rapid.SliceOfN(rapid.Byte(), prinLen, prinLen).Draw(rt, "encodedPrincipal")
		}

		leadershipTermID := rapid.Int64().Draw(rt, "leadershipTermID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		clusterSessionID := rapid.Int64().Draw(rt, "clusterSessionID")
		timestamp := rapid.Int64().Draw(rt, "timestamp")
		responseStreamID := rapid.Int32().Draw(rt, "responseStreamID")

		total, lenOK := codecs.ClusterWireMessageLen(
			clusterwire.SessionOpenEventSbeBlockLength,
			chLen,
			prinLen,
		)
		accept := lenOK && total <= maxMessage
		pre := p.Position()

		pos := p.AppendSessionOpen(
			leadershipTermID,
			correlationID,
			clusterSessionID,
			timestamp,
			responseStreamID,
			responseChannel,
			encodedPrincipal,
		)

		if !accept {
			require.Equal(rt, int64(aeron.UnsupportedMessageLength), pos)
			require.Equal(rt, pre, p.Position())
			return
		}

		require.Positive(rt, pos)
		require.Greater(rt, p.Position(), pre)

		if total > maxPayload {
			// Fragmented offer: skip single-frame decode.
			return
		}

		fields, ok := codecs.TryDecodeSessionOpenEventAt(termStartWire(t, pub), 0)
		require.True(rt, ok)
		require.Equal(rt, leadershipTermID, fields.LeadershipTermID)
		require.Equal(rt, correlationID, fields.CorrelationID)
		require.Equal(rt, clusterSessionID, fields.ClusterSessionID)
		require.Equal(rt, timestamp, fields.Timestamp)
		require.Equal(rt, responseStreamID, fields.ResponseStreamID)
		require.Equal(rt, responseChannel, fields.ResponseChannel)
		require.Equal(rt, encodedPrincipal, fields.EncodedPrincipal)
	})
}
