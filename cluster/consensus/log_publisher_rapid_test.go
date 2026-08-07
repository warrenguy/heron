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
// accepts payloads within MaxPayloadLength and rejects oversize totals with
// UnsupportedMessageLength before Offer.
//
// Fresh publisher per draw keeps term capacity out of the oracle.
func TestLogPublisher_AppendSessionOpen_lengthBounds_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		p, pub := boundLogPublisher(t)
		maxPayload := pub.MaxPayloadLength()

		// Lengths span the MaxPayloadLength boundary. Cap slightly above max so
		// rejects are reachable without huge allocations. Zero length stays nil so
		// decode roundtrip matches wire empty vars.
		chLen := rapid.IntRange(0, maxPayload+32).Draw(rt, "chLen")
		prinLen := rapid.IntRange(0, maxPayload+32).Draw(rt, "prinLen")
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
		accept := lenOK && total <= maxPayload
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
