//go:build rapid

package sbe_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/aeron/sbe"
)

func TestEncodeIngressHeader_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		headerLen := sbe.SessionMessageHeaderEncodedLen()
		bufLen := rapid.IntRange(0, 512).Draw(rt, "bufLen")
		buf := make([]byte, bufLen)

		frameOffset := rapid.OneOf(
			rapid.IntRange(-logbuffer.OffsetFramePayload-8, bufLen+8),
			rapid.Just(math.MaxInt-logbuffer.OffsetFramePayload),
			rapid.Just(math.MaxInt),
		).Draw(rt, "frameOffset")

		fields := sbe.IngressHeaderFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
		}

		wantValid := encodeIngressHeaderFits(buf, frameOffset, headerLen)

		var encoded int
		var err error
		require.NotPanics(rt, func() {
			encoded, err = sbe.EncodeIngressHeader(buf, frameOffset, fields)
		})

		if !wantValid {
			require.ErrorIs(rt, err, sbe.ErrIngressHeaderBufferTooSmall)
			return
		}

		require.NoError(rt, err)
		require.Equal(rt, headerLen, encoded)

		payloadOffset := frameOffset + logbuffer.OffsetFramePayload
		requireDecodedIngressHeader(rt, buf[payloadOffset:payloadOffset+headerLen], fields)
	})
}

func encodeIngressHeaderFits(buf []byte, frameOffset, headerLen int) bool {
	payloadOffset := frameOffset + logbuffer.OffsetFramePayload
	return payloadOffset >= 0 && payloadOffset <= len(buf)-headerLen
}
