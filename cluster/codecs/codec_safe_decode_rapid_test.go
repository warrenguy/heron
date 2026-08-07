//go:build rapid

package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestSessionMessageHeader_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		buf, offset := rapidSessionMessageHeaderParityWire(rt)
		requireSessionMessageHeaderFastSlowParity(rt, buf, offset)
	})
}

func TestSessionConnectRequest_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSessionConnectReq, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidSessionConnectRequestFields(rt, actingVersion)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeSessionConnectRequest(buf, offset, actingVersion, fields)
		require.True(rt, ok)
		requireSessionConnectRequestFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func rapidSessionMessageHeaderParityWire(rt *rapid.T) (buf []byte, offset int) {
	rt.Helper()
	if rapid.Bool().Draw(rt, "useValidWire") {
		var actingVersion uint16
		actingVersion, offset = rapidCodecContext(rt)
		fields := codecs.SessionMessageHeaderFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
		}
		return encodeSessionMessageHeaderAt(rt, offset, actingVersion, fields), offset
	}

	bufLen := rapid.IntRange(0, 256).Draw(rt, "bufLen")
	buf = rapid.SliceOfN(rapid.Byte(), bufLen, bufLen).Draw(rt, "buf")
	offset = rapid.OneOf(
		rapid.IntRange(-16, bufLen+16),
		rapid.Just(math.MaxInt),
	).Draw(rt, "offset")
	return buf, offset
}
