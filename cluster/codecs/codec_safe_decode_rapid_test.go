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

func TestChallengeResponse_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ChallengeResponseFields{
			CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
			ClusterSessionID:   rapid.Int64().Draw(rt, "clusterSessionID"),
			EncodedCredentials: rapidVarDataBytes(rt, "encodedCredentials"),
		}
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeChallengeResponse(buf, offset, actingVersion, fields)
		require.True(rt, ok)
		requireChallengeResponseFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestSessionCloseRequest_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.SessionCloseRequestFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
		}
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeSessionCloseRequest(buf, offset, actingVersion, fields)
		require.True(rt, ok)
		requireSessionCloseRequestFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestSessionKeepAlive_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.SessionKeepAliveFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
		}
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeSessionKeepAlive(buf, offset, actingVersion, fields)
		require.True(rt, ok)
		requireSessionKeepAliveFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestSnapshotMarker_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeSnapshotMarker(
			buf, offset, actingVersion, rapidSnapshotMarkerFields(rt),
		)
		require.True(rt, ok)
		requireSnapshotMarkerFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestClientSession_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeClientSession(
			buf, offset, actingVersion, rapidClientSessionFields(rt),
		)
		require.True(rt, ok)
		requireClientSessionFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestTimer_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeTimer(buf, offset, actingVersion, rapidTimerFields(rt))
		require.True(rt, ok)
		requireTimerFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestConsensusModule_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeConsensusModule(
			buf, offset, actingVersion, rapidConsensusModuleFields(rt),
		)
		require.True(rt, ok)
		requireConsensusModuleFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestClusterSession_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodeClusterSession(
			buf, offset, actingVersion, rapidClusterSessionFields(rt),
		)
		require.True(rt, ok)
		requireClusterSessionFastSlowParity(rt, buf[:wireEnd], offset)
	})
}

func TestPendingMessageTracker_fastSlowDecodeParity(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		buf := make([]byte, offset+fuzzClusterCodecMaxLen)
		wireEnd, ok := codecs.EncodePendingMessageTracker(
			buf, offset, actingVersion, rapidPendingMessageTrackerFields(rt),
		)
		require.True(rt, ok)
		requirePendingMessageTrackerFastSlowParity(rt, buf[:wireEnd], offset)
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
