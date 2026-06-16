//go:build rapid

package sbe_test

import (
	"math"
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

func TestSessionMessageHeader_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		fields := sbe.SessionMessageHeaderFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionMessageHeaderSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeSessionMessageHeader(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func TestSessionEvent_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSessionEvent, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		blockLength := sessionEventBlockLength(actingVersion)

		fields := sbe.SessionEventFields{
			ClusterSessionID:         rapid.Int64().Draw(rt, "clusterSessionID"),
			CorrelationID:            rapid.Int64().Draw(rt, "correlationID"),
			LeadershipTermID:         rapid.Int64().Draw(rt, "leadershipTermID"),
			LeaderMemberID:           rapid.Int32().Draw(rt, "leaderMemberID"),
			Code:                     rapidEventCode(rt),
			Version:                  rapidVersionIf(rt, actingVersion, 6),
			LeaderHeartbeatTimeoutNs: rapidLeaderHeartbeatTimeoutNs(rt, actingVersion),
			Detail:                   rapidASCIIBytes(rt, "detail"),
		}

		wantFields := sessionEventOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionEventSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeSessionEvent(buf, offset, actingVersion, blockLength, fields)
			},
			wantFields,
		)
	})
}

func TestSessionConnectRequest_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSessionConnectReq, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)

		fields := sbe.SessionConnectRequestFields{
			CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
			ResponseStreamID:   rapid.Int32().Draw(rt, "responseStreamID"),
			Version:            rapidVersionIf(rt, actingVersion, 2),
			ResponseChannel:    rapidASCIIBytes(rt, "responseChannel"),
			EncodedCredentials: rapidVarDataBytes(rt, "encodedCredentials"),
			ClientInfo:         rapidASCIIBytesIf(rt, actingVersion, 14, "clientInfo"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionConnectRequestSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeSessionConnectRequest(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func TestSessionCloseAndKeepAlive_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		closeFields := sbe.SessionCloseRequestFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "closeLeadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "closeClusterSessionID"),
		}
		keepAliveFields := sbe.SessionKeepAliveFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "keepAliveLeadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "keepAliveClusterSessionID"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionCloseRequestSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeSessionCloseRequest(buf, offset, actingVersion, closeFields)
			},
			closeFields,
		)

		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionKeepAliveSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeSessionKeepAlive(buf, offset, actingVersion, keepAliveFields)
			},
			keepAliveFields,
		)
	})
}

func TestNewLeaderEvent_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		fields := sbe.NewLeaderEventFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			LeaderMemberID:   rapid.Int32().Draw(rt, "leaderMemberID"),
			IngressEndpoints: rapidASCIIBytes(rt, "ingressEndpoints"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.NewLeaderEventSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeNewLeaderEvent(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func TestChallenge_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		fields := sbe.ChallengeFields{
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			EncodedChallenge: rapidVarDataBytes(rt, "encodedChallenge"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.ChallengeSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeChallenge(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func TestChallengeResponse_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		fields := sbe.ChallengeResponseFields{
			CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
			ClusterSessionID:   rapid.Int64().Draw(rt, "clusterSessionID"),
			EncodedCredentials: rapidVarDataBytes(rt, "encodedCredentials"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.ChallengeResponseSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeChallengeResponse(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func TestAdminRequest_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		fields := sbe.AdminRequestFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			RequestType:      gensbe.AdminRequestType_SNAPSHOT,
			Payload:          rapidVarDataBytes(rt, "payload"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.AdminRequestSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeAdminRequest(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func TestAdminResponse_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion := rapidActingVersion(rt)
		offset := rapidCodecOffset(rt)
		fields := sbe.AdminResponseFields{
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			RequestType:      gensbe.AdminRequestType_SNAPSHOT,
			ResponseCode:     rapidAdminResponseCode(rt),
			Message:          rapidASCIIBytes(rt, "message"),
			Payload:          rapidVarDataBytes(rt, "payload"),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.AdminResponseSbeTemplateID,
			actingVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeAdminResponse(buf, offset, actingVersion, fields)
			},
			fields,
		)
	})
}

func rapidActingVersion(rt *rapid.T) uint16 {
	rt.Helper()
	return uint16(rapid.IntRange(0, 16).Draw(rt, "actingVersion"))
}

func rapidCodecOffset(rt *rapid.T) int {
	rt.Helper()
	return rapid.IntRange(0, codecFuzzMaxOffset).Draw(rt, "offset")
}

func rapidVersionIf(rt *rapid.T, actingVersion, minVersion uint16) int32 {
	rt.Helper()
	if actingVersion < minVersion {
		return 0
	}
	return rapid.Int32Range(0, 16).Draw(rt, "version")
}

func rapidASCIIBytes(rt *rapid.T, name string) []byte {
	rt.Helper()
	return rapid.SliceOfN(rapid.ByteRange(' ', '~'), 0, codecTestMaxVarLen).Draw(rt, name)
}

func rapidASCIIBytesIf(rt *rapid.T, actingVersion, minVersion uint16, name string) []byte {
	rt.Helper()
	if actingVersion < minVersion {
		return nil
	}
	return rapidASCIIBytes(rt, name)
}

func rapidVarDataBytes(rt *rapid.T, name string) []byte {
	rt.Helper()
	return rapid.SliceOfN(rapid.Byte(), 0, codecTestMaxVarLen).Draw(rt, name)
}

// rapidLeaderHeartbeatTimeoutNs draws encode input. math.MinInt64 means omit setter.
func rapidLeaderHeartbeatTimeoutNs(rt *rapid.T, actingVersion uint16) int64 {
	rt.Helper()
	if actingVersion < 13 || rapid.Bool().Draw(rt, "omitLeaderHeartbeat") {
		return math.MinInt64
	}
	// Exclude MinInt64 so draw is always an explicit on-wire value.
	return rapid.Int64Range(math.MinInt64+1, math.MaxInt64).Draw(rt, "leaderHeartbeatTimeoutNs")
}

func rapidEventCode(rt *rapid.T) gensbe.EventCode {
	rt.Helper()
	return rapid.SampledFrom(codecAllEventCodes).Draw(rt, "eventCode")
}

func rapidAdminResponseCode(rt *rapid.T) gensbe.AdminResponseCode {
	rt.Helper()
	return rapid.SampledFrom(codecAllAdminResponseCodes).Draw(rt, "responseCode")
}
