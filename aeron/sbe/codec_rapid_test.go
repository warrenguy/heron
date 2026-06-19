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
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, fields, sbe.EncodeSessionMessageHeader),
			fields,
		)
	})
}

func TestSessionEvent_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSessionEvent, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		blockLength := sbe.SessionEventBlockLength(actingVersion)

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

		wantFields := sbe.SessionEventOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionEventSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeSessionEvent(actingVersion, blockLength, fields),
			wantFields,
		)
	})
}

func TestSessionConnectRequest_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSessionConnectReq, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)

		fields := sbe.SessionConnectRequestFields{
			CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
			ResponseStreamID:   rapid.Int32().Draw(rt, "responseStreamID"),
			Version:            rapidVersionIf(rt, actingVersion, 2),
			ResponseChannel:    rapidASCIIBytes(rt, "responseChannel"),
			EncodedCredentials: rapidVarDataBytes(rt, "encodedCredentials"),
			ClientInfo: rapidASCIIBytesIf(
				rt,
				actingVersion,
				sessionConnectClientInfoMinVersion,
				"clientInfo",
			),
		}

		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionConnectRequestSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, sbe.EncodeSessionConnectRequest),
			fields,
		)
	})
}

func TestSessionCloseAndKeepAlive_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, closeFields, sbe.EncodeSessionCloseRequest),
			closeFields,
		)

		runCodecRoundtripAtOffset(
			rt,
			gensbe.SessionKeepAliveSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, keepAliveFields, sbe.EncodeSessionKeepAlive),
			keepAliveFields,
		)
	})
}

func TestNewLeaderEvent_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, fields, sbe.EncodeNewLeaderEvent),
			fields,
		)
	})
}

func TestChallenge_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, fields, sbe.EncodeChallenge),
			fields,
		)
	})
}

func TestChallengeResponse_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, fields, sbe.EncodeChallengeResponse),
			fields,
		)
	})
}

func TestAdminRequest_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, fields, sbe.EncodeAdminRequest),
			fields,
		)
	})
}

func TestAdminResponse_codec_rapid(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
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
			rapidEncodeAt(actingVersion, fields, sbe.EncodeAdminResponse),
			fields,
		)
	})
}

func rapidCodecContext(rt *rapid.T) (actingVersion uint16, offset int) {
	rt.Helper()
	return rapidActingVersion(rt), rapidCodecOffset(rt)
}

func rapidEncodeAt[T any](
	actingVersion uint16,
	fields T,
	encode func([]byte, int, uint16, T) (int, bool),
) func([]byte, int) (int, bool) {
	return func(buf []byte, offset int) (int, bool) {
		return encode(buf, offset, actingVersion, fields)
	}
}

func rapidEncodeSessionEvent(
	actingVersion, blockLength uint16,
	fields sbe.SessionEventFields,
) func([]byte, int) (int, bool) {
	return func(buf []byte, offset int) (int, bool) {
		return sbe.EncodeSessionEvent(buf, offset, actingVersion, blockLength, fields)
	}
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
	omit := rapid.Bool().Draw(rt, "omitLeaderHeartbeat")
	if actingVersion < sessionEventLeaderHeartbeatMinVersion || omit {
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
