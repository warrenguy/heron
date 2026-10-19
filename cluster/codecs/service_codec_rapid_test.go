//go:build rapid

package codecs_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func TestTimerEvent_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.TimerEventFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.TimerEventSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeTimerEvent),
			fields,
		)
	})
}

func TestSessionOpenEvent_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.SessionOpenEventFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
			ResponseStreamID: rapid.Int32().Draw(rt, "responseStreamID"),
			ResponseChannel:  rapidASCIIBytes(rt, "responseChannel"),
			EncodedPrincipal: rapidVarDataBytes(rt, "encodedPrincipal"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.SessionOpenEventSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeSessionOpenEvent),
			fields,
		)
	})
}

func TestSessionCloseEvent_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.SessionCloseEventFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
			CloseReason: rapid.SampledFrom([]clusterwire.CloseReason{
				clusterwire.CloseReason_CLIENT_ACTION,
				clusterwire.CloseReason_SERVICE_ACTION,
				clusterwire.CloseReason_TIMEOUT,
			}).Draw(rt, "closeReason"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.SessionCloseEventSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeSessionCloseEvent),
			fields,
		)
	})
}

func TestClusterActionRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ClusterActionRequestFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			LogPosition:      rapid.Int64().Draw(rt, "logPosition"),
			Timestamp:        rapid.Int64().Draw(rt, "timestamp"),
			Action: rapid.SampledFrom([]clusterwire.ClusterAction{
				clusterwire.ClusterAction_SUSPEND,
				clusterwire.ClusterAction_RESUME,
				clusterwire.ClusterAction_SNAPSHOT,
				clusterwire.ClusterAction_NULL_VALUE,
			}).Draw(rt, "action"),
			Flags: rapid.Int32().Draw(rt, "flags"),
		}
		wantFields := clusterActionRequestWantFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.ClusterActionRequestSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeClusterActionRequest),
			wantFields,
		)
	})
}

func TestScheduleTimer_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ScheduleTimerFields{
			CorrelationID: rapid.Int64().Draw(rt, "correlationID"),
			Deadline:      rapid.Int64().Draw(rt, "deadline"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.ScheduleTimerSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeScheduleTimer),
			fields,
		)
	})
}

func TestCancelTimer_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.CancelTimerFields{
			CorrelationID: rapid.Int64().Draw(rt, "correlationID"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.CancelTimerSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeCancelTimer),
			fields,
		)
	})
}

func TestCloseSession_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.CloseSessionFields{
			ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.CloseSessionSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeCloseSession),
			fields,
		)
	})
}

func TestServiceAck_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ServiceAckFields{
			LogPosition: rapid.Int64().Draw(rt, "logPosition"),
			Timestamp:   rapid.Int64().Draw(rt, "timestamp"),
			AckID:       rapid.Int64().Draw(rt, "ackID"),
			RelevantID:  rapid.Int64().Draw(rt, "relevantID"),
			ServiceID:   rapid.Int32().Draw(rt, "serviceID"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.ServiceAckSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeServiceAck),
			fields,
		)
	})
}

func TestJoinLog_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.JoinLogFields{
			LogPosition:    rapid.Int64().Draw(rt, "logPosition"),
			MaxLogPosition: rapid.Int64().Draw(rt, "maxLogPosition"),
			MemberID:       rapid.Int32().Draw(rt, "memberID"),
			LogSessionID:   rapid.Int32().Draw(rt, "logSessionID"),
			LogStreamID:    rapid.Int32().Draw(rt, "logStreamID"),
			IsStartup:      rapidBooleanType(rt, "isStartup"),
			Role:           rapid.Int32().Draw(rt, "role"),
			IsStandby:      rapidBooleanType(rt, "isStandby"),
			LogChannel:     rapidASCIIBytes(rt, "logChannel"),
		}
		wantFields := codecs.JoinLogOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.JoinLogSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeJoinLog),
			wantFields,
		)
	})
}

func TestRequestServiceAck_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.RequestServiceAckFields{
			LogPosition: rapid.Int64().Draw(rt, "logPosition"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.RequestServiceAckSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeRequestServiceAck),
			fields,
		)
	})
}
