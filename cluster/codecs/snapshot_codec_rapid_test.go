//go:build rapid

package codecs_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func TestSnapshotMarker_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidSnapshotMarkerFields(rt)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.SnapshotMarkerSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeSnapshotMarker),
			codecs.SnapshotMarkerOracleFields(fields, actingVersion),
		)
	})
}

func TestClientSession_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidClientSessionFields(rt)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.ClientSessionSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeClientSession),
			fields,
		)
	})
}

func TestTimer_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidTimerFields(rt)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.TimerSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeTimer),
			fields,
		)
	})
}

func TestConsensusModule_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidConsensusModuleFields(rt)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.ConsensusModuleSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeConsensusModule),
			codecs.ConsensusModuleOracleFields(fields, actingVersion),
		)
	})
}

func TestClusterSession_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidClusterSessionFields(rt)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.ClusterSessionSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodeClusterSession),
			fields,
		)
	})
}

func TestPendingMessageTracker_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := rapidPendingMessageTrackerFields(rt)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.PendingMessageTrackerSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAt(actingVersion, fields, codecs.EncodePendingMessageTracker),
			fields,
		)
	})
}

// rapidEnumOrRaw draws a defined enum value or any raw int32. Decode passes
// raw values through.
func rapidEnumOrRaw[T ~int32](rt *rapid.T, name string, defined []T) T {
	rt.Helper()
	return rapid.OneOf(
		rapid.SampledFrom(defined),
		rapid.Map(rapid.Int32(), func(v int32) T { return T(v) }),
	).Draw(rt, name)
}

func rapidSnapshotMarkerFields(rt *rapid.T) codecs.SnapshotMarkerFields {
	rt.Helper()
	return codecs.SnapshotMarkerFields{
		TypeID:           rapid.Int64().Draw(rt, "typeID"),
		LogPosition:      rapid.Int64().Draw(rt, "logPosition"),
		LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
		Index:            rapid.Int32().Draw(rt, "index"),
		Mark: rapidEnumOrRaw(rt, "mark", []clusterwire.SnapshotMark{
			clusterwire.SnapshotMark_BEGIN,
			clusterwire.SnapshotMark_SECTION,
			clusterwire.SnapshotMark_END,
			clusterwire.SnapshotMark_NULL_VALUE,
		}),
		TimeUnit: rapidEnumOrRaw(rt, "timeUnit", []clusterwire.ClusterTimeUnit{
			clusterwire.ClusterTimeUnit_MILLIS,
			clusterwire.ClusterTimeUnit_MICROS,
			clusterwire.ClusterTimeUnit_NANOS,
			clusterwire.ClusterTimeUnit_NULL_VALUE,
		}),
		AppVersion: rapid.Int32().Draw(rt, "appVersion"),
	}
}

func rapidClientSessionFields(rt *rapid.T) codecs.ClientSessionFields {
	rt.Helper()
	return codecs.ClientSessionFields{
		ClusterSessionID: rapid.Int64().Draw(rt, "clusterSessionID"),
		ResponseStreamID: rapid.Int32().Draw(rt, "responseStreamID"),
		ResponseChannel:  rapidASCIIBytes(rt, "responseChannel"),
		EncodedPrincipal: rapidVarDataBytes(rt, "encodedPrincipal"),
	}
}

func rapidTimerFields(rt *rapid.T) codecs.TimerFields {
	rt.Helper()
	return codecs.TimerFields{
		CorrelationID: rapid.Int64().Draw(rt, "correlationID"),
		Deadline:      rapid.Int64().Draw(rt, "deadline"),
	}
}

func rapidConsensusModuleFields(rt *rapid.T) codecs.ConsensusModuleFields {
	rt.Helper()
	return codecs.ConsensusModuleFields{
		NextSessionID:          rapid.Int64().Draw(rt, "nextSessionID"),
		NextServiceSessionID:   rapid.Int64().Draw(rt, "nextServiceSessionID"),
		LogServiceSessionID:    rapid.Int64().Draw(rt, "logServiceSessionID"),
		PendingMessageCapacity: rapid.Int32().Draw(rt, "pendingMessageCapacity"),
	}
}

func rapidClusterSessionFields(rt *rapid.T) codecs.ClusterSessionFields {
	rt.Helper()
	return codecs.ClusterSessionFields{
		ClusterSessionID:   rapid.Int64().Draw(rt, "clusterSessionID"),
		CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
		OpenedLogPosition:  rapid.Int64().Draw(rt, "openedLogPosition"),
		TimeOfLastActivity: rapid.Int64().Draw(rt, "timeOfLastActivity"),
		CloseReason: rapidEnumOrRaw(rt, "closeReason", []clusterwire.CloseReason{
			clusterwire.CloseReason_CLIENT_ACTION,
			clusterwire.CloseReason_SERVICE_ACTION,
			clusterwire.CloseReason_TIMEOUT,
			clusterwire.CloseReason_NULL_VALUE,
		}),
		ResponseStreamID: rapid.Int32().Draw(rt, "responseStreamID"),
		ResponseChannel:  rapidASCIIBytes(rt, "responseChannel"),
	}
}

func rapidPendingMessageTrackerFields(rt *rapid.T) codecs.PendingMessageTrackerFields {
	rt.Helper()
	return codecs.PendingMessageTrackerFields{
		NextServiceSessionID:   rapid.Int64().Draw(rt, "nextServiceSessionID"),
		LogServiceSessionID:    rapid.Int64().Draw(rt, "logServiceSessionID"),
		PendingMessageCapacity: rapid.Int32().Draw(rt, "pendingMessageCapacity"),
		ServiceID:              rapid.Int32().Draw(rt, "serviceID"),
	}
}
