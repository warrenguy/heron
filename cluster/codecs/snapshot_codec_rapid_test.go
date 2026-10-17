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

// rapidSnapshotMark draws a defined SnapshotMark or any raw int32. Decode
// passes raw values through.
func rapidSnapshotMark(rt *rapid.T, name string) clusterwire.SnapshotMark {
	rt.Helper()
	return rapid.OneOf(
		rapid.SampledFrom([]clusterwire.SnapshotMark{
			clusterwire.SnapshotMark_BEGIN,
			clusterwire.SnapshotMark_SECTION,
			clusterwire.SnapshotMark_END,
			clusterwire.SnapshotMark_NULL_VALUE,
		}),
		rapid.Map(rapid.Int32(), func(v int32) clusterwire.SnapshotMark {
			return clusterwire.SnapshotMark(v)
		}),
	).Draw(rt, name)
}

// rapidClusterTimeUnit draws a defined ClusterTimeUnit or any raw int32.
// Decode passes raw values through.
func rapidClusterTimeUnit(rt *rapid.T, name string) clusterwire.ClusterTimeUnit {
	rt.Helper()
	return rapid.OneOf(
		rapid.SampledFrom([]clusterwire.ClusterTimeUnit{
			clusterwire.ClusterTimeUnit_MILLIS,
			clusterwire.ClusterTimeUnit_MICROS,
			clusterwire.ClusterTimeUnit_NANOS,
			clusterwire.ClusterTimeUnit_NULL_VALUE,
		}),
		rapid.Map(rapid.Int32(), func(v int32) clusterwire.ClusterTimeUnit {
			return clusterwire.ClusterTimeUnit(v)
		}),
	).Draw(rt, name)
}

func rapidSnapshotMarkerFields(rt *rapid.T) codecs.SnapshotMarkerFields {
	rt.Helper()
	return codecs.SnapshotMarkerFields{
		TypeID:           rapid.Int64().Draw(rt, "typeID"),
		LogPosition:      rapid.Int64().Draw(rt, "logPosition"),
		LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
		Index:            rapid.Int32().Draw(rt, "index"),
		Mark:             rapidSnapshotMark(rt, "mark"),
		TimeUnit:         rapidClusterTimeUnit(rt, "timeUnit"),
		AppVersion:       rapid.Int32().Draw(rt, "appVersion"),
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
