package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Golden wire bytes for the snapshot stream templates. Each constant equals
// the hex of the cluster SBE catalog row with the same fields.
const (
	goldenSnapshotMarkerConsensusModuleBeginHex = "280064006f001100" +
		"0100000000000000000400000000000003000000000000000000000000000000" +
		"0200000001000000"
	goldenSnapshotMarkerServiceEndHex = "280064006f001100" +
		"0200000000000000000800000000000004000000000000000000000002000000" +
		"0000000003020100"
	goldenClientSessionHex = "0c0066006f001100" +
		"070000000000000066000000" +
		"090000006165726f6e3a69706302000000abcd"
	goldenClientSessionEmptyVarsHex = "0c0066006f001100" +
		"080000000000000067000000" +
		"0000000000000000"
	goldenTimerHex = "100068006f001100" +
		"09000000000000001027000000000000"
	goldenConsensusModuleNoServiceHex = "1c0069006f001100" +
		"0b00000000000000" +
		"0000000000000000000000000000000000000000"
	goldenConsensusModuleMaxHex = "1c0069006f001100" +
		"ffffffffffffff7fffffffffffffff7fffffffffffffff7fffffff7f"
	goldenSnapshotMarkerV3Hex = "200064006f000300" +
		"0200000000000000000200000000000001000000000000000000000000000000"
	goldenConsensusModuleV2Hex = "080069006f000200" +
		"0500000000000000"
	goldenClusterSessionOpenHex = "280067006f001100" +
		"07000000000000000800000000000000" +
		"0004000000000000ffffffffffffffff" +
		"0000008066000000" +
		"090000006165726f6e3a697063"
	goldenClusterSessionClosingHex = "280067006f001100" +
		"09000000000000000a00000000000000" +
		"0008000000000000ffffffffffffffff" +
		"0200000067000000" +
		"00000000"
	goldenPendingMessageTrackerService0Hex = "18006b006f001100" +
		"01000000000000800000000000000080" +
		"0000000000000000"
	goldenPendingMessageTrackerService1Hex = "18006b006f001100" +
		"01000000000000810000000000000081" +
		"0000000001000000"
)

func goldenClusterSessionClosingFields() codecs.ClusterSessionFields {
	return codecs.ClusterSessionFields{
		ClusterSessionID: 9, CorrelationID: 10, OpenedLogPosition: 2048,
		TimeOfLastActivity: -1, CloseReason: clusterwire.CloseReason_TIMEOUT,
		ResponseStreamID: 103,
	}
}

// goldenEmptyTrackerFields returns the fields of a tracker that never held a
// message. The log service session id is the lowest session id for serviceID.
func goldenEmptyTrackerFields(serviceID int32) codecs.PendingMessageTrackerFields {
	logServiceSessionID := int64(serviceID)<<56 | math.MinInt64
	return codecs.PendingMessageTrackerFields{
		NextServiceSessionID: logServiceSessionID + 1,
		LogServiceSessionID:  logServiceSessionID,
		ServiceID:            serviceID,
	}
}

func TestSnapshotWireBytes_goldenHex(t *testing.T) {
	const actingVersion = uint16(clusterwire.SnapshotMarkerSbeSchemaVersion)
	runGoldenHexCases(t, actingVersion, []goldenHexCase{
		goldenCodecCase(
			"SnapshotMarker_consensusModuleBegin", goldenSnapshotMarkerConsensusModuleBeginHex,
			clusterwire.SnapshotMarkerSbeTemplateID, actingVersion,
			codecs.SnapshotMarkerFields{
				TypeID: codecs.ConsensusModuleSnapshotTypeID, LogPosition: 1024,
				LeadershipTermID: 3, Index: 0, Mark: clusterwire.SnapshotMark_BEGIN,
				TimeUnit: clusterwire.ClusterTimeUnit_NANOS, AppVersion: 1,
			},
			codecs.EncodeSnapshotMarker, codecs.TryDecodeSnapshotMarkerAt,
		),
		goldenCodecCase(
			"SnapshotMarker_serviceEnd", goldenSnapshotMarkerServiceEndHex,
			clusterwire.SnapshotMarkerSbeTemplateID, actingVersion,
			codecs.SnapshotMarkerFields{
				TypeID: codecs.ServiceSnapshotTypeID, LogPosition: 2048,
				LeadershipTermID: 4, Index: 0, Mark: clusterwire.SnapshotMark_END,
				TimeUnit: clusterwire.ClusterTimeUnit_MILLIS, AppVersion: 66051,
			},
			codecs.EncodeSnapshotMarker, codecs.TryDecodeSnapshotMarkerAt,
		),
		goldenCodecCase(
			"ClientSession", goldenClientSessionHex,
			clusterwire.ClientSessionSbeTemplateID, actingVersion,
			codecs.ClientSessionFields{
				ClusterSessionID: 7, ResponseStreamID: 102,
				ResponseChannel: []byte("aeron:ipc"), EncodedPrincipal: []byte{0xAB, 0xCD},
			},
			codecs.EncodeClientSession, codecs.TryDecodeClientSessionAt,
		),
		goldenCodecCase(
			"ClientSession_emptyVars", goldenClientSessionEmptyVarsHex,
			clusterwire.ClientSessionSbeTemplateID, actingVersion,
			codecs.ClientSessionFields{ClusterSessionID: 8, ResponseStreamID: 103},
			codecs.EncodeClientSession, codecs.TryDecodeClientSessionAt,
		),
		goldenCodecCase(
			"Timer", goldenTimerHex,
			clusterwire.TimerSbeTemplateID, actingVersion,
			codecs.TimerFields{CorrelationID: 9, Deadline: 10000},
			codecs.EncodeTimer, codecs.TryDecodeTimerAt,
		),
		goldenCodecCase(
			"ConsensusModule_noService", goldenConsensusModuleNoServiceHex,
			clusterwire.ConsensusModuleSbeTemplateID, actingVersion,
			codecs.ConsensusModuleFields{NextSessionID: 11},
			codecs.EncodeConsensusModule, codecs.TryDecodeConsensusModuleAt,
		),
		goldenCodecCase(
			"ConsensusModule_max", goldenConsensusModuleMaxHex,
			clusterwire.ConsensusModuleSbeTemplateID, actingVersion,
			codecs.ConsensusModuleFields{
				NextSessionID:          math.MaxInt64,
				NextServiceSessionID:   math.MaxInt64,
				LogServiceSessionID:    math.MaxInt64,
				PendingMessageCapacity: math.MaxInt32,
			},
			codecs.EncodeConsensusModule, codecs.TryDecodeConsensusModuleAt,
		),
		goldenCodecCase(
			"ClusterSession_open", goldenClusterSessionOpenHex,
			clusterwire.ClusterSessionSbeTemplateID, actingVersion,
			clusterSessionSampleFields(),
			codecs.EncodeClusterSession, codecs.TryDecodeClusterSessionAt,
		),
		goldenCodecCase(
			"ClusterSession_closing", goldenClusterSessionClosingHex,
			clusterwire.ClusterSessionSbeTemplateID, actingVersion,
			goldenClusterSessionClosingFields(),
			codecs.EncodeClusterSession, codecs.TryDecodeClusterSessionAt,
		),
		goldenCodecCase(
			"PendingMessageTracker_service0", goldenPendingMessageTrackerService0Hex,
			clusterwire.PendingMessageTrackerSbeTemplateID, actingVersion,
			goldenEmptyTrackerFields(0),
			codecs.EncodePendingMessageTracker, codecs.TryDecodePendingMessageTrackerAt,
		),
		goldenCodecCase(
			"PendingMessageTracker_service1", goldenPendingMessageTrackerService1Hex,
			clusterwire.PendingMessageTrackerSbeTemplateID, actingVersion,
			goldenEmptyTrackerFields(1),
			codecs.EncodePendingMessageTracker, codecs.TryDecodePendingMessageTrackerAt,
		),
	})
}

func TestSnapshotWireBytes_goldenHexBelowMinVersion(t *testing.T) {
	t.Run("SnapshotMarker_v3", func(t *testing.T) {
		const actingVersion = 3
		runGoldenHexCases(t, actingVersion, []goldenHexCase{
			{
				name:    "service_begin",
				wantHex: goldenSnapshotMarkerV3Hex,
				encode: func(b []byte) (int, bool) {
					return codecs.EncodeSnapshotMarker(b, 0, actingVersion, codecs.SnapshotMarkerFields{
						TypeID: codecs.ServiceSnapshotTypeID, LogPosition: 512,
						LeadershipTermID: 1, Index: 0, Mark: clusterwire.SnapshotMark_BEGIN,
					})
				},
				templateID: clusterwire.SnapshotMarkerSbeTemplateID,
			},
		})
	})
	t.Run("ConsensusModule_v2", func(t *testing.T) {
		const actingVersion = 2
		runGoldenHexCases(t, actingVersion, []goldenHexCase{
			{
				name:    "next_session_id",
				wantHex: goldenConsensusModuleV2Hex,
				encode: func(b []byte) (int, bool) {
					return codecs.EncodeConsensusModule(b, 0, actingVersion, codecs.ConsensusModuleFields{
						NextSessionID: 5,
					})
				},
				templateID: clusterwire.ConsensusModuleSbeTemplateID,
			},
		})
	})
}

func TestSnapshotFlyweightConstants(t *testing.T) {
	require.Equal(t, uint16(40), clusterwire.SnapshotMarkerSbeBlockLength)
	require.Equal(t, uint16(100), clusterwire.SnapshotMarkerSbeTemplateID)
	require.Equal(t, 48, codecs.SnapshotMarkerEncodedLen())

	require.Equal(t, uint16(12), clusterwire.ClientSessionSbeBlockLength)
	require.Equal(t, uint16(102), clusterwire.ClientSessionSbeTemplateID)

	require.Equal(t, uint16(16), clusterwire.TimerSbeBlockLength)
	require.Equal(t, uint16(104), clusterwire.TimerSbeTemplateID)
	require.Equal(t, 24, codecs.TimerEncodedLen())

	require.Equal(t, uint16(28), clusterwire.ConsensusModuleSbeBlockLength)
	require.Equal(t, uint16(105), clusterwire.ConsensusModuleSbeTemplateID)
	require.Equal(t, 36, codecs.ConsensusModuleEncodedLen())

	require.Equal(t, uint16(40), clusterwire.ClusterSessionSbeBlockLength)
	require.Equal(t, uint16(103), clusterwire.ClusterSessionSbeTemplateID)

	require.Equal(t, uint16(24), clusterwire.PendingMessageTrackerSbeBlockLength)
	require.Equal(t, uint16(107), clusterwire.PendingMessageTrackerSbeTemplateID)
	require.Equal(t, 32, codecs.PendingMessageTrackerEncodedLen())

	require.Equal(t, int64(1), codecs.ConsensusModuleSnapshotTypeID)
	require.Equal(t, int64(2), codecs.ServiceSnapshotTypeID)

	for _, id := range []uint16{
		clusterwire.SnapshotMarkerSbeSchemaID,
		clusterwire.ClientSessionSbeSchemaID,
		clusterwire.TimerSbeSchemaID,
		clusterwire.ConsensusModuleSbeSchemaID,
		clusterwire.ClusterSessionSbeSchemaID,
		clusterwire.PendingMessageTrackerSbeSchemaID,
	} {
		require.Equal(t, uint16(111), id)
	}
	for _, version := range []uint16{
		clusterwire.SnapshotMarkerSbeSchemaVersion,
		clusterwire.ClientSessionSbeSchemaVersion,
		clusterwire.TimerSbeSchemaVersion,
		clusterwire.ConsensusModuleSbeSchemaVersion,
		clusterwire.ClusterSessionSbeSchemaVersion,
		clusterwire.PendingMessageTrackerSbeSchemaVersion,
	} {
		require.Equal(t, uint16(17), version)
	}
}

func TestSnapshotMarker_fieldOffsets(t *testing.T) {
	var msg clusterwire.SnapshotMarker
	require.Equal(t, uint(0), msg.TypeIdEncodingOffset())
	require.Equal(t, uint(8), msg.LogPositionEncodingOffset())
	require.Equal(t, uint(16), msg.LeadershipTermIdEncodingOffset())
	require.Equal(t, uint(24), msg.IndexEncodingOffset())
	require.Equal(t, uint(28), msg.MarkEncodingOffset())
	require.Equal(t, uint(32), msg.TimeUnitEncodingOffset())
	require.Equal(t, uint(36), msg.AppVersionEncodingOffset())
	require.Equal(t, uint16(4), codecs.SnapshotMarkerTimeUnitMinVersion)
	require.Equal(t, uint64(4), msg.AppVersionSinceVersion())
}

func TestConsensusModule_fieldOffsets(t *testing.T) {
	var msg clusterwire.ConsensusModule
	require.Equal(t, uint(0), msg.NextSessionIdEncodingOffset())
	require.Equal(t, uint(8), msg.NextServiceSessionIdEncodingOffset())
	require.Equal(t, uint(16), msg.LogServiceSessionIdEncodingOffset())
	require.Equal(t, uint(24), msg.PendingMessageCapacityEncodingOffset())
	require.Equal(t, uint16(3), codecs.ConsensusModuleNextServiceSessionIDMinVersion)
	require.Equal(t, uint64(3), msg.LogServiceSessionIdSinceVersion())
	require.Equal(t, uint64(3), msg.PendingMessageCapacitySinceVersion())
}

func TestClusterSession_fieldOffsets(t *testing.T) {
	var msg clusterwire.ClusterSession
	require.Equal(t, uint(0), msg.ClusterSessionIdEncodingOffset())
	require.Equal(t, uint(8), msg.CorrelationIdEncodingOffset())
	require.Equal(t, uint(16), msg.OpenedLogPositionEncodingOffset())
	require.Equal(t, uint(24), msg.TimeOfLastActivityEncodingOffset())
	require.Equal(t, uint(32), msg.CloseReasonEncodingOffset())
	require.Equal(t, uint(36), msg.ResponseStreamIdEncodingOffset())
	require.Equal(t, uint64(0), msg.ResponseChannelSinceVersion())
	require.Equal(t, uint64(4), msg.ResponseChannelHeaderLength())
}

func TestPendingMessageTracker_fieldOffsets(t *testing.T) {
	var msg clusterwire.PendingMessageTracker
	require.Equal(t, uint(0), msg.NextServiceSessionIdEncodingOffset())
	require.Equal(t, uint(8), msg.LogServiceSessionIdEncodingOffset())
	require.Equal(t, uint(16), msg.PendingMessageCapacityEncodingOffset())
	require.Equal(t, uint(20), msg.ServiceIdEncodingOffset())
	require.Equal(t, uint64(0), msg.ServiceIdSinceVersion())
}
