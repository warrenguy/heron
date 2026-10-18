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
)

func TestSnapshotWireBytes_goldenHex(t *testing.T) {
	actingVersion := uint16(clusterwire.SnapshotMarkerSbeSchemaVersion)
	runGoldenHexCases(t, actingVersion, []goldenHexCase{
		{
			name:    "SnapshotMarker_consensusModuleBegin",
			wantHex: goldenSnapshotMarkerConsensusModuleBeginHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSnapshotMarker(b, 0, actingVersion, codecs.SnapshotMarkerFields{
					TypeID: codecs.ConsensusModuleSnapshotTypeID, LogPosition: 1024,
					LeadershipTermID: 3, Index: 0, Mark: clusterwire.SnapshotMark_BEGIN,
					TimeUnit: clusterwire.ClusterTimeUnit_NANOS, AppVersion: 1,
				})
			},
			templateID: clusterwire.SnapshotMarkerSbeTemplateID,
		},
		{
			name:    "SnapshotMarker_serviceEnd",
			wantHex: goldenSnapshotMarkerServiceEndHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSnapshotMarker(b, 0, actingVersion, codecs.SnapshotMarkerFields{
					TypeID: codecs.ServiceSnapshotTypeID, LogPosition: 2048,
					LeadershipTermID: 4, Index: 0, Mark: clusterwire.SnapshotMark_END,
					TimeUnit: clusterwire.ClusterTimeUnit_MILLIS, AppVersion: 66051,
				})
			},
			templateID: clusterwire.SnapshotMarkerSbeTemplateID,
		},
		{
			name:    "ClientSession",
			wantHex: goldenClientSessionHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeClientSession(b, 0, actingVersion, codecs.ClientSessionFields{
					ClusterSessionID: 7, ResponseStreamID: 102,
					ResponseChannel: []byte("aeron:ipc"), EncodedPrincipal: []byte{0xAB, 0xCD},
				})
			},
			templateID: clusterwire.ClientSessionSbeTemplateID,
		},
		{
			name:    "ClientSession_emptyVars",
			wantHex: goldenClientSessionEmptyVarsHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeClientSession(b, 0, actingVersion, codecs.ClientSessionFields{
					ClusterSessionID: 8, ResponseStreamID: 103,
				})
			},
			templateID: clusterwire.ClientSessionSbeTemplateID,
		},
		{
			name:    "Timer",
			wantHex: goldenTimerHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeTimer(b, 0, actingVersion, codecs.TimerFields{
					CorrelationID: 9, Deadline: 10000,
				})
			},
			templateID: clusterwire.TimerSbeTemplateID,
		},
		{
			name:    "ConsensusModule_noService",
			wantHex: goldenConsensusModuleNoServiceHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeConsensusModule(b, 0, actingVersion, codecs.ConsensusModuleFields{
					NextSessionID: 11,
				})
			},
			templateID: clusterwire.ConsensusModuleSbeTemplateID,
		},
		{
			name:    "ConsensusModule_max",
			wantHex: goldenConsensusModuleMaxHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeConsensusModule(b, 0, actingVersion, codecs.ConsensusModuleFields{
					NextSessionID:          math.MaxInt64,
					NextServiceSessionID:   math.MaxInt64,
					LogServiceSessionID:    math.MaxInt64,
					PendingMessageCapacity: math.MaxInt32,
				})
			},
			templateID: clusterwire.ConsensusModuleSbeTemplateID,
		},
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

	require.Equal(t, int64(1), codecs.ConsensusModuleSnapshotTypeID)
	require.Equal(t, int64(2), codecs.ServiceSnapshotTypeID)

	for _, id := range []uint16{
		clusterwire.SnapshotMarkerSbeSchemaID,
		clusterwire.ClientSessionSbeSchemaID,
		clusterwire.TimerSbeSchemaID,
		clusterwire.ConsensusModuleSbeSchemaID,
	} {
		require.Equal(t, uint16(111), id)
	}
	for _, version := range []uint16{
		clusterwire.SnapshotMarkerSbeSchemaVersion,
		clusterwire.ClientSessionSbeSchemaVersion,
		clusterwire.TimerSbeSchemaVersion,
		clusterwire.ConsensusModuleSbeSchemaVersion,
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
