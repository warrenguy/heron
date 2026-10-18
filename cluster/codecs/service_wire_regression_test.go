package codecs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Golden wire bytes for service catalogue messages at actingVersion 17.
const (
	goldenTimerEventHex = "180014006f001100" +
		"010000000000000002000000000000000300000000000000"
	goldenSessionOpenEventHex = "240015006f001100" +
		"0100000000000000020000000000000003000000000000000400000000000000" +
		"66000000090000006165726f6e3a69706302000000abcd"
	goldenSessionCloseEventHex = "1c0016006f001100" +
		"05000000000000000600000000000000070000000000000000000000"
	goldenClusterActionRequestHex = "200017006f001100" +
		"0100000000000000020000000000000003000000000000000200000004000000"
	goldenScheduleTimerHex = "10001f006f001100" +
		"01000000000000000200000000000000"
	goldenCancelTimerHex = "080020006f0011000100000000000000"
	goldenServiceAckHex  = "240021006f001100" +
		"0a000000000000000b000000000000000c000000000000000d0000000000000001000000"
	goldenJoinLogHex = "280028006f001100" +
		"6400000000000000c80000000000000001000000020000006400000001000000" +
		"0100000000000000210000006165726f6e3a7564703f656e64706f696e743d6c6f63616c686f73743a39303034"
	goldenRequestServiceAckHex = "08006c006f0011002a00000000000000"
)

func TestServiceWireBytes_goldenHex(t *testing.T) {
	actingVersion := uint16(clusterwire.TimerEventSbeSchemaVersion)
	runGoldenHexCases(t, actingVersion, []goldenHexCase{
		{
			name:    "TimerEvent",
			wantHex: goldenTimerEventHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeTimerEvent(b, 0, actingVersion, codecs.TimerEventFields{
					LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
				})
			},
			templateID: clusterwire.TimerEventSbeTemplateID,
		},
		{
			name:    "SessionOpenEvent",
			wantHex: goldenSessionOpenEventHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionOpenEvent(b, 0, actingVersion, codecs.SessionOpenEventFields{
					LeadershipTermID: 1, CorrelationID: 2, ClusterSessionID: 3, Timestamp: 4,
					ResponseStreamID: 102, ResponseChannel: []byte("aeron:ipc"),
					EncodedPrincipal: []byte{0xAB, 0xCD},
				})
			},
			templateID: clusterwire.SessionOpenEventSbeTemplateID,
		},
		{
			name:    "SessionCloseEvent",
			wantHex: goldenSessionCloseEventHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionCloseEvent(b, 0, actingVersion, codecs.SessionCloseEventFields{
					LeadershipTermID: 5, ClusterSessionID: 6, Timestamp: 7,
					CloseReason: clusterwire.CloseReason_CLIENT_ACTION,
				})
			},
			templateID: clusterwire.SessionCloseEventSbeTemplateID,
		},
		{
			name:    "ClusterActionRequest",
			wantHex: goldenClusterActionRequestHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeClusterActionRequest(b, 0, actingVersion, codecs.ClusterActionRequestFields{
					LeadershipTermID: 1, LogPosition: 2, Timestamp: 3,
					Action: clusterwire.ClusterAction_SNAPSHOT, Flags: 4,
				})
			},
			templateID: clusterwire.ClusterActionRequestSbeTemplateID,
		},
		{
			name:    "ScheduleTimer",
			wantHex: goldenScheduleTimerHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeScheduleTimer(b, 0, actingVersion, codecs.ScheduleTimerFields{
					CorrelationID: 1, Deadline: 2,
				})
			},
			templateID: clusterwire.ScheduleTimerSbeTemplateID,
		},
		{
			name:    "CancelTimer",
			wantHex: goldenCancelTimerHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeCancelTimer(b, 0, actingVersion, codecs.CancelTimerFields{
					CorrelationID: 1,
				})
			},
			templateID: clusterwire.CancelTimerSbeTemplateID,
		},
		{
			name:    "ServiceAck",
			wantHex: goldenServiceAckHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeServiceAck(b, 0, actingVersion, codecs.ServiceAckFields{
					LogPosition: 10, Timestamp: 11, AckID: 12, RelevantID: 13, ServiceID: 1,
				})
			},
			templateID: clusterwire.ServiceAckSbeTemplateID,
		},
		{
			name:    "JoinLog",
			wantHex: goldenJoinLogHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeJoinLog(b, 0, actingVersion, codecs.JoinLogFields{
					LogPosition: 100, MaxLogPosition: 200, MemberID: 1, LogSessionID: 2,
					LogStreamID: 100, IsStartup: clusterwire.BooleanType_TRUE, Role: 1,
					IsStandby:  clusterwire.BooleanType_FALSE,
					LogChannel: []byte("aeron:udp?endpoint=localhost:9004"),
				})
			},
			templateID: clusterwire.JoinLogSbeTemplateID,
		},
		{
			name:    "RequestServiceAck",
			wantHex: goldenRequestServiceAckHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeRequestServiceAck(b, 0, actingVersion, codecs.RequestServiceAckFields{
					LogPosition: 42,
				})
			},
			templateID: clusterwire.RequestServiceAckSbeTemplateID,
		},
	})
}

func TestServiceFlyweightConstants(t *testing.T) {
	require.Equal(t, uint16(24), clusterwire.TimerEventSbeBlockLength)
	require.Equal(t, uint16(20), clusterwire.TimerEventSbeTemplateID)
	require.Equal(t, 32, codecs.TimerEventEncodedLen())

	require.Equal(t, uint16(36), clusterwire.SessionOpenEventSbeBlockLength)
	require.Equal(t, uint16(21), clusterwire.SessionOpenEventSbeTemplateID)

	require.Equal(t, uint16(28), clusterwire.SessionCloseEventSbeBlockLength)
	require.Equal(t, uint16(22), clusterwire.SessionCloseEventSbeTemplateID)
	require.Equal(t, 36, codecs.SessionCloseEventEncodedLen())

	require.Equal(t, uint16(32), clusterwire.ClusterActionRequestSbeBlockLength)
	require.Equal(t, uint16(23), clusterwire.ClusterActionRequestSbeTemplateID)
	require.Equal(t, 40, codecs.ClusterActionRequestEncodedLen())

	require.Equal(t, uint16(16), clusterwire.ScheduleTimerSbeBlockLength)
	require.Equal(t, uint16(31), clusterwire.ScheduleTimerSbeTemplateID)
	require.Equal(t, 24, codecs.ScheduleTimerEncodedLen())

	require.Equal(t, uint16(8), clusterwire.CancelTimerSbeBlockLength)
	require.Equal(t, uint16(32), clusterwire.CancelTimerSbeTemplateID)
	require.Equal(t, 16, codecs.CancelTimerEncodedLen())

	require.Equal(t, uint16(36), clusterwire.ServiceAckSbeBlockLength)
	require.Equal(t, uint16(33), clusterwire.ServiceAckSbeTemplateID)
	require.Equal(t, 44, codecs.ServiceAckEncodedLen())

	require.Equal(t, uint16(40), clusterwire.JoinLogSbeBlockLength)
	require.Equal(t, uint16(40), clusterwire.JoinLogSbeTemplateID)

	require.Equal(t, uint16(8), clusterwire.RequestServiceAckSbeBlockLength)
	require.Equal(t, uint16(108), clusterwire.RequestServiceAckSbeTemplateID)
	require.Equal(t, 16, codecs.RequestServiceAckEncodedLen())

	// Schema id and version are schema-wide. A single divergence means the
	// generated sources were regenerated from a different schema.
	for _, id := range []uint16{
		clusterwire.TimerEventSbeSchemaID,
		clusterwire.SessionOpenEventSbeSchemaID,
		clusterwire.SessionCloseEventSbeSchemaID,
		clusterwire.ClusterActionRequestSbeSchemaID,
		clusterwire.ScheduleTimerSbeSchemaID,
		clusterwire.CancelTimerSbeSchemaID,
		clusterwire.ServiceAckSbeSchemaID,
		clusterwire.JoinLogSbeSchemaID,
		clusterwire.RequestServiceAckSbeSchemaID,
	} {
		require.Equal(t, uint16(111), id)
	}
	for _, version := range []uint16{
		clusterwire.TimerEventSbeSchemaVersion,
		clusterwire.SessionOpenEventSbeSchemaVersion,
		clusterwire.SessionCloseEventSbeSchemaVersion,
		clusterwire.ClusterActionRequestSbeSchemaVersion,
		clusterwire.ScheduleTimerSbeSchemaVersion,
		clusterwire.CancelTimerSbeSchemaVersion,
		clusterwire.ServiceAckSbeSchemaVersion,
		clusterwire.JoinLogSbeSchemaVersion,
		clusterwire.RequestServiceAckSbeSchemaVersion,
	} {
		require.Equal(t, uint16(17), version)
	}
}

func TestServiceAck_fieldOffsets(t *testing.T) {
	var msg clusterwire.ServiceAck
	require.Equal(t, uint(0), msg.LogPositionEncodingOffset())
	require.Equal(t, uint(8), msg.TimestampEncodingOffset())
	require.Equal(t, uint(16), msg.AckIdEncodingOffset())
	require.Equal(t, uint(24), msg.RelevantIdEncodingOffset())
	require.Equal(t, uint(32), msg.ServiceIdEncodingOffset())
}

func TestClusterActionRequest_fieldOffsets(t *testing.T) {
	var msg clusterwire.ClusterActionRequest
	require.Equal(t, uint(0), msg.LeadershipTermIdEncodingOffset())
	require.Equal(t, uint(8), msg.LogPositionEncodingOffset())
	require.Equal(t, uint(16), msg.TimestampEncodingOffset())
	require.Equal(t, uint(24), msg.ActionEncodingOffset())
	require.Equal(t, uint(28), msg.FlagsEncodingOffset())
	require.Equal(t, uint64(11), msg.FlagsSinceVersion())
}

func TestScheduleTimer_fieldOffsets(t *testing.T) {
	var msg clusterwire.ScheduleTimer
	require.Equal(t, uint(0), msg.CorrelationIdEncodingOffset())
	require.Equal(t, uint(8), msg.DeadlineEncodingOffset())
}

func TestCancelTimer_fieldOffsets(t *testing.T) {
	var msg clusterwire.CancelTimer
	require.Equal(t, uint(0), msg.CorrelationIdEncodingOffset())
}

func TestJoinLog_isStandbySinceVersion(t *testing.T) {
	var msg clusterwire.JoinLog
	require.Equal(t, uint64(16), msg.IsStandbySinceVersion())
	require.Equal(t, uint16(16), codecs.JoinLogIsStandbyMinVersion)
}
