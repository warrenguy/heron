package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestTimerEvent_roundtrip(t *testing.T) {
	v := clusterwire.TimerEventSbeSchemaVersion
	runCodecRoundtripCases(
		t,
		clusterwire.TimerEventSbeTemplateID,
		v,
		[]codecRoundtripCase{
			{
				name: "sample",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeTimerEvent(buf, offset, v, codecs.TimerEventFields{
						LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
					})
				},
				wantFields: codecs.TimerEventFields{
					LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
				},
			},
			{
				name: "max_int",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeTimerEvent(buf, offset, v, codecs.TimerEventFields{
						LeadershipTermID: math.MaxInt64,
						CorrelationID:    math.MaxInt64,
						Timestamp:        math.MaxInt64,
					})
				},
				wantFields: codecs.TimerEventFields{
					LeadershipTermID: math.MaxInt64,
					CorrelationID:    math.MaxInt64,
					Timestamp:        math.MaxInt64,
				},
			},
		},
	)
}

func TestSessionOpenEvent_roundtrip(t *testing.T) {
	v := clusterwire.SessionOpenEventSbeSchemaVersion
	fields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1, CorrelationID: 2, ClusterSessionID: 3, Timestamp: 4,
		ResponseStreamID: 102, ResponseChannel: []byte("aeron:ipc"),
		EncodedPrincipal: []byte{0xAB, 0xCD},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.SessionOpenEventSbeTemplateID,
		v,
		[]codecRoundtripCase{
			{
				name: "sample",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeSessionOpenEvent(buf, offset, v, fields)
				},
				wantFields: fields,
			},
			{
				name: "empty_vars",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeSessionOpenEvent(buf, offset, v, codecs.SessionOpenEventFields{
						LeadershipTermID: 9, CorrelationID: 8, ClusterSessionID: 7, Timestamp: 6,
						ResponseStreamID: 1,
					})
				},
				wantFields: codecs.SessionOpenEventFields{
					LeadershipTermID: 9, CorrelationID: 8, ClusterSessionID: 7, Timestamp: 6,
					ResponseStreamID: 1,
				},
			},
		},
	)
}

func TestSessionCloseEvent_roundtrip(t *testing.T) {
	v := clusterwire.SessionCloseEventSbeSchemaVersion
	runCodecRoundtripCases(
		t,
		clusterwire.SessionCloseEventSbeTemplateID,
		v,
		[]codecRoundtripCase{
			{
				name: "client_action",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeSessionCloseEvent(buf, offset, v, codecs.SessionCloseEventFields{
						LeadershipTermID: 5, ClusterSessionID: 6, Timestamp: 7,
						CloseReason: clusterwire.CloseReason_CLIENT_ACTION,
					})
				},
				wantFields: codecs.SessionCloseEventFields{
					LeadershipTermID: 5, ClusterSessionID: 6, Timestamp: 7,
					CloseReason: clusterwire.CloseReason_CLIENT_ACTION,
				},
			},
			{
				name: "timeout",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeSessionCloseEvent(buf, offset, v, codecs.SessionCloseEventFields{
						LeadershipTermID: 1, ClusterSessionID: 2, Timestamp: 3,
						CloseReason: clusterwire.CloseReason_TIMEOUT,
					})
				},
				wantFields: codecs.SessionCloseEventFields{
					LeadershipTermID: 1, ClusterSessionID: 2, Timestamp: 3,
					CloseReason: clusterwire.CloseReason_TIMEOUT,
				},
			},
		},
	)
}

func TestClusterActionRequest_roundtrip(t *testing.T) {
	v := clusterwire.ClusterActionRequestSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.ClusterActionRequestFields]{
		{name: "sample", fields: clusterActionRequestSampleFields()},
		{
			name: "nonzero_flags",
			fields: codecs.ClusterActionRequestFields{
				LeadershipTermID: 1, LogPosition: 2, Timestamp: 3,
				Action: clusterwire.ClusterAction_SNAPSHOT, Flags: 4,
			},
		},
		{
			name: "max_int",
			fields: codecs.ClusterActionRequestFields{
				LeadershipTermID: math.MaxInt64,
				LogPosition:      math.MaxInt64,
				Timestamp:        math.MaxInt64,
				Action:           clusterwire.ClusterAction_RESUME,
				Flags:            math.MaxInt32,
			},
		},
		{
			name: "raw_action",
			fields: codecs.ClusterActionRequestFields{
				LeadershipTermID: 5, LogPosition: 6, Timestamp: 7,
				Action: clusterwire.ClusterAction(99), Flags: 1,
			},
		},
		{
			name: "null_flags",
			fields: codecs.ClusterActionRequestFields{
				LeadershipTermID: 1, LogPosition: 2, Timestamp: 3,
				Action: clusterwire.ClusterAction_SUSPEND, Flags: math.MinInt32,
			},
		},
		{
			name:      "acting_version_10",
			actingVer: 10,
			fields: codecs.ClusterActionRequestFields{
				LeadershipTermID: 7, LogPosition: 8, Timestamp: 9,
				Action: clusterwire.ClusterAction_SNAPSHOT, Flags: 5,
			},
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.ClusterActionRequestSbeTemplateID,
		v,
		buildActingFieldRoundtripCases(
			v,
			fieldCases,
			codecs.EncodeClusterActionRequest,
			clusterActionRequestWantFields,
		),
	)
}

func TestEncodeClusterActionRequest_dirtyBufferFlagsZero(t *testing.T) {
	actingVersion := uint16(clusterwire.ClusterActionRequestSbeSchemaVersion)
	fields := clusterActionRequestSampleFields()

	buf := make([]byte, codecs.ClusterActionRequestEncodedLen())
	const residual byte = 0xFF
	for i := range buf {
		buf[i] = residual
	}

	wireEnd, ok := codecs.EncodeClusterActionRequest(buf, 0, actingVersion, fields)
	require.True(t, ok)
	require.Equal(t, codecs.ClusterActionRequestEncodedLen(), wireEnd)

	var msg clusterwire.ClusterActionRequest
	flagsOff := messageHeaderLen + int(msg.FlagsEncodingOffset())
	require.Equal(t, []byte{0, 0, 0, 0}, buf[flagsOff:flagsOff+4])

	got, ok := codecs.TryDecodeClusterActionRequestAt(buf, 0)
	require.True(t, ok)
	require.Equal(t, int32(0), got.Flags)
}

func TestClusterActionRequest_blockLength28Version10_flagsZero(t *testing.T) {
	// Asserts a 28-byte root at actingVersion 10 decodes. Flags is absent
	// below sinceVersion 11. Decode sets Flags to 0.
	v := clusterwire.ClusterActionRequestSbeSchemaVersion
	fields := clusterActionRequestSampleFields()
	wire := encodeClusterActionRequestWire(t, v, fields)
	wire = cloneMutatedWire(wire, func(w []byte) {
		w[0], w[1] = 28, 0
		w[6], w[7] = 10, 0
	})
	want := clusterActionRequestWantFields(fields, 10)
	got, ok := codecs.TryDecodeClusterActionRequestAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, want, got)

	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecTestMaxVarLen)
	require.True(t, ok)
	require.Equal(t, clusterwire.ClusterActionRequestSbeTemplateID, decoded.TemplateID)
	require.Equal(t, want, decoded.Fields)
}

func TestClusterActionRequest_blockLength28Version11_rejects(t *testing.T) {
	// Asserts a 28-byte root at actingVersion 11 fails decode. The Flags
	// field requires a 32-byte root at sinceVersion 11.
	v := clusterwire.ClusterActionRequestSbeSchemaVersion
	wire := encodeClusterActionRequestWire(t, v, clusterActionRequestSampleFields())
	wire = cloneMutatedWire(wire, func(w []byte) {
		w[0], w[1] = 28, 0
		w[6], w[7] = 11, 0
	})
	_, ok := codecs.TryDecodeClusterActionRequestAt(wire, 0)
	require.False(t, ok)

	_, ok = codecs.TryDecodeByTemplate(wire, 0, codecTestMaxVarLen)
	require.False(t, ok)
}

func TestScheduleTimer_roundtrip(t *testing.T) {
	v := clusterwire.ScheduleTimerSbeSchemaVersion
	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.ScheduleTimerSbeTemplateID,
		v,
		[]codecFieldsActingCase[codecs.ScheduleTimerFields]{
			{name: "sample", fields: scheduleTimerSampleFields()},
			{
				name: "max_int",
				fields: codecs.ScheduleTimerFields{
					CorrelationID: math.MaxInt64,
					Deadline:      math.MaxInt64,
				},
			},
		},
		codecs.EncodeScheduleTimer,
	)
}

func TestCancelTimer_roundtrip(t *testing.T) {
	v := clusterwire.CancelTimerSbeSchemaVersion
	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.CancelTimerSbeTemplateID,
		v,
		[]codecFieldsActingCase[codecs.CancelTimerFields]{
			{name: "sample", fields: cancelTimerSampleFields()},
			{
				name:   "max_int",
				fields: codecs.CancelTimerFields{CorrelationID: math.MaxInt64},
			},
		},
		codecs.EncodeCancelTimer,
	)
}

func TestCloseSession_roundtrip(t *testing.T) {
	v := clusterwire.CloseSessionSbeSchemaVersion
	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.CloseSessionSbeTemplateID,
		v,
		[]codecFieldsActingCase[codecs.CloseSessionFields]{
			{name: "sample", fields: closeSessionSampleFields()},
			{
				name:   "max_int",
				fields: codecs.CloseSessionFields{ClusterSessionID: math.MaxInt64},
			},
		},
		codecs.EncodeCloseSession,
	)
}

func TestServiceAck_roundtrip(t *testing.T) {
	v := clusterwire.ServiceAckSbeSchemaVersion
	fields := codecs.ServiceAckFields{
		LogPosition: 10, Timestamp: 11, AckID: 12, RelevantID: 13, ServiceID: 1,
	}
	runCodecRoundtripCases(
		t,
		clusterwire.ServiceAckSbeTemplateID,
		v,
		[]codecRoundtripCase{
			{
				name: "sample",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeServiceAck(buf, offset, v, fields)
				},
				wantFields: fields,
			},
		},
	)
}

func TestJoinLog_roundtrip(t *testing.T) {
	currentVersion := clusterwire.JoinLogSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.JoinLogFields]{
		{
			name:      "current_version_standby_false",
			actingVer: currentVersion,
			fields: codecs.JoinLogFields{
				LogPosition: 100, MaxLogPosition: 200, MemberID: 1, LogSessionID: 2,
				LogStreamID: 100, IsStartup: clusterwire.BooleanType_TRUE, Role: 1,
				IsStandby:  clusterwire.BooleanType_FALSE,
				LogChannel: []byte("aeron:udp?endpoint=localhost:9004"),
			},
		},
		{
			name:      "v15_no_isStandby",
			actingVer: 15,
			fields: codecs.JoinLogFields{
				LogPosition: 1, MaxLogPosition: 2, MemberID: 3, LogSessionID: 4,
				LogStreamID: 5, IsStartup: clusterwire.BooleanType_FALSE, Role: 0,
				IsStandby:  clusterwire.BooleanType_TRUE,
				LogChannel: []byte("ch"),
			},
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.JoinLogSbeTemplateID,
		currentVersion,
		buildActingFieldRoundtripCases(
			currentVersion,
			fieldCases,
			func(buf []byte, offset int, actingVer uint16, fields codecs.JoinLogFields) (int, bool) {
				return codecs.EncodeJoinLog(buf, offset, actingVer, fields)
			},
			codecs.JoinLogOracleFields,
		),
	)
}

func TestJoinLogBlockLength_versionGates(t *testing.T) {
	cases := []struct {
		name      string
		actingVer uint16
		want      uint16
	}{
		{"v0_base", 0, 36},
		{"v15_base", 15, 36},
		{"v16_full", 16, 40},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, codecs.JoinLogBlockLength(tc.actingVer))
			minBlock, maxBlock, ok := codecs.ClusterRootBlockLengthBounds(
				clusterwire.JoinLogSbeTemplateID, tc.actingVer,
			)
			require.True(t, ok)
			require.Equal(t, tc.want, minBlock)
			require.Equal(t, clusterwire.JoinLogSbeBlockLength, maxBlock)
		})
	}
}

func TestRequestServiceAck_roundtrip(t *testing.T) {
	v := clusterwire.RequestServiceAckSbeSchemaVersion
	fields := codecs.RequestServiceAckFields{LogPosition: 42}
	runCodecRoundtripCases(
		t,
		clusterwire.RequestServiceAckSbeTemplateID,
		v,
		[]codecRoundtripCase{
			{
				name: "sample",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeRequestServiceAck(buf, offset, v, fields)
				},
				wantFields: fields,
			},
		},
	)
}

func TestServiceEncode_rejects(t *testing.T) {
	v := clusterwire.TimerEventSbeSchemaVersion
	buf := make([]byte, 8)
	requireEncodeRejects(
		t,
		buf,
		v,
		codecs.TimerEventFields{
			LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
		},
		codecs.EncodeTimerEvent,
	)
	requireEncodeRejects(t, buf, v, scheduleTimerSampleFields(), codecs.EncodeScheduleTimer)
	requireEncodeRejects(t, buf, v, cancelTimerSampleFields(), codecs.EncodeCancelTimer)
	requireEncodeRejects(t, buf, v, closeSessionSampleFields(), codecs.EncodeCloseSession)
	requireEncodeRejects(
		t, buf, v, clusterActionRequestSampleFields(), codecs.EncodeClusterActionRequest,
	)

	openFields := codecs.SessionOpenEventFields{
		ResponseChannel:  make([]byte, codecs.ClusterEncodeMaxVarLen+1),
		EncodedPrincipal: nil,
	}
	big := make([]byte, 1024+codecs.ClusterEncodeMaxVarLen)
	end, ok := codecs.EncodeSessionOpenEvent(big, 0, v, openFields)
	require.False(t, ok)
	require.Equal(t, 0, end)
}

func TestServiceTryDecode_rejects(t *testing.T) {
	v := clusterwire.TimerEventSbeSchemaVersion
	validTimer := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeTimerEvent(buf, 0, v, codecs.TimerEventFields{
			LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
		})
	})
	validOpen := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeSessionOpenEvent(buf, 0, v, codecs.SessionOpenEventFields{
			LeadershipTermID: 1, CorrelationID: 2, ClusterSessionID: 3, Timestamp: 4,
			ResponseStreamID: 1, ResponseChannel: []byte("abcd"), EncodedPrincipal: []byte{1, 2, 3, 4},
		})
	})
	validJoin := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeJoinLog(buf, 0, v, codecs.JoinLogFields{
			LogPosition: 1, MaxLogPosition: 2, MemberID: 3, LogSessionID: 4, LogStreamID: 5,
			IsStartup: clusterwire.BooleanType_TRUE, Role: 1, IsStandby: clusterwire.BooleanType_FALSE,
			LogChannel: []byte("channel"),
		})
	})

	cases := []tryDecodeRejectCase{}
	cases = append(cases, wireHeaderRejectCases(validTimer)...)
	cases = append(cases, fixedBlockRejectCases(
		"timer", validTimer, clusterwire.TimerEventSbeBlockLength, 23, 8,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"cluster_action_request",
		encodeClusterActionRequestWire(t, v, clusterActionRequestSampleFields()),
		clusterwire.ClusterActionRequestSbeBlockLength, 31, 8,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"schedule_timer",
		encodeScheduleTimerWire(t, v, scheduleTimerSampleFields()),
		clusterwire.ScheduleTimerSbeBlockLength, 15, 8,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"cancel_timer",
		encodeCancelTimerWire(t, v, cancelTimerSampleFields()),
		clusterwire.CancelTimerSbeBlockLength, 7, 4,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"close_session",
		encodeCloseSessionWire(t, v, closeSessionSampleFields()),
		clusterwire.CloseSessionSbeBlockLength, 7, 4,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"service_ack",
		encodeClusterWire(t, func(buf []byte) (int, bool) {
			return codecs.EncodeServiceAck(buf, 0, v, codecs.ServiceAckFields{
				LogPosition: 1, Timestamp: 2, AckID: 3, RelevantID: 4, ServiceID: 5,
			})
		}),
		clusterwire.ServiceAckSbeBlockLength, 35, 8,
	)...)
	cases = append(
		cases,
		tryDecodeRejectCase{
			name:   "open_var_field_exceeds_max",
			buf:    func() []byte { return validOpen },
			maxLen: 1,
		},
		tryDecodeRejectCase{
			name:   "join_var_field_exceeds_max",
			buf:    func() []byte { return validJoin },
			maxLen: 1,
		},
		tryDecodeRejectCase{
			name: "open_truncated_var",
			buf:  func() []byte { return validOpen[:len(validOpen)-1] },
		},
	)
	runTryDecodeRejectCases(t, cases)
}

func TestSessionOpenEventFieldsFromLayout_wrongKind(t *testing.T) {
	v := clusterwire.TimerEventSbeSchemaVersion
	wire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeTimerEvent(buf, 0, v, codecs.TimerEventFields{
			LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
		})
	})
	layout, ok := codecs.ClusterMessageLayoutAt(wire, 0, 0)
	require.True(t, ok)
	_, ok = codecs.SessionOpenEventFieldsFromLayout(wire, layout)
	require.False(t, ok)
}

func TestClusterActionRequestFieldsFromLayout(t *testing.T) {
	v := clusterwire.ClusterActionRequestSbeSchemaVersion
	want := clusterActionRequestSampleFields()
	requireFieldsFromLayout(
		t,
		clusterActionRequestWantFields(want, v),
		encodeClusterActionRequestWire(t, v, want),
		codecs.ClusterActionRequestFieldsFromLayout,
		encodeScheduleTimerWire(t, v, scheduleTimerSampleFields()),
	)
}

func TestScheduleTimerFieldsFromLayout(t *testing.T) {
	v := clusterwire.ScheduleTimerSbeSchemaVersion
	want := scheduleTimerSampleFields()
	requireFieldsFromLayout(
		t,
		want,
		encodeScheduleTimerWire(t, v, want),
		codecs.ScheduleTimerFieldsFromLayout,
		encodeCancelTimerWire(t, v, cancelTimerSampleFields()),
	)
}

func TestCancelTimerFieldsFromLayout(t *testing.T) {
	v := clusterwire.CancelTimerSbeSchemaVersion
	want := cancelTimerSampleFields()
	requireFieldsFromLayout(
		t,
		want,
		encodeCancelTimerWire(t, v, want),
		codecs.CancelTimerFieldsFromLayout,
		encodeScheduleTimerWire(t, v, scheduleTimerSampleFields()),
	)
}

func TestTryDecodeAt_serviceCatalogue(t *testing.T) {
	v := clusterwire.TimerEventSbeSchemaVersion
	buf := make([]byte, 512)

	t.Run("timer_event", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.TimerEventFields{
				LeadershipTermID: 9, CorrelationID: 8, Timestamp: 7,
			},
			codecs.EncodeTimerEvent,
			codecs.TryDecodeTimerEventAt,
		)
	})
	t.Run("session_open_event", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.SessionOpenEventFields{
				LeadershipTermID: 1, CorrelationID: 2, ClusterSessionID: 3, Timestamp: 4,
				ResponseStreamID: 102, ResponseChannel: []byte("aeron:ipc"),
				EncodedPrincipal: []byte{0xAB, 0xCD},
			},
			codecs.EncodeSessionOpenEvent,
			codecs.TryDecodeSessionOpenEventAt,
		)
	})
	t.Run("session_close_event", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.SessionCloseEventFields{
				LeadershipTermID: 5, ClusterSessionID: 6, Timestamp: 7,
				CloseReason: clusterwire.CloseReason_TIMEOUT,
			},
			codecs.EncodeSessionCloseEvent,
			codecs.TryDecodeSessionCloseEventAt,
		)
	})
	t.Run("cluster_action_request", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			clusterActionRequestSampleFields(),
			codecs.EncodeClusterActionRequest,
			codecs.TryDecodeClusterActionRequestAt,
		)
	})
	t.Run("schedule_timer", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.ScheduleTimerFields{
				CorrelationID: 1, Deadline: 2,
			},
			codecs.EncodeScheduleTimer,
			codecs.TryDecodeScheduleTimerAt,
		)
	})
	t.Run("cancel_timer", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.CancelTimerFields{
				CorrelationID: 1,
			},
			codecs.EncodeCancelTimer,
			codecs.TryDecodeCancelTimerAt,
		)
	})
	t.Run("close_session", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.CloseSessionFields{
				ClusterSessionID: 1,
			},
			codecs.EncodeCloseSession,
			codecs.TryDecodeCloseSessionAt,
		)
	})
	t.Run("service_ack", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.ServiceAckFields{
				LogPosition: 1, Timestamp: 2, AckID: 3, RelevantID: 4, ServiceID: 5,
			},
			codecs.EncodeServiceAck,
			codecs.TryDecodeServiceAckAt,
		)
	})
	t.Run("join_log", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.JoinLogFields{
				LogPosition: 100, MaxLogPosition: 200, MemberID: 1, LogSessionID: 2,
				LogStreamID: 100, IsStartup: clusterwire.BooleanType_TRUE, Role: 1,
				IsStandby:  clusterwire.BooleanType_FALSE,
				LogChannel: []byte("aeron:udp?endpoint=localhost:9004"),
			},
			codecs.EncodeJoinLog,
			codecs.TryDecodeJoinLogAt,
		)
	})
	t.Run("request_service_ack", func(t *testing.T) {
		requireTryDecodeAt(
			t,
			buf,
			v,
			codecs.RequestServiceAckFields{
				LogPosition: 99,
			},
			codecs.EncodeRequestServiceAck,
			codecs.TryDecodeRequestServiceAckAt,
		)
	})
}

func TestTryDecodeClusterActionRequestAt_rejects(t *testing.T) {
	v := clusterwire.ClusterActionRequestSbeSchemaVersion
	requireTryDecodeAtRejects(
		t,
		encodeClusterActionRequestWire(t, v, clusterActionRequestSampleFields()),
		encodeScheduleTimerWire(t, v, scheduleTimerSampleFields()),
		codecs.TryDecodeClusterActionRequestAt,
	)
}

func TestTryDecodeScheduleTimerAt_rejects(t *testing.T) {
	v := clusterwire.ScheduleTimerSbeSchemaVersion
	requireTryDecodeAtRejects(
		t,
		encodeScheduleTimerWire(t, v, scheduleTimerSampleFields()),
		encodeCancelTimerWire(t, v, cancelTimerSampleFields()),
		codecs.TryDecodeScheduleTimerAt,
	)
}

func TestTryDecodeCancelTimerAt_rejects(t *testing.T) {
	v := clusterwire.CancelTimerSbeSchemaVersion
	requireTryDecodeAtRejects(
		t,
		encodeCancelTimerWire(t, v, cancelTimerSampleFields()),
		encodeScheduleTimerWire(t, v, scheduleTimerSampleFields()),
		codecs.TryDecodeCancelTimerAt,
	)
}

func TestTryDecodeCloseSessionAt_rejects(t *testing.T) {
	v := clusterwire.CloseSessionSbeSchemaVersion
	requireTryDecodeAtRejects(
		t,
		encodeCloseSessionWire(t, v, closeSessionSampleFields()),
		encodeCancelTimerWire(t, v, cancelTimerSampleFields()),
		codecs.TryDecodeCloseSessionAt,
	)
}

func TestEncodeServiceTemplates_ZeroAllocs(t *testing.T) {
	actingVersion := uint16(clusterwire.TimerEventSbeSchemaVersion)
	channel := []byte("aeron:udp?endpoint=localhost:9004")
	principal := []byte("principal")
	varLen := func(block uint16, fields ...[]byte) int {
		total := messageHeaderLen + int(block)
		for _, f := range fields {
			total += codecs.VarLengthPrefixSize + len(f)
		}
		return total
	}

	cases := []struct {
		name   string
		bufLen int
		encode func([]byte)
	}{
		{
			name:   "EncodeTimerEvent",
			bufLen: codecs.TimerEventEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeTimerEvent(buf, 0, actingVersion, codecs.TimerEventFields{
					LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeSessionOpenEvent",
			bufLen: varLen(clusterwire.SessionOpenEventSbeBlockLength, channel, principal),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeSessionOpenEvent(buf, 0, actingVersion, codecs.SessionOpenEventFields{
					LeadershipTermID: 1, CorrelationID: 2, ClusterSessionID: 3, Timestamp: 4,
					ResponseStreamID: 102, ResponseChannel: channel, EncodedPrincipal: principal,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeSessionCloseEvent",
			bufLen: codecs.SessionCloseEventEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeSessionCloseEvent(buf, 0, actingVersion, codecs.SessionCloseEventFields{
					LeadershipTermID: 1, ClusterSessionID: 2, Timestamp: 3,
					CloseReason: clusterwire.CloseReason_TIMEOUT,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeClusterActionRequest",
			bufLen: codecs.ClusterActionRequestEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeClusterActionRequest(
					buf, 0, actingVersion, clusterActionRequestSampleFields(),
				)
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeScheduleTimer",
			bufLen: codecs.ScheduleTimerEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeScheduleTimer(buf, 0, actingVersion, codecs.ScheduleTimerFields{
					CorrelationID: 1, Deadline: 2,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeCancelTimer",
			bufLen: codecs.CancelTimerEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeCancelTimer(buf, 0, actingVersion, codecs.CancelTimerFields{
					CorrelationID: 1,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeCloseSession",
			bufLen: codecs.CloseSessionEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeCloseSession(buf, 0, actingVersion, codecs.CloseSessionFields{
					ClusterSessionID: 1,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeServiceAck",
			bufLen: codecs.ServiceAckEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeServiceAck(buf, 0, actingVersion, codecs.ServiceAckFields{
					LogPosition: 1, Timestamp: 2, AckID: 3, RelevantID: 4, ServiceID: 5,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeJoinLog",
			bufLen: varLen(clusterwire.JoinLogSbeBlockLength, channel),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeJoinLog(buf, 0, actingVersion, codecs.JoinLogFields{
					LogPosition: 100, MaxLogPosition: 200, MemberID: 1, LogSessionID: 2,
					LogStreamID: 100, IsStartup: clusterwire.BooleanType_TRUE, Role: 1,
					IsStandby: clusterwire.BooleanType_FALSE, LogChannel: channel,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeRequestServiceAck",
			bufLen: codecs.RequestServiceAckEncodedLen(),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeRequestServiceAck(buf, 0, actingVersion, codecs.RequestServiceAckFields{
					LogPosition: 42,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			testutil.AssertZeroAllocs(t, tc.name, 1000, func() {
				tc.encode(buf)
			})
		})
	}
}

func clusterActionRequestSampleFields() codecs.ClusterActionRequestFields {
	return codecs.ClusterActionRequestFields{
		LeadershipTermID: 1,
		LogPosition:      2,
		Timestamp:        3,
		Action:           clusterwire.ClusterAction_SNAPSHOT,
		Flags:            0,
	}
}

func encodeClusterActionRequestWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.ClusterActionRequestFields,
) []byte {
	t.Helper()
	return encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeClusterActionRequest(buf, 0, actingVersion, fields)
	})
}

func scheduleTimerSampleFields() codecs.ScheduleTimerFields {
	return codecs.ScheduleTimerFields{CorrelationID: 1, Deadline: 2}
}

func cancelTimerSampleFields() codecs.CancelTimerFields {
	return codecs.CancelTimerFields{CorrelationID: 1}
}

func encodeScheduleTimerWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.ScheduleTimerFields,
) []byte {
	t.Helper()
	return encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeScheduleTimer(buf, 0, actingVersion, fields)
	})
}

func encodeCancelTimerWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.CancelTimerFields,
) []byte {
	t.Helper()
	return encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeCancelTimer(buf, 0, actingVersion, fields)
	})
}

func closeSessionSampleFields() codecs.CloseSessionFields {
	return codecs.CloseSessionFields{ClusterSessionID: 1}
}

func encodeCloseSessionWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.CloseSessionFields,
) []byte {
	t.Helper()
	return encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeCloseSession(buf, 0, actingVersion, fields)
	})
}

func requireEncodeRejects[T any](
	t *testing.T,
	buf []byte,
	actingVersion uint16,
	fields T,
	encode func(buf []byte, offset int, actingVersion uint16, fields T) (int, bool),
) {
	t.Helper()
	end, ok := encode(buf, 0, actingVersion, fields)
	require.False(t, ok)
	require.Equal(t, 0, end)
	end, ok = encode(buf, -1, actingVersion, fields)
	require.False(t, ok)
	require.Equal(t, 0, end)
}

func requireFieldsFromLayout[T any](
	t *testing.T,
	want T,
	wire []byte,
	fromLayout func([]byte, codecs.ClusterMessageLayout) (T, bool),
	wrongWire []byte,
) {
	t.Helper()
	layout, ok := codecs.ClusterMessageLayoutAt(wire, 0, 0)
	require.True(t, ok)
	got, ok := fromLayout(wire, layout)
	require.True(t, ok)
	require.Equal(t, want, got)

	wrongLayout, ok := codecs.ClusterMessageLayoutAt(wrongWire, 0, 0)
	require.True(t, ok)
	_, ok = fromLayout(wrongWire, wrongLayout)
	require.False(t, ok)
}

func requireTryDecodeAtRejects[T any](
	t *testing.T,
	validWire, wrongKindWire []byte,
	tryDecode func([]byte, int) (T, bool),
) {
	t.Helper()
	_, ok := tryDecode(nil, 0)
	require.False(t, ok)
	_, ok = tryDecode(validWire, -1)
	require.False(t, ok)
	_, ok = tryDecode(validWire[:4], 0)
	require.False(t, ok)
	_, ok = tryDecode(wrongKindWire, 0)
	require.False(t, ok)
}
