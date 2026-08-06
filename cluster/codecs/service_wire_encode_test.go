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
	v16 := clusterwire.JoinLogSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.JoinLogFields]{
		{
			name:      "v16_standby_false",
			actingVer: v16,
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
		v16,
		buildActingFieldRoundtripCases(
			v16,
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
	fields := codecs.TimerEventFields{LeadershipTermID: 1, CorrelationID: 2, Timestamp: 3}
	buf := make([]byte, 8)

	end, ok := codecs.EncodeTimerEvent(buf, 0, v, fields)
	require.False(t, ok)
	require.Equal(t, 0, end)

	end, ok = codecs.EncodeTimerEvent(buf, -1, v, fields)
	require.False(t, ok)
	require.Equal(t, 0, end)

	openFields := codecs.SessionOpenEventFields{
		ResponseChannel:  make([]byte, codecs.ClusterEncodeMaxVarLen+1),
		EncodedPrincipal: nil,
	}
	big := make([]byte, 1024+codecs.ClusterEncodeMaxVarLen)
	end, ok = codecs.EncodeSessionOpenEvent(big, 0, v, openFields)
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

func TestTryDecodeAt_serviceCatalogue(t *testing.T) {
	v := clusterwire.TimerEventSbeSchemaVersion
	buf := make([]byte, 512)

	t.Run("timer_event", func(t *testing.T) {
		requireTryDecodeAt(t, buf, v, codecs.TimerEventFields{
			LeadershipTermID: 9, CorrelationID: 8, Timestamp: 7,
		}, codecs.EncodeTimerEvent, codecs.TryDecodeTimerEventAt)
	})
	t.Run("session_open_event", func(t *testing.T) {
		requireTryDecodeAt(t, buf, v, codecs.SessionOpenEventFields{
			LeadershipTermID: 1, CorrelationID: 2, ClusterSessionID: 3, Timestamp: 4,
			ResponseStreamID: 102, ResponseChannel: []byte("aeron:ipc"),
			EncodedPrincipal: []byte{0xAB, 0xCD},
		}, codecs.EncodeSessionOpenEvent, codecs.TryDecodeSessionOpenEventAt)
	})
	t.Run("session_close_event", func(t *testing.T) {
		requireTryDecodeAt(t, buf, v, codecs.SessionCloseEventFields{
			LeadershipTermID: 5, ClusterSessionID: 6, Timestamp: 7,
			CloseReason: clusterwire.CloseReason_TIMEOUT,
		}, codecs.EncodeSessionCloseEvent, codecs.TryDecodeSessionCloseEventAt)
	})
	t.Run("service_ack", func(t *testing.T) {
		requireTryDecodeAt(t, buf, v, codecs.ServiceAckFields{
			LogPosition: 1, Timestamp: 2, AckID: 3, RelevantID: 4, ServiceID: 5,
		}, codecs.EncodeServiceAck, codecs.TryDecodeServiceAckAt)
	})
	t.Run("join_log", func(t *testing.T) {
		requireTryDecodeAt(t, buf, v, codecs.JoinLogFields{
			LogPosition: 100, MaxLogPosition: 200, MemberID: 1, LogSessionID: 2,
			LogStreamID: 100, IsStartup: clusterwire.BooleanType_TRUE, Role: 1,
			IsStandby:  clusterwire.BooleanType_FALSE,
			LogChannel: []byte("aeron:udp?endpoint=localhost:9004"),
		}, codecs.EncodeJoinLog, codecs.TryDecodeJoinLogAt)
	})
	t.Run("request_service_ack", func(t *testing.T) {
		requireTryDecodeAt(t, buf, v, codecs.RequestServiceAckFields{
			LogPosition: 99,
		}, codecs.EncodeRequestServiceAck, codecs.TryDecodeRequestServiceAckAt)
	})
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
