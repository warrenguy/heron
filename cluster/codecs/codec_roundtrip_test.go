package codecs_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

type codecFieldsActingCase[T any] struct {
	name      string
	actingVer uint16
	fields    T
}

func buildActingFieldRoundtripCases[T any](
	defaultActingVer uint16,
	fieldCases []codecFieldsActingCase[T],
	encode func(buf []byte, offset int, actingVer uint16, fields T) (int, bool),
	wantFields func(fields T, actingVer uint16) T,
) []codecRoundtripCase {
	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		actingVer := tc.actingVer
		if actingVer == 0 {
			actingVer = defaultActingVer
		}
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name:      tc.name,
			actingVer: tc.actingVer,
			encode: func(buf []byte, offset int) (int, bool) {
				return encode(buf, offset, actingVer, fields)
			},
			wantFields: wantFields(fields, actingVer),
		}
	}
	return cases
}

func runCodecFieldsActingRoundtrips[T any](
	t *testing.T,
	templateID uint16,
	defaultActingVer uint16,
	fieldCases []codecFieldsActingCase[T],
	encode func(buf []byte, offset int, actingVer uint16, fields T) (int, bool),
) {
	t.Helper()
	runCodecRoundtripCases(
		t,
		templateID,
		defaultActingVer,
		buildActingFieldRoundtripCases(defaultActingVer, fieldCases, encode, func(fields T, _ uint16) T {
			return fields
		}),
	)
}

func encodeSessionEventRoundtrip(
	buf []byte,
	offset int,
	actingVer uint16,
	fields codecs.SessionEventFields,
) (int, bool) {
	return codecs.EncodeSessionEvent(
		buf, offset, actingVer, codecs.SessionEventBlockLength(actingVer), fields,
	)
}

func sessionEventCodeRoundtripCase(
	actingVersion uint16,
	code clusterwire.EventCode,
) codecRoundtripCase {
	return buildActingFieldRoundtripCases(
		actingVersion,
		[]codecFieldsActingCase[codecs.SessionEventFields]{{
			name:      "event_code_" + code.String(),
			actingVer: actingVersion,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         50,
				CorrelationID:            51,
				LeadershipTermID:         52,
				LeaderMemberID:           53,
				Code:                     code,
				Detail:                   []byte("code"),
				LeaderHeartbeatTimeoutNs: 0,
			},
		}},
		encodeSessionEventRoundtrip,
		codecs.SessionEventOracleFields,
	)[0]
}

func TestSessionMessageHeader_roundtrip(t *testing.T) {
	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 42,
		ClusterSessionID: 99,
		Timestamp:        123456789,
	}
	actingVersion := uint16(clusterwire.SessionMessageHeaderSbeSchemaVersion)
	runCodecRoundtrip(
		t,
		clusterwire.SessionMessageHeaderSbeTemplateID,
		actingVersion,
		func(buf []byte, offset int) (int, bool) {
			return codecs.EncodeSessionMessageHeader(buf, offset, actingVersion, fields)
		},
		fields,
	)
}

func TestSessionEvent_roundtrip(t *testing.T) {
	actingVersion := uint16(clusterwire.SessionEventSbeSchemaVersion)

	fieldCases := []codecFieldsActingCase[codecs.SessionEventFields]{
		{
			name:      "empty_detail",
			actingVer: actingVersion,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         1,
				CorrelationID:            2,
				LeadershipTermID:         3,
				LeaderMemberID:           4,
				Code:                     clusterwire.EventCode_OK,
				LeaderHeartbeatTimeoutNs: 0,
			},
		},
		{
			name:      "error_with_detail",
			actingVer: actingVersion,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         10,
				CorrelationID:            11,
				LeadershipTermID:         12,
				LeaderMemberID:           13,
				Code:                     clusterwire.EventCode_ERROR,
				Detail:                   []byte("boom"),
				LeaderHeartbeatTimeoutNs: 0,
			},
		},
		{
			name:      "redirect_with_version",
			actingVer: 6,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         20,
				CorrelationID:            21,
				LeadershipTermID:         22,
				LeaderMemberID:           23,
				Code:                     clusterwire.EventCode_REDIRECT,
				Version:                  7,
				Detail:                   []byte("redirect"),
				LeaderHeartbeatTimeoutNs: math.MinInt64,
			},
		},
		{
			name:      "leader_heartbeat_present",
			actingVer: actingVersion,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         30,
				CorrelationID:            31,
				LeadershipTermID:         32,
				LeaderMemberID:           33,
				Code:                     clusterwire.EventCode_OK,
				LeaderHeartbeatTimeoutNs: 5_000_000_000,
				Detail:                   []byte("hb"),
			},
		},
		{
			name:      "leader_heartbeat_omitted_v13",
			actingVer: 13,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         60,
				CorrelationID:            61,
				LeadershipTermID:         62,
				LeaderMemberID:           63,
				Code:                     clusterwire.EventCode_OK,
				LeaderHeartbeatTimeoutNs: math.MinInt64, // encode omit sentinel; decode 0
				Detail:                   []byte("omit-hb"),
			},
		},
		{
			name:      "sessionEvent_actingVersion5_blockLength32",
			actingVer: 5,
			fields: codecs.SessionEventFields{
				ClusterSessionID:         40,
				CorrelationID:            41,
				LeadershipTermID:         42,
				LeaderMemberID:           43,
				Code:                     clusterwire.EventCode_CLOSED,
				Version:                  0,
				LeaderHeartbeatTimeoutNs: math.MinInt64,
				Detail:                   []byte("detail-v5"),
			},
		},
	}

	cases := buildActingFieldRoundtripCases(
		actingVersion,
		fieldCases,
		encodeSessionEventRoundtrip,
		codecs.SessionEventOracleFields,
	)
	for _, code := range codecAllEventCodes {
		cases = append(cases, sessionEventCodeRoundtripCase(actingVersion, code))
	}

	runCodecRoundtripCases(t, clusterwire.SessionEventSbeTemplateID, actingVersion, cases)
}

func TestSessionConnectRequest_roundtrip(t *testing.T) {
	maxVar := bytes.Repeat([]byte{0x5A}, codecTestMaxVarLen)
	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)

	fieldCases := []codecFieldsActingCase[codecs.SessionConnectRequestFields]{
		{
			name:      "empty_vars",
			actingVer: actingVersion,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      1,
				ResponseStreamID:   100,
				Version:            0,
				ResponseChannel:    nil,
				EncodedCredentials: nil,
			},
		},
		{
			name:      "max_practical_vars",
			actingVer: actingVersion,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      2,
				ResponseStreamID:   101,
				Version:            3,
				ResponseChannel:    maxVar,
				EncodedCredentials: maxVar,
			},
		},
		{
			name:      "version_null_pre_v2",
			actingVer: 1,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:    3,
				ResponseStreamID: 102,
				Version:          0,
			},
		},
		{
			name:      "clientInfo_absent_pre_v14",
			actingVer: 6,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      4,
				ResponseStreamID:   103,
				Version:            2,
				ResponseChannel:    []byte("aeron:ipc"),
				EncodedCredentials: []byte{1, 2, 3},
			},
		},
		{
			name:      "clientInfo_present_v14",
			actingVer: 14,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      5,
				ResponseStreamID:   104,
				Version:            4,
				ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
				EncodedCredentials: []byte{9, 8, 7},
				ClientInfo:         []byte("heron-client"),
			},
		},
	}

	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.SessionConnectRequestSbeTemplateID,
		actingVersion,
		fieldCases,
		codecs.EncodeSessionConnectRequest,
	)
}

func TestSessionCloseRequest_roundtrip(t *testing.T) {
	fields := codecs.SessionCloseRequestFields{LeadershipTermID: 7, ClusterSessionID: 8}
	actingVersion := uint16(clusterwire.SessionCloseRequestSbeSchemaVersion)
	runCodecRoundtrip(
		t,
		clusterwire.SessionCloseRequestSbeTemplateID,
		actingVersion,
		func(buf []byte, offset int) (int, bool) {
			return codecs.EncodeSessionCloseRequest(buf, offset, actingVersion, fields)
		},
		fields,
	)
}

func TestSessionKeepAlive_roundtrip(t *testing.T) {
	fields := codecs.SessionKeepAliveFields{LeadershipTermID: 9, ClusterSessionID: 10}
	actingVersion := uint16(clusterwire.SessionKeepAliveSbeSchemaVersion)
	runCodecRoundtrip(
		t,
		clusterwire.SessionKeepAliveSbeTemplateID,
		actingVersion,
		func(buf []byte, offset int) (int, bool) {
			return codecs.EncodeSessionKeepAlive(buf, offset, actingVersion, fields)
		},
		fields,
	)
}

func TestNewLeaderEvent_roundtrip(t *testing.T) {
	actingVersion := uint16(clusterwire.NewLeaderEventSbeSchemaVersion)
	fieldCases := []codecFieldsActingCase[codecs.NewLeaderEventFields]{
		{
			name: "empty_ingress_endpoints",
			fields: codecs.NewLeaderEventFields{
				LeadershipTermID: 1,
				ClusterSessionID: 2,
				LeaderMemberID:   3,
			},
		},
		{
			name: "single_endpoint",
			fields: codecs.NewLeaderEventFields{
				LeadershipTermID: 4,
				ClusterSessionID: 5,
				LeaderMemberID:   6,
				IngressEndpoints: []byte("localhost:40123"),
			},
		},
	}

	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.NewLeaderEventSbeTemplateID,
		actingVersion,
		fieldCases,
		codecs.EncodeNewLeaderEvent,
	)
}

func TestChallenge_roundtrip(t *testing.T) {
	actingVersion := uint16(clusterwire.ChallengeSbeSchemaVersion)
	fieldCases := []codecFieldsActingCase[codecs.ChallengeFields]{
		{
			name: "empty_challenge",
			fields: codecs.ChallengeFields{
				CorrelationID:    1,
				ClusterSessionID: 2,
				EncodedChallenge: nil,
			},
		},
		{
			name: "max_practical_challenge",
			fields: codecs.ChallengeFields{
				CorrelationID:    3,
				ClusterSessionID: 4,
				EncodedChallenge: bytes.Repeat([]byte{0x11}, codecTestMaxVarLen),
			},
		},
	}

	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.ChallengeSbeTemplateID,
		actingVersion,
		fieldCases,
		codecs.EncodeChallenge,
	)
}

func TestChallengeResponse_roundtrip(t *testing.T) {
	actingVersion := uint16(clusterwire.ChallengeResponseSbeSchemaVersion)
	fieldCases := []codecFieldsActingCase[codecs.ChallengeResponseFields]{
		{
			name: "empty_credentials",
			fields: codecs.ChallengeResponseFields{
				CorrelationID:      1,
				ClusterSessionID:   2,
				EncodedCredentials: nil,
			},
		},
		{
			name: "max_practical_credentials",
			fields: codecs.ChallengeResponseFields{
				CorrelationID:      3,
				ClusterSessionID:   4,
				EncodedCredentials: bytes.Repeat([]byte{0x22}, codecTestMaxVarLen),
			},
		},
	}

	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.ChallengeResponseSbeTemplateID,
		actingVersion,
		fieldCases,
		codecs.EncodeChallengeResponse,
	)
}

func TestAdminRequest_roundtrip(t *testing.T) {
	actingVersion := uint16(clusterwire.AdminRequestSbeSchemaVersion)
	fieldCases := []codecFieldsActingCase[codecs.AdminRequestFields]{
		{
			name: "empty_payload",
			fields: codecs.AdminRequestFields{
				LeadershipTermID: 1,
				ClusterSessionID: 2,
				CorrelationID:    3,
				RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
			},
		},
		{
			name: "payload_16_bytes",
			fields: codecs.AdminRequestFields{
				LeadershipTermID: 4,
				ClusterSessionID: 5,
				CorrelationID:    6,
				RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
				Payload:          bytes.Repeat([]byte{0x33}, 16),
			},
		},
	}

	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.AdminRequestSbeTemplateID,
		actingVersion,
		fieldCases,
		codecs.EncodeAdminRequest,
	)
}

func TestAdminResponse_roundtrip(t *testing.T) {
	actingVersion := uint16(clusterwire.AdminResponseSbeSchemaVersion)
	fieldCases := []codecFieldsActingCase[codecs.AdminResponseFields]{
		{
			name: "ok_with_message",
			fields: codecs.AdminResponseFields{
				ClusterSessionID: 1,
				CorrelationID:    2,
				RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
				ResponseCode:     clusterwire.AdminResponseCode_OK,
				Message:          []byte("done"),
			},
		},
		{
			name: "error_with_message_and_payload",
			fields: codecs.AdminResponseFields{
				ClusterSessionID: 3,
				CorrelationID:    4,
				RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
				ResponseCode:     clusterwire.AdminResponseCode_ERROR,
				Message:          []byte("failed"),
				Payload:          []byte("detail"),
			},
		},
		{
			name: "unauthorised_access",
			fields: codecs.AdminResponseFields{
				ClusterSessionID: 5,
				CorrelationID:    6,
				RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
				ResponseCode:     clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
				Message:          []byte("denied"),
			},
		},
	}

	runCodecFieldsActingRoundtrips(
		t,
		clusterwire.AdminResponseSbeTemplateID,
		actingVersion,
		fieldCases,
		codecs.EncodeAdminResponse,
	)
}

func TestTryDecodeByTemplate_largeDetail(t *testing.T) {
	actingVersion := uint16(clusterwire.SessionEventSbeSchemaVersion)
	fields := codecs.SessionEventFields{
		ClusterSessionID:         1,
		CorrelationID:            2,
		LeadershipTermID:         3,
		LeaderMemberID:           4,
		Code:                     clusterwire.EventCode_OK,
		Detail:                   clusterLargeVarPayload,
		LeaderHeartbeatTimeoutNs: 0,
	}
	wire := encodeSessionEventWire(t, actingVersion, fields)

	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, clusterwire.SessionEventSbeTemplateID, decoded.TemplateID)

	gotFields, ok := decoded.Fields.(codecs.SessionEventFields)
	require.True(t, ok)
	require.Len(t, gotFields.Detail, len(clusterLargeVarPayload))
}
