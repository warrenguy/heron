package sbe_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

func TestSessionMessageHeader_roundtrip(t *testing.T) {
	fields := sbe.SessionMessageHeaderFields{
		LeadershipTermID: 42,
		ClusterSessionID: 99,
		Timestamp:        123456789,
	}
	actingVersion := uint16(gensbe.SessionMessageHeaderSbeSchemaVersion)
	runCodecRoundtrip(
		t,
		gensbe.SessionMessageHeaderSbeTemplateID,
		actingVersion,
		func(buf []byte, offset int) (int, bool) {
			return encodeSessionMessageHeader(buf, offset, actingVersion, fields)
		},
		fields,
	)
}

func TestSessionEvent_roundtrip(t *testing.T) {
	actingVersion := uint16(gensbe.SessionEventSbeSchemaVersion)

	fieldCases := []struct {
		name      string
		actingVer uint16
		fields    sbe.SessionEventFields
	}{
		{
			name:      "empty_detail",
			actingVer: actingVersion,
			fields: sbe.SessionEventFields{
				ClusterSessionID:         1,
				CorrelationID:            2,
				LeadershipTermID:         3,
				LeaderMemberID:           4,
				Code:                     gensbe.EventCode_OK,
				LeaderHeartbeatTimeoutNs: 0,
			},
		},
		{
			name:      "error_with_detail",
			actingVer: actingVersion,
			fields: sbe.SessionEventFields{
				ClusterSessionID:         10,
				CorrelationID:            11,
				LeadershipTermID:         12,
				LeaderMemberID:           13,
				Code:                     gensbe.EventCode_ERROR,
				Detail:                   []byte("boom"),
				LeaderHeartbeatTimeoutNs: 0,
			},
		},
		{
			name:      "redirect_with_version",
			actingVer: 6,
			fields: sbe.SessionEventFields{
				ClusterSessionID:         20,
				CorrelationID:            21,
				LeadershipTermID:         22,
				LeaderMemberID:           23,
				Code:                     gensbe.EventCode_REDIRECT,
				Version:                  7,
				Detail:                   []byte("redirect"),
				LeaderHeartbeatTimeoutNs: math.MinInt64,
			},
		},
		{
			name:      "leader_heartbeat_present",
			actingVer: actingVersion,
			fields: sbe.SessionEventFields{
				ClusterSessionID:         30,
				CorrelationID:            31,
				LeadershipTermID:         32,
				LeaderMemberID:           33,
				Code:                     gensbe.EventCode_OK,
				LeaderHeartbeatTimeoutNs: 5_000_000_000,
				Detail:                   []byte("hb"),
			},
		},
		{
			name:      "leader_heartbeat_omitted_v13",
			actingVer: 13,
			fields: sbe.SessionEventFields{
				ClusterSessionID:         60,
				CorrelationID:            61,
				LeadershipTermID:         62,
				LeaderMemberID:           63,
				Code:                     gensbe.EventCode_OK,
				LeaderHeartbeatTimeoutNs: math.MinInt64, // encode omit sentinel; decode 0
				Detail:                   []byte("omit-hb"),
			},
		},
		{
			name:      "sessionEvent_actingVersion5_blockLength32",
			actingVer: 5,
			fields: sbe.SessionEventFields{
				ClusterSessionID:         40,
				CorrelationID:            41,
				LeadershipTermID:         42,
				LeaderMemberID:           43,
				Code:                     gensbe.EventCode_CLOSED,
				Version:                  0,
				LeaderHeartbeatTimeoutNs: math.MinInt64,
				Detail:                   []byte("legacy"),
			},
		},
	}

	cases := make([]codecRoundtripCase, 0, len(fieldCases)+5)
	for _, tc := range fieldCases {
		actingVer := tc.actingVer
		blockLength := sessionEventBlockLength(actingVer)
		fields := tc.fields
		cases = append(cases, codecRoundtripCase{
			name:      tc.name,
			actingVer: actingVer,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeSessionEvent(buf, offset, actingVer, blockLength, fields)
			},
			wantFields: sessionEventOracleFields(fields, actingVer),
		})
	}

	for _, code := range codecAllEventCodes {
		fields := sbe.SessionEventFields{
			ClusterSessionID:         50,
			CorrelationID:            51,
			LeadershipTermID:         52,
			LeaderMemberID:           53,
			Code:                     code,
			Detail:                   []byte("code"),
			LeaderHeartbeatTimeoutNs: 0,
		}
		cases = append(cases, codecRoundtripCase{
			name:      "event_code_" + code.String(),
			actingVer: actingVersion,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeSessionEvent(
					buf, offset, actingVersion, gensbe.SessionEventSbeBlockLength, fields,
				)
			},
			wantFields: fields,
		})
	}

	runCodecRoundtripCases(t, gensbe.SessionEventSbeTemplateID, actingVersion, cases)
}

func TestSessionConnectRequest_roundtrip(t *testing.T) {
	maxVar := bytes.Repeat([]byte{0x5A}, codecTestMaxVarLen)
	actingVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)

	fieldCases := []struct {
		name      string
		actingVer uint16
		fields    sbe.SessionConnectRequestFields
	}{
		{
			name:      "empty_vars",
			actingVer: actingVersion,
			fields: sbe.SessionConnectRequestFields{
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
			fields: sbe.SessionConnectRequestFields{
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
			fields: sbe.SessionConnectRequestFields{
				CorrelationID:    3,
				ResponseStreamID: 102,
				Version:          0,
			},
		},
		{
			name:      "clientInfo_absent_pre_v14",
			actingVer: 6,
			fields: sbe.SessionConnectRequestFields{
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
			fields: sbe.SessionConnectRequestFields{
				CorrelationID:      5,
				ResponseStreamID:   104,
				Version:            4,
				ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
				EncodedCredentials: []byte{9, 8, 7},
				ClientInfo:         []byte("heron-client"),
			},
		},
	}

	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		actingVer := tc.actingVer
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name:      tc.name,
			actingVer: actingVer,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeSessionConnectRequest(buf, offset, actingVer, fields)
			},
			wantFields: fields,
		}
	}

	runCodecRoundtripCases(t, gensbe.SessionConnectRequestSbeTemplateID, actingVersion, cases)
}

func TestSessionCloseRequest_roundtrip(t *testing.T) {
	fields := sbe.SessionCloseRequestFields{LeadershipTermID: 7, ClusterSessionID: 8}
	actingVersion := uint16(gensbe.SessionCloseRequestSbeSchemaVersion)
	runCodecRoundtrip(
		t,
		gensbe.SessionCloseRequestSbeTemplateID,
		actingVersion,
		func(buf []byte, offset int) (int, bool) {
			return encodeSessionCloseRequest(buf, offset, actingVersion, fields)
		},
		fields,
	)
}

func TestSessionKeepAlive_roundtrip(t *testing.T) {
	fields := sbe.SessionKeepAliveFields{LeadershipTermID: 9, ClusterSessionID: 10}
	actingVersion := uint16(gensbe.SessionKeepAliveSbeSchemaVersion)
	runCodecRoundtrip(
		t,
		gensbe.SessionKeepAliveSbeTemplateID,
		actingVersion,
		func(buf []byte, offset int) (int, bool) {
			return encodeSessionKeepAlive(buf, offset, actingVersion, fields)
		},
		fields,
	)
}

func TestNewLeaderEvent_roundtrip(t *testing.T) {
	actingVersion := uint16(gensbe.NewLeaderEventSbeSchemaVersion)
	fieldCases := []struct {
		name   string
		fields sbe.NewLeaderEventFields
	}{
		{
			name: "empty_ingress_endpoints",
			fields: sbe.NewLeaderEventFields{
				LeadershipTermID: 1,
				ClusterSessionID: 2,
				LeaderMemberID:   3,
			},
		},
		{
			name: "single_endpoint",
			fields: sbe.NewLeaderEventFields{
				LeadershipTermID: 4,
				ClusterSessionID: 5,
				LeaderMemberID:   6,
				IngressEndpoints: []byte("localhost:40123"),
			},
		},
	}

	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name: tc.name,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeNewLeaderEvent(buf, offset, actingVersion, fields)
			},
			wantFields: fields,
		}
	}

	runCodecRoundtripCases(t, gensbe.NewLeaderEventSbeTemplateID, actingVersion, cases)
}

func TestChallenge_roundtrip(t *testing.T) {
	actingVersion := uint16(gensbe.ChallengeSbeSchemaVersion)
	fieldCases := []struct {
		name   string
		fields sbe.ChallengeFields
	}{
		{
			name: "empty_challenge",
			fields: sbe.ChallengeFields{
				CorrelationID:    1,
				ClusterSessionID: 2,
				EncodedChallenge: nil,
			},
		},
		{
			name: "max_practical_challenge",
			fields: sbe.ChallengeFields{
				CorrelationID:    3,
				ClusterSessionID: 4,
				EncodedChallenge: bytes.Repeat([]byte{0x11}, codecTestMaxVarLen),
			},
		},
	}

	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name: tc.name,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeChallenge(buf, offset, actingVersion, fields)
			},
			wantFields: fields,
		}
	}

	runCodecRoundtripCases(t, gensbe.ChallengeSbeTemplateID, actingVersion, cases)
}

func TestChallengeResponse_roundtrip(t *testing.T) {
	actingVersion := uint16(gensbe.ChallengeResponseSbeSchemaVersion)
	fieldCases := []struct {
		name   string
		fields sbe.ChallengeResponseFields
	}{
		{
			name: "empty_credentials",
			fields: sbe.ChallengeResponseFields{
				CorrelationID:      1,
				ClusterSessionID:   2,
				EncodedCredentials: nil,
			},
		},
		{
			name: "max_practical_credentials",
			fields: sbe.ChallengeResponseFields{
				CorrelationID:      3,
				ClusterSessionID:   4,
				EncodedCredentials: bytes.Repeat([]byte{0x22}, codecTestMaxVarLen),
			},
		},
	}

	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name: tc.name,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeChallengeResponse(buf, offset, actingVersion, fields)
			},
			wantFields: fields,
		}
	}

	runCodecRoundtripCases(t, gensbe.ChallengeResponseSbeTemplateID, actingVersion, cases)
}

func TestAdminRequest_roundtrip(t *testing.T) {
	actingVersion := uint16(gensbe.AdminRequestSbeSchemaVersion)
	fieldCases := []struct {
		name   string
		fields sbe.AdminRequestFields
	}{
		{
			name: "empty_payload",
			fields: sbe.AdminRequestFields{
				LeadershipTermID: 1,
				ClusterSessionID: 2,
				CorrelationID:    3,
				RequestType:      gensbe.AdminRequestType_SNAPSHOT,
			},
		},
		{
			name: "payload_16_bytes",
			fields: sbe.AdminRequestFields{
				LeadershipTermID: 4,
				ClusterSessionID: 5,
				CorrelationID:    6,
				RequestType:      gensbe.AdminRequestType_SNAPSHOT,
				Payload:          bytes.Repeat([]byte{0x33}, 16),
			},
		},
	}

	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name: tc.name,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeAdminRequest(buf, offset, actingVersion, fields)
			},
			wantFields: fields,
		}
	}

	runCodecRoundtripCases(t, gensbe.AdminRequestSbeTemplateID, actingVersion, cases)
}

func TestAdminResponse_roundtrip(t *testing.T) {
	actingVersion := uint16(gensbe.AdminResponseSbeSchemaVersion)
	fieldCases := []struct {
		name   string
		fields sbe.AdminResponseFields
	}{
		{
			name: "ok_with_message",
			fields: sbe.AdminResponseFields{
				ClusterSessionID: 1,
				CorrelationID:    2,
				RequestType:      gensbe.AdminRequestType_SNAPSHOT,
				ResponseCode:     gensbe.AdminResponseCode_OK,
				Message:          []byte("done"),
			},
		},
		{
			name: "error_with_message_and_payload",
			fields: sbe.AdminResponseFields{
				ClusterSessionID: 3,
				CorrelationID:    4,
				RequestType:      gensbe.AdminRequestType_SNAPSHOT,
				ResponseCode:     gensbe.AdminResponseCode_ERROR,
				Message:          []byte("failed"),
				Payload:          []byte("detail"),
			},
		},
	}

	cases := make([]codecRoundtripCase, len(fieldCases))
	for i, tc := range fieldCases {
		fields := tc.fields
		cases[i] = codecRoundtripCase{
			name: tc.name,
			encode: func(buf []byte, offset int) (int, bool) {
				return encodeAdminResponse(buf, offset, actingVersion, fields)
			},
			wantFields: fields,
		}
	}

	runCodecRoundtripCases(t, gensbe.AdminResponseSbeTemplateID, actingVersion, cases)
}
