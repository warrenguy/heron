package codecs_test

import (
	"bytes"
	"testing"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Golden wire bytes for client session messages at actingVersion 17.
const (
	goldenSessionMessageHeaderHex = "180001006f001100" +
		"2a00000000000000630000000000000015cd5b0700000000"
	goldenSessionEventHex = "2c0002006f001100" +
		"010000000000000002000000000000000300000000000000" +
		"0400000000000000000000000000000000000000020000006f6b"
	goldenSessionConnectHex = "100003006f0011006400000000000000c800000001000000" +
		"220000006165726f6e3a7564703f656e64706f696e743d6c6f63616c686f73743a3430313233" +
		"20000000abababababababababababababababababababababababababababababababab" +
		"09000000676f2d636c69656e74"
	goldenSessionCloseHex     = "100004006f00110005000000000000000600000000000000"
	goldenSessionKeepAliveHex = "100005006f00110007000000000000000800000000000000"
	goldenNewLeaderEventHex   = "140006006f00110001000000000000000200000000000000" +
		"030000000f0000006c6f63616c686f73743a3430313233"
	goldenChallengeHex = "100007006f0011000a000000000000000b0000000000000020000000" +
		"cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	goldenChallengeResponseHex = "100008006f0011000c000000000000000d0000000000000020000000" +
		"efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"
	goldenAdminRequestHex = "1c001a006f001100" +
		"0100000000000000020000000000000003000000000000000000000000000000"
	goldenAdminResponseHex = "18001b006f00110004000000000000000500000000000000" +
		"000000000000000011000000736e617073686f7420636f6d706c65746500000000"
)

func TestClientWireBytes_unchangedFromBaseline(t *testing.T) {
	actingVersion := uint16(clusterwire.SessionMessageHeaderSbeSchemaVersion)
	runGoldenHexCases(t, actingVersion, []goldenHexCase{
		{
			name:    "SessionMessageHeader",
			wantHex: goldenSessionMessageHeaderHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionMessageHeader(b, 0, actingVersion, codecs.SessionMessageHeaderFields{
					LeadershipTermID: 42, ClusterSessionID: 99, Timestamp: 123456789,
				})
			},
			templateID: clusterwire.SessionMessageHeaderSbeTemplateID,
		},
		{
			name:    "SessionEvent",
			wantHex: goldenSessionEventHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionEvent(
					b, 0, actingVersion, codecs.SessionEventBlockLength(actingVersion), codecs.SessionEventFields{
						ClusterSessionID: 1, CorrelationID: 2, LeadershipTermID: 3, LeaderMemberID: 4,
						Code: clusterwire.EventCode_OK, Detail: []byte("ok"), LeaderHeartbeatTimeoutNs: 0,
					},
				)
			},
			templateID: clusterwire.SessionEventSbeTemplateID,
		},
		{
			name:    "SessionConnectRequest",
			wantHex: goldenSessionConnectHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionConnectRequest(
					b, 0, actingVersion, codecs.SessionConnectRequestFields{
						CorrelationID: 100, ResponseStreamID: 200, Version: 1,
						ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
						EncodedCredentials: bytes.Repeat([]byte{0xAB}, 32),
						ClientInfo:         []byte("go-client"),
					},
				)
			},
			templateID: clusterwire.SessionConnectRequestSbeTemplateID,
		},
		{
			name:    "SessionCloseRequest",
			wantHex: goldenSessionCloseHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionCloseRequest(b, 0, actingVersion, codecs.SessionCloseRequestFields{
					LeadershipTermID: 5, ClusterSessionID: 6,
				})
			},
			templateID: clusterwire.SessionCloseRequestSbeTemplateID,
		},
		{
			name:    "SessionKeepAlive",
			wantHex: goldenSessionKeepAliveHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeSessionKeepAlive(b, 0, actingVersion, codecs.SessionKeepAliveFields{
					LeadershipTermID: 7, ClusterSessionID: 8,
				})
			},
			templateID: clusterwire.SessionKeepAliveSbeTemplateID,
		},
		{
			name:    "NewLeaderEvent",
			wantHex: goldenNewLeaderEventHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeNewLeaderEvent(b, 0, actingVersion, codecs.NewLeaderEventFields{
					LeadershipTermID: 1, ClusterSessionID: 2, LeaderMemberID: 3,
					IngressEndpoints: []byte("localhost:40123"),
				})
			},
			templateID: clusterwire.NewLeaderEventSbeTemplateID,
		},
		{
			name:    "Challenge",
			wantHex: goldenChallengeHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeChallenge(b, 0, actingVersion, codecs.ChallengeFields{
					CorrelationID: 10, ClusterSessionID: 11, EncodedChallenge: bytes.Repeat([]byte{0xCD}, 32),
				})
			},
			templateID: clusterwire.ChallengeSbeTemplateID,
		},
		{
			name:    "ChallengeResponse",
			wantHex: goldenChallengeResponseHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeChallengeResponse(b, 0, actingVersion, codecs.ChallengeResponseFields{
					CorrelationID: 12, ClusterSessionID: 13, EncodedCredentials: bytes.Repeat([]byte{0xEF}, 32),
				})
			},
			templateID: clusterwire.ChallengeResponseSbeTemplateID,
		},
		{
			name:    "AdminRequest",
			wantHex: goldenAdminRequestHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeAdminRequest(b, 0, actingVersion, codecs.AdminRequestFields{
					LeadershipTermID: 1, ClusterSessionID: 2, CorrelationID: 3,
					RequestType: clusterwire.AdminRequestType_SNAPSHOT,
				})
			},
			templateID: clusterwire.AdminRequestSbeTemplateID,
		},
		{
			name:    "AdminResponse",
			wantHex: goldenAdminResponseHex,
			encode: func(b []byte) (int, bool) {
				return codecs.EncodeAdminResponse(b, 0, actingVersion, codecs.AdminResponseFields{
					ClusterSessionID: 4, CorrelationID: 5, RequestType: clusterwire.AdminRequestType_SNAPSHOT,
					ResponseCode: clusterwire.AdminResponseCode_OK, Message: []byte("snapshot complete"),
				})
			},
			templateID: clusterwire.AdminResponseSbeTemplateID,
		},
	})
}
