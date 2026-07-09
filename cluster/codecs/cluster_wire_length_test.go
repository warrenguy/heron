package codecs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

type clusterWireOracleCase struct {
	name          string
	templateID    uint16
	oracleOnly    bool // excluded from worst-case template sweep
	blockLength   uint16
	actingVersion uint16
	varLens       []int
	encodeBody    func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int
}

var clusterWireOracleCasesList = clusterWireOracleCases()

func clusterVarLenWireBudget(blockLength uint16) int {
	return logbuffer.MaxMessageLength - MessageHeaderEncodedLen() - int(blockLength)
}

func clusterEncodeMaxVarLensWithValue(count, perField int) []int {
	varLens := make([]int, count)
	for i := range varLens {
		varLens[i] = perField
	}
	return varLens
}

func flyweightBufLens(
	buf []byte,
	bodyOffset int,
	blockLength, actingVersion uint16,
) (uint64, uint64, uint64, uint64) {
	return uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)
}

func clusterWireOracleCases() []clusterWireOracleCase {
	twoVarConnectVersion := SessionConnectClientInfoMinVersion - 1
	threeVarConnectVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)

	return []clusterWireOracleCase{
		{
			name:          "session_message_header",
			templateID:    clusterwire.SessionMessageHeaderSbeTemplateID,
			blockLength:   clusterwire.SessionMessageHeaderSbeBlockLength,
			actingVersion: clusterwire.SessionMessageHeaderSbeSchemaVersion,
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionMessageHeader
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2).
					SetTimestamp(3)
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "session_event",
			templateID:    clusterwire.SessionEventSbeTemplateID,
			blockLength:   SessionEventBlockLength(clusterwire.SessionEventSbeSchemaVersion),
			actingVersion: clusterwire.SessionEventSbeSchemaVersion,
			varLens:       []int{5},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionEvent
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetClusterSessionId(1).
					SetCorrelationId(2).
					SetLeadershipTermId(3).
					SetLeaderMemberId(4).
					SetCode(clusterwire.EventCode_OK)
				msg.PutDetail("hello")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "session_event_acting_v5_block32",
			oracleOnly:    true,
			blockLength:   32,
			actingVersion: 5,
			varLens:       []int{9},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionEvent
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetClusterSessionId(40).
					SetCorrelationId(41).
					SetLeadershipTermId(42).
					SetLeaderMemberId(43).
					SetCode(clusterwire.EventCode_CLOSED)
				msg.PutDetail("detail-v5")
				return int(msg.EncodedLength())
			},
		},
		{
			name:       "session_connect_request_two_var",
			templateID: clusterwire.SessionConnectRequestSbeTemplateID,
			blockLength: ClusterMinRootBlockLength(
				clusterwire.SessionConnectRequestSbeTemplateID,
				twoVarConnectVersion,
				clusterwire.SessionConnectRequestSbeBlockLength,
			),
			actingVersion: twoVarConnectVersion,
			varLens:       []int{9, 3},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionConnectRequest
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetCorrelationId(1).
					SetResponseStreamId(2)
				msg.PutResponseChannel("aeron:ipc")
				msg.PutEncodedCredentials("abc")
				return int(msg.EncodedLength())
			},
		},
		{
			name:       "session_connect_request_three_var",
			templateID: clusterwire.SessionConnectRequestSbeTemplateID,
			blockLength: ClusterMinRootBlockLength(
				clusterwire.SessionConnectRequestSbeTemplateID,
				threeVarConnectVersion,
				clusterwire.SessionConnectRequestSbeBlockLength,
			),
			actingVersion: threeVarConnectVersion,
			varLens:       []int{9, 3, 6},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionConnectRequest
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetCorrelationId(1).
					SetResponseStreamId(2)
				msg.PutResponseChannel("aeron:ipc")
				msg.PutEncodedCredentials("abc")
				msg.PutClientInfo("client")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "session_close_request",
			templateID:    clusterwire.SessionCloseRequestSbeTemplateID,
			blockLength:   clusterwire.SessionCloseRequestSbeBlockLength,
			actingVersion: clusterwire.SessionCloseRequestSbeSchemaVersion,
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionCloseRequest
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2)
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "session_keep_alive",
			templateID:    clusterwire.SessionKeepAliveSbeTemplateID,
			blockLength:   clusterwire.SessionKeepAliveSbeBlockLength,
			actingVersion: clusterwire.SessionKeepAliveSbeSchemaVersion,
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.SessionKeepAlive
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2)
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "new_leader_event",
			templateID:    clusterwire.NewLeaderEventSbeTemplateID,
			blockLength:   clusterwire.NewLeaderEventSbeBlockLength,
			actingVersion: clusterwire.NewLeaderEventSbeSchemaVersion,
			varLens:       []int{15},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.NewLeaderEvent
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2).
					SetLeaderMemberId(3)
				msg.PutIngressEndpoints("localhost:40123")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "challenge",
			templateID:    clusterwire.ChallengeSbeTemplateID,
			blockLength:   clusterwire.ChallengeSbeBlockLength,
			actingVersion: clusterwire.ChallengeSbeSchemaVersion,
			varLens:       []int{4},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.Challenge
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetCorrelationId(1).
					SetClusterSessionId(2)
				msg.PutEncodedChallenge("abcd")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "challenge_response",
			templateID:    clusterwire.ChallengeResponseSbeTemplateID,
			blockLength:   clusterwire.ChallengeResponseSbeBlockLength,
			actingVersion: clusterwire.ChallengeResponseSbeSchemaVersion,
			varLens:       []int{4},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.ChallengeResponse
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetCorrelationId(1).
					SetClusterSessionId(2)
				msg.PutEncodedCredentials("abcd")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "admin_request",
			templateID:    clusterwire.AdminRequestSbeTemplateID,
			blockLength:   clusterwire.AdminRequestSbeBlockLength,
			actingVersion: clusterwire.AdminRequestSbeSchemaVersion,
			varLens:       []int{7},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.AdminRequest
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2).
					SetCorrelationId(3).
					SetRequestType(clusterwire.AdminRequestType_SNAPSHOT)
				msg.PutPayload("payload")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "admin_response",
			templateID:    clusterwire.AdminResponseSbeTemplateID,
			blockLength:   clusterwire.AdminResponseSbeBlockLength,
			actingVersion: clusterwire.AdminResponseSbeSchemaVersion,
			varLens:       []int{7, 7},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg clusterwire.AdminResponse
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetClusterSessionId(1).
					SetCorrelationId(2).
					SetRequestType(clusterwire.AdminRequestType_SNAPSHOT).
					SetResponseCode(clusterwire.AdminResponseCode_OK)
				msg.PutMessage("message")
				msg.PutPayload("payload")
				return int(msg.EncodedLength())
			},
		},
	}
}

func runClusterWireOracleCase(t *testing.T, tc clusterWireOracleCase) {
	t.Helper()
	buf := make([]byte, 256)
	want := tc.encodeBody(buf, 0, tc.blockLength, tc.actingVersion)
	got, ok := clusterWireBodyLen(tc.blockLength, tc.varLens...)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func requireClusterWireAccepts(
	t *testing.T,
	blockLength uint16,
	varLens ...int,
) (bodyLen, wireLen int) {
	t.Helper()
	bodyLen, ok := clusterWireBodyLen(blockLength, varLens...)
	require.True(t, ok)

	wireLen, ok = ClusterWireMessageLen(blockLength, varLens...)
	require.True(t, ok)
	require.Equal(t, MessageHeaderEncodedLen()+bodyLen, wireLen)
	return bodyLen, wireLen
}

func TestClusterWireBodyLen_matchesEncodedLength(t *testing.T) {
	for _, tc := range clusterWireOracleCasesList {
		t.Run(tc.name, func(t *testing.T) {
			runClusterWireOracleCase(t, tc)
		})
	}

	t.Run("cluster_wire_message_len_is_header_plus_body", func(t *testing.T) {
		headerCase := clusterWireOracleCaseByName("session_message_header")
		requireClusterWireAccepts(t, headerCase.blockLength, headerCase.varLens...)
	})
}

func clusterWireOracleCaseByName(name string) clusterWireOracleCase {
	for _, tc := range clusterWireOracleCasesList {
		if tc.name == name {
			return tc
		}
	}
	panic("missing cluster wire oracle case: " + name)
}

func syntheticVarLensOverMaxMessageLength(blockLength uint16) []int {
	perField := VarLengthPrefixSize + ClusterEncodeMaxVarLen
	fieldCount := clusterVarLenWireBudget(blockLength)/perField + 1
	return clusterEncodeMaxVarLensWithValue(fieldCount, ClusterEncodeMaxVarLen)
}

func requireClusterWireRejects(
	t *testing.T,
	blockLength uint16,
	varLens ...int,
) {
	t.Helper()
	_, ok := clusterWireBodyLen(blockLength, varLens...)
	require.False(t, ok)

	wireLen, ok := ClusterWireMessageLen(blockLength, varLens...)
	require.False(t, ok)
	require.Zero(t, wireLen)
}

func requireSessionConnectRequestWireRejects(
	t *testing.T,
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) {
	t.Helper()
	wireLen, ok := SessionConnectRequestWireLen(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	require.False(t, ok)
	require.Zero(t, wireLen)
}

func requireSessionConnectRequestVarLensIntoRejects(
	t *testing.T,
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
	dst []int,
) {
	t.Helper()
	count, ok := sessionConnectRequestVarLensInto(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
		dst,
	)
	require.False(t, ok)
	require.Zero(t, count)
}

func TestClusterVarLenConstants_matchReassemblyBudget(t *testing.T) {
	wantFixedOverhead := MessageHeaderEncodedLen() +
		int(clusterWireMaxRootBlockLength) +
		clusterWireMaxVarFieldCount*VarLengthPrefixSize
	require.Equal(t, logbuffer.MaxMessageLength-wantFixedOverhead, ClusterEncodeMaxVarLen)
	require.Equal(t, ClusterEncodeMaxVarLen, ClusterDecodeMaxVarLen)
}

func TestClusterWireMetadataConstants_matchTemplateExtrema(t *testing.T) {
	wireTemplates := []struct {
		templateID uint16
		version    uint16
	}{
		{clusterwire.SessionMessageHeaderSbeTemplateID, clusterwire.SessionMessageHeaderSbeSchemaVersion},
		{clusterwire.SessionEventSbeTemplateID, clusterwire.SessionEventSbeSchemaVersion},
		{
			clusterwire.SessionConnectRequestSbeTemplateID,
			clusterwire.SessionConnectRequestSbeSchemaVersion,
		},
		{clusterwire.SessionCloseRequestSbeTemplateID, clusterwire.SessionCloseRequestSbeSchemaVersion},
		{clusterwire.SessionKeepAliveSbeTemplateID, clusterwire.SessionKeepAliveSbeSchemaVersion},
		{clusterwire.NewLeaderEventSbeTemplateID, clusterwire.NewLeaderEventSbeSchemaVersion},
		{clusterwire.ChallengeSbeTemplateID, clusterwire.ChallengeSbeSchemaVersion},
		{clusterwire.ChallengeResponseSbeTemplateID, clusterwire.ChallengeResponseSbeSchemaVersion},
		{clusterwire.AdminRequestSbeTemplateID, clusterwire.AdminRequestSbeSchemaVersion},
		{clusterwire.AdminResponseSbeTemplateID, clusterwire.AdminResponseSbeSchemaVersion},
	}

	var maxRoot uint16
	var maxVarCount int
	for _, tmpl := range wireTemplates {
		_, maxBlock, ok := ClusterRootBlockLengthBounds(tmpl.templateID, tmpl.version)
		require.True(t, ok, "templateID=%d", tmpl.templateID)
		maxRoot = max(maxRoot, maxBlock)

		varCount, ok := ClusterVarFieldCount(tmpl.templateID, tmpl.version)
		require.True(t, ok, "templateID=%d", tmpl.templateID)
		maxVarCount = max(maxVarCount, varCount)
	}

	require.Equal(t, clusterWireMaxRootBlockLength, maxRoot)
	require.Equal(t, clusterWireMaxVarFieldCount, maxVarCount)
}

func TestSessionConnectRequestMaxVarFieldCount_matchesOracle(t *testing.T) {
	count, ok := ClusterVarFieldCount(
		clusterwire.SessionConnectRequestSbeTemplateID,
		uint16(clusterwire.SessionConnectRequestSbeSchemaVersion),
	)
	require.True(t, ok)
	require.Equal(t, sessionConnectRequestMaxVarFieldCount, count)
}

func TestSessionConnectRequestWireLen_rejectsInvalidVarLens(t *testing.T) {
	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)

	tests := []struct {
		name                  string
		responseChannelLen    int
		encodedCredentialsLen int
		clientInfoLen         int
	}{
		{name: "negative_response_channel_len", responseChannelLen: -1},
		{name: "negative_encoded_credentials_len", encodedCredentialsLen: -1},
		{name: "negative_client_info_len", clientInfoLen: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireSessionConnectRequestWireRejects(
				t,
				actingVersion,
				tc.responseChannelLen,
				tc.encodedCredentialsLen,
				tc.clientInfoLen,
			)
		})
	}
}

func TestSessionConnectRequestVarLensInto_rejectsInsufficientDst(t *testing.T) {
	twoVarConnectVersion := SessionConnectClientInfoMinVersion - 1
	threeVarConnectVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)

	tests := []struct {
		name          string
		actingVersion uint16
		dstLen        int
	}{
		{
			name:          "two_var_fields_need_two_slots",
			actingVersion: twoVarConnectVersion,
			dstLen:        1,
		},
		{
			name:          "three_var_fields_need_three_slots",
			actingVersion: threeVarConnectVersion,
			dstLen:        2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]int, tc.dstLen)
			requireSessionConnectRequestVarLensIntoRejects(
				t, tc.actingVersion, 9, 0, 20, dst,
			)
		})
	}
}

func TestClusterWireBodyLen_rejectsInvalidLengths(t *testing.T) {
	blockLength := clusterwire.SessionEventSbeBlockLength

	t.Run("negative_var_len", func(t *testing.T) {
		requireClusterWireRejects(t, blockLength, -1)
	})

	t.Run("var_len_over_encode_max", func(t *testing.T) {
		requireClusterWireRejects(t, blockLength, ClusterEncodeMaxVarLen+1)
	})

	t.Run("total_wire_over_max_message_length", func(t *testing.T) {
		requireClusterWireRejects(t, blockLength, syntheticVarLensOverMaxMessageLength(blockLength)...)
	})

	t.Run("at_encode_max_var_len_ok", func(t *testing.T) {
		bodyLen, _ := requireClusterWireAccepts(t, blockLength, ClusterEncodeMaxVarLen)
		require.Equal(t, int(blockLength)+VarLengthPrefixSize+ClusterEncodeMaxVarLen, bodyLen)
		require.LessOrEqual(t, MessageHeaderEncodedLen()+bodyLen, logbuffer.MaxMessageLength)
	})
}
