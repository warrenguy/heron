package codecs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
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
	threeVarConnectVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)

	return []clusterWireOracleCase{
		{
			name:          "session_message_header",
			templateID:    gensbe.SessionMessageHeaderSbeTemplateID,
			blockLength:   gensbe.SessionMessageHeaderSbeBlockLength,
			actingVersion: gensbe.SessionMessageHeaderSbeSchemaVersion,
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.SessionMessageHeader
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
			templateID:    gensbe.SessionEventSbeTemplateID,
			blockLength:   SessionEventBlockLength(gensbe.SessionEventSbeSchemaVersion),
			actingVersion: gensbe.SessionEventSbeSchemaVersion,
			varLens:       []int{5},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.SessionEvent
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetClusterSessionId(1).
					SetCorrelationId(2).
					SetLeadershipTermId(3).
					SetLeaderMemberId(4).
					SetCode(gensbe.EventCode_OK)
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
				var msg gensbe.SessionEvent
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetClusterSessionId(40).
					SetCorrelationId(41).
					SetLeadershipTermId(42).
					SetLeaderMemberId(43).
					SetCode(gensbe.EventCode_CLOSED)
				msg.PutDetail("detail-v5")
				return int(msg.EncodedLength())
			},
		},
		{
			name:       "session_connect_request_two_var",
			templateID: gensbe.SessionConnectRequestSbeTemplateID,
			blockLength: ClusterMinRootBlockLength(
				gensbe.SessionConnectRequestSbeTemplateID,
				twoVarConnectVersion,
				gensbe.SessionConnectRequestSbeBlockLength,
			),
			actingVersion: twoVarConnectVersion,
			varLens:       []int{9, 3},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.SessionConnectRequest
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
			templateID: gensbe.SessionConnectRequestSbeTemplateID,
			blockLength: ClusterMinRootBlockLength(
				gensbe.SessionConnectRequestSbeTemplateID,
				threeVarConnectVersion,
				gensbe.SessionConnectRequestSbeBlockLength,
			),
			actingVersion: threeVarConnectVersion,
			varLens:       []int{9, 3, 6},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.SessionConnectRequest
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
			templateID:    gensbe.SessionCloseRequestSbeTemplateID,
			blockLength:   gensbe.SessionCloseRequestSbeBlockLength,
			actingVersion: gensbe.SessionCloseRequestSbeSchemaVersion,
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.SessionCloseRequest
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2)
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "session_keep_alive",
			templateID:    gensbe.SessionKeepAliveSbeTemplateID,
			blockLength:   gensbe.SessionKeepAliveSbeBlockLength,
			actingVersion: gensbe.SessionKeepAliveSbeSchemaVersion,
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.SessionKeepAlive
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2)
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "new_leader_event",
			templateID:    gensbe.NewLeaderEventSbeTemplateID,
			blockLength:   gensbe.NewLeaderEventSbeBlockLength,
			actingVersion: gensbe.NewLeaderEventSbeSchemaVersion,
			varLens:       []int{15},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.NewLeaderEvent
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
			templateID:    gensbe.ChallengeSbeTemplateID,
			blockLength:   gensbe.ChallengeSbeBlockLength,
			actingVersion: gensbe.ChallengeSbeSchemaVersion,
			varLens:       []int{4},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.Challenge
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
			templateID:    gensbe.ChallengeResponseSbeTemplateID,
			blockLength:   gensbe.ChallengeResponseSbeBlockLength,
			actingVersion: gensbe.ChallengeResponseSbeSchemaVersion,
			varLens:       []int{4},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.ChallengeResponse
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
			templateID:    gensbe.AdminRequestSbeTemplateID,
			blockLength:   gensbe.AdminRequestSbeBlockLength,
			actingVersion: gensbe.AdminRequestSbeSchemaVersion,
			varLens:       []int{7},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.AdminRequest
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetLeadershipTermId(1).
					SetClusterSessionId(2).
					SetCorrelationId(3).
					SetRequestType(gensbe.AdminRequestType_SNAPSHOT)
				msg.PutPayload("payload")
				return int(msg.EncodedLength())
			},
		},
		{
			name:          "admin_response",
			templateID:    gensbe.AdminResponseSbeTemplateID,
			blockLength:   gensbe.AdminResponseSbeBlockLength,
			actingVersion: gensbe.AdminResponseSbeSchemaVersion,
			varLens:       []int{7, 7},
			encodeBody: func(buf []byte, bodyOffset int, blockLength, actingVersion uint16) int {
				var msg gensbe.AdminResponse
				o, l, bl, av := flyweightBufLens(buf, bodyOffset, blockLength, actingVersion)
				msg.Wrap(buf, o, l, bl, av).
					SetClusterSessionId(1).
					SetCorrelationId(2).
					SetRequestType(gensbe.AdminRequestType_SNAPSHOT).
					SetResponseCode(gensbe.AdminResponseCode_OK)
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
		{gensbe.SessionMessageHeaderSbeTemplateID, gensbe.SessionMessageHeaderSbeSchemaVersion},
		{gensbe.SessionEventSbeTemplateID, gensbe.SessionEventSbeSchemaVersion},
		{gensbe.SessionConnectRequestSbeTemplateID, gensbe.SessionConnectRequestSbeSchemaVersion},
		{gensbe.SessionCloseRequestSbeTemplateID, gensbe.SessionCloseRequestSbeSchemaVersion},
		{gensbe.SessionKeepAliveSbeTemplateID, gensbe.SessionKeepAliveSbeSchemaVersion},
		{gensbe.NewLeaderEventSbeTemplateID, gensbe.NewLeaderEventSbeSchemaVersion},
		{gensbe.ChallengeSbeTemplateID, gensbe.ChallengeSbeSchemaVersion},
		{gensbe.ChallengeResponseSbeTemplateID, gensbe.ChallengeResponseSbeSchemaVersion},
		{gensbe.AdminRequestSbeTemplateID, gensbe.AdminRequestSbeSchemaVersion},
		{gensbe.AdminResponseSbeTemplateID, gensbe.AdminResponseSbeSchemaVersion},
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

func TestClusterWireBodyLen_rejectsInvalidLengths(t *testing.T) {
	blockLength := gensbe.SessionEventSbeBlockLength

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
