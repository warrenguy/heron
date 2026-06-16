package sbe_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

const (
	codecTestMaxVarLen     = 256
	fuzzClusterCodecMaxLen = 4096
	codecFuzzMaxOffset     = 8
	codecFuzzOffsetSpan    = codecFuzzMaxOffset + 1
	clusterSchemaID        = gensbe.MessageHeaderSbeSchemaID
	messageHeaderLen       = int(gensbe.MessageHeaderEncodedLength)
)

var (
	codecAllEventCodes = []gensbe.EventCode{
		gensbe.EventCode_OK,
		gensbe.EventCode_ERROR,
		gensbe.EventCode_REDIRECT,
		gensbe.EventCode_AUTHENTICATION_REJECTED,
		gensbe.EventCode_CLOSED,
	}
	codecAllAdminResponseCodes = []gensbe.AdminResponseCode{
		gensbe.AdminResponseCode_OK,
		gensbe.AdminResponseCode_ERROR,
		gensbe.AdminResponseCode_UNAUTHORISED_ACCESS,
	}
)

var codecRoundtripOffsetValues = []int{0, 4}

func applyClusterMessageHeader(
	buf []byte,
	offset int,
	blockLength, templateID, actingVersion uint16,
) (bodyOffset int, ok bool) {
	if offset < 0 || offset > len(buf)-messageHeaderLen {
		return 0, false
	}
	binary.LittleEndian.PutUint16(buf[offset:], blockLength)
	binary.LittleEndian.PutUint16(buf[offset+2:], templateID)
	binary.LittleEndian.PutUint16(buf[offset+4:], clusterSchemaID)
	binary.LittleEndian.PutUint16(buf[offset+6:], actingVersion)
	return offset + messageHeaderLen, true
}

func encodeWithClusterHeader(
	buf []byte,
	offset int,
	blockLength, templateID, actingVersion uint16,
	encodeBody func(bodyOffset int, blockLength, actingVersion uint16) int,
) (wireEnd int, ok bool) {
	bodyOffset, ok := applyClusterMessageHeader(buf, offset, blockLength, templateID, actingVersion)
	if !ok {
		return 0, false
	}
	return offset + messageHeaderLen + encodeBody(bodyOffset, blockLength, actingVersion), true
}

func encodeLeadershipSessionMessage(
	buf []byte,
	offset int,
	actingVersion, blockLength uint16,
	leadershipTermID, clusterSessionID int64,
	encode func(buf []byte, offset, limit int, leadershipTermID, clusterSessionID int64) int,
) (wireEnd int, ok bool) {
	wireLen := encode(buf, offset, len(buf), leadershipTermID, clusterSessionID)
	patchClusterMessageHeader(buf, offset, blockLength, actingVersion)
	return offset + wireLen, true
}

func encodeSessionMessageHeader(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.SessionMessageHeaderFields,
) (wireEnd int, ok bool) {
	var msg gensbe.SessionMessageHeader
	msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
		SetLeadershipTermId(fields.LeadershipTermID).
		SetClusterSessionId(fields.ClusterSessionID).
		SetTimestamp(fields.Timestamp)
	patchClusterMessageHeader(buf, offset, gensbe.SessionMessageHeaderSbeBlockLength, actingVersion)
	return offset + int(msg.SbeBlockAndHeaderLength()), true
}

func encodeSessionEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	blockLength uint16,
	fields sbe.SessionEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterHeader(
		buf,
		offset,
		blockLength,
		gensbe.SessionEventSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionEvent
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetLeaderMemberId(fields.LeaderMemberID).
				SetCode(fields.Code)
			if actingVersion >= 6 && fields.Version != 0 {
				msg.SetVersion(fields.Version)
			}
			if actingVersion >= 13 && fields.LeaderHeartbeatTimeoutNs != math.MinInt64 {
				msg.SetLeaderHeartbeatTimeoutNs(fields.LeaderHeartbeatTimeoutNs)
			}
			putVarBytes(func(s string, l uint32) { msg.PutDetailLen(s, l) }, fields.Detail)
			return int(msg.EncodedLength())
		},
	)
}

// sessionEventOracleFields maps encode input to expected decode output.
// math.MinInt64 is the encode omit sentinel at actingVersion >= 13 (decodes as 0).
func sessionEventOracleFields(
	fields sbe.SessionEventFields,
	actingVersion uint16,
) sbe.SessionEventFields {
	if actingVersion < 13 {
		fields.LeaderHeartbeatTimeoutNs = math.MinInt64
		return fields
	}
	if fields.LeaderHeartbeatTimeoutNs == math.MinInt64 {
		fields.LeaderHeartbeatTimeoutNs = 0
	}
	return fields
}

func sessionEventBlockLength(actingVersion uint16) uint16 {
	minBlock, _, ok := sbe.ClusterRootBlockLengthBounds(
		gensbe.SessionEventSbeTemplateID,
		actingVersion,
	)
	if !ok {
		return gensbe.SessionEventSbeBlockLength
	}
	return minBlock
}

func encodeSessionConnectRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.SessionConnectRequestFields,
) (wireEnd int, ok bool) {
	blockLength, _, ok := sbe.ClusterRootBlockLengthBounds(
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
	)
	if !ok {
		blockLength = gensbe.SessionConnectRequestSbeBlockLength
	}
	return encodeWithClusterHeader(
		buf,
		offset,
		blockLength,
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionConnectRequest
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetResponseStreamId(fields.ResponseStreamID)
			if actingVersion >= 2 && fields.Version != 0 {
				msg.SetVersion(fields.Version)
			}
			putVarBytes(func(s string, l uint32) { msg.PutResponseChannelLen(s, l) }, fields.ResponseChannel)
			putVarBytes(
				func(s string, l uint32) { msg.PutEncodedCredentialsLen(s, l) },
				fields.EncodedCredentials,
			)
			if actingVersion >= 14 {
				putVarBytes(func(s string, l uint32) { msg.PutClientInfoLen(s, l) }, fields.ClientInfo)
			}
			return int(msg.EncodedLength())
		},
	)
}

func encodeSessionCloseRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.SessionCloseRequestFields,
) (wireEnd int, ok bool) {
	return encodeLeadershipSessionMessage(
		buf,
		offset,
		actingVersion,
		gensbe.SessionCloseRequestSbeBlockLength,
		fields.LeadershipTermID,
		fields.ClusterSessionID,
		func(buf []byte, offset, limit int, leadershipTermID, clusterSessionID int64) int {
			var msg gensbe.SessionCloseRequest
			msg.WrapAndApplyHeader(buf, uint64(offset), uint64(limit)).
				SetLeadershipTermId(leadershipTermID).
				SetClusterSessionId(clusterSessionID)
			return int(msg.SbeBlockAndHeaderLength())
		},
	)
}

func encodeSessionKeepAlive(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.SessionKeepAliveFields,
) (wireEnd int, ok bool) {
	return encodeLeadershipSessionMessage(
		buf,
		offset,
		actingVersion,
		gensbe.SessionKeepAliveSbeBlockLength,
		fields.LeadershipTermID,
		fields.ClusterSessionID,
		func(buf []byte, offset, limit int, leadershipTermID, clusterSessionID int64) int {
			var msg gensbe.SessionKeepAlive
			msg.WrapAndApplyHeader(buf, uint64(offset), uint64(limit)).
				SetLeadershipTermId(leadershipTermID).
				SetClusterSessionId(clusterSessionID)
			return int(msg.SbeBlockAndHeaderLength())
		},
	)
}

func encodeNewLeaderEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.NewLeaderEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterHeader(
		buf,
		offset,
		gensbe.NewLeaderEventSbeBlockLength,
		gensbe.NewLeaderEventSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.NewLeaderEvent
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetLeaderMemberId(fields.LeaderMemberID)
			putVarBytes(
				func(s string, l uint32) { msg.PutIngressEndpointsLen(s, l) },
				fields.IngressEndpoints,
			)
			return int(msg.EncodedLength())
		},
	)
}

func encodeChallenge(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.ChallengeFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterHeader(
		buf,
		offset,
		gensbe.ChallengeSbeBlockLength,
		gensbe.ChallengeSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.Challenge
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID)
			putVarBytes(
				func(s string, l uint32) { msg.PutEncodedChallengeLen(s, l) },
				fields.EncodedChallenge,
			)
			return int(msg.EncodedLength())
		},
	)
}

func encodeChallengeResponse(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.ChallengeResponseFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterHeader(
		buf,
		offset,
		gensbe.ChallengeResponseSbeBlockLength,
		gensbe.ChallengeResponseSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.ChallengeResponse
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID)
			putVarBytes(
				func(s string, l uint32) { msg.PutEncodedCredentialsLen(s, l) },
				fields.EncodedCredentials,
			)
			return int(msg.EncodedLength())
		},
	)
}

func encodeAdminRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.AdminRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterHeader(
		buf,
		offset,
		gensbe.AdminRequestSbeBlockLength,
		gensbe.AdminRequestSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.AdminRequest
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRequestType(fields.RequestType)
			putVarBytes(func(s string, l uint32) { msg.PutPayloadLen(s, l) }, fields.Payload)
			return int(msg.EncodedLength())
		},
	)
}

func encodeAdminResponse(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.AdminResponseFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterHeader(
		buf,
		offset,
		gensbe.AdminResponseSbeBlockLength,
		gensbe.AdminResponseSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.AdminResponse
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRequestType(fields.RequestType).
				SetResponseCode(fields.ResponseCode)
			putVarBytes(func(s string, l uint32) { msg.PutMessageLen(s, l) }, fields.Message)
			putVarBytes(func(s string, l uint32) { msg.PutPayloadLen(s, l) }, fields.Payload)
			return int(msg.EncodedLength())
		},
	)
}

func patchClusterMessageHeader(buf []byte, offset int, blockLength, actingVersion uint16) {
	binary.LittleEndian.PutUint16(buf[offset:], blockLength)
	binary.LittleEndian.PutUint16(buf[offset+6:], actingVersion)
}

func requireBytesEqual(tb require.TestingT, want, got []byte) {
	if len(want) == 0 && len(got) == 0 {
		return
	}
	require.Equal(tb, want, got)
}

func putVarBytes(put func(string, uint32), data []byte) {
	if len(data) == 0 {
		put("", 0)
		return
	}
	put(string(data), uint32(len(data)))
}

// requireEncodedLengthMatchesWire checks header fields and total wire length.
// Body length uses ClusterMessageWireEnd rather than generated DecodeLength/EncodedLength,
// which assume the header blockLength equals SbeBlockLength() and panic on legacy
// partial blocks (e.g. SessionEvent v5/32).
func requireEncodedLengthMatchesWire(
	tb require.TestingT,
	buf []byte,
	offset, wireEnd int,
	templateID uint16,
	actingVersion uint16,
) {
	_, gotTemplateID, schemaID, version, ok := sbe.DecodeMessageHeader(buf, offset)
	require.True(tb, ok)
	require.Equal(tb, templateID, gotTemplateID)
	require.Equal(tb, clusterSchemaID, schemaID)
	require.Equal(tb, actingVersion, version)

	bodyEnd, ok := sbe.ClusterMessageWireEnd(buf, offset, codecTestMaxVarLen)
	require.True(tb, ok)
	require.Equal(tb, wireEnd, bodyEnd)
}

func requireDecodedClusterFields(tb require.TestingT, want, got any) {
	switch wantFields := want.(type) {
	case sbe.SessionMessageHeaderFields:
		gotFields, ok := got.(sbe.SessionMessageHeaderFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields, gotFields)
	case sbe.SessionEventFields:
		gotFields, ok := got.(sbe.SessionEventFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.LeaderMemberID, gotFields.LeaderMemberID)
		require.Equal(tb, wantFields.Code, gotFields.Code)
		require.Equal(tb, wantFields.Version, gotFields.Version)
		require.Equal(tb, wantFields.LeaderHeartbeatTimeoutNs, gotFields.LeaderHeartbeatTimeoutNs)
		requireBytesEqual(tb, wantFields.Detail, gotFields.Detail)
	case sbe.SessionConnectRequestFields:
		gotFields, ok := got.(sbe.SessionConnectRequestFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ResponseStreamID, gotFields.ResponseStreamID)
		require.Equal(tb, wantFields.Version, gotFields.Version)
		requireBytesEqual(tb, wantFields.ResponseChannel, gotFields.ResponseChannel)
		requireBytesEqual(tb, wantFields.EncodedCredentials, gotFields.EncodedCredentials)
		requireBytesEqual(tb, wantFields.ClientInfo, gotFields.ClientInfo)
	case sbe.SessionCloseRequestFields:
		gotFields, ok := got.(sbe.SessionCloseRequestFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields, gotFields)
	case sbe.SessionKeepAliveFields:
		gotFields, ok := got.(sbe.SessionKeepAliveFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields, gotFields)
	case sbe.NewLeaderEventFields:
		gotFields, ok := got.(sbe.NewLeaderEventFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.LeaderMemberID, gotFields.LeaderMemberID)
		requireBytesEqual(tb, wantFields.IngressEndpoints, gotFields.IngressEndpoints)
	case sbe.ChallengeFields:
		gotFields, ok := got.(sbe.ChallengeFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		requireBytesEqual(tb, wantFields.EncodedChallenge, gotFields.EncodedChallenge)
	case sbe.ChallengeResponseFields:
		gotFields, ok := got.(sbe.ChallengeResponseFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		requireBytesEqual(tb, wantFields.EncodedCredentials, gotFields.EncodedCredentials)
	case sbe.AdminRequestFields:
		gotFields, ok := got.(sbe.AdminRequestFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.RequestType, gotFields.RequestType)
		requireBytesEqual(tb, wantFields.Payload, gotFields.Payload)
	case sbe.AdminResponseFields:
		gotFields, ok := got.(sbe.AdminResponseFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.RequestType, gotFields.RequestType)
		require.Equal(tb, wantFields.ResponseCode, gotFields.ResponseCode)
		requireBytesEqual(tb, wantFields.Message, gotFields.Message)
		requireBytesEqual(tb, wantFields.Payload, gotFields.Payload)
	default:
		require.Fail(tb, "unsupported fields type")
	}
}

func runCodecRoundtripAtOffset(
	tb require.TestingT,
	templateID uint16,
	actingVersion uint16,
	offset int,
	encode func(buf []byte, offset int) (wireEnd int, ok bool),
	wantFields any,
) {
	buf := make([]byte, offset+fuzzClusterCodecMaxLen)
	wireEnd, ok := encode(buf, offset)
	require.True(tb, ok)

	requireEncodedLengthMatchesWire(tb, buf, offset, wireEnd, templateID, actingVersion)

	decoded, ok := sbe.TryDecodeByTemplate(buf[:wireEnd], offset, codecTestMaxVarLen)
	require.True(tb, ok)
	require.Equal(tb, templateID, decoded.TemplateID)
	requireDecodedClusterFields(tb, wantFields, decoded.Fields)
}

func runCodecRoundtrip(
	tb require.TestingT,
	templateID uint16,
	actingVersion uint16,
	encode func(buf []byte, offset int) (wireEnd int, ok bool),
	wantFields any,
) {
	for _, offset := range codecRoundtripOffsetValues {
		runCodecRoundtripAtOffset(tb, templateID, actingVersion, offset, encode, wantFields)
	}
}

type codecRoundtripCase struct {
	name       string
	actingVer  uint16
	encode     func(buf []byte, offset int) (wireEnd int, ok bool)
	wantFields any
}

func runCodecRoundtripCases(
	t *testing.T,
	templateID uint16,
	defaultActingVer uint16,
	cases []codecRoundtripCase,
) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actingVer := tc.actingVer
			if actingVer == 0 {
				actingVer = defaultActingVer
			}
			runCodecRoundtrip(t, templateID, actingVer, tc.encode, tc.wantFields)
		})
	}
}

func codecFuzzSeedBuffers() [][]byte {
	seeds := make([][]byte, 0, 30)
	addSeed := func(buf []byte, offset, wireEnd int) {
		wire := append([]byte(nil), buf[offset:wireEnd]...)
		seeds = append(seeds, wire)
		for trunc := 1; trunc <= codecFuzzMaxOffset && trunc < len(wire); trunc++ {
			seeds = append(seeds, wire[:len(wire)-trunc])
		}
	}

	const actingVersion = gensbe.SessionMessageHeaderSbeSchemaVersion
	buf := make([]byte, fuzzClusterCodecMaxLen)

	end, _ := encodeSessionMessageHeader(buf, 0, actingVersion, sbe.SessionMessageHeaderFields{
		LeadershipTermID: 11,
		ClusterSessionID: 22,
		Timestamp:        33,
	})
	addSeed(buf, 0, end)

	end, _ = encodeSessionEvent(
		buf, 0, actingVersion, gensbe.SessionEventSbeBlockLength, sbe.SessionEventFields{
			ClusterSessionID: 1,
			CorrelationID:    2,
			LeadershipTermID: 3,
			LeaderMemberID:   4,
			Code:             gensbe.EventCode_OK,
			Detail:           []byte("ok"),
		},
	)
	addSeed(buf, 0, end)

	end, _ = encodeSessionEvent(buf, 0, 5, 32, sbe.SessionEventFields{
		ClusterSessionID: 9,
		CorrelationID:    8,
		LeadershipTermID: 7,
		LeaderMemberID:   6,
		Code:             gensbe.EventCode_ERROR,
		Detail:           nil,
	})
	addSeed(buf, 0, end)

	end, _ = encodeSessionConnectRequest(buf, 0, actingVersion, sbe.SessionConnectRequestFields{
		CorrelationID:      100,
		ResponseStreamID:   200,
		Version:            1,
		ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
		EncodedCredentials: bytes.Repeat([]byte{0xAB}, 32),
		ClientInfo:         []byte("go-client"),
	})
	addSeed(buf, 0, end)

	end, _ = encodeSessionConnectRequest(buf, 0, 1, sbe.SessionConnectRequestFields{
		CorrelationID:    101,
		ResponseStreamID: 201,
		ResponseChannel:  nil,
	})
	addSeed(buf, 0, end)

	end, _ = encodeSessionCloseRequest(buf, 0, actingVersion, sbe.SessionCloseRequestFields{
		LeadershipTermID: 5,
		ClusterSessionID: 6,
	})
	addSeed(buf, 0, end)

	end, _ = encodeSessionKeepAlive(buf, 0, actingVersion, sbe.SessionKeepAliveFields{
		LeadershipTermID: 7,
		ClusterSessionID: 8,
	})
	addSeed(buf, 0, end)

	end, _ = encodeNewLeaderEvent(buf, 0, actingVersion, sbe.NewLeaderEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		LeaderMemberID:   3,
		IngressEndpoints: []byte("localhost:40123"),
	})
	addSeed(buf, 0, end)

	end, _ = encodeChallenge(buf, 0, actingVersion, sbe.ChallengeFields{
		CorrelationID:    10,
		ClusterSessionID: 11,
		EncodedChallenge: bytes.Repeat([]byte{0xCD}, 32),
	})
	addSeed(buf, 0, end)

	end, _ = encodeChallengeResponse(buf, 0, actingVersion, sbe.ChallengeResponseFields{
		CorrelationID:      12,
		ClusterSessionID:   13,
		EncodedCredentials: bytes.Repeat([]byte{0xEF}, 32),
	})
	addSeed(buf, 0, end)

	end, _ = encodeAdminRequest(buf, 0, actingVersion, sbe.AdminRequestFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		CorrelationID:    3,
		RequestType:      gensbe.AdminRequestType_SNAPSHOT,
	})
	addSeed(buf, 0, end)

	end, _ = encodeAdminResponse(buf, 0, actingVersion, sbe.AdminResponseFields{
		ClusterSessionID: 4,
		CorrelationID:    5,
		RequestType:      gensbe.AdminRequestType_SNAPSHOT,
		ResponseCode:     gensbe.AdminResponseCode_OK,
		Message:          []byte("snapshot complete"),
	})
	addSeed(buf, 0, end)

	return seeds
}
