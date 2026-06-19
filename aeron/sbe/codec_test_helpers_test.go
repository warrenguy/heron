package sbe_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

const (
	codecTestMaxVarLen     = sbe.ClusterCodecMaxVarLen
	fuzzClusterCodecMaxLen = 4096
	codecFuzzMaxOffset     = 8
	codecFuzzOffsetSpan    = codecFuzzMaxOffset + 1
)

var (
	messageHeaderLen                      = sbe.MessageHeaderEncodedLen()
	sessionConnectClientInfoMinVersion    = sbe.SessionConnectClientInfoMinVersion
	sessionEventLeaderHeartbeatMinVersion = sbe.SessionEventLeaderHeartbeatMinVersion
	codecAllEventCodes                    = []gensbe.EventCode{
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

func encodeClusterInto(
	t *testing.T,
	encode func(buf []byte) (wireEnd int, ok bool),
) (buf []byte, wireEnd int) {
	t.Helper()
	buf = make([]byte, fuzzClusterCodecMaxLen)
	wireEnd, ok := encode(buf)
	require.True(t, ok)
	return buf, wireEnd
}

func encodeClusterWire(
	t *testing.T,
	encode func(buf []byte) (wireEnd int, ok bool),
) []byte {
	t.Helper()
	buf, wireEnd := encodeClusterInto(t, encode)
	return buf[:wireEnd]
}

func sessionEventEncodeFunc(
	actingVersion uint16,
	fields sbe.SessionEventFields,
) func(buf []byte) (wireEnd int, ok bool) {
	return func(buf []byte) (int, bool) {
		return sbe.EncodeSessionEvent(
			buf, 0, actingVersion, sbe.SessionEventBlockLength(actingVersion), fields,
		)
	}
}

func encodeSessionEventInto(
	t *testing.T,
	actingVersion uint16,
	fields sbe.SessionEventFields,
) (buf []byte, wireEnd int) {
	return encodeClusterInto(t, sessionEventEncodeFunc(actingVersion, fields))
}

func encodeSessionEventWire(
	t *testing.T,
	actingVersion uint16,
	fields sbe.SessionEventFields,
) []byte {
	return encodeClusterWire(t, sessionEventEncodeFunc(actingVersion, fields))
}

func encodeSessionConnectRequestWire(
	t *testing.T,
	actingVersion uint16,
	fields sbe.SessionConnectRequestFields,
) []byte {
	return encodeClusterWire(t, func(buf []byte) (int, bool) {
		return sbe.EncodeSessionConnectRequest(buf, 0, actingVersion, fields)
	})
}

func requireBytesEqual(tb require.TestingT, want, got []byte) {
	require.True(tb, bytes.Equal(want, got))
}

func requireDecodedFieldsEqual[T any](tb require.TestingT, want, got any) {
	wantFields, ok := want.(T)
	require.True(tb, ok)
	gotFields, ok := got.(T)
	require.True(tb, ok)
	require.Equal(tb, wantFields, gotFields)
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
	require.Equal(tb, gensbe.MessageHeaderSbeSchemaID, schemaID)
	require.Equal(tb, actingVersion, version)

	bodyEnd, ok := sbe.ClusterMessageWireEnd(buf, offset, codecTestMaxVarLen)
	require.True(tb, ok)
	require.Equal(tb, wireEnd, bodyEnd)
}

func requireDecodedClusterFields(tb require.TestingT, want, got any) {
	switch wantFields := want.(type) {
	case sbe.SessionMessageHeaderFields:
		requireDecodedFieldsEqual[sbe.SessionMessageHeaderFields](tb, wantFields, got)
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
		requireDecodedFieldsEqual[sbe.SessionCloseRequestFields](tb, wantFields, got)
	case sbe.SessionKeepAliveFields:
		requireDecodedFieldsEqual[sbe.SessionKeepAliveFields](tb, wantFields, got)
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
	const (
		actingVersion   = gensbe.SessionMessageHeaderSbeSchemaVersion
		seedEncodeCount = 12
	)
	buf := make([]byte, fuzzClusterCodecMaxLen)
	seeds := make([][]byte, 0, seedEncodeCount*(1+codecFuzzMaxOffset))
	addSeed := func(wireEnd int) {
		wire := bytes.Clone(buf[:wireEnd])
		seeds = append(seeds, wire)
		for trunc := 1; trunc <= codecFuzzMaxOffset && trunc < len(wire); trunc++ {
			seeds = append(seeds, wire[:len(wire)-trunc])
		}
	}

	end, _ := sbe.EncodeSessionMessageHeader(buf, 0, actingVersion, sbe.SessionMessageHeaderFields{
		LeadershipTermID: 11,
		ClusterSessionID: 22,
		Timestamp:        33,
	})
	addSeed(end)

	end, _ = sbe.EncodeSessionEvent(
		buf, 0, actingVersion, sbe.SessionEventBlockLength(actingVersion), sbe.SessionEventFields{
			ClusterSessionID: 1,
			CorrelationID:    2,
			LeadershipTermID: 3,
			LeaderMemberID:   4,
			Code:             gensbe.EventCode_OK,
			Detail:           []byte("ok"),
		},
	)
	addSeed(end)

	end, _ = sbe.EncodeSessionEvent(buf, 0, 5, sbe.SessionEventBlockLength(5), sbe.SessionEventFields{
		ClusterSessionID: 9,
		CorrelationID:    8,
		LeadershipTermID: 7,
		LeaderMemberID:   6,
		Code:             gensbe.EventCode_ERROR,
		Detail:           nil,
	})
	addSeed(end)

	end, _ = sbe.EncodeSessionConnectRequest(buf, 0, actingVersion, sbe.SessionConnectRequestFields{
		CorrelationID:      100,
		ResponseStreamID:   200,
		Version:            1,
		ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
		EncodedCredentials: bytes.Repeat([]byte{0xAB}, 32),
		ClientInfo:         []byte("go-client"),
	})
	addSeed(end)

	end, _ = sbe.EncodeSessionConnectRequest(buf, 0, 1, sbe.SessionConnectRequestFields{
		CorrelationID:    101,
		ResponseStreamID: 201,
		ResponseChannel:  nil,
	})
	addSeed(end)

	end, _ = sbe.EncodeSessionCloseRequest(buf, 0, actingVersion, sbe.SessionCloseRequestFields{
		LeadershipTermID: 5,
		ClusterSessionID: 6,
	})
	addSeed(end)

	end, _ = sbe.EncodeSessionKeepAlive(buf, 0, actingVersion, sbe.SessionKeepAliveFields{
		LeadershipTermID: 7,
		ClusterSessionID: 8,
	})
	addSeed(end)

	end, _ = sbe.EncodeNewLeaderEvent(buf, 0, actingVersion, sbe.NewLeaderEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		LeaderMemberID:   3,
		IngressEndpoints: []byte("localhost:40123"),
	})
	addSeed(end)

	end, _ = sbe.EncodeChallenge(buf, 0, actingVersion, sbe.ChallengeFields{
		CorrelationID:    10,
		ClusterSessionID: 11,
		EncodedChallenge: bytes.Repeat([]byte{0xCD}, 32),
	})
	addSeed(end)

	end, _ = sbe.EncodeChallengeResponse(buf, 0, actingVersion, sbe.ChallengeResponseFields{
		CorrelationID:      12,
		ClusterSessionID:   13,
		EncodedCredentials: bytes.Repeat([]byte{0xEF}, 32),
	})
	addSeed(end)

	end, _ = sbe.EncodeAdminRequest(buf, 0, actingVersion, sbe.AdminRequestFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		CorrelationID:    3,
		RequestType:      gensbe.AdminRequestType_SNAPSHOT,
	})
	addSeed(end)

	end, _ = sbe.EncodeAdminResponse(buf, 0, actingVersion, sbe.AdminResponseFields{
		ClusterSessionID: 4,
		CorrelationID:    5,
		RequestType:      gensbe.AdminRequestType_SNAPSHOT,
		ResponseCode:     gensbe.AdminResponseCode_OK,
		Message:          []byte("snapshot complete"),
	})
	addSeed(end)

	return seeds
}
