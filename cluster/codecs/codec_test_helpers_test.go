package codecs_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/gensbe"
)

const (
	codecTestMaxVarLen     = codecs.ClusterCodecMaxVarLen
	fuzzClusterCodecMaxLen = 4096
	codecFuzzMaxOffset     = 8
	codecFuzzOffsetSpan    = codecFuzzMaxOffset + 1
)

var (
	messageHeaderLen                      = codecs.MessageHeaderEncodedLen()
	sessionConnectClientInfoMinVersion    = codecs.SessionConnectClientInfoMinVersion
	sessionEventLeaderHeartbeatMinVersion = codecs.SessionEventLeaderHeartbeatMinVersion
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
	// clusterLargeVarPayload is a multi-kilobyte var field for large-field encode/decode
	// tests without materializing the full ClusterEncodeMaxVarLen bound.
	clusterLargeVarPayload = bytes.Repeat([]byte{0x5A}, 2000)
)

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
	fields codecs.SessionEventFields,
) func(buf []byte) (wireEnd int, ok bool) {
	return func(buf []byte) (wireEnd int, ok bool) {
		return codecs.EncodeSessionEvent(
			buf, 0, actingVersion, codecs.SessionEventBlockLength(actingVersion), fields,
		)
	}
}

func encodeSessionEventInto(
	t *testing.T,
	actingVersion uint16,
	fields codecs.SessionEventFields,
) (buf []byte, wireEnd int) {
	return encodeClusterInto(t, sessionEventEncodeFunc(actingVersion, fields))
}

func encodeSessionEventWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.SessionEventFields,
) []byte {
	return encodeClusterWire(t, sessionEventEncodeFunc(actingVersion, fields))
}

func sessionConnectRequestEncodeFunc(
	actingVersion uint16,
	fields codecs.SessionConnectRequestFields,
) func(buf []byte) (wireEnd int, ok bool) {
	return func(buf []byte) (wireEnd int, ok bool) {
		return codecs.EncodeSessionConnectRequest(buf, 0, actingVersion, fields)
	}
}

func encodeSessionConnectRequestWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.SessionConnectRequestFields,
) []byte {
	return encodeClusterWire(t, sessionConnectRequestEncodeFunc(actingVersion, fields))
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
// which assume the header blockLength equals SbeBlockLength() and panic on reduced
// root blocks (e.g. SessionEvent acting version 5, block length 32).
func requireEncodedLengthMatchesWire(
	tb require.TestingT,
	buf []byte,
	offset, wireEnd int,
	templateID uint16,
	actingVersion uint16,
) {
	_, gotTemplateID, schemaID, version, ok := codecs.DecodeMessageHeader(buf, offset)
	require.True(tb, ok)
	require.Equal(tb, templateID, gotTemplateID)
	require.Equal(tb, gensbe.MessageHeaderSbeSchemaID, schemaID)
	require.Equal(tb, actingVersion, version)

	bodyEnd, ok := codecs.ClusterMessageWireEnd(buf, offset, codecTestMaxVarLen)
	require.True(tb, ok)
	require.Equal(tb, wireEnd, bodyEnd)
}

func requireDecodedClusterFields(tb require.TestingT, want, got any) {
	switch wantFields := want.(type) {
	case codecs.SessionMessageHeaderFields:
		requireDecodedFieldsEqual[codecs.SessionMessageHeaderFields](tb, wantFields, got)
	case codecs.SessionEventFields:
		gotFields, ok := got.(codecs.SessionEventFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.LeaderMemberID, gotFields.LeaderMemberID)
		require.Equal(tb, wantFields.Code, gotFields.Code)
		require.Equal(tb, wantFields.Version, gotFields.Version)
		require.Equal(tb, wantFields.LeaderHeartbeatTimeoutNs, gotFields.LeaderHeartbeatTimeoutNs)
		requireBytesEqual(tb, wantFields.Detail, gotFields.Detail)
	case codecs.SessionConnectRequestFields:
		gotFields, ok := got.(codecs.SessionConnectRequestFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ResponseStreamID, gotFields.ResponseStreamID)
		require.Equal(tb, wantFields.Version, gotFields.Version)
		requireBytesEqual(tb, wantFields.ResponseChannel, gotFields.ResponseChannel)
		requireBytesEqual(tb, wantFields.EncodedCredentials, gotFields.EncodedCredentials)
		requireBytesEqual(tb, wantFields.ClientInfo, gotFields.ClientInfo)
	case codecs.SessionCloseRequestFields:
		requireDecodedFieldsEqual[codecs.SessionCloseRequestFields](tb, wantFields, got)
	case codecs.SessionKeepAliveFields:
		requireDecodedFieldsEqual[codecs.SessionKeepAliveFields](tb, wantFields, got)
	case codecs.NewLeaderEventFields:
		gotFields, ok := got.(codecs.NewLeaderEventFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.LeaderMemberID, gotFields.LeaderMemberID)
		requireBytesEqual(tb, wantFields.IngressEndpoints, gotFields.IngressEndpoints)
	case codecs.ChallengeFields:
		gotFields, ok := got.(codecs.ChallengeFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		requireBytesEqual(tb, wantFields.EncodedChallenge, gotFields.EncodedChallenge)
	case codecs.ChallengeResponseFields:
		gotFields, ok := got.(codecs.ChallengeResponseFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		requireBytesEqual(tb, wantFields.EncodedCredentials, gotFields.EncodedCredentials)
	case codecs.AdminRequestFields:
		gotFields, ok := got.(codecs.AdminRequestFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.RequestType, gotFields.RequestType)
		requireBytesEqual(tb, wantFields.Payload, gotFields.Payload)
	case codecs.AdminResponseFields:
		gotFields, ok := got.(codecs.AdminResponseFields)
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

	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd], offset, codecTestMaxVarLen)
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
	for _, offset := range []int{0, 4} {
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
	addEncoded := func(encode func([]byte) (int, bool)) {
		end, _ := encode(buf)
		addSeed(end)
	}

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionMessageHeader(
			b, 0, actingVersion, codecs.SessionMessageHeaderFields{
				LeadershipTermID: 11,
				ClusterSessionID: 22,
				Timestamp:        33,
			},
		)
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionEvent(
			b, 0, actingVersion, codecs.SessionEventBlockLength(actingVersion), codecs.SessionEventFields{
				ClusterSessionID: 1,
				CorrelationID:    2,
				LeadershipTermID: 3,
				LeaderMemberID:   4,
				Code:             gensbe.EventCode_OK,
				Detail:           []byte("ok"),
			},
		)
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionEvent(
			b, 0, 5, codecs.SessionEventBlockLength(5), codecs.SessionEventFields{
				ClusterSessionID: 9,
				CorrelationID:    8,
				LeadershipTermID: 7,
				LeaderMemberID:   6,
				Code:             gensbe.EventCode_ERROR,
				Detail:           nil,
			},
		)
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionConnectRequest(
			b, 0, actingVersion, codecs.SessionConnectRequestFields{
				CorrelationID:      100,
				ResponseStreamID:   200,
				Version:            1,
				ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
				EncodedCredentials: bytes.Repeat([]byte{0xAB}, 32),
				ClientInfo:         []byte("go-client"),
			},
		)
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionConnectRequest(b, 0, 1, codecs.SessionConnectRequestFields{
			CorrelationID:    101,
			ResponseStreamID: 201,
			ResponseChannel:  nil,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionCloseRequest(b, 0, actingVersion, codecs.SessionCloseRequestFields{
			LeadershipTermID: 5,
			ClusterSessionID: 6,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionKeepAlive(b, 0, actingVersion, codecs.SessionKeepAliveFields{
			LeadershipTermID: 7,
			ClusterSessionID: 8,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeNewLeaderEvent(b, 0, actingVersion, codecs.NewLeaderEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: 2,
			LeaderMemberID:   3,
			IngressEndpoints: []byte("localhost:40123"),
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeChallenge(b, 0, actingVersion, codecs.ChallengeFields{
			CorrelationID:    10,
			ClusterSessionID: 11,
			EncodedChallenge: bytes.Repeat([]byte{0xCD}, 32),
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeChallengeResponse(b, 0, actingVersion, codecs.ChallengeResponseFields{
			CorrelationID:      12,
			ClusterSessionID:   13,
			EncodedCredentials: bytes.Repeat([]byte{0xEF}, 32),
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeAdminRequest(b, 0, actingVersion, codecs.AdminRequestFields{
			LeadershipTermID: 1,
			ClusterSessionID: 2,
			CorrelationID:    3,
			RequestType:      gensbe.AdminRequestType_SNAPSHOT,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeAdminResponse(b, 0, actingVersion, codecs.AdminResponseFields{
			ClusterSessionID: 4,
			CorrelationID:    5,
			RequestType:      gensbe.AdminRequestType_SNAPSHOT,
			ResponseCode:     gensbe.AdminResponseCode_OK,
			Message:          []byte("snapshot complete"),
		})
	})

	return seeds
}
