package codecs_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
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
	codecAllEventCodes                    = []clusterwire.EventCode{
		clusterwire.EventCode_OK,
		clusterwire.EventCode_ERROR,
		clusterwire.EventCode_REDIRECT,
		clusterwire.EventCode_AUTHENTICATION_REJECTED,
		clusterwire.EventCode_CLOSED,
	}
	codecAllAdminResponseCodes = []clusterwire.AdminResponseCode{
		clusterwire.AdminResponseCode_OK,
		clusterwire.AdminResponseCode_ERROR,
		clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
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
	require.Equal(tb, clusterwire.MessageHeaderSbeSchemaID, schemaID)
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
	case codecs.NewLeadershipTermEventFields:
		requireDecodedFieldsEqual[codecs.NewLeadershipTermEventFields](tb, wantFields, got)
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
	case codecs.CanvassPositionFields:
		requireDecodedFieldsEqual[codecs.CanvassPositionFields](tb, wantFields, got)
	case codecs.RequestVoteFields:
		requireDecodedFieldsEqual[codecs.RequestVoteFields](tb, wantFields, got)
	case codecs.VoteFields:
		requireDecodedFieldsEqual[codecs.VoteFields](tb, wantFields, got)
	case codecs.NewLeadershipTermFields:
		requireDecodedFieldsEqual[codecs.NewLeadershipTermFields](tb, wantFields, got)
	case codecs.AppendPositionFields:
		requireDecodedFieldsEqual[codecs.AppendPositionFields](tb, wantFields, got)
	case codecs.CommitPositionFields:
		requireDecodedFieldsEqual[codecs.CommitPositionFields](tb, wantFields, got)
	case codecs.TimerEventFields:
		requireDecodedFieldsEqual[codecs.TimerEventFields](tb, wantFields, got)
	case codecs.SessionOpenEventFields:
		gotFields, ok := got.(codecs.SessionOpenEventFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.LeadershipTermID, gotFields.LeadershipTermID)
		require.Equal(tb, wantFields.CorrelationID, gotFields.CorrelationID)
		require.Equal(tb, wantFields.ClusterSessionID, gotFields.ClusterSessionID)
		require.Equal(tb, wantFields.Timestamp, gotFields.Timestamp)
		require.Equal(tb, wantFields.ResponseStreamID, gotFields.ResponseStreamID)
		requireBytesEqual(tb, wantFields.ResponseChannel, gotFields.ResponseChannel)
		requireBytesEqual(tb, wantFields.EncodedPrincipal, gotFields.EncodedPrincipal)
	case codecs.SessionCloseEventFields:
		requireDecodedFieldsEqual[codecs.SessionCloseEventFields](tb, wantFields, got)
	case codecs.ServiceAckFields:
		requireDecodedFieldsEqual[codecs.ServiceAckFields](tb, wantFields, got)
	case codecs.JoinLogFields:
		gotFields, ok := got.(codecs.JoinLogFields)
		require.True(tb, ok)
		require.Equal(tb, wantFields.LogPosition, gotFields.LogPosition)
		require.Equal(tb, wantFields.MaxLogPosition, gotFields.MaxLogPosition)
		require.Equal(tb, wantFields.MemberID, gotFields.MemberID)
		require.Equal(tb, wantFields.LogSessionID, gotFields.LogSessionID)
		require.Equal(tb, wantFields.LogStreamID, gotFields.LogStreamID)
		require.Equal(tb, wantFields.IsStartup, gotFields.IsStartup)
		require.Equal(tb, wantFields.Role, gotFields.Role)
		require.Equal(tb, wantFields.IsStandby, gotFields.IsStandby)
		requireBytesEqual(tb, wantFields.LogChannel, gotFields.LogChannel)
	case codecs.RequestServiceAckFields:
		requireDecodedFieldsEqual[codecs.RequestServiceAckFields](tb, wantFields, got)
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

// goldenHexCase holds one encoder's committed hex wire bytes. runGoldenHexCases
// asserts the encode output matches the hex and catches silent layout drift
// after a schema regenerate.
type goldenHexCase struct {
	name       string
	wantHex    string
	encode     func([]byte) (int, bool)
	templateID uint16
}

func runGoldenHexCases(t *testing.T, actingVersion uint16, cases []goldenHexCase) {
	t.Helper()
	buf := make([]byte, fuzzClusterCodecMaxLen)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			end, ok := tc.encode(buf)
			require.True(t, ok)
			require.Equal(t, tc.wantHex, hex.EncodeToString(buf[:end]))
			requireEncodedLengthMatchesWire(t, buf, 0, end, tc.templateID, actingVersion)
		})
	}
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

// requireTryDecodeAt encodes fields through encode, then decodes through
// tryDecode. Asserts both succeed and the decoded fields equal the input.
func requireTryDecodeAt[T any](
	t *testing.T,
	buf []byte,
	actingVersion uint16,
	fields T,
	encode func(buf []byte, offset int, actingVersion uint16, fields T) (int, bool),
	tryDecode func(buf []byte, offset int) (T, bool),
) {
	t.Helper()
	end, ok := encode(buf, 0, actingVersion, fields)
	require.True(t, ok)
	got, ok := tryDecode(buf[:end], 0)
	require.True(t, ok)
	require.Equal(t, fields, got)
}

func codecFuzzSeedBuffers() [][]byte {
	const (
		actingVersion   = clusterwire.SessionMessageHeaderSbeSchemaVersion
		seedEncodeCount = 26
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
				Code:             clusterwire.EventCode_OK,
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
				Code:             clusterwire.EventCode_ERROR,
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
		return codecs.EncodeNewLeadershipTermEvent(
			b,
			0,
			actingVersion,
			codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    1,
				LogPosition:         2,
				Timestamp:           3,
				TermBaseLogPosition: 4,
				LeaderMemberID:      5,
				LogSessionID:        6,
				TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
				AppVersion:          7,
			},
		)
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeNewLeadershipTermEvent(
			b,
			0,
			3,
			codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    8,
				LogPosition:         9,
				Timestamp:           10,
				TermBaseLogPosition: 11,
				LeaderMemberID:      12,
				LogSessionID:        13,
			},
		)
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
			RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeAdminResponse(b, 0, actingVersion, codecs.AdminResponseFields{
			ClusterSessionID: 4,
			CorrelationID:    5,
			RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
			ResponseCode:     clusterwire.AdminResponseCode_OK,
			Message:          []byte("snapshot complete"),
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeCanvassPosition(b, 0, actingVersion, codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
			ProtocolVersion:     5,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeRequestVote(b, 0, actingVersion, codecs.RequestVoteFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			CandidateTermID:     3,
			CandidateMemberID:   4,
			ProtocolVersion:     5,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeVote(b, 0, actingVersion, codecs.VoteFields{
			CandidateTermID:     1,
			LogLeadershipTermID: 2,
			LogPosition:         3,
			CandidateMemberID:   4,
			FollowerMemberID:    5,
			Vote:                clusterwire.BooleanType_TRUE,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeNewLeadershipTerm(b, 0, actingVersion, codecs.NewLeadershipTermFields{
			LogLeadershipTermID:     1,
			NextLeadershipTermID:    2,
			NextTermBaseLogPosition: 3,
			NextLogPosition:         4,
			LeadershipTermID:        5,
			TermBaseLogPosition:     6,
			LogPosition:             7,
			LeaderRecordingID:       8,
			Timestamp:               9,
			LeaderMemberID:          10,
			LogSessionID:            11,
			AppVersion:              12,
			IsStartup:               clusterwire.BooleanType_TRUE,
			CommitPosition:          13,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeAppendPosition(b, 0, actingVersion, codecs.AppendPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			FollowerMemberID: 3,
			Flags:            4,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeCommitPosition(b, 0, actingVersion, codecs.CommitPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			LeaderMemberID:   3,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeTimerEvent(b, 0, actingVersion, codecs.TimerEventFields{
			LeadershipTermID: 1,
			CorrelationID:    2,
			Timestamp:        3,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionOpenEvent(b, 0, actingVersion, codecs.SessionOpenEventFields{
			LeadershipTermID: 1,
			CorrelationID:    2,
			ClusterSessionID: 3,
			Timestamp:        4,
			ResponseStreamID: 102,
			ResponseChannel:  []byte("aeron:ipc"),
			EncodedPrincipal: []byte{0xAB, 0xCD},
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeSessionCloseEvent(b, 0, actingVersion, codecs.SessionCloseEventFields{
			LeadershipTermID: 5,
			ClusterSessionID: 6,
			Timestamp:        7,
			CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeServiceAck(b, 0, actingVersion, codecs.ServiceAckFields{
			LogPosition: 10,
			Timestamp:   11,
			AckID:       12,
			RelevantID:  13,
			ServiceID:   1,
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeJoinLog(b, 0, actingVersion, codecs.JoinLogFields{
			LogPosition:    100,
			MaxLogPosition: 200,
			MemberID:       1,
			LogSessionID:   2,
			LogStreamID:    100,
			IsStartup:      clusterwire.BooleanType_TRUE,
			Role:           1,
			IsStandby:      clusterwire.BooleanType_FALSE,
			LogChannel:     []byte("aeron:udp?endpoint=localhost:9004"),
		})
	})

	addEncoded(func(b []byte) (int, bool) {
		return codecs.EncodeRequestServiceAck(b, 0, actingVersion, codecs.RequestServiceAckFields{
			LogPosition: 42,
		})
	})

	return seeds
}

type tryDecodeRejectCase struct {
	name   string
	buf    func() []byte
	offset int
	maxLen int
}

func rejectCaseName(prefix, suffix string) string {
	if prefix == "" {
		return suffix
	}
	return prefix + "_" + suffix
}

func cloneMutatedWire(wire []byte, mutate func([]byte)) []byte {
	out := append([]byte(nil), wire...)
	mutate(out)
	return out
}

func fixedBlockRejectCases(
	prefix string,
	validWire []byte,
	maxBlock uint16,
	belowMin uint8,
	truncateBodyLen int,
) []tryDecodeRejectCase {
	return []tryDecodeRejectCase{
		{
			name: rejectCaseName(prefix, "block_length_below_min"),
			buf: func() []byte {
				return cloneMutatedWire(validWire, func(w []byte) { w[0] = belowMin })
			},
		},
		{
			name: rejectCaseName(prefix, "block_length_above_max"),
			buf: func() []byte {
				return cloneMutatedWire(validWire, func(w []byte) {
					w[0] = byte(maxBlock + 1)
					w[1] = 0
				})
			},
		},
		{
			name: rejectCaseName(prefix, "truncated_fixed_block"),
			buf: func() []byte {
				return validWire[:messageHeaderLen+truncateBodyLen]
			},
		},
	}
}

func wireHeaderRejectCases(validWire []byte) []tryDecodeRejectCase {
	return []tryDecodeRejectCase{
		{
			name: "truncated_header",
			buf:  func() []byte { return validWire[:4] },
		},
		{
			name: "schema_mismatch",
			buf: func() []byte {
				return cloneMutatedWire(validWire, func(w []byte) { w[4], w[5] = 0, 0 })
			},
		},
		{
			name: "unknown_template_id",
			buf: func() []byte {
				return cloneMutatedWire(validWire, func(w []byte) { w[2], w[3] = 0xFF, 0x7F })
			},
		},
	}
}

func fixedBlockRejectAboveMaxAtVersion(
	prefix string,
	validWire []byte,
	actingVersion uint16,
	maxBlock uint16,
	versionSuffix string,
) tryDecodeRejectCase {
	return tryDecodeRejectCase{
		name: prefix + "_block_length_above_max_" + versionSuffix,
		buf: func() []byte {
			return cloneMutatedWire(validWire, func(w []byte) {
				w[6], w[7] = byte(actingVersion), 0
				w[0] = byte(maxBlock + 1)
				w[1] = 0
			})
		},
	}
}

func runTryDecodeRejectCases(t *testing.T, cases []tryDecodeRejectCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maxLen := tc.maxLen
			if maxLen == 0 {
				maxLen = codecTestMaxVarLen
			}
			_, ok := codecs.TryDecodeByTemplate(tc.buf(), tc.offset, maxLen)
			require.False(t, ok)
		})
	}
}
