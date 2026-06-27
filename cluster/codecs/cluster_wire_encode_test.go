package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/gensbe"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestEncodeSessionKeepAlive_ZeroAllocs(t *testing.T) {
	fields := codecs.SessionKeepAliveFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
	}
	buf := make([]byte, messageHeaderLen+int(gensbe.SessionKeepAliveSbeBlockLength))

	testutil.AssertZeroAllocs(t, "EncodeSessionKeepAlive", 1000, func() {
		_, _ = codecs.EncodeSessionKeepAlive(
			buf, 0, uint16(gensbe.SessionKeepAliveSbeSchemaVersion), fields,
		)
	})
}

func TestEncodeClusterWire_BufferBounds(t *testing.T) {
	actingVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)
	connectFields := codecs.SessionConnectRequestFields{
		CorrelationID:      1,
		ResponseStreamID:   2,
		Version:            3,
		ResponseChannel:    []byte("aeron:ipc"),
		EncodedCredentials: []byte{1, 2, 3},
		ClientInfo:         []byte("client"),
	}
	connectBlockLength := codecs.ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	)
	connectWireLen, ok := codecs.ClusterWireMessageLen(
		connectBlockLength,
		len(connectFields.ResponseChannel),
		len(connectFields.EncodedCredentials),
		len(connectFields.ClientInfo),
	)
	require.True(t, ok)

	challengeFields := codecs.ChallengeResponseFields{
		CorrelationID:      4,
		ClusterSessionID:   5,
		EncodedCredentials: []byte{6, 7, 8},
	}
	challengeWireLen, ok := codecs.ClusterWireMessageLen(
		gensbe.ChallengeResponseSbeBlockLength,
		len(challengeFields.EncodedCredentials),
	)
	require.True(t, ok)

	connectEncode := sessionConnectRequestEncodeFunc(actingVersion, connectFields)
	challengeEncode := func(buf []byte) (int, bool) {
		return codecs.EncodeChallengeResponse(
			buf, 0, uint16(gensbe.ChallengeResponseSbeSchemaVersion), challengeFields,
		)
	}

	cases := []struct {
		name              string
		bufLen            int
		wantOK            bool
		wantWireEnd       int
		verifyConnectWire bool
		encode            func(buf []byte) (wireEnd int, ok bool)
	}{
		{
			name:   "connect_header_only",
			bufLen: messageHeaderLen,
			wantOK: false,
			encode: connectEncode,
		},
		{
			name:   "connect_one_byte_short",
			bufLen: connectWireLen - 1,
			wantOK: false,
			encode: connectEncode,
		},
		{
			name:              "connect_exact_fit",
			bufLen:            connectWireLen,
			wantOK:            true,
			wantWireEnd:       connectWireLen,
			verifyConnectWire: true,
			encode:            connectEncode,
		},
		{
			name:   "challenge_header_only",
			bufLen: messageHeaderLen,
			wantOK: false,
			encode: challengeEncode,
		},
		{
			name:   "challenge_one_byte_short",
			bufLen: challengeWireLen - 1,
			wantOK: false,
			encode: challengeEncode,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			wireEnd, ok := tc.encode(buf)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantWireEnd, wireEnd)
				if tc.verifyConnectWire {
					connectWire := encodeSessionConnectRequestWire(t, actingVersion, connectFields)
					require.Equal(t, connectWire, buf[:wireEnd])
				}
				return
			}
			require.Zero(t, wireEnd)
		})
	}
}

func repeatLargeVarLens(n int) []int {
	varLens := make([]int, n)
	largeVarLen := len(clusterLargeVarPayload)
	for i := range varLens {
		varLens[i] = largeVarLen
	}
	return varLens
}

func assertEncodeWireEndMatchesMessageLen(
	t *testing.T,
	offset int,
	blockLength uint16,
	varLens []int,
	encode func(buf []byte, offset int) (int, bool),
) {
	t.Helper()

	wantWireLen, ok := codecs.ClusterWireMessageLen(blockLength, varLens...)
	require.True(t, ok)

	buf := make([]byte, offset+wantWireLen)
	wireEnd, ok := encode(buf, offset)
	require.True(t, ok)
	require.Equal(t, offset+wantWireLen, wireEnd)
}

func TestEncodeClusterWire_wireEndMatchesClusterWireMessageLen(t *testing.T) {
	twoVarConnectVersion := codecs.SessionConnectClientInfoMinVersion - 1
	threeVarConnectVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)
	maxVar := clusterLargeVarPayload

	twoVarConnectBlockLength := codecs.ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		twoVarConnectVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	)
	threeVarConnectBlockLength := codecs.ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		threeVarConnectVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	)
	sessionEventVersion := uint16(gensbe.SessionEventSbeSchemaVersion)
	sessionEventBlockLength := codecs.SessionEventBlockLength(sessionEventVersion)

	cases := []struct {
		name        string
		offset      int
		blockLength uint16
		varLens     []int
		encode      func(buf []byte, offset int) (int, bool)
	}{
		{
			name:        "session_message_header",
			offset:      4,
			blockLength: gensbe.SessionMessageHeaderSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				return codecs.EncodeSessionMessageHeader(
					buf, offset, gensbe.SessionMessageHeaderSbeSchemaVersion,
					codecs.SessionMessageHeaderFields{
						LeadershipTermID: 1,
						ClusterSessionID: 2,
						Timestamp:        3,
					},
				)
			},
		},
		{
			name:        "session_event_at_max_var",
			blockLength: sessionEventBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionEventFields{
					ClusterSessionID: 1,
					CorrelationID:    2,
					Detail:           maxVar,
				}
				return codecs.EncodeSessionEvent(
					buf, offset, sessionEventVersion, sessionEventBlockLength, fields,
				)
			},
		},
		{
			name:        "session_event_acting_v5_block32",
			blockLength: 32,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				return codecs.EncodeSessionEvent(buf, offset, 5, 32, codecs.SessionEventFields{
					ClusterSessionID:         40,
					CorrelationID:            41,
					LeadershipTermID:         42,
					LeaderMemberID:           43,
					Code:                     gensbe.EventCode_CLOSED,
					LeaderHeartbeatTimeoutNs: math.MinInt64,
					Detail:                   maxVar,
				})
			},
		},
		{
			name:        "session_connect_request_two_var_at_max",
			blockLength: twoVarConnectBlockLength,
			varLens:     repeatLargeVarLens(2),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionConnectRequestFields{
					CorrelationID:      1,
					ResponseStreamID:   2,
					ResponseChannel:    maxVar,
					EncodedCredentials: maxVar,
				}
				return codecs.EncodeSessionConnectRequest(buf, offset, twoVarConnectVersion, fields)
			},
		},
		{
			name:        "session_connect_request_three_var_at_max",
			blockLength: threeVarConnectBlockLength,
			varLens:     repeatLargeVarLens(3),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionConnectRequestFields{
					CorrelationID:      1,
					ResponseStreamID:   2,
					ResponseChannel:    maxVar,
					EncodedCredentials: maxVar,
					ClientInfo:         maxVar,
				}
				return codecs.EncodeSessionConnectRequest(buf, offset, threeVarConnectVersion, fields)
			},
		},
		{
			name:        "session_close_request",
			blockLength: gensbe.SessionCloseRequestSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionCloseRequestFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
				}
				return codecs.EncodeSessionCloseRequest(
					buf, offset, uint16(gensbe.SessionCloseRequestSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "session_keep_alive",
			blockLength: gensbe.SessionKeepAliveSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionKeepAliveFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
				}
				return codecs.EncodeSessionKeepAlive(
					buf, offset, uint16(gensbe.SessionKeepAliveSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "new_leader_event_at_max_var",
			blockLength: gensbe.NewLeaderEventSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.NewLeaderEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
					LeaderMemberID:   3,
					IngressEndpoints: maxVar,
				}
				return codecs.EncodeNewLeaderEvent(
					buf, offset, uint16(gensbe.NewLeaderEventSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "challenge_at_max_var",
			blockLength: gensbe.ChallengeSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.ChallengeFields{
					CorrelationID:    1,
					ClusterSessionID: 2,
					EncodedChallenge: maxVar,
				}
				return codecs.EncodeChallenge(buf, offset, uint16(gensbe.ChallengeSbeSchemaVersion), fields)
			},
		},
		{
			name:        "challenge_response_at_max_var",
			blockLength: gensbe.ChallengeResponseSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.ChallengeResponseFields{
					CorrelationID:      1,
					ClusterSessionID:   2,
					EncodedCredentials: maxVar,
				}
				return codecs.EncodeChallengeResponse(
					buf, offset, uint16(gensbe.ChallengeResponseSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "admin_request_at_max_var",
			blockLength: gensbe.AdminRequestSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.AdminRequestFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
					CorrelationID:    3,
					RequestType:      gensbe.AdminRequestType_SNAPSHOT,
					Payload:          maxVar,
				}
				return codecs.EncodeAdminRequest(
					buf, offset, uint16(gensbe.AdminRequestSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "admin_response_at_max_var",
			blockLength: gensbe.AdminResponseSbeBlockLength,
			varLens:     repeatLargeVarLens(2),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.AdminResponseFields{
					ClusterSessionID: 1,
					CorrelationID:    2,
					RequestType:      gensbe.AdminRequestType_SNAPSHOT,
					ResponseCode:     gensbe.AdminResponseCode_OK,
					Message:          maxVar,
					Payload:          maxVar,
				}
				return codecs.EncodeAdminResponse(
					buf, offset, uint16(gensbe.AdminResponseSbeSchemaVersion), fields,
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertEncodeWireEndMatchesMessageLen(t, tc.offset, tc.blockLength, tc.varLens, tc.encode)
		})
	}
}

func TestEncodeClusterWire_OversizeVarField_returnsOkFalse(t *testing.T) {
	actingVersion := uint16(gensbe.SessionEventSbeSchemaVersion)
	blockLength := codecs.SessionEventBlockLength(actingVersion)
	oversizeVarLen := codecs.ClusterEncodeMaxVarLen + 1

	t.Run("cluster_wire_message_len_rejects_per_field_over_encode_max", func(t *testing.T) {
		wireLen, ok := codecs.ClusterWireMessageLen(blockLength, oversizeVarLen)
		require.False(t, ok)
		require.Zero(t, wireLen)
	})

	t.Run("encode_session_event_rejects_per_field_over_encode_max", func(t *testing.T) {
		fields := codecs.SessionEventFields{
			ClusterSessionID: 1,
			CorrelationID:    2,
			Detail:           make([]byte, oversizeVarLen),
		}
		buf := make([]byte, messageHeaderLen+int(blockLength)+codecs.VarLengthPrefixSize)

		wireEnd, ok := codecs.EncodeSessionEvent(buf, 0, actingVersion, blockLength, fields)

		require.False(t, ok)
		require.Zero(t, wireEnd)
	})
}
