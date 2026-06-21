package codecs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

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
	connectWire := encodeSessionConnectRequestWire(t, actingVersion, connectFields)
	connectWireLen := len(connectWire)

	challengeFields := codecs.ChallengeResponseFields{
		CorrelationID:      4,
		ClusterSessionID:   5,
		EncodedCredentials: []byte{6, 7, 8},
	}
	challengeWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeChallengeResponse(
			buf,
			0,
			uint16(gensbe.ChallengeResponseSbeSchemaVersion),
			challengeFields,
		)
	})
	challengeWireLen := len(challengeWire)

	cases := []struct {
		name   string
		bufLen int
		encode func(buf []byte) (wireEnd int, ok bool)
	}{
		{
			name:   "connect_header_only",
			bufLen: messageHeaderLen,
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeSessionConnectRequest(buf, 0, actingVersion, connectFields)
			},
		},
		{
			name:   "connect_one_byte_short",
			bufLen: connectWireLen - 1,
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeSessionConnectRequest(buf, 0, actingVersion, connectFields)
			},
		},
		{
			name:   "connect_exact_fit",
			bufLen: connectWireLen,
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeSessionConnectRequest(buf, 0, actingVersion, connectFields)
			},
		},
		{
			name:   "challenge_header_only",
			bufLen: messageHeaderLen,
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeChallengeResponse(
					buf,
					0,
					uint16(gensbe.ChallengeResponseSbeSchemaVersion),
					challengeFields,
				)
			},
		},
		{
			name:   "challenge_one_byte_short",
			bufLen: challengeWireLen - 1,
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeChallengeResponse(
					buf,
					0,
					uint16(gensbe.ChallengeResponseSbeSchemaVersion),
					challengeFields,
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			wireEnd, ok := tc.encode(buf)
			if tc.name == "connect_exact_fit" {
				require.True(t, ok)
				require.Equal(t, connectWireLen, wireEnd)
				require.Equal(t, connectWire, buf[:wireEnd])
				return
			}
			require.False(t, ok)
			require.Zero(t, wireEnd)
		})
	}
}

func TestEncodeClusterWire_OversizeVarField_returnsOkFalse(t *testing.T) {
	actingVersion := uint16(gensbe.SessionEventSbeSchemaVersion)
	blockLength := codecs.SessionEventBlockLength(actingVersion)
	detail := make([]byte, codecs.ClusterMaxVarLen+1)
	fields := codecs.SessionEventFields{
		ClusterSessionID: 1,
		CorrelationID:    2,
		Detail:           detail,
	}
	buf := make([]byte, codecs.ClusterMaxVarLen+int(blockLength)+codecs.MessageHeaderEncodedLen())

	wireEnd, ok := codecs.EncodeSessionEvent(buf, 0, actingVersion, blockLength, fields)

	require.False(t, ok)
	require.Zero(t, wireEnd)
}
