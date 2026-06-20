package codecs_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

func TestEncodeIngressHeader_PayloadOffsetGolden(t *testing.T) {
	const frameOffset = 128
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	payloadOffset := frameOffset + logbuffer.OffsetFramePayload
	buf := bytes.Repeat([]byte{0xAA}, payloadOffset+headerLen)

	fields := codecs.IngressHeaderFields{
		LeadershipTermID: 11,
		ClusterSessionID: 22,
		Timestamp:        33,
	}
	encoded, err := codecs.EncodeIngressHeader(buf, frameOffset, fields)
	require.NoError(t, err)
	require.Equal(t, headerLen, encoded)

	wantFrame := bytes.Repeat([]byte{0xAA}, logbuffer.OffsetFramePayload)
	require.Equal(t, wantFrame, buf[frameOffset:payloadOffset])

	requireDecodedIngressHeader(t, buf[payloadOffset:payloadOffset+headerLen], fields)
}

func TestEncodeIngressHeader_BufferBounds(t *testing.T) {
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	_, fields := ingressEncodeFixture(0)

	cases := []struct {
		name        string
		bufLen      int
		frameOffset int
		wantErr     error
	}{
		{
			name:    "exact_fit",
			bufLen:  logbuffer.OffsetFramePayload + headerLen,
			wantErr: nil,
		},
		{
			name:    "one_byte_short",
			bufLen:  logbuffer.OffsetFramePayload + headerLen - 1,
			wantErr: codecs.ErrIngressHeaderBufferTooSmall,
		},
		{
			name:    "no_payload_room",
			bufLen:  logbuffer.OffsetFramePayload,
			wantErr: codecs.ErrIngressHeaderBufferTooSmall,
		},
		{
			name:        "negative_frameOffset",
			bufLen:      logbuffer.OffsetFramePayload + headerLen,
			frameOffset: -logbuffer.OffsetFramePayload - 1,
			wantErr:     codecs.ErrIngressHeaderBufferTooSmall,
		},
		{
			name:        "frameOffset_near_maxint",
			bufLen:      logbuffer.OffsetFramePayload + headerLen,
			frameOffset: math.MaxInt - logbuffer.OffsetFramePayload,
			wantErr:     codecs.ErrIngressHeaderBufferTooSmall,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			encoded, err := codecs.EncodeIngressHeader(buf, tc.frameOffset, fields)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, headerLen, encoded)
		})
	}
}

func ingressEncodeFixture(frameOffset int) ([]byte, codecs.IngressHeaderFields) {
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	buf := make([]byte, frameOffset+logbuffer.OffsetFramePayload+headerLen)
	return buf, codecs.IngressHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	}
}
