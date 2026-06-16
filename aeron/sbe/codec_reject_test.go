package sbe_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

func TestTryDecodeByTemplate_rejects(t *testing.T) {
	actingVersion := uint16(gensbe.SessionEventSbeSchemaVersion)
	validEvent := sbe.SessionEventFields{
		ClusterSessionID:         1,
		CorrelationID:            2,
		LeadershipTermID:         3,
		LeaderMemberID:           4,
		Code:                     gensbe.EventCode_OK,
		Detail:                   []byte("x"),
		LeaderHeartbeatTimeoutNs: 0,
	}
	validConnect := sbe.SessionConnectRequestFields{
		CorrelationID:      10,
		ResponseStreamID:   20,
		ResponseChannel:    []byte("aeron:ipc"),
		EncodedCredentials: []byte{1, 2},
		ClientInfo:         []byte("client"),
	}

	encodeValid := func(template string) []byte {
		t.Helper()
		buf := make([]byte, fuzzClusterCodecMaxLen)
		var wireEnd int
		var ok bool
		switch template {
		case "sessionEvent":
			wireEnd, ok = encodeSessionEvent(
				buf, 0, actingVersion, gensbe.SessionEventSbeBlockLength, validEvent,
			)
		case "sessionConnect":
			wireEnd, ok = encodeSessionConnectRequest(buf, 0, 14, validConnect)
		default:
			t.Fatalf("unknown template %q", template)
		}
		require.True(t, ok)
		return buf[:wireEnd]
	}

	mutateEventWire := func(mutate func([]byte)) []byte {
		wire := append([]byte(nil), encodeValid("sessionEvent")...)
		mutate(wire)
		return wire
	}

	cases := []struct {
		name   string
		buf    func() []byte
		offset int
		maxLen int
	}{
		{
			name: "truncated_header",
			buf: func() []byte {
				return encodeValid("sessionEvent")[:4]
			},
		},
		{
			name:   "offset_out_of_bounds",
			buf:    func() []byte { return encodeValid("sessionEvent") },
			offset: 10000,
		},
		{
			name:   "offset_near_maxint",
			buf:    func() []byte { return encodeValid("sessionEvent") },
			offset: math.MaxInt,
		},
		{
			name: "schema_mismatch",
			buf: func() []byte {
				return mutateEventWire(func(w []byte) { w[4], w[5] = 0, 0 })
			},
		},
		{
			name: "unknown_template_id",
			buf: func() []byte {
				return mutateEventWire(func(w []byte) { w[2], w[3] = 0xFF, 0x7F })
			},
		},
		{
			name: "block_length_below_min",
			buf: func() []byte {
				return mutateEventWire(func(w []byte) { w[0] = 31 })
			},
		},
		{
			name: "block_length_above_max",
			buf: func() []byte {
				return mutateEventWire(func(w []byte) {
					w[0] = byte(gensbe.SessionEventSbeBlockLength + 1)
					w[1] = 0
				})
			},
		},
		{
			name: "truncated_fixed_block",
			buf: func() []byte {
				return encodeValid("sessionEvent")[:messageHeaderLen+10]
			},
		},
		{
			name: "truncated_var_field",
			buf: func() []byte {
				wire := encodeValid("sessionEvent")
				return wire[:len(wire)-2]
			},
		},
		{
			name: "var_field_exceeds_max_var_len",
			buf: func() []byte {
				fields := validEvent
				fields.Detail = bytes.Repeat([]byte{0x41}, 4)
				buf := make([]byte, fuzzClusterCodecMaxLen)
				wireEnd, ok := encodeSessionEvent(
					buf, 0, actingVersion, gensbe.SessionEventSbeBlockLength, fields,
				)
				require.True(t, ok)
				return buf[:wireEnd]
			},
			maxLen: 1,
		},
		{
			name: "connect_truncated_client_info",
			buf: func() []byte {
				wire := encodeValid("sessionConnect")
				return wire[:len(wire)-1]
			},
		},
		{
			name:   "negative_max_var_len",
			buf:    func() []byte { return encodeValid("sessionEvent") },
			maxLen: -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maxLen := tc.maxLen
			if maxLen == 0 {
				maxLen = codecTestMaxVarLen
			}
			_, ok := sbe.TryDecodeByTemplate(tc.buf(), tc.offset, maxLen)
			require.False(t, ok)
		})
	}
}

func TestClusterMessageWireEnd_rejects_negative_max_var_len(t *testing.T) {
	buf := make([]byte, fuzzClusterCodecMaxLen)
	fields := sbe.SessionEventFields{
		ClusterSessionID: 1,
		CorrelationID:    2,
		LeadershipTermID: 3,
		LeaderMemberID:   4,
		Code:             gensbe.EventCode_OK,
		Detail:           []byte("x"),
	}
	wireEnd, ok := encodeSessionEvent(
		buf, 0, gensbe.SessionEventSbeSchemaVersion, gensbe.SessionEventSbeBlockLength, fields,
	)
	require.True(t, ok)

	_, ok = sbe.ClusterMessageWireEnd(buf[:wireEnd], 0, -1)
	require.False(t, ok)
}

func TestTryDecodeByTemplate_allows_trailing_bytes(t *testing.T) {
	fields := sbe.SessionEventFields{
		ClusterSessionID:         1,
		CorrelationID:            2,
		LeadershipTermID:         3,
		LeaderMemberID:           4,
		Code:                     gensbe.EventCode_OK,
		Detail:                   []byte("tail"),
		LeaderHeartbeatTimeoutNs: 0,
	}
	buf := make([]byte, fuzzClusterCodecMaxLen)
	wireEnd, ok := encodeSessionEvent(
		buf, 0, gensbe.SessionEventSbeSchemaVersion, gensbe.SessionEventSbeBlockLength, fields,
	)
	require.True(t, ok)

	// Simulate Aeron fragment buffer with trailing garbage after the message.
	buf[wireEnd] = 0xDE
	buf[wireEnd+1] = 0xAD
	decoded, ok := sbe.TryDecodeByTemplate(buf[:wireEnd+8], 0, codecTestMaxVarLen)
	require.True(t, ok)
	require.Equal(t, gensbe.SessionEventSbeTemplateID, decoded.TemplateID)
	requireDecodedClusterFields(
		t,
		sessionEventOracleFields(fields, gensbe.SessionEventSbeSchemaVersion),
		decoded.Fields,
	)
}
