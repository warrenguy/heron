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
	actingVersion := gensbe.SessionEventSbeSchemaVersion
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
	validEventWire := encodeSessionEventWire(t, actingVersion, validEvent)
	validConnectWire := encodeSessionConnectRequestWire(
		t, sessionConnectClientInfoMinVersion, validConnect,
	)

	mutateEventWire := func(mutate func([]byte)) []byte {
		wire := append([]byte(nil), validEventWire...)
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
			buf:  func() []byte { return validEventWire[:4] },
		},
		{
			name:   "offset_out_of_bounds",
			buf:    func() []byte { return validEventWire },
			offset: 10000,
		},
		{
			name:   "offset_near_maxint",
			buf:    func() []byte { return validEventWire },
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
			buf:  func() []byte { return validEventWire[:messageHeaderLen+10] },
		},
		{
			name: "truncated_var_field",
			buf:  func() []byte { return validEventWire[:len(validEventWire)-2] },
		},
		{
			name: "var_field_exceeds_max_var_len",
			buf: func() []byte {
				fields := validEvent
				fields.Detail = bytes.Repeat([]byte{0x41}, 4)
				return encodeSessionEventWire(t, actingVersion, fields)
			},
			maxLen: 1,
		},
		{
			name: "connect_truncated_client_info",
			buf:  func() []byte { return validConnectWire[:len(validConnectWire)-1] },
		},
		{
			name:   "negative_max_var_len",
			buf:    func() []byte { return validEventWire },
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
	fields := sbe.SessionEventFields{
		ClusterSessionID: 1,
		CorrelationID:    2,
		LeadershipTermID: 3,
		LeaderMemberID:   4,
		Code:             gensbe.EventCode_OK,
		Detail:           []byte("x"),
	}
	wire := encodeSessionEventWire(t, gensbe.SessionEventSbeSchemaVersion, fields)

	_, ok := sbe.ClusterMessageWireEnd(wire, 0, -1)
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
	buf, wireEnd := encodeSessionEventInto(t, gensbe.SessionEventSbeSchemaVersion, fields)

	// Simulate Aeron fragment buffer with trailing garbage after the message.
	buf[wireEnd] = 0xDE
	buf[wireEnd+1] = 0xAD
	decoded, ok := sbe.TryDecodeByTemplate(buf[:wireEnd+8], 0, codecTestMaxVarLen)
	require.True(t, ok)
	require.Equal(t, gensbe.SessionEventSbeTemplateID, decoded.TemplateID)
	requireDecodedClusterFields(
		t,
		sbe.SessionEventOracleFields(fields, gensbe.SessionEventSbeSchemaVersion),
		decoded.Fields,
	)
}
