package codecs_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

func requireSessionMessageHeaderFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	fast, fastOK := codecs.TryDecodeSessionMessageHeaderAt(buf, offset)
	slow, slowOK := codecs.TryDecodeByTemplate(buf, offset, codecTestMaxVarLen)

	slowHeaderOK := slowOK && slow.TemplateID == gensbe.SessionMessageHeaderSbeTemplateID
	require.Equal(tb, fastOK, slowHeaderOK)

	if fastOK {
		requireDecodedFieldsEqual[codecs.SessionMessageHeaderFields](tb, fast, slow.Fields)
	}
}

func encodeSessionMessageHeaderAt(
	tb require.TestingT,
	offset int,
	actingVersion uint16,
	fields codecs.SessionMessageHeaderFields,
) []byte {
	buf := make([]byte, offset+fuzzClusterCodecMaxLen)
	wireEnd, ok := codecs.EncodeSessionMessageHeader(buf, offset, actingVersion, fields)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func TestSessionMessageHeader_fastSlowDecodeParity_table(t *testing.T) {
	actingVersion := uint16(gensbe.SessionMessageHeaderSbeSchemaVersion)
	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	}
	validWire := encodeSessionMessageHeaderAt(t, 0, actingVersion, fields)

	mutateWire := func(mutate func([]byte)) []byte {
		wire := bytes.Clone(validWire)
		mutate(wire)
		return wire
	}

	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "valid", buf: validWire},
		{name: "truncated_header", buf: validWire[:messageHeaderLen-1]},
		{name: "one_byte_short", buf: validWire[:len(validWire)-1]},
		{
			name: "unknown_template_id",
			buf: mutateWire(func(w []byte) {
				w[2], w[3] = 0xFF, 0x7F
			}),
		},
		{
			name: "schema_mismatch",
			buf: mutateWire(func(w []byte) {
				w[4], w[5] = 0, 0
			}),
		},
		{name: "offset_out_of_bounds", buf: validWire, offset: 10_000},
		{name: "offset_near_maxint", buf: validWire, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireSessionMessageHeaderFastSlowParity(t, tc.buf, tc.offset)
		})
	}
}
