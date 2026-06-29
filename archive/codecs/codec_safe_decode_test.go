package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

func requireFastSlowParity[T any](
	tb require.TestingT,
	buf []byte,
	offset, maxVarLen int,
	templateID uint16,
	fast func([]byte, int, int) (T, bool),
) {
	got, fastOK := fast(buf, offset, maxVarLen)
	slow, slowOK := codecs.TryDecodeByTemplate(buf, offset, maxVarLen)

	slowMatchOK := slowOK && slow.TemplateID == templateID
	require.Equal(tb, fastOK, slowMatchOK)

	if fastOK {
		requireDecodedArchiveFields(tb, got, slow.Fields)
	}
}

func runFastSlowParityTable[T any](
	t *testing.T,
	fields T,
	encode func([]byte, int, T) (int, bool),
	templateID uint16,
	fast func([]byte, int, int) (T, bool),
) {
	t.Helper()
	validWire := encodeArchiveWireAt(t, 0, fields, encode)

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
			buf:  cloneAndMutate(validWire, func(w []byte) { w[2], w[3] = 0xFF, 0x7F }),
		},
		{
			name: "schema_mismatch",
			buf:  cloneAndMutate(validWire, func(w []byte) { w[4], w[5] = 0, 0 }),
		},
		{name: "offset_out_of_bounds", buf: validWire, offset: 10_000},
		{name: "offset_near_maxint", buf: validWire, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireFastSlowParity(t, tc.buf, tc.offset, codecTestMaxVarLen, templateID, fast)
		})
	}
}

func TestControlResponse_fastSlowDecodeParity_table(t *testing.T) {
	runFastSlowParityTable(
		t,
		testControlResponseBoom,
		encodeControlResponseAt,
		gensbe.ControlResponseSbeTemplateID,
		codecs.TryDecodeControlResponseAt,
	)
}

func TestRecordingSignalEvent_fastSlowDecodeParity_table(t *testing.T) {
	runFastSlowParityTable(
		t,
		testRecordingSignalEventStart,
		encodeRecordingSignalEventAt,
		gensbe.RecordingSignalEventSbeTemplateID,
		codecs.TryDecodeRecordingSignalEventAt,
	)
}

func TestRecordingDescriptor_fastSlowDecodeParity_table(t *testing.T) {
	runFastSlowParityTable(
		t,
		testRecordingDescriptorChannels,
		encodeRecordingDescriptorAt,
		gensbe.RecordingDescriptorSbeTemplateID,
		codecs.TryDecodeRecordingDescriptorAt,
	)
}

func TestChallenge_fastSlowDecodeParity_table(t *testing.T) {
	runFastSlowParityTable(
		t,
		testChallengeWithBytes,
		encodeChallengeAt,
		gensbe.ChallengeSbeTemplateID,
		codecs.TryDecodeChallengeAt,
	)
}
