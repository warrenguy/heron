package codecs_test

import (
	"encoding/binary"
	"testing"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestReadVarSlice_ZeroAllocs(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[2:], 4)
	copy(buf[6:], []byte("data"))

	testutil.AssertZeroAllocs(t, "ReadVarSlice", 1000, func() {
		_, _, _ = codecs.ReadVarSlice(buf, 2, 8)
	})
}

func runTryDecodeAtZeroAllocs(
	t *testing.T,
	name string,
	wire []byte,
	decode func([]byte, int, int),
) {
	t.Helper()
	testutil.AssertZeroAllocs(t, name, 1000, func() {
		decode(wire, 0, codecTestMaxVarLen)
	})
}

func TestTryDecodeAt_ZeroAllocs(t *testing.T) {
	cases := []struct {
		name   string
		wire   func(t *testing.T) []byte
		decode func([]byte, int, int)
	}{
		{
			name: "ControlResponse",
			wire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testControlResponseBoom, encodeControlResponseAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) {
				_, _ = codecs.TryDecodeControlResponseAt(buf, offset, maxVarLen)
			},
		},
		{
			name: "RecordingSignalEvent",
			wire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testRecordingSignalEventStart, encodeRecordingSignalEventAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) {
				_, _ = codecs.TryDecodeRecordingSignalEventAt(buf, offset, maxVarLen)
			},
		},
		{
			name: "RecordingDescriptor",
			wire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testRecordingDescriptorChannels, encodeRecordingDescriptorAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) {
				_, _ = codecs.TryDecodeRecordingDescriptorAt(buf, offset, maxVarLen)
			},
		},
		{
			name: "Challenge",
			wire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testChallengeWithBytes, encodeChallengeAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) {
				_, _ = codecs.TryDecodeChallengeAt(buf, offset, maxVarLen)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runTryDecodeAtZeroAllocs(t, "TryDecode"+tc.name+"At", tc.wire(t), tc.decode)
		})
	}
}
