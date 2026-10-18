package codecs_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
)

func mustEncodeArchiveWire[T any](
	fields T,
	encode func([]byte, int, T) (int, bool),
) []byte {
	buf := make([]byte, fuzzArchiveCodecMaxLen)
	wireEnd, ok := encode(buf, 0, fields)
	if !ok {
		panic("encode failed")
	}
	return bytes.Clone(buf[:wireEnd])
}

func archiveCodecFuzzSeedBuffers() [][]byte {
	return [][]byte{
		mustEncodeArchiveWire(testControlResponseBoom, encodeControlResponseAt),
		mustEncodeArchiveWire(testRecordingDescriptorChannels, encodeRecordingDescriptorAt),
		mustEncodeArchiveWire(
			testStartRecordingRequestIPC,
			func(buf []byte, offset int, fields codecs.StartRecordingRequestFields) (int, bool) {
				return codecs.EncodeStartRecordingRequest(buf, offset, archiveSchemaVersion, fields)
			},
		),
		mustEncodeArchiveWire(
			codecs.StopRecordingByIdentityRequestFields{
				ControlSessionID: 1,
				CorrelationID:    2,
				RecordingID:      3,
			},
			func(
				buf []byte,
				offset int,
				fields codecs.StopRecordingByIdentityRequestFields,
			) (int, bool) {
				return codecs.EncodeStopRecordingByIdentityRequest(buf, offset, archiveSchemaVersion, fields)
			},
		),
	}
}

func FuzzArchiveCodecDecode(f *testing.F) {
	for _, seed := range archiveCodecFuzzSeedBuffers() {
		f.Add(seed, uint8(0))
	}
	offsetProbeWire := []byte{0x20, 0x00, 0x01, 0x00, 0x65, 0x00, 0x0d, 0x00}
	for offset := uint8(1); offset <= codecFuzzMaxOffset; offset++ {
		f.Add(offsetProbeWire, offset)
	}

	f.Fuzz(func(t *testing.T, data []byte, offset uint8) {
		if len(data) == 0 || len(data) > fuzzArchiveCodecMaxLen {
			return
		}
		require.NotPanics(t, func() {
			_, _ = codecs.TryDecodeByTemplate(
				data,
				int(offset%codecFuzzOffsetSpan),
				codecs.ArchiveDecodeMaxVarLen,
			)
		})
	})
}
