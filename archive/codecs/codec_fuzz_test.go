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
		mustEncodeArchiveWire(
			codecs.ReplicateRequest2Fields{
				ControlSessionID:     1,
				CorrelationID:        2,
				SrcRecordingID:       3,
				DstRecordingID:       -1,
				StopPosition:         -1,
				ChannelTagID:         -1,
				SubscriptionTagID:    -1,
				SrcControlStreamID:   10,
				FileIoMaxLength:      -1,
				ReplicationSessionID: -1,
				SrcControlChannel:    []byte("aeron:udp?endpoint=localhost:8010"),
				LiveDestination:      []byte("aeron:udp?endpoint=localhost:40456"),
				ReplicationChannel:   []byte("aeron:udp?endpoint=localhost:0"),
				EncodedCredentials:   []byte{1, 2, 3},
				SrcResponseChannel:   []byte("aeron:udp?control-mode=response"),
			},
			func(buf []byte, offset int, fields codecs.ReplicateRequest2Fields) (int, bool) {
				return codecs.EncodeReplicateRequest2(buf, offset, archiveSchemaVersion, fields)
			},
		),
		mustEncodeArchiveWire(
			codecs.StopReplicationRequestFields{
				ControlSessionID: 1,
				CorrelationID:    2,
				ReplicationID:    3,
			},
			func(buf []byte, offset int, fields codecs.StopReplicationRequestFields) (int, bool) {
				return codecs.EncodeStopReplicationRequest(buf, offset, archiveSchemaVersion, fields)
			},
		),
		mustEncodeArchiveWire(
			codecs.ArchiveIDRequestFields{ControlSessionID: 1, CorrelationID: 2},
			func(buf []byte, offset int, fields codecs.ArchiveIDRequestFields) (int, bool) {
				return codecs.EncodeArchiveIDRequest(buf, offset, archiveSchemaVersion, fields)
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
