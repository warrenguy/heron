//go:build integration && archive_java

package archivejava_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/test/integration/archive_java/archivejava"
)

func TestHarnessConstants(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "RecordStreamID", got: archivejava.RecordStreamID, want: int32(1200)},
		{name: "ReplayStreamID", got: archivejava.ReplayStreamID, want: int32(1201)},
		{name: "RecordChannel", got: archivejava.RecordChannel, want: "aeron:ipc?term-length=64k"},
		{name: "ReplayChannel", got: archivejava.ReplayChannel, want: "aeron:ipc?term-length=64k"},
		{
			name: "LocalControlChannel",
			got:  archivejava.LocalControlChannel,
			want: archive.DefaultLocalControlChannel,
		},
		{
			name: "LocalControlStreamID",
			got:  archivejava.LocalControlStreamID,
			want: archive.DefaultLocalControlStreamID,
		},
		{name: "ClusterDir", got: archivejava.ClusterDir, want: "/tmp/heron-cluster-0"},
		{name: "RecordingLogFileName", got: archivejava.RecordingLogFileName, want: "recording.log"},
		{name: "ArchiveDir", got: archivejava.ArchiveDir, want: "/tmp/heron-archive"},
		{name: "ReadyMarker", got: archivejava.ReadyMarker, want: ".heron-archive-ready"},
		{name: "ComposeAeronDir", got: archivejava.ComposeAeronDir, want: "/dev/shm/aeron"},
		{name: "MessageCount", got: archivejava.MessageCount, want: 16},
		{name: "PayloadSize", got: archivejava.PayloadSize, want: 64},
		{
			name: "LowStorageSpaceThresholdBytes",
			got:  archivejava.LowStorageSpaceThresholdBytes,
			want: 16 * 1024 * 1024,
		},
		{
			name: "SegmentFileLength",
			got:  archivejava.SegmentFileLength,
			want: 16 * 1024 * 1024,
		},
		{name: "TermMinLength", got: archivejava.TermMinLength, want: 65536},
		{
			name: "ComposeGateEnvKey",
			got:  archivejava.ComposeGateEnvKey,
			want: "HERON_ARCHIVE_JAVA_COMPOSE",
		},
		{
			name: "ComposeSkipHint",
			got:  archivejava.ComposeSkipHint,
			want: "archive java requires docker compose; run make smoke-archive-java",
		},
		{
			name: "ArchiveDirEnvKey",
			got:  archivejava.ArchiveDirEnvKey,
			want: "HERON_ARCHIVE_DIR",
		},
		{
			name: "ControlChannelEnvKey",
			got:  archivejava.ControlChannelEnvKey,
			want: "HERON_ARCHIVE_CONTROL_CHANNEL",
		},
		{
			name: "ReplicationChannelEnvKey",
			got:  archivejava.ReplicationChannelEnvKey,
			want: "HERON_ARCHIVE_REPLICATION_CHANNEL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func archivePayloadGoldenHex(prefixHex string) string {
	prefixBytes := len(prefixHex) / 2
	padBytes := archivejava.PayloadSize - prefixBytes
	return prefixHex + strings.Repeat("00", padBytes)
}

func TestPayloads_goldenBytes(t *testing.T) {
	tests := []struct {
		name    string
		index   int
		wantHex string
	}{
		{name: "index0", index: 0, wantHex: archivePayloadGoldenHex("617263686976652d30")},
		{name: "index1", index: 1, wantHex: archivePayloadGoldenHex("617263686976652d31")},
		{name: "index10", index: 10, wantHex: archivePayloadGoldenHex("617263686976652d3130")},
		{name: "index15", index: 15, wantHex: archivePayloadGoldenHex("617263686976652d3135")},
	}

	payloads := archivejava.Payloads(archivejava.MessageCount, archivejava.PayloadSize)
	require.Len(t, payloads, archivejava.MessageCount)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantHex, hex.EncodeToString(payloads[tt.index]))
		})
	}
}
