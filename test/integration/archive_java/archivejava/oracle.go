//go:build integration && archive_java

// Package archivejava is the Java archive integration contract oracle.
// Hand-sync with ArchiveConstantsTest (like java_interop).
package archivejava

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/client"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	RecordStreamID int32 = 1200
	ReplayStreamID int32 = 1201
)

const (
	termLength64kParam   = "term-length=64k"
	RecordChannel        = harness.IPCChannel + "?" + termLength64kParam
	ReplayChannel        = harness.IPCChannel + "?" + termLength64kParam
	LocalControlChannel  = client.DefaultLocalControlChannel
	LocalControlStreamID = client.DefaultLocalControlStreamID
)

const (
	ClusterDir           = "/tmp/heron-cluster-0"
	RecordingLogFileName = "recording.log"
	ArchiveDir           = "/tmp/heron-archive"
	ReadyMarker          = ".heron-archive-ready"
	ComposeAeronDir      = "/dev/shm/aeron"
)

const (
	MessageCount = 16
	PayloadSize  = 64
)

const (
	// LowStorageSpaceThresholdBytes matches ArchiveConstants.LOW_STORAGE_SPACE_THRESHOLD_BYTES.
	LowStorageSpaceThresholdBytes = 16 * 1024 * 1024

	// SegmentFileLength matches ArchiveConstants.SEGMENT_FILE_LENGTH.
	SegmentFileLength = 16 * 1024 * 1024
)

const (
	ComposeGateEnvKey  = "HERON_ARCHIVE_JAVA_COMPOSE"
	composeSkipRunMake = "requires docker compose; run make "
	ComposeSkipHint    = "archive java " + composeSkipRunMake + "test-archive-java"
)

const TermMinLength = logbuffer.TermMinLength

// Payloads builds count fixed-size payloads for archive record smoke parity.
//
// Index i is ASCII "archive-" followed by decimal i, padded with 0x00 to size bytes.
func Payloads(count, size int) [][]byte {
	return harness.PayloadsWithPrefix("archive-", count, size)
}
