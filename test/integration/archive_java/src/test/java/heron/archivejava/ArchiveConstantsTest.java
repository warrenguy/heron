package heron.archivejava;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArchiveConstantsTest {

    @Test
    void recordStreamId_matchesOracle() {
        assertEquals(1200, ArchiveConstants.RECORD_STREAM_ID);
    }

    @Test
    void replayStreamId_matchesOracle() {
        assertEquals(1201, ArchiveConstants.REPLAY_STREAM_ID);
    }

    @Test
    void localChannel_matchesOracle() {
        assertEquals("aeron:ipc?term-length=64k", ArchiveConstants.LOCAL_CHANNEL);
    }

    @Test
    void replayChannel_matchesOracle() {
        assertEquals("aeron:ipc?term-length=64k", ArchiveConstants.REPLAY_CHANNEL);
    }

    @Test
    void localControlChannel_matchesOracle() {
        assertEquals("aeron:ipc?term-length=64k", ArchiveConstants.LOCAL_CONTROL_CHANNEL);
    }

    @Test
    void localControlStreamId_matchesOracle() {
        assertEquals(10, ArchiveConstants.LOCAL_CONTROL_STREAM_ID);
    }

    @Test
    void clusterDir_matchesOracle() {
        assertEquals("/tmp/heron-cluster-0", ArchiveConstants.CLUSTER_DIR);
    }

    @Test
    void recordingLogFileName_matchesOracle() {
        assertEquals("recording.log", ArchiveConstants.RECORDING_LOG_FILE_NAME);
    }

    @Test
    void archiveDir_matchesOracle() {
        assertEquals("/tmp/heron-archive", ArchiveConstants.ARCHIVE_DIR);
    }

    @Test
    void readyMarker_matchesOracle() {
        assertEquals(".heron-archive-ready", ArchiveConstants.READY_MARKER);
    }

    @Test
    void composeAeronDir_matchesOracle() {
        assertEquals("/dev/shm/aeron", ArchiveConstants.COMPOSE_AERON_DIR);
    }

    @Test
    void termMinLength_matchesOracle() {
        assertEquals(65536, ArchiveConstants.TERM_MIN_LENGTH);
    }

    @Test
    void messageCount_matchesOracle() {
        assertEquals(16, ArchiveConstants.MESSAGE_COUNT);
    }

    @Test
    void payloadSize_matchesOracle() {
        assertEquals(64, ArchiveConstants.PAYLOAD_SIZE);
    }

    @Test
    void lowStorageSpaceThresholdBytes_matchesOracle() {
        assertEquals(16L * 1024 * 1024, ArchiveConstants.LOW_STORAGE_SPACE_THRESHOLD_BYTES);
    }

    @Test
    void segmentFileLength_matchesOracle() {
        assertEquals(16 * 1024 * 1024, ArchiveConstants.SEGMENT_FILE_LENGTH);
    }

    @Test
    void composeGateEnvKey_matchesOracle() {
        assertEquals("HERON_ARCHIVE_JAVA_COMPOSE", ArchiveConstants.COMPOSE_GATE_ENV_KEY);
    }
}
