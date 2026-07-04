package heron.archivejava;

public final class ArchiveConstants {

    // archivejava.RecordStreamID
    public static final int RECORD_STREAM_ID = 1200;

    // archivejava.ReplayStreamID
    public static final int REPLAY_STREAM_ID = 1201;

    // harness.IPCChannel
    public static final String IPC_CHANNEL = "aeron:ipc";

    private static final String TERM_LENGTH_64K_PARAM = "term-length=64k";
    private static final String IPC_CHANNEL_64K = IPC_CHANNEL + "?" + TERM_LENGTH_64K_PARAM;

    // archivejava.RecordChannel
    public static final String LOCAL_CHANNEL = IPC_CHANNEL_64K;

    // archivejava.ReplayChannel
    public static final String REPLAY_CHANNEL = IPC_CHANNEL_64K;

    // Server-side UDP control (Aeron 1.51+); Go client uses local IPC control only.
    public static final String CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9010";

    // Required by Aeron 1.51+ Archive.Context; unused in this co-located smoke.
    public static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    // archivejava.LocalControlChannel
    public static final String LOCAL_CONTROL_CHANNEL = IPC_CHANNEL_64K;

    // archivejava.LocalControlStreamID
    public static final int LOCAL_CONTROL_STREAM_ID = 10;

    // archivejava.ClusterDir
    public static final String CLUSTER_DIR = "/tmp/heron-cluster-0";

    // archivejava.RecordingLogFileName
    public static final String RECORDING_LOG_FILE_NAME = "recording.log";

    // archivejava.ArchiveDir
    public static final String ARCHIVE_DIR = "/tmp/heron-archive";

    // Below compose archive-data tmpfs (256m); Java default 128m rejects StartRecording.
    public static final long LOW_STORAGE_SPACE_THRESHOLD_BYTES = 16L * 1024 * 1024;

    // Java default segment file length is 128m; must fit archive-data tmpfs (256m).
    public static final int SEGMENT_FILE_LENGTH = 16 * 1024 * 1024;

    // archivejava.ReadyMarker
    public static final String READY_MARKER = ".heron-archive-ready";

    // archivejava.ComposeAeronDir
    public static final String COMPOSE_AERON_DIR = "/dev/shm/aeron";

    // archivejava.TermMinLength / logbuffer.TermMinLength
    public static final int TERM_MIN_LENGTH = 65536;

    // archivejava.MessageCount
    public static final int MESSAGE_COUNT = 16;

    // archivejava.PayloadSize
    public static final int PAYLOAD_SIZE = 64;

    // archivejava.ComposeGateEnvKey
    public static final String COMPOSE_GATE_ENV_KEY = "HERON_ARCHIVE_JAVA_COMPOSE";

    private ArchiveConstants() {
    }
}
