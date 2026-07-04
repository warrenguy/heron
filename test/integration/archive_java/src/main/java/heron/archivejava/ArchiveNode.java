package heron.archivejava;

import io.aeron.archive.Archive;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.nio.file.Files;

public final class ArchiveNode {

    private ArchiveNode() {
    }

    public static void main(final String[] args) throws Exception {
        final String aeronDir = System.getenv("AERON_DIR");
        if (aeronDir == null || aeronDir.isEmpty()) {
            throw new IllegalStateException("AERON_DIR required");
        }

        final File archiveDir = new File(ArchiveConstants.ARCHIVE_DIR);
        if (!archiveDir.exists() && !archiveDir.mkdirs()) {
            throw new IllegalStateException("failed to create archive dir: " + archiveDir);
        }

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(archiveDir)
            .controlChannel(ArchiveConstants.CONTROL_CHANNEL)
            .replicationChannel(ArchiveConstants.REPLICATION_CHANNEL)
            .localControlChannel(ArchiveConstants.LOCAL_CONTROL_CHANNEL)
            .localControlStreamId(ArchiveConstants.LOCAL_CONTROL_STREAM_ID)
            .lowStorageSpaceThreshold(ArchiveConstants.LOW_STORAGE_SPACE_THRESHOLD_BYTES)
            .segmentFileLength(ArchiveConstants.SEGMENT_FILE_LENGTH)
            .errorHandler(Throwable::printStackTrace);

        final File readyMarker = new File(archiveDir, ArchiveConstants.READY_MARKER);
        if (readyMarker.exists() && !readyMarker.delete()) {
            throw new IllegalStateException("failed to delete stale ready marker: " + readyMarker);
        }

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
             Archive archive = Archive.launch(archiveCtx)) {
            Files.writeString(readyMarker.toPath(), Long.toString(archive.context().archiveId()));
            System.out.println("heron: archive ready archiveId=" + archive.context().archiveId());
            barrier.await();
        }
    }
}
