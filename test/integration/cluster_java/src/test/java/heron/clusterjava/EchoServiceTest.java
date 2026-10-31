package heron.clusterjava;

import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EchoServiceTest {
    @Test
    void onSessionMessage_offersInboundBytes() {
        final RecordingClientSession session = new RecordingClientSession(1L);
        final EchoService service = newStartedEchoService();
        final byte[] inbound = { 1, 2, 3, 4, 5 };
        final UnsafeBuffer buffer = new UnsafeBuffer(inbound);

        service.onSessionMessage(session, 0L, buffer, 1, 3, null);

        assertEquals(1, session.offerCount());
        session.assertLastOfferUsed(buffer, 1, 3);
        assertArrayEquals(new byte[] { 2, 3, 4 }, session.offeredBytes());
    }

    @Test
    void onSessionMessage_retriesOnBackPressure() {
        final RecordingClientSession session = RecordingClientSession.backPressuredThen(42L);
        final EchoService service = newStartedEchoService();
        final byte[] inbound = { 9, 8, 7 };
        final UnsafeBuffer buffer = new UnsafeBuffer(inbound);

        service.onSessionMessage(session, 0L, buffer, 0, inbound.length, null);

        assertEquals(EchoService.SEND_ATTEMPTS, session.offerCount());
        session.assertLastOfferUsed(buffer, 0, inbound.length);
        assertArrayEquals(inbound, session.offeredBytes());
        assertEquals(42L, session.lastReturnedResult());
    }

    @Test
    void onSessionMessage_logsWhenOfferExhausted() {
        final RecordingClientSession session = RecordingClientSession.allBackPressured();
        final EchoService service = newStartedEchoService();
        final byte[] inbound = { 5, 6 };
        final UnsafeBuffer buffer = new UnsafeBuffer(inbound);

        final String stderr = ClusterNodeTest.captureStderr(
            () -> service.onSessionMessage(session, 0L, buffer, 0, inbound.length, null)
        );

        assertEquals(EchoService.SEND_ATTEMPTS, session.offerCount());
        session.assertLastOfferUsed(buffer, 0, inbound.length);
        assertTrue(session.lastReturnedResult() <= 0);
        assertEquals(EchoService.OFFER_EXHAUSTED_MESSAGE + System.lineSeparator(), stderr);
    }

    @Test
    void formatServiceProbe_keyValueLines() {
        final String content = EchoService.formatServiceProbe(7L, 128L, "FOLLOWER", 1);
        assertEquals(
            "term=7\nlogPosition=128\nrole=FOLLOWER\nmemberId=1\n",
            content);
    }

    @Test
    void formatSnapshotProbe_keyValueLine() {
        assertEquals("logPosition=128\n", EchoService.formatSnapshotProbe(128L));
    }

    @Test
    void writeSnapshotProbe_writesLogPositionUnderBaseDir(@TempDir final Path tempDir) throws IOException {
        final EchoService service = startedWithClusterDir(tempDir, 1, Cluster.Role.FOLLOWER);

        service.writeSnapshotProbe(4096L);

        final Path probe = tempDir.resolve(ClusterConstants.SNAPSHOT_PROBE_FILENAME);
        assertTrue(Files.isRegularFile(probe), "probe file missing: " + probe);
        assertFalse(Files.exists(tempDir.resolve(ClusterConstants.SNAPSHOT_PROBE_FILENAME + ".tmp")));
        assertEquals("logPosition=4096\n", Files.readString(probe, StandardCharsets.US_ASCII));
    }

    @Test
    void onStart_skipsSnapshotProbeWithoutImage(@TempDir final Path tempDir) throws IOException {
        final ProbeCluster cluster = newProbeCluster(tempDir, 1, Cluster.Role.FOLLOWER, 4096L);
        assertEquals(4096L, cluster.logPosition());

        final EchoService service = new EchoService();
        service.onStart(cluster, null);

        assertFalse(Files.exists(tempDir.resolve(ClusterConstants.SNAPSHOT_PROBE_FILENAME)));
    }

    @Test
    void onNewLeadershipTermEvent_writesProbeUnderBaseDir(@TempDir final Path tempDir)
        throws IOException
    {
        final EchoService service = startedWithClusterDir(tempDir, 1, Cluster.Role.FOLLOWER);

        service.onNewLeadershipTermEvent(
            3L,
            64L,
            0L,
            0L,
            2,
            99,
            TimeUnit.NANOSECONDS,
            1);

        final Path probe = tempDir.resolve(ClusterConstants.SERVICE_PROBE_FILENAME);
        assertTrue(Files.isRegularFile(probe), "probe file missing: " + probe);
        assertFalse(Files.exists(tempDir.resolve(ClusterConstants.SERVICE_PROBE_FILENAME + ".tmp")));
        assertEquals(
            EchoService.formatServiceProbe(3L, 64L, "FOLLOWER", 1),
            Files.readString(probe, StandardCharsets.US_ASCII));
    }

    @Test
    void onNewLeadershipTermEvent_overwritesProbeOnSecondTerm(@TempDir final Path tempDir)
        throws IOException
    {
        final EchoService service = startedWithClusterDir(tempDir, 0, Cluster.Role.FOLLOWER);

        service.onNewLeadershipTermEvent(0L, 0L, 0L, 0L, 2, 1, TimeUnit.NANOSECONDS, 1);
        service.onNewLeadershipTermEvent(1L, 256L, 0L, 0L, 2, 1, TimeUnit.NANOSECONDS, 1);

        assertEquals(
            EchoService.formatServiceProbe(1L, 256L, "FOLLOWER", 0),
            Files.readString(
                tempDir.resolve(ClusterConstants.SERVICE_PROBE_FILENAME),
                StandardCharsets.US_ASCII));
    }

    @Test
    void writeServiceProbe_skipsWhenClusterMissing() {
        // Cluster is null (no onStart). writeServiceProbe must not throw.
        final EchoService service = new EchoService();
        service.writeServiceProbe(0L, 0L);
    }

    @Test
    void writeServiceProbe_skipsWhenContextMissing() {
        // Context is null. writeServiceProbe must not throw.
        final EchoService service = new EchoService();
        service.onStart(
            ProbeCluster.withoutContext(NoOpIdleStrategy.INSTANCE, 0, Cluster.Role.LEADER),
            null);

        service.writeServiceProbe(0L, 0L);
    }

    @Test
    void writeServiceProbe_skipsWhenClusterDirMissing() {
        // Context is non-null. clusterDir is null. writeServiceProbe must not throw.
        final EchoService service = new EchoService();
        service.onStart(
            ProbeCluster.withContext(
                NoOpIdleStrategy.INSTANCE,
                0,
                Cluster.Role.LEADER,
                new ClusteredServiceContainer.Context()),
            null);

        service.writeServiceProbe(0L, 0L);
    }

    @Test
    void writeServiceProbe_logsWhenWriteFails(@TempDir final Path tempDir) throws IOException {
        // baseDir is a regular file. Probe tmp write must fail.
        final Path baseFile = tempDir.resolve("blocked-base");
        Files.writeString(baseFile, "not-a-directory", StandardCharsets.US_ASCII);
        final Path clusterDir = baseFile.resolve(ClusterConfig.CLUSTER_SUB_DIR);

        final EchoService service = new EchoService();
        service.onStart(
            ProbeCluster.withClusterDir(
                NoOpIdleStrategy.INSTANCE,
                0,
                Cluster.Role.FOLLOWER,
                clusterDir.toFile()),
            null);

        final String stderr = ClusterNodeTest.captureStderr(
            () -> service.writeServiceProbe(1L, 0L));

        assertTrue(
            stderr.startsWith(EchoService.SERVICE_PROBE_WRITE_FAILED_PREFIX),
            "stderr=" + stderr);
        assertFalse(Files.exists(baseFile.resolve(ClusterConstants.SERVICE_PROBE_FILENAME)));
        assertFalse(Files.exists(
            baseFile.resolve(ClusterConstants.SERVICE_PROBE_FILENAME + ".tmp")));
    }

    @Test
    void writeServiceProbe_usesRoleAndMemberIdFromCluster(@TempDir final Path tempDir)
        throws IOException
    {
        final EchoService service = startedWithClusterDir(tempDir, 2, Cluster.Role.LEADER);

        service.writeServiceProbe(9L, 512L);

        assertEquals(
            EchoService.formatServiceProbe(9L, 512L, "LEADER", 2),
            Files.readString(
                tempDir.resolve(ClusterConstants.SERVICE_PROBE_FILENAME),
                StandardCharsets.US_ASCII));
    }

    private static EchoService newStartedEchoService() {
        final EchoService service = new EchoService();
        service.onStart(clusterWithIdleStrategy(NoOpIdleStrategy.INSTANCE), null);
        return service;
    }

    private static EchoService startedWithClusterDir(
        final Path tempDir,
        final int memberId,
        final Cluster.Role role) throws IOException
    {
        final EchoService service = new EchoService();
        service.onStart(newProbeCluster(tempDir, memberId, role, 0L), null);
        return service;
    }

    private static ProbeCluster newProbeCluster(
        final Path tempDir,
        final int memberId,
        final Cluster.Role role,
        final long logPosition) throws IOException
    {
        final Path clusterDir = tempDir.resolve(ClusterConfig.CLUSTER_SUB_DIR);
        Files.createDirectories(clusterDir);
        return ProbeCluster.withClusterDir(
            NoOpIdleStrategy.INSTANCE,
            memberId,
            role,
            clusterDir.toFile(),
            logPosition);
    }

    private static Cluster clusterWithIdleStrategy(final IdleStrategy idleStrategy) {
        return ProbeCluster.withoutContext(idleStrategy, 0, Cluster.Role.FOLLOWER);
    }

    /**
     * Minimal {@link Cluster} for EchoService unit tests.
     */
    private static final class ProbeCluster implements Cluster {
        private final IdleStrategy idleStrategy;
        private final int memberId;
        private final Role role;
        private final ClusteredServiceContainer.Context context;
        private final long logPosition;

        private static ProbeCluster withClusterDir(
            final IdleStrategy idleStrategy,
            final int memberId,
            final Role role,
            final java.io.File clusterDir)
        {
            return withClusterDir(idleStrategy, memberId, role, clusterDir, 0L);
        }

        private static ProbeCluster withClusterDir(
            final IdleStrategy idleStrategy,
            final int memberId,
            final Role role,
            final java.io.File clusterDir,
            final long logPosition)
        {
            return new ProbeCluster(
                idleStrategy,
                memberId,
                role,
                new ClusteredServiceContainer.Context().clusterDir(clusterDir),
                logPosition);
        }

        private static ProbeCluster withContext(
            final IdleStrategy idleStrategy,
            final int memberId,
            final Role role,
            final ClusteredServiceContainer.Context context)
        {
            return new ProbeCluster(idleStrategy, memberId, role, context, 0L);
        }

        private static ProbeCluster withoutContext(
            final IdleStrategy idleStrategy,
            final int memberId,
            final Role role)
        {
            return new ProbeCluster(idleStrategy, memberId, role, null, 0L);
        }

        private ProbeCluster(
            final IdleStrategy idleStrategy,
            final int memberId,
            final Role role,
            final ClusteredServiceContainer.Context context,
            final long logPosition)
        {
            this.idleStrategy = idleStrategy;
            this.memberId = memberId;
            this.role = role;
            this.context = context;
            this.logPosition = logPosition;
        }

        @Override
        public IdleStrategy idleStrategy() {
            return idleStrategy;
        }

        @Override
        public int memberId() {
            return memberId;
        }

        @Override
        public Role role() {
            return role;
        }

        @Override
        public long logPosition() {
            return logPosition;
        }

        @Override
        public Aeron aeron() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusteredServiceContainer.Context context() {
            return context;
        }

        @Override
        public ClientSession getClientSession(final long sessionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<ClientSession> clientSessions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEachClientSession(final Consumer<? super ClientSession> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean closeClientSession(final long sessionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long time() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TimeUnit timeUnit() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean scheduleTimer(final long correlationId, final long deadline) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancelTimer(final long correlationId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long offer(final DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long tryClaim(final int length, final BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RecordingClientSession implements ClientSession {
        private final long[] offerResults;
        private int offerCount;
        private DirectBuffer lastBuffer;
        private int lastOffset;
        private int lastLength;

        private RecordingClientSession(final long... offerResults) {
            this.offerResults = offerResults;
        }

        private static RecordingClientSession backPressuredThen(final long finalResult) {
            final long[] results = new long[EchoService.SEND_ATTEMPTS];
            Arrays.fill(results, Publication.BACK_PRESSURED);
            results[results.length - 1] = finalResult;
            return new RecordingClientSession(results);
        }

        private static RecordingClientSession allBackPressured() {
            return backPressuredThen(Publication.BACK_PRESSURED);
        }

        private int offerCount() {
            return offerCount;
        }

        private byte[] offeredBytes() {
            final byte[] copy = new byte[lastLength];
            lastBuffer.getBytes(lastOffset, copy);
            return copy;
        }

        private long lastReturnedResult() {
            return offerCount == 0 ? 0 : offerResults[offerCount - 1];
        }

        private void assertLastOfferUsed(final DirectBuffer buffer, final int offset, final int length) {
            assertSame(buffer, lastBuffer);
            assertEquals(offset, lastOffset);
            assertEquals(length, lastLength);
        }

        @Override
        public long id() {
            return 1L;
        }

        @Override
        public int responseStreamId() {
            return 0;
        }

        @Override
        public String responseChannel() {
            return "";
        }

        @Override
        public byte[] encodedPrincipal() {
            return new byte[0];
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosing() {
            return false;
        }

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            if (offerCount >= offerResults.length) {
                throw new IllegalStateException("unexpected offer call " + (offerCount + 1));
            }
            lastBuffer = buffer;
            lastOffset = offset;
            lastLength = length;
            return offerResults[offerCount++];
        }

        @Override
        public long offer(final DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long tryClaim(final int length, final BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }
    }
}
