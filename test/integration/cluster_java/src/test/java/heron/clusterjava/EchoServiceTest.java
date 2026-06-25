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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static EchoService newStartedEchoService() {
        final EchoService service = new EchoService();
        service.onStart(clusterWithIdleStrategy(NoOpIdleStrategy.INSTANCE), null);
        return service;
    }

    private static Cluster clusterWithIdleStrategy(final IdleStrategy idleStrategy) {
        return new Cluster() {
            @Override
            public IdleStrategy idleStrategy() {
                return idleStrategy;
            }

            @Override
            public int memberId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Role role() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long logPosition() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Aeron aeron() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ClusteredServiceContainer.Context context() {
                throw new UnsupportedOperationException();
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
        };
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
