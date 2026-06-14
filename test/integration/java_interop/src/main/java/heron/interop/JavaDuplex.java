package heron.interop;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public final class JavaDuplex {
    // javainterop.StreamGoToJava
    static final int STREAM_GO_TO_JAVA = 1101;
    // javainterop.MessageCount
    static final int MESSAGE_COUNT = 32;
    // javainterop.PayloadSize
    static final int PAYLOAD_SIZE = 32;
    // harness.ConnectedPollTimeout
    static final long POLL_TIMEOUT_MS = 10_000L;
    static final long POLL_TIMEOUT_NANOS = POLL_TIMEOUT_MS * 1_000_000L;
    static final int FRAGMENT_LIMIT = 10;
    // aeron/logbuffer.DataHeaderLength
    static final int DATA_HEADER_LENGTH = 32;

    private JavaDuplex() {
    }

    public static void main(final String[] args) {
        try {
            run();
            System.exit(0);
        } catch (final DuplexException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (final Exception e) {
            if (Boolean.getBoolean("heron.debug")) {
                e.printStackTrace(System.err);
            }
            System.err.println("java duplex failed: " + e.getMessage());
            System.exit(1);
        }
    }

    static void run() {
        final String aeronDir = JavaPublisher.requireAeronDir();
        final String channel = JavaPublisher.parseChannel();

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        try {
            final Publication javaToGoPub = aeron.addPublication(channel, JavaPublisher.DEFAULT_STREAM_ID);
            final Subscription goToJavaSub = aeron.addSubscription(channel, STREAM_GO_TO_JAVA);
            try {
                JavaPublisher.offerInteropPayloads(javaToGoPub);
                System.out.println(formatOfferedLine(
                    MESSAGE_COUNT,
                    JavaPublisher.DEFAULT_STREAM_ID,
                    javaToGoPub.position(),
                    javaToGoPub.sessionId()
                ));

                // Poll budget starts after Java round 1; Go must finish 1100 before offering 1101.
                final List<byte[]> received = pollDelivered(goToJavaSub, MESSAGE_COUNT, POLL_TIMEOUT_NANOS);
                assertGoPayloads(received);
                final long subscribeAdvance = alignedOfferAdvance(PAYLOAD_SIZE, MESSAGE_COUNT);
                assertSubscribePosition(goToJavaSub, subscribeAdvance);

                final Image goToJavaImage = goToJavaSub.imageAtIndex(0);
                System.out.println(formatReceivedLine(
                    MESSAGE_COUNT,
                    STREAM_GO_TO_JAVA,
                    goToJavaImage.joinPosition(),
                    goToJavaImage.position(),
                    goToJavaImage.sessionId()
                ));
                System.out.println("java interop duplex smoke passed");
                System.out.flush();
            } finally {
                javaToGoPub.close();
                goToJavaSub.close();
            }
        } finally {
            aeron.close();
        }
    }

    static byte[] goPayloadBytes(final int index) {
        return JavaPublisher.prefixPaddedPayload("go-", index, PAYLOAD_SIZE);
    }

    static String formatOfferedLine(
        final int count,
        final int stream,
        final long pubPosition,
        final int sessionId
    ) {
        return "offered " + count + " java payloads on stream " + stream
            + " (pubPosition=" + pubPosition + " sessionId=" + sessionId + ")";
    }

    static String formatReceivedLine(
        final int count,
        final int stream,
        final long joinPosition,
        final long position,
        final int sessionId
    ) {
        return "received " + count + " go payloads on stream " + stream
            + " (joinPosition=" + joinPosition + " position=" + position
            + " sessionId=" + sessionId + ")";
    }

    static long alignedOfferAdvance(final int payloadSize, final int offerCount) {
        final int frameLength = BitUtil.align(
            payloadSize + DATA_HEADER_LENGTH,
            FrameDescriptor.FRAME_ALIGNMENT
        );
        return (long) frameLength * offerCount;
    }

    static List<byte[]> pollDelivered(
        final Subscription subscription,
        final int want,
        final long timeoutNanos
    ) {
        if (want <= 0) {
            throw new DuplexException("\"" + want + "\" is not positive");
        }

        final List<byte[]> received = new ArrayList<>(want);
        final FragmentAssembler assembler = new FragmentAssembler(
            (final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header header) -> {
                final byte[] copy = new byte[length];
                buffer.getBytes(offset, copy);
                received.add(copy);
            }
        );
        final IdleStrategy idleStrategy = new BackoffIdleStrategy();

        final long deadlineNanos = System.nanoTime() + timeoutNanos;
        final long timeoutMs = timeoutNanos / 1_000_000L;
        while (received.size() < want) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new DuplexException(
                    "subscription " + subscription.registrationId()
                        + " (stream " + subscription.streamId()
                        + ") delivered " + received.size()
                        + " of " + want
                        + " within " + timeoutMs + "ms"
                );
            }

            final int fragments = subscription.poll(assembler, FRAGMENT_LIMIT);
            idleStrategy.idle(fragments);
        }

        return received;
    }

    static void assertGoPayloads(final List<byte[]> received) {
        if (received.size() != MESSAGE_COUNT) {
            throw new DuplexException(
                "expected " + MESSAGE_COUNT + " go payloads, got " + received.size()
            );
        }

        for (int index = 0; index < MESSAGE_COUNT; index++) {
            if (!Arrays.equals(goPayloadBytes(index), received.get(index))) {
                throw new DuplexException("go payload mismatch at index " + index);
            }
        }
    }

    static void assertSubscribePosition(final Subscription subscription, final long wantAdvance) {
        final boolean connected = subscription.isConnected();
        final long joinPosition;
        final long position;
        if (connected) {
            final Image image = subscription.imageAtIndex(0);
            joinPosition = image.joinPosition();
            position = image.position();
        } else {
            joinPosition = 0L;
            position = 0L;
        }

        assertSubscribePositionState(
            connected,
            subscription.registrationId(),
            subscription.streamId(),
            joinPosition,
            position,
            wantAdvance
        );
    }

    static void assertSubscribePositionState(
        final boolean connected,
        final long registrationId,
        final int streamId,
        final long joinPosition,
        final long position,
        final long wantAdvance
    ) {
        if (!connected) {
            throw new DuplexException(
                "subscription " + registrationId + " (stream " + streamId + ") has no active image"
            );
        }

        if (streamId != STREAM_GO_TO_JAVA) {
            throw new DuplexException(
                "subscription stream " + streamId + " != " + STREAM_GO_TO_JAVA
            );
        }

        final long wantPosition = joinPosition + wantAdvance;
        if (position != wantPosition) {
            throw new DuplexException(
                "image position " + position + " != " + wantPosition
                    + " (join " + joinPosition + " + advance " + wantAdvance + ")"
            );
        }
    }

    static final class DuplexException extends RuntimeException {
        DuplexException(final String message) {
            super(message);
        }
    }
}
