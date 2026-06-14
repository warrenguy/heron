package heron.interop;

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.nio.charset.StandardCharsets;

public final class JavaPublisher {
    // javainterop.StreamID
    static final int DEFAULT_STREAM_ID = 1100;
    // harness.IPCChannel
    static final String DEFAULT_CHANNEL = "aeron:ipc";
    // javainterop.MessageCount
    static final int MESSAGE_COUNT = 32;
    // javainterop.PayloadSize
    static final int PAYLOAD_SIZE = 32;
    // harness.ConnectedPollTimeout
    static final long OFFER_TIMEOUT_MS = 10_000L;
    static final long OFFER_TIMEOUT_NANOS = OFFER_TIMEOUT_MS * 1_000_000L;
    // javainterop.OfferStallTimeout
    static final long OFFER_STALL_TIMEOUT_MS = offerStallTimeoutMs(OFFER_TIMEOUT_MS);
    static final long OFFER_STALL_TIMEOUT_NANOS = OFFER_STALL_TIMEOUT_MS * 1_000_000L;
    static final long OFFER_RETRY_SLEEP_MS = 16L;

    private JavaPublisher() {
    }

    public static void main(final String[] args) {
        try {
            run();
            System.exit(0);
        } catch (final PublisherException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (final Exception e) {
            if (Boolean.getBoolean("heron.debug")) {
                e.printStackTrace(System.err);
            }
            System.err.println("java publisher failed: " + e.getMessage());
            System.exit(1);
        }
    }

    static void run() {
        final String aeronDir = requireAeronDir();
        final int streamId = parseStreamId();
        final String channel = parseChannel();

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        try {
            final Publication publication = aeron.addPublication(channel, streamId);
            try {
                offerInteropPayloads(publication);

                final long pubPosition = publication.position();
                final int sessionId = publication.sessionId();

                System.out.print(formatWitnessJson(MESSAGE_COUNT, pubPosition, sessionId, streamId));
                System.out.flush();
            } finally {
                publication.close();
            }
        } finally {
            aeron.close();
        }
    }

    static String formatWitnessJson(
        final int k,
        final long pubPosition,
        final int sessionId,
        final int streamId
    ) {
        return "{\"k\":" + k
            + ",\"pubPosition\":" + pubPosition
            + ",\"sessionId\":" + sessionId
            + ",\"streamId\":" + streamId
            + "}\n";
    }

    static byte[] prefixPaddedPayload(final String prefix, final int index, final int size) {
        final byte[] prefixBytes = (prefix + index).getBytes(StandardCharsets.US_ASCII);
        return Arrays.copyOf(prefixBytes, size);
    }

    static byte[] interopPayloadBytes(final int index) {
        return prefixPaddedPayload("java-", index, PAYLOAD_SIZE);
    }

    static void offerInteropPayloads(final Publication publication) {
        final UnsafeBuffer offerBuffer = new UnsafeBuffer();
        for (int index = 0; index < MESSAGE_COUNT; index++) {
            offerUntilSuccess(publication, offerBuffer, interopPayloadBytes(index));
        }
    }

    // Retries transient NOT_CONNECTED, BACK_PRESSURED, and ADMIN_ACTION
    // like harness OfferWithRetry. Sleep only on BACK_PRESSURED and NOT_CONNECTED;
    // ADMIN_ACTION retries without sleep.
    static void offerUntilSuccess(
        final Publication publication,
        final UnsafeBuffer buffer,
        final byte[] payload
    ) {
        buffer.wrap(payload);
        final long deadlineNanos = System.nanoTime() + OFFER_TIMEOUT_NANOS;
        long lastStatus = 0L;
        long retryPosition = publication.position();
        long stallMarkedNanos = 0L;

        while (true) {
            final long now = System.nanoTime();
            if (now >= deadlineNanos) {
                break;
            }

            final long result = publication.offer(buffer, 0, payload.length);
            if (result > 0L) {
                return;
            }

            lastStatus = result;
            final long streamPosition = publication.position();
            stallMarkedNanos = updateStallState(
                publication,
                result,
                streamPosition,
                retryPosition,
                stallMarkedNanos,
                now
            );
            retryPosition = Math.max(retryPosition, streamPosition);

            if (result == Publication.BACK_PRESSURED || result == Publication.NOT_CONNECTED) {
                sleepBeforeRetry();
            }
        }

        throwOfferFailure(
            publication,
            "offer timed out within " + OFFER_TIMEOUT_MS + "ms",
            publication.position(),
            lastStatus
        );
    }

    private static long updateStallState(
        final Publication publication,
        final long result,
        final long position,
        final long retryPosition,
        final long stallMarkedNanos,
        final long now
    ) {
        if (position > retryPosition) {
            return 0L;
        }

        return switch ((int) result) {
            case (int) Publication.BACK_PRESSURED, (int) Publication.ADMIN_ACTION -> {
                if (stallMarkedNanos == 0L) {
                    yield now;
                }
                if ((now - stallMarkedNanos) >= OFFER_STALL_TIMEOUT_NANOS) {
                    throwOfferFailure(
                        publication,
                        "offer stalled within " + OFFER_STALL_TIMEOUT_MS + "ms",
                        position,
                        result
                    );
                }
                yield stallMarkedNanos;
            }
            case (int) Publication.NOT_CONNECTED -> 0L;
            case (int) Publication.CLOSED -> throwOfferFailure(
                publication,
                "publication closed",
                position,
                result
            );
            case (int) Publication.MAX_POSITION_EXCEEDED -> throwOfferFailure(
                publication,
                "max position exceeded",
                position,
                result
            );
            default -> throwOfferFailure(publication, "offer failed", position, result);
        };
    }

    private static void sleepBeforeRetry() {
        try {
            Thread.sleep(OFFER_RETRY_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublisherException("interrupted while offering", e);
        }
    }

    private static long throwOfferFailure(
        final Publication publication,
        final String reason,
        final long position,
        final long status
    ) {
        throw new PublisherException(offerFailureMessage(publication, reason, position, status));
    }

    private static String offerFailureMessage(
        final Publication publication,
        final String reason,
        final long position,
        final long status
    ) {
        return "publication " + publication.registrationId()
            + " (stream " + publication.streamId()
            + " session " + publication.sessionId()
            + ") " + reason
            + "; position=" + position
            + "; status=" + status;
    }

    static long offerStallTimeoutMs(final long offerTimeoutMs) {
        final long stall = offerTimeoutMs / 5L;
        if (stall < 1_000L && offerTimeoutMs >= 5_000L) {
            return 1_000L;
        }
        return stall;
    }

    static String requireAeronDir() {
        return requireNonBlankSystemProperty(
            "aeron.dir",
            "missing required system property: aeron.dir"
        );
    }

    static int parseStreamId() {
        return switch (System.getProperty("heron.streamId")) {
            case null -> DEFAULT_STREAM_ID;
            case String raw when raw.isBlank() -> throw new PublisherException(
                "invalid heron.streamId: must be an integer"
            );
            case String raw -> {
                try {
                    yield Integer.parseInt(raw.trim());
                } catch (final NumberFormatException _) {
                    throw new PublisherException("invalid heron.streamId: must be an integer");
                }
            }
        };
    }

    static String parseChannel() {
        return switch (System.getProperty("heron.channel")) {
            case null -> DEFAULT_CHANNEL;
            case String raw when raw.isBlank() -> throw new PublisherException(
                "invalid heron.channel: must not be blank"
            );
            case String raw -> raw.trim();
        };
    }

    private static String requireNonBlankSystemProperty(
        final String key,
        final String missingMessage
    ) {
        return switch (System.getProperty(key)) {
            case null -> throw new PublisherException(missingMessage);
            case String value when value.isBlank() -> throw new PublisherException(missingMessage);
            case String value -> value.trim();
        };
    }

    static final class PublisherException extends RuntimeException {
        PublisherException(final String message) {
            super(message);
        }

        PublisherException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
