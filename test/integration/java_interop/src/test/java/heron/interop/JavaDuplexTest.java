package heron.interop;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JavaDuplexTest {
    private static final HexFormat HEX = HexFormat.of();

    @ParameterizedTest
    @MethodSource("goldenGoPayloadCases")
    void goPayloadBytes_matchesGolden(final int index, final String wantHex) {
        assertArrayEquals(HEX.parseHex(wantHex), JavaDuplex.goPayloadBytes(index));
    }

    private static Stream<Arguments> goldenGoPayloadCases() {
        return Stream.of(
            Arguments.of(0, "676f2d3000000000000000000000000000000000000000000000000000000000"),
            Arguments.of(1, "676f2d3100000000000000000000000000000000000000000000000000000000"),
            Arguments.of(10, "676f2d3130000000000000000000000000000000000000000000000000000000"),
            Arguments.of(31, "676f2d3331000000000000000000000000000000000000000000000000000000")
        );
    }

    @Test
    void formatOfferedLine_matchesGoldenShape() {
        assertEquals(
            "offered " + JavaDuplex.MESSAGE_COUNT
                + " java payloads on stream " + JavaPublisher.DEFAULT_STREAM_ID
                + " (pubPosition=2048 sessionId=3)",
            JavaDuplex.formatOfferedLine(
                JavaDuplex.MESSAGE_COUNT,
                JavaPublisher.DEFAULT_STREAM_ID,
                2_048L,
                3
            )
        );
    }

    @Test
    void formatReceivedLine_matchesGoldenShape() {
        assertEquals(
            "received " + JavaDuplex.MESSAGE_COUNT
                + " go payloads on stream " + JavaDuplex.STREAM_GO_TO_JAVA
                + " (joinPosition=0 position=2048 sessionId=4)",
            JavaDuplex.formatReceivedLine(
                JavaDuplex.MESSAGE_COUNT,
                JavaDuplex.STREAM_GO_TO_JAVA,
                0L,
                2_048L,
                4
            )
        );
    }

    @Test
    void alignedOfferAdvance_matchesHarnessOracle() {
        assertEquals(
            2_048L,
            JavaDuplex.alignedOfferAdvance(JavaDuplex.PAYLOAD_SIZE, JavaDuplex.MESSAGE_COUNT)
        );
    }

    @Test
    void streamConstants_matchGoOracle() {
        assertEquals(1101, JavaDuplex.STREAM_GO_TO_JAVA);
        assertEquals(JavaPublisher.DEFAULT_STREAM_ID, 1100);
    }

    @Test
    void pollDelivered_rejectsNonPositiveWant() {
        final JavaDuplex.DuplexException ex = assertThrows(
            JavaDuplex.DuplexException.class,
            () -> JavaDuplex.pollDelivered(null, 0, JavaDuplex.POLL_TIMEOUT_NANOS)
        );
        assertEquals("\"0\" is not positive", ex.getMessage());
    }

    @Test
    void assertGoPayloads_rejectsWrongCount() {
        final JavaDuplex.DuplexException ex = assertThrows(
            JavaDuplex.DuplexException.class,
            () -> JavaDuplex.assertGoPayloads(List.of(new byte[JavaDuplex.PAYLOAD_SIZE]))
        );
        assertEquals(
            "expected " + JavaDuplex.MESSAGE_COUNT + " go payloads, got 1",
            ex.getMessage()
        );
    }

    @Test
    void assertGoPayloads_rejectsPayloadMismatch() {
        final List<byte[]> payloads = new ArrayList<>(JavaDuplex.MESSAGE_COUNT);
        for (int index = 0; index < JavaDuplex.MESSAGE_COUNT; index++) {
            payloads.add(JavaDuplex.goPayloadBytes(index));
        }
        payloads.set(3, new byte[JavaDuplex.PAYLOAD_SIZE]);

        final JavaDuplex.DuplexException ex = assertThrows(
            JavaDuplex.DuplexException.class,
            () -> JavaDuplex.assertGoPayloads(payloads)
        );
        assertEquals("go payload mismatch at index 3", ex.getMessage());
    }

    @Test
    void assertSubscribePositionState_rejectsNotConnected() {
        final JavaDuplex.DuplexException ex = assertThrows(
            JavaDuplex.DuplexException.class,
            () -> JavaDuplex.assertSubscribePositionState(false, 42L, 1101, 0L, 0L, 2_048L)
        );
        assertEquals("subscription 42 (stream 1101) has no active image", ex.getMessage());
    }

    @Test
    void assertSubscribePositionState_rejectsPositionMismatch() {
        final JavaDuplex.DuplexException ex = assertThrows(
            JavaDuplex.DuplexException.class,
            () -> JavaDuplex.assertSubscribePositionState(true, 7L, 1101, 0L, 512L, 2_048L)
        );
        assertEquals(
            "image position 512 != 2048 (join 0 + advance 2048)",
            ex.getMessage()
        );
    }
}
