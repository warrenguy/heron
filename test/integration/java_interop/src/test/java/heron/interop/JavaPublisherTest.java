package heron.interop;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HexFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JavaPublisherTest {
    private static final HexFormat HEX = HexFormat.of();

    private final Map<String, String> savedProperties = new HashMap<>();

    @AfterEach
    void restoreSystemProperties() {
        savedProperties.forEach(JavaPublisherTest::applySystemProperty);
        savedProperties.clear();
    }

    @ParameterizedTest
    @MethodSource("goldenPayloadCases")
    void interopPayloadBytes_matchesGolden(final int index, final String wantHex) {
        assertArrayEquals(HEX.parseHex(wantHex), JavaPublisher.interopPayloadBytes(index));
    }

    private static Stream<Arguments> goldenPayloadCases() {
        return Stream.of(
            Arguments.of(0, "6a6176612d300000000000000000000000000000000000000000000000000000"),
            Arguments.of(1, "6a6176612d310000000000000000000000000000000000000000000000000000"),
            Arguments.of(10, "6a6176612d313000000000000000000000000000000000000000000000000000"),
            Arguments.of(31, "6a6176612d333100000000000000000000000000000000000000000000000000")
        );
    }

    @ParameterizedTest
    @MethodSource("offerStallTimeoutCases")
    void offerStallTimeoutMs_matchesHarness(final long offerTimeoutMs, final long wantStallMs) {
        assertEquals(wantStallMs, JavaPublisher.offerStallTimeoutMs(offerTimeoutMs));
    }

    private static Stream<Arguments> offerStallTimeoutCases() {
        return Stream.of(
            Arguments.of(10_000L, 2_000L),
            Arguments.of(5_000L, 1_000L),
            Arguments.of(3_000L, 600L),
            Arguments.of(4_999L, 999L)
        );
    }

    @Test
    void formatWitnessJson_matchesGoldenFieldOrder() {
        assertEquals(
            "{\"k\":" + JavaPublisher.MESSAGE_COUNT
                + ",\"pubPosition\":512,\"sessionId\":42,\"streamId\":"
                + JavaPublisher.DEFAULT_STREAM_ID
                + "}\n",
            JavaPublisher.formatWitnessJson(
                JavaPublisher.MESSAGE_COUNT,
                512L,
                42,
                JavaPublisher.DEFAULT_STREAM_ID
            )
        );
    }

    @ParameterizedTest
    @MethodSource("invalidPropertyCases")
    void rejectsInvalidProperty(
        final String key,
        final String value,
        final Executable action,
        final String wantMessage
    ) {
        withProperty(key, value);

        final JavaPublisher.PublisherException ex = assertThrows(
            JavaPublisher.PublisherException.class,
            action
        );
        assertEquals(wantMessage, ex.getMessage());
    }

    private static Stream<Arguments> invalidPropertyCases() {
        return Stream.of(
            Arguments.of(
                "aeron.dir",
                null,
                (Executable) JavaPublisher::requireAeronDir,
                "missing required system property: aeron.dir"
            ),
            Arguments.of(
                "aeron.dir",
                "   ",
                (Executable) JavaPublisher::requireAeronDir,
                "missing required system property: aeron.dir"
            ),
            Arguments.of(
                "heron.streamId",
                "   ",
                (Executable) JavaPublisher::parseStreamId,
                "invalid heron.streamId: must be an integer"
            ),
            Arguments.of(
                "heron.streamId",
                "foo",
                (Executable) JavaPublisher::parseStreamId,
                "invalid heron.streamId: must be an integer"
            ),
            Arguments.of(
                "heron.channel",
                "  ",
                (Executable) JavaPublisher::parseChannel,
                "invalid heron.channel: must not be blank"
            )
        );
    }

    @Test
    void requireAeronDir_trimsValue() {
        withProperty("aeron.dir", "  /tmp/aeron  ");

        assertEquals("/tmp/aeron", JavaPublisher.requireAeronDir());
    }

    @Test
    void parseStreamId_usesDefaultWhenUnset() {
        withProperty("heron.streamId", null);

        assertEquals(JavaPublisher.DEFAULT_STREAM_ID, JavaPublisher.parseStreamId());
    }

    @Test
    void parseChannel_usesDefaultWhenUnset() {
        withProperty("heron.channel", null);

        assertEquals(JavaPublisher.DEFAULT_CHANNEL, JavaPublisher.parseChannel());
    }

    private void withProperty(final String key, final String value) {
        if (!savedProperties.containsKey(key)) {
            savedProperties.put(key, System.getProperty(key));
        }
        applySystemProperty(key, value);
    }

    private static void applySystemProperty(final String key, final String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
