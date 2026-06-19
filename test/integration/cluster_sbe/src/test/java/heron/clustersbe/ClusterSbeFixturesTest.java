package heron.clustersbe;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterSbeFixturesTest {
    private static final HexFormat HEX = HexFormat.of();
    private static final FixtureCatalog.CatalogFile CATALOG;
    private static final Map<String, byte[]> BLOBS;

    static {
        try {
            CATALOG = FixtureCatalog.load();
            BLOBS = FixtureCatalog.resolveBlobs(CATALOG.blobs());
        } catch (final IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("catalogFixtureCases")
    void encode_matchesCatalogHex(
        final String name,
        final byte[] wantWire,
        final FixtureEntry entry
    ) {
        assertArrayEquals(wantWire, ClusterSbeFixtures.encode(entry, BLOBS), name);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("encodeFailureCases")
    void encode_rejectsInvalidEntry(
        final String caseName,
        final FixtureEntry entry,
        final String messagePart
    ) {
        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ClusterSbeFixtures.encode(entry, BLOBS)
        );
        assertTrue(ex.getMessage().contains(messagePart), caseName + ": " + ex.getMessage());
    }

    private static Stream<Arguments> catalogFixtureCases() {
        return CATALOG.fixtures().stream()
            .map(entry -> Arguments.of(entry.name(), HEX.parseHex(entry.hex()), entry));
    }

    private static Stream<Arguments> encodeFailureCases() {
        return Stream.of(
            Arguments.of(
                "unknownTemplate",
                badEntry("notATemplate", Map.of()),
                "unknown template"
            ),
            Arguments.of(
                "missingCorrelationId",
                badEntry("challenge", Map.of("clusterSessionId", BigInteger.ONE)),
                "missing correlationId"
            ),
            Arguments.of(
                "badEventCode",
                badEntry("sessionEvent", sessionEventFields(Map.of(
                    "code", "NOPE",
                    "leaderHeartbeatTimeoutNs", BigInteger.ZERO
                ))),
                "NOPE"
            ),
            Arguments.of(
                "bytesHexAndRef",
                badChallenge(Map.of("hex", "01", "ref", "challenge32")),
                "exactly one of hex or ref"
            ),
            Arguments.of(
                "unknownBlobRef",
                badChallenge(Map.of("ref", "missing")),
                "unknown blob ref"
            ),
            Arguments.of(
                "badHeartbeatSentinel",
                badEntry("sessionEvent", sessionEventFields(Map.of(
                    "code", "OK",
                    "leaderHeartbeatTimeoutNs", "gone"
                ))),
                "unknown heartbeat sentinel"
            ),
            Arguments.of(
                "missingHeartbeat",
                badEntry("sessionEvent", sessionEventFields(Map.of("code", "OK"))),
                "missing leaderHeartbeatTimeoutNs"
            ),
            Arguments.of(
                "int64OutOfRange",
                badEntry("sessionMessageHeader", Map.of(
                    "leadershipTermId", new BigInteger("9223372036854775808"),
                    "clusterSessionId", BigInteger.ZERO,
                    "timestamp", BigInteger.ZERO
                )),
                "out of int64 range"
            )
        );
    }

    private static FixtureEntry badEntry(final String template, final Map<String, Object> fields) {
        return new FixtureEntry("bad", template, 16, null, "00", fields);
    }

    private static FixtureEntry badChallenge(final Map<String, Object> encodedChallenge) {
        return badEntry("challenge", Map.of(
            "correlationId", BigInteger.ONE,
            "clusterSessionId", BigInteger.TWO,
            "encodedChallenge", encodedChallenge
        ));
    }

    private static Map<String, Object> sessionEventFields(final Map<String, Object> overrides) {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("clusterSessionId", BigInteger.ONE);
        fields.put("correlationId", BigInteger.TWO);
        fields.put("leadershipTermId", BigInteger.valueOf(3));
        fields.put("leaderMemberId", BigInteger.valueOf(4));
        fields.putAll(overrides);
        return Map.copyOf(fields);
    }
}
