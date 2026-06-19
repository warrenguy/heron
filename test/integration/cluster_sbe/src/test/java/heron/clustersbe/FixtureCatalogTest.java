package heron.clustersbe;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixtureCatalogTest {
    private static final String NESTED_PAYLOAD_HEX =
        "1c001a006f001000040000000000000005000000000000000600000000000000000000001000000033333333333333333333333333333333";

    private static List<FixtureEntry> catalogEntries;
    private static Map<String, FixtureEntry> catalogByName;

    @BeforeAll
    static void loadCatalog() throws IOException {
        catalogEntries = FixtureCatalog.load().fixtures();
        catalogByName = catalogEntries.stream()
            .collect(Collectors.toUnmodifiableMap(
                FixtureEntry::name,
                Function.identity()
            ));
    }

    @Test
    void load_matchesClasspathCatalogFixtureNames() throws IOException {
        assertEquals(parseFixtureNames(FixtureCatalog.classpathJson()), loadedFixtureNames());
    }

    @Test
    void parseCatalogJson_fixtureOrderStableAcrossIndependentParses() throws IOException {
        final String json = FixtureCatalog.classpathJson();
        final List<String> first = parseFixtureNames(json);
        final List<String> second = parseFixtureNames(json);
        assertEquals(first, second);
    }

    @Test
    void load_fixtureLevelHexBeforeNestedPayloadHex() {
        final FixtureEntry entry = catalogByName.get("adminRequest_snapshotPayload16");
        assertNotNull(entry);
        assertEquals(NESTED_PAYLOAD_HEX, entry.hex());
    }

    @Test
    void load_preservesMaxInt64FieldValues() {
        final FixtureEntry entry = catalogByName.get("sessionMessageHeader_maxInt64");
        assertNotNull(entry);

        final BigInteger want = BigInteger.valueOf(Long.MAX_VALUE);
        assertEquals(want, entry.fields().get("leadershipTermId"));
        assertEquals(want, entry.fields().get("clusterSessionId"));
        assertEquals(want, entry.fields().get("timestamp"));
        assertEquals(Long.MAX_VALUE, FixtureCatalog.int64FromJsonValue(entry.fields().get("timestamp")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parseFailureCases")
    void parseCatalogJson_rejectsStructuralFaults(
        final String caseName,
        final String json,
        final String messagePart
    ) {
        final IOException ex = assertThrows(
            IOException.class,
            () -> FixtureCatalog.parseCatalogJson(json)
        );
        assertTrue(ex.getMessage().startsWith("heron: "), caseName);
        assertTrue(ex.getMessage().contains(messagePart), caseName + ": " + ex.getMessage());
    }

    private static Stream<Arguments> parseFailureCases() {
        return Stream.of(
            Arguments.of(
                "malformedJson",
                "{",
                "invalid catalog json"
            ),
            Arguments.of(
                "missingFixtures",
                """
                    {"schemaVersion":1,"blobs":{}}
                    """,
                "missing or invalid fixtures"
            ),
            Arguments.of(
                "invalidBlobs",
                """
                    {"schemaVersion":1,"blobs":[],"fixtures":[]}
                    """,
                "invalid blobs"
            ),
            Arguments.of(
                "nonObjectFixture",
                """
                    {"schemaVersion":1,"fixtures":[1]}
                    """,
                "fixture entry must be object"
            ),
            Arguments.of(
                "duplicateName",
                """
                    {"schemaVersion":1,"fixtures":[
                      {"name":"a","template":"challenge","actingVersion":16,"hex":"00","fields":{"correlationId":1,"clusterSessionId":2}},
                      {"name":"a","template":"challenge","actingVersion":16,"hex":"01","fields":{"correlationId":3,"clusterSessionId":4}}
                    ]}
                    """,
                "duplicate fixture name"
            ),
            Arguments.of(
                "preV2ConnectMissingVersion",
                """
                    {"schemaVersion":1,"fixtures":[{
                      "name":"bad","template":"sessionConnectRequest","actingVersion":1,
                      "hex":"00","fields":{"correlationId":1,"responseStreamId":2}
                    }]}
                    """,
                "missing version for actingVersion < 2"
            ),
            Arguments.of(
                "blobRepeatLenZero",
                """
                    {"schemaVersion":1,"blobs":{"bad":{"repeat":{"byte":1,"len":0}}},"fixtures":[]}
                    """,
                "repeat len must be positive"
            ),
            Arguments.of(
                "blobRepeatLenNegative",
                """
                    {"schemaVersion":1,"blobs":{"bad":{"repeat":{"byte":1,"len":-1}}},"fixtures":[]}
                    """,
                "repeat len must be positive"
            ),
            Arguments.of(
                "blobRepeatByteOutOfRange",
                """
                    {"schemaVersion":1,"blobs":{"bad":{"repeat":{"byte":300,"len":1}}},"fixtures":[]}
                    """,
                "repeat byte out of range"
            ),
            Arguments.of(
                "blobRepeatByteNegative",
                """
                    {"schemaVersion":1,"blobs":{"bad":{"repeat":{"byte":-1,"len":1}}},"fixtures":[]}
                    """,
                "repeat byte out of range"
            )
        );
    }

    @Test
    void resolveBlobs_materializesChallenge32() {
        final Map<String, FixtureCatalog.BlobSpec> blobs = Map.of(
            "challenge32",
            new FixtureCatalog.BlobSpec(new FixtureCatalog.BlobRepeat(171, 32))
        );

        final byte[] data = FixtureCatalog.resolveBlobs(blobs).get("challenge32");
        assertEquals(32, data.length);
        for (final byte value : data) {
            assertEquals((byte) 171, value);
        }
    }

    @Test
    void int64FromJsonValue_acceptsMaxInt64AndRejectsInvalid() {
        assertEquals(Long.MAX_VALUE, FixtureCatalog.int64FromJsonValue(new BigInteger("9223372036854775807")));
        assertEquals(42L, FixtureCatalog.int64FromJsonValue(new BigInteger("42")));

        final List<Object> rejected = List.of(
            "bad",
            1L,
            1,
            1.5d,
            new BigInteger("9223372036854775808")
        );
        for (final Object value : rejected) {
            assertThrows(IllegalArgumentException.class, () -> FixtureCatalog.int64FromJsonValue(value));
        }
    }

    private static List<String> loadedFixtureNames() {
        return catalogEntries.stream().map(FixtureEntry::name).toList();
    }

    private static List<String> parseFixtureNames(final String json) throws IOException {
        return FixtureCatalog.parseCatalogJson(json).fixtures().stream()
            .map(FixtureEntry::name)
            .toList();
    }

}
