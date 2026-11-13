package luas.archivesbe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveSbeFixturesTest {
    private static final HexFormat HEX = HexFormat.of();
    private static final Path CATALOG_PATH = Path.of("archivesbe", "fixtures", "catalog.json");
    private static final ArchiveSbeCatalog CATALOG = load();

    @ParameterizedTest(name = "{0}")
    @MethodSource("catalogFixtureCases")
    void encode_matchesCatalogHex(final String name, final byte[] wantWire, final FixtureEntry entry) {
        assertArrayEquals(wantWire, ArchiveSbeFixtures.encode(entry), name);
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
            () -> ArchiveSbeFixtures.encode(entry)
        );
        assertTrue(ex.getMessage().contains(messagePart), caseName + ": " + ex.getMessage());
    }

    @Test
    void coverage_rowAtSchemaVersion() {
        for (final String template : ArchiveSbeFixtures.templateNames()) {
            final int schemaVersion = ArchiveSbeFixtures.schemaVersion(template);
            final boolean found = rowsFor(template).anyMatch(entry -> entry.actingVersion() == schemaVersion);
            assertTrue(found, template);
        }
    }

    @Test
    void coverage_versionBranches() {
        for (final String template : ArchiveSbeFixtures.templateNames()) {
            for (final ArchiveSbeFixtures.FieldSpec field : ArchiveSbeFixtures.fields(template)) {
                if (field.sinceVersion() <= 0) {
                    continue;
                }
                final int below = field.sinceVersion() - 1;
                final boolean found = rowsFor(template).anyMatch(entry -> entry.actingVersion() == below);
                assertTrue(found, template + " " + field.name() + " since " + field.sinceVersion());
            }
        }
    }

    @Test
    void coverage_enumValues() {
        for (final String template : ArchiveSbeFixtures.templateNames()) {
            for (final ArchiveSbeFixtures.FieldSpec field : ArchiveSbeFixtures.fields(template)) {
                if (field.enumType() == null) {
                    continue;
                }
                for (final Object constant : field.enumType().getEnumConstants()) {
                    final Enum<?> value = (Enum<?>) constant;
                    if ("NULL_VAL".equals(value.name())) {
                        continue;
                    }
                    final boolean found = rowsFor(template)
                        .anyMatch(entry -> value.name().equals(entry.fields().get(field.name())));
                    assertTrue(found, template + " " + field.name() + " " + value.name());
                }
            }
        }
    }

    @Test
    void coverage_varFieldContent() {
        for (final String template : ArchiveSbeFixtures.templateNames()) {
            for (final ArchiveSbeFixtures.FieldSpec field : ArchiveSbeFixtures.fields(template)) {
                if (field.fixed()) {
                    continue;
                }
                final boolean found = rowsFor(template)
                    .filter(entry -> entry.actingVersion() >= field.sinceVersion())
                    .anyMatch(entry -> nonEmptyVar(entry.fields().get(field.name())));
                assertTrue(found, template + " " + field.name());
            }
        }
    }

    private static Stream<Arguments> catalogFixtureCases() {
        return CATALOG.fixtures().stream()
            .map(entry -> Arguments.of(entry.name(), HEX.parseHex(entry.hex()), entry));
    }

    private static Stream<Arguments> encodeFailureCases() {
        return Stream.of(
            Arguments.of("unknownTemplate", entry("notATemplate", 14, Map.of()), "unknown template"),
            Arguments.of(
                "missingFixedKey",
                entry("closeSessionRequest", 14, Map.of()),
                "missing controlSessionId"
            ),
            Arguments.of(
                "unknownKey",
                entry("closeSessionRequest", 14, Map.of("controlSessionId", BigInteger.ONE, "nope", BigInteger.ONE)),
                "unknown key"
            ),
            Arguments.of(
                "gatedKey",
                entry("authConnectRequest", 11, Map.of(
                    "correlationId", BigInteger.ONE,
                    "responseStreamId", BigInteger.TWO,
                    "version", BigInteger.valueOf(3),
                    "clientInfo", "name=client"
                )),
                "gated field"
            ),
            Arguments.of(
                "emptyVarString",
                entry("startRecordingRequest", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "streamId", BigInteger.valueOf(3),
                    "sourceLocation", "LOCAL",
                    "channel", ""
                )),
                "empty var value"
            ),
            Arguments.of(
                "nonAsciiString",
                entry("startRecordingRequest", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "streamId", BigInteger.valueOf(3),
                    "sourceLocation", "LOCAL",
                    "channel", "aeron:udp?endpoint=h\u00e9st"
                )),
                "non-ASCII character"
            ),
            Arguments.of(
                "nullEnum",
                entry("startRecordingRequest", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "streamId", BigInteger.valueOf(3),
                    "sourceLocation", "NULL_VAL"
                )),
                "NULL_VAL"
            ),
            Arguments.of(
                "emptyDataHex",
                entry("challengeResponse", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "encodedCredentials", Map.of("hex", "")
                )),
                "empty var value"
            ),
            Arguments.of(
                "int32OutOfRange",
                entry("startRecordingRequest", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "streamId", new BigInteger("3000000000"),
                    "sourceLocation", "LOCAL"
                )),
                "out of int32 range"
            ),
            Arguments.of(
                "int64OutOfRange",
                entry(
                    "closeSessionRequest",
                    14,
                    Map.of("controlSessionId", new BigInteger("9223372036854775808"))
                ),
                "out of int64 range"
            ),
            Arguments.of(
                "nonIntegral",
                entry("closeSessionRequest", 14, Map.of("controlSessionId", new BigDecimal("1.5"))),
                "integral number"
            ),
            Arguments.of(
                "integralDecimal",
                entry("closeSessionRequest", 14, Map.of("controlSessionId", new BigDecimal("1.0"))),
                "integral number"
            ),
            Arguments.of(
                "stringForData",
                entry("challengeResponse", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "encodedCredentials", "abcd"
                )),
                "data field"
            ),
            Arguments.of(
                "objectForAscii",
                entry("startRecordingRequest", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "streamId", BigInteger.valueOf(3),
                    "sourceLocation", "LOCAL",
                    "channel", Map.of("hex", "61")
                )),
                "ascii field"
            ),
            Arguments.of(
                "extraDataKey",
                entry("challengeResponse", 14, Map.of(
                    "controlSessionId", BigInteger.ONE,
                    "correlationId", BigInteger.TWO,
                    "encodedCredentials", Map.of("hex", "0a", "ref", "x")
                )),
                "expected {\"hex\": string} object"
            )
        );
    }

    private static FixtureEntry entry(final String template, final int actingVersion, final Map<String, Object> fields) {
        return new FixtureEntry("bad", template, actingVersion, "00", fields);
    }

    private static Stream<FixtureEntry> rowsFor(final String template) {
        return CATALOG.fixtures().stream().filter(entry -> entry.template().equals(template));
    }

    private static boolean nonEmptyVar(final Object value) {
        if (value instanceof String text) {
            return !text.isEmpty();
        }
        if (!(value instanceof Map<?, ?> map)) {
            return false;
        }
        return map.get("hex") instanceof String hex && !hex.isEmpty();
    }

    private static ArchiveSbeCatalog load() {
        try {
            return ArchiveSbeCatalog.parse(Files.readString(CATALOG_PATH));
        } catch (final IOException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }
}
