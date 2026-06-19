package heron.clustersbe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads cluster SBE fixture catalog.json from the test classpath.
 * JSON uses {@value CatalogConstants#HEARTBEAT_ABSENT_SENTINEL} for omitted
 * leaderHeartbeatTimeoutNs; {@link ClusterSbeFixtures#encode} maps it to {@link Long#MIN_VALUE}.
 * <p>
 * Structural parse only. Duplicate fixture names and {@code sessionConnectRequest} version
 * presence when {@code actingVersion < 2} are enforced at parse time. Field value typing and
 * template decode rules are enforced by encode ({@link CatalogFieldDecoder}); the contract
 * oracle ({@code encode == catalog.hex}) catches drift on every committed fixture.
 * <p>
 * {@link #load()} returns {@link CatalogFile} (schemaVersion, blobs, fixtures); callers
 * needing only fixture rows use {@link CatalogFile#fixtures()}.
 */
final class FixtureCatalog {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    record BlobRepeat(int byteValue, int len) {
    }

    record BlobSpec(BlobRepeat repeat) {
    }

    record CatalogFile(
        int schemaVersion,
        Map<String, BlobSpec> blobs,
        List<FixtureEntry> fixtures
    ) {
    }

    private static CatalogFile cached;
    private static String cachedJson;

    private FixtureCatalog() {
    }

    static CatalogFile load() throws IOException {
        if (cached != null) {
            return cached;
        }

        return cached = parseJson(classpathJson());
    }

    static String classpathJson() throws IOException {
        if (cachedJson != null) {
            return cachedJson;
        }

        try (InputStream in = FixtureCatalog.class.getResourceAsStream("/catalog.json")) {
            if (in == null) {
                throw catalogError("catalog.json not found on classpath");
            }
            return cachedJson = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static CatalogFile parseCatalogJson(final String json) throws IOException {
        return parseJson(json);
    }

    static Map<String, byte[]> resolveBlobs(final Map<String, BlobSpec> specs) {
        final Map<String, byte[]> out = new HashMap<>(specs.size());
        for (final Map.Entry<String, BlobSpec> entry : specs.entrySet()) {
            final BlobRepeat repeat = entry.getValue().repeat();
            final byte[] data = new byte[repeat.len()];
            Arrays.fill(data, (byte) repeat.byteValue());
            out.put(entry.getKey(), data);
        }
        return out;
    }

    static long int64FromJsonValue(final Object value) {
        return CatalogJsonValues.int64FromJsonValue(value);
    }

    private static CatalogFile parseJson(final String json) throws IOException {
        final JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (final JsonProcessingException ex) {
            throw catalogError("invalid catalog json: " + ex.getOriginalMessage());
        }
        if (!root.isObject()) {
            throw catalogError("catalog root must be object");
        }

        final int schemaVersion = requireIntegral(
            root.path("schemaVersion"),
            "missing or invalid schemaVersion"
        );

        final JsonNode blobsNode = root.get("blobs");
        final Map<String, BlobSpec> blobs;
        if (blobsNode == null || blobsNode.isNull()) {
            blobs = Map.of();
        } else if (!blobsNode.isObject()) {
            throw catalogError("invalid blobs");
        } else {
            blobs = parseBlobs(blobsNode);
        }

        final JsonNode fixturesNode = root.get("fixtures");
        if (fixturesNode == null || !fixturesNode.isArray()) {
            throw catalogError("missing or invalid fixtures");
        }

        final List<FixtureEntry> fixtures = new ArrayList<>(fixturesNode.size());
        final Set<String> seenNames = new HashSet<>();
        for (final JsonNode fixtureNode : fixturesNode) {
            final FixtureEntry entry = parseFixtureEntry(fixtureNode);
            if (!seenNames.add(entry.name())) {
                throw catalogError("duplicate fixture name \"" + entry.name() + "\"");
            }
            fixtures.add(entry);
        }

        return new CatalogFile(schemaVersion, blobs, List.copyOf(fixtures));
    }

    private static Map<String, BlobSpec> parseBlobs(final JsonNode blobsNode) throws IOException {
        final Map<String, BlobSpec> blobs = new LinkedHashMap<>();
        for (final Map.Entry<String, JsonNode> entry : blobsNode.properties()) {
            final JsonNode repeatNode = entry.getValue().get("repeat");
            if (repeatNode == null || !repeatNode.isObject()) {
                throw catalogError("blob \"" + entry.getKey() + "\": missing repeat");
            }
            final JsonNode byteNode = repeatNode.get("byte");
            final JsonNode lenNode = repeatNode.get("len");
            final String blobName = entry.getKey();
            final int byteValue = requireIntegral(byteNode, "blob \"" + blobName + "\": missing repeat byte");
            final int len = requireIntegral(lenNode, "blob \"" + blobName + "\": missing repeat len");
            if (len <= 0) {
                throw catalogError("blob \"" + blobName + "\": repeat len must be positive");
            }
            if (byteValue < 0 || byteValue > 255) {
                throw catalogError("blob \"" + blobName + "\": repeat byte out of range");
            }
            blobs.put(blobName, new BlobSpec(new BlobRepeat(byteValue, len)));
        }
        return Map.copyOf(blobs);
    }

    private static FixtureEntry parseFixtureEntry(final JsonNode fixtureNode) throws IOException {
        if (!fixtureNode.isObject()) {
            throw catalogError("fixture entry must be object");
        }

        final String name = requireText(fixtureNode, "name");
        final String template = requireText(fixtureNode, "template");
        final int actingVersion = requireIntegral(
            fixtureNode.path("actingVersion"),
            "fixture \"" + name + "\": missing actingVersion"
        );
        final String hex = requireText(fixtureNode, "hex");
        final JsonNode fieldsNode = fixtureNode.path("fields");
        if (!fieldsNode.isObject()) {
            throw catalogError("fixture \"" + name + "\": missing fields");
        }

        Integer blockLength = null;
        final JsonNode blockLengthNode = fixtureNode.get("blockLength");
        if (blockLengthNode != null) {
            blockLength = requireIntegral(
                blockLengthNode,
                "fixture \"" + name + "\": invalid blockLength"
            );
        }

        final Map<String, Object> fields = parseFieldsObject(fieldsNode);
        if ("sessionConnectRequest".equals(template) && actingVersion < 2 && !fields.containsKey("version")) {
            throw catalogError(
                "fixture \"" + name + "\": missing version for actingVersion < 2"
            );
        }

        return new FixtureEntry(
            name,
            template,
            actingVersion,
            blockLength,
            hex,
            fields
        );
    }

    private static String requireText(final JsonNode node, final String key) throws IOException {
        final JsonNode value = node.path(key);
        if (!value.isTextual()) {
            throw catalogError("missing or invalid " + key);
        }
        return value.asText();
    }

    private static int requireIntegral(final JsonNode node, final String detail) throws IOException {
        if (!node.isIntegralNumber()) {
            throw catalogError(detail);
        }
        return node.intValue();
    }

    private static Map<String, Object> parseFieldsObject(final JsonNode fieldsNode) throws IOException {
        final Map<String, Object> fields = new LinkedHashMap<>();
        for (final Map.Entry<String, JsonNode> entry : fieldsNode.properties()) {
            fields.put(entry.getKey(), parseFieldValue(entry.getValue()));
        }
        return fields;
    }

    private static Object parseFieldValue(final JsonNode value) throws IOException {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isTextual()) {
            return value.asText();
        }
        if (value.isIntegralNumber()) {
            return value.bigIntegerValue();
        }
        if (value.isFloatingPointNumber()) {
            return value.decimalValue();
        }
        if (value.isObject()) {
            final Map<String, Object> object = new LinkedHashMap<>();
            for (final Map.Entry<String, JsonNode> entry : value.properties()) {
                final JsonNode child = entry.getValue();
                if (child == null || !child.isTextual()) {
                    throw catalogError("bytes object values must be strings");
                }
                object.put(entry.getKey(), child.asText());
            }
            return object;
        }
        throw catalogError("unsupported field value type");
    }

    private static IOException catalogError(final String detail) {
        return new IOException("heron: " + detail);
    }
}
