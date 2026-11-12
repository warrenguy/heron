package luas.archivesbe;

import io.aeron.archive.codecs.MessageHeaderEncoder;
import tools.jackson.core.JacksonException;
import tools.jackson.core.util.DefaultIndenter;
import tools.jackson.core.util.DefaultPrettyPrinter;
import tools.jackson.core.util.Separators;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Parses catalog.json and writes the canonical form. */
public final class ArchiveSbeCatalog {
    static final int SCHEMA_VERSION = 1;

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final HexFormat HEX = HexFormat.of();

    private final List<FixtureEntry> fixtures;

    private ArchiveSbeCatalog(final List<FixtureEntry> fixtures) {
        this.fixtures = fixtures;
    }

    public List<FixtureEntry> fixtures() {
        return fixtures;
    }

    public static ArchiveSbeCatalog parse(final String json) {
        return fromTree(readTree(json));
    }

    public static void regenerate(final Path path) throws IOException {
        final JsonNode root = readTree(Files.readString(path));
        final ArchiveSbeCatalog catalog = fromTree(root);
        final ArrayNode fixtures = (ArrayNode) root.get("fixtures");
        for (int i = 0; i < fixtures.size(); i++) {
            final FixtureEntry entry = catalog.fixtures().get(i);
            final byte[] wire = ArchiveSbeFixtures.encode(entry);
            ((ObjectNode) fixtures.get(i)).put("hex", HEX.formatHex(wire));
        }
        Files.writeString(path, canonical(root));
    }

    static String canonical(final JsonNode root) {
        final String text = MAPPER.writer().with(canonicalPrinter()).writeValueAsString(root);
        if (text.endsWith("\n")) {
            return text;
        }
        return text + "\n";
    }

    // Committed catalog form: two-space indent, ": " after each key, LF.
    private static DefaultPrettyPrinter canonicalPrinter() {
        final DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        return new DefaultPrettyPrinter()
            .withSeparators(Separators.createDefaultInstance()
                .withObjectNameValueSpacing(Separators.Spacing.AFTER))
            .withObjectIndenter(indenter)
            .withArrayIndenter(indenter);
    }

    private static JsonNode readTree(final String json) {
        try {
            return MAPPER.readTree(json);
        } catch (final JacksonException ex) {
            throw new IllegalArgumentException("malformed json", ex);
        }
    }

    private static ArchiveSbeCatalog fromTree(final JsonNode root) {
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("malformed json");
        }
        if (integralField(root.get("schemaVersion"), "missing schemaVersion") != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schemaVersion");
        }

        final JsonNode fixturesNode = root.get("fixtures");
        if (fixturesNode == null || !fixturesNode.isArray()) {
            throw new IllegalArgumentException("missing fixtures");
        }

        final List<FixtureEntry> fixtures = new ArrayList<>();
        final Set<String> names = new HashSet<>();
        for (final JsonNode node : fixturesNode) {
            final FixtureEntry entry = parseFixture(node);
            if (!names.add(entry.name())) {
                throw new IllegalArgumentException("duplicate name \"" + entry.name() + "\"");
            }
            fixtures.add(entry);
        }
        return new ArchiveSbeCatalog(List.copyOf(fixtures));
    }

    private static FixtureEntry parseFixture(final JsonNode node) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("fixture entry must be object");
        }
        final JsonNode nameNode = node.get("name");
        if (nameNode == null || !nameNode.isString() || nameNode.asString().isEmpty()) {
            throw new IllegalArgumentException("empty name");
        }
        final String name = nameNode.asString();
        final String template = textField(node, "template");
        final int actingVersion = integralField(
            node.get("actingVersion"), "fixture \"" + name + "\": missing actingVersion");
        if (actingVersion < 0 || actingVersion > MessageHeaderEncoder.SCHEMA_VERSION) {
            throw new IllegalArgumentException("fixture \"" + name + "\": acting version " + actingVersion);
        }
        final JsonNode hexNode = node.get("hex");
        if (hexNode == null || !hexNode.isString() || hexNode.asString().isEmpty()) {
            throw new IllegalArgumentException("fixture \"" + name + "\": missing hex");
        }
        final String hex = hexNode.asString();
        JsonValues.checkHexDigits(hex, "fixture \"" + name + "\"");
        final JsonNode fieldsNode = node.get("fields");
        if (fieldsNode == null || !fieldsNode.isObject()) {
            throw new IllegalArgumentException("fixture \"" + name + "\": missing fields");
        }
        return new FixtureEntry(name, template, actingVersion, hex, parseFields(fieldsNode));
    }

    private static Map<String, Object> parseFields(final JsonNode node) {
        final Map<String, Object> fields = new LinkedHashMap<>();
        for (final Map.Entry<String, JsonNode> entry : node.properties()) {
            fields.put(entry.getKey(), parseValue(entry.getValue(), entry.getKey()));
        }
        return Map.copyOf(fields);
    }

    private static Object parseValue(final JsonNode node, final String key) {
        if (node.isString()) {
            return node.asString();
        }
        if (node.isIntegralNumber()) {
            return node.bigIntegerValue();
        }
        if (node.isNumber()) {
            return node.decimalValue();
        }
        if (node.isObject()) {
            final Map<String, Object> object = new LinkedHashMap<>();
            for (final Map.Entry<String, JsonNode> entry : node.properties()) {
                object.put(entry.getKey(), parseValue(entry.getValue(), key));
            }
            return Map.copyOf(object);
        }
        throw new IllegalArgumentException(key + ": unsupported json value");
    }

    private static String textField(final JsonNode node, final String key) {
        final JsonNode value = node.get(key);
        if (value == null || !value.isString()) {
            throw new IllegalArgumentException("missing " + key);
        }
        return value.asString();
    }

    private static int integralField(final JsonNode node, final String detail) {
        if (node == null || !node.isIntegralNumber()) {
            throw new IllegalArgumentException(detail);
        }
        try {
            return node.bigIntegerValue().intValueExact();
        } catch (final ArithmeticException ex) {
            throw new IllegalArgumentException(detail, ex);
        }
    }
}
