package luas.archivesbe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveSbeCatalogTest {
    private static final Path CATALOG_PATH = Path.of("archivesbe", "fixtures", "catalog.json");
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Test
    void regenerate_matchesCommittedCatalog(@TempDir final Path tempDir) throws Exception {
        final Path copy = tempDir.resolve("catalog.json");
        Files.copy(CATALOG_PATH, copy);
        ArchiveSbeCatalogGenerator.main(new String[] {copy.toString()});
        assertArrayEquals(
            Files.readAllBytes(CATALOG_PATH),
            Files.readAllBytes(copy),
            "stale catalog; run make archive-sbe-catalog-regen"
        );
    }

    @Test
    void regenerate_refreshesStaleHex(@TempDir final Path tempDir) throws Exception {
        final JsonNode root = MAPPER.readTree(CATALOG_PATH);
        final ObjectNode first = (ObjectNode) root.get("fixtures").get(0);
        final String fresh = first.get("hex").asString();
        first.put("hex", "00");
        final Path stale = tempDir.resolve("catalog.json");
        Files.writeString(stale, ArchiveSbeCatalog.canonical(root));
        ArchiveSbeCatalogGenerator.main(new String[] {stale.toString()});
        final String rewritten = MAPPER.readTree(stale).get("fixtures").get(0).get("hex").asString();
        assertEquals(fresh, rewritten);
    }

    @Test
    void parse_rejectsStructuralFaults() {
        assertParse("malformed json", "{");
        assertParse("missing fixtures", "{\"schemaVersion\":1}");
        assertParse("missing schemaVersion", "{\"schemaVersion\":4294967296,\"fixtures\":[]}");
        assertParse("empty name", fixtureDoc(closeRow("")));
        assertParse("duplicate name", fixtureDoc(closeRow("same") + "," + closeRow("same")));
        assertParse("missing hex", fixtureDoc(closeRow("closeSessionRequest_row", null)));
        assertParse("uppercase hex", fixtureDoc(closeRow("closeSessionRequest_row", "00AA")));
        assertParse("acting version 15", fixtureDoc(closeRow("closeSessionRequest_row").replace(
            "\"actingVersion\":14", "\"actingVersion\":15")));
    }

    private static void assertParse(final String messagePart, final String json) {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> ArchiveSbeCatalog.parse(json));
        assertTrue(ex.getMessage().contains(messagePart), messagePart + ": " + ex.getMessage());
    }

    private static String fixtureDoc(final String fixtures) {
        return "{\"schemaVersion\":1,\"fixtures\":[" + fixtures + "]}";
    }

    private static String closeRow(final String name) {
        return closeRow(name, "00");
    }

    // A null hex omits the member.
    private static String closeRow(final String name, final String hex) {
        final String hexMember = hex == null ? "" : "\"hex\":\"" + hex + "\",";
        return "{\"name\":\"" + name + "\",\"template\":\"closeSessionRequest\",\"actingVersion\":14,"
            + hexMember + "\"fields\":{\"controlSessionId\":1}}";
    }
}
