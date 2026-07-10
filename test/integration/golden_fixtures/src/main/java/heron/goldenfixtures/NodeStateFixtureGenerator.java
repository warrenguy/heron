package heron.goldenfixtures;

import io.aeron.cluster.NodeStateFile;
import org.agrona.IoUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Writes cluster/election/testdata/node-state golden fixtures via the Aeron
 * NodeStateFile write API. Emits the fixtures.json manifest from values read
 * back out of each generated file so the manifest reflects actual bytes.
 */
public final class NodeStateFixtureGenerator {
    private static final int PREFIX_LENGTH = 128;
    private static final int SYNC_LEVEL = 0;

    private NodeStateFixtureGenerator() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: NodeStateFixtureGenerator <testdata-dir>");
        }

        final Path outputDir = Path.of(args[0]);
        Files.createDirectories(outputDir);

        final List<Fixture> fixtures = new ArrayList<>();
        fixtures.add(writeFixture(
            outputDir,
            "new.dat",
            "create",
            "Java NodeStateFile after createNew=true (initialiseDecodersOnCreation)",
            nodeStateFile -> {
            }));
        fixtures.add(writeFixture(
            outputDir,
            "updated.dat",
            "update",
            "Java NodeStateFile after updateCandidateTermId (NodeStateFileTest.shouldPersistCandidateTermId)",
            nodeStateFile -> nodeStateFile.updateCandidateTermId(832234L, 8923423L, 324234L)));

        final String json = renderManifest(fixtures);
        Files.write(outputDir.resolve("fixtures.json"), json.getBytes(StandardCharsets.UTF_8));
    }

    private static Fixture writeFixture(
        final Path outputDir,
        final String name,
        final String action,
        final String description,
        final Consumer<NodeStateFile> populate) throws IOException {
        final Path work = Files.createTempDirectory("node-state-fixture-");
        try {
            final CandidateTermValues values;
            try (NodeStateFile nodeStateFile = new NodeStateFile(work.toFile(), true, SYNC_LEVEL)) {
                populate.accept(nodeStateFile);

                final NodeStateFile.CandidateTerm candidateTerm = nodeStateFile.candidateTerm();
                values = new CandidateTermValues(
                    candidateTerm.candidateTermId(),
                    candidateTerm.timestamp(),
                    candidateTerm.logPosition());
            }
            writePrefix(work.resolve(NodeStateFile.FILENAME), outputDir.resolve(name));
            return new Fixture(name, action, description, values);
        } finally {
            IoUtil.delete(work.toFile(), true);
        }
    }

    private static void writePrefix(final Path source, final Path out) throws IOException {
        final byte[] all = Files.readAllBytes(source);
        if (all.length < PREFIX_LENGTH) {
            throw new IllegalStateException("node-state file too short: " + all.length);
        }
        Files.write(out, Arrays.copyOf(all, PREFIX_LENGTH));
    }

    private static String renderManifest(final List<Fixture> fixtures) {
        final StringBuilder out = new StringBuilder();
        out.append("{\n");
        out.append("  \"prefixLength\": ").append(PREFIX_LENGTH).append(",\n");
        out.append("  \"fixtures\": [\n");
        for (int i = 0; i < fixtures.size(); i++) {
            final Fixture fixture = fixtures.get(i);
            final CandidateTermValues values = fixture.values();
            out.append("    {\n");
            out.append("      \"file\": ").append(Json.quote(fixture.name())).append(",\n");
            out.append("      \"action\": ").append(Json.quote(fixture.action())).append(",\n");
            out.append("      \"description\": ").append(Json.quote(fixture.description())).append(",\n");
            out.append("      \"candidateTerm\": {\n");
            out.append("        \"candidateTermId\": ").append(values.candidateTermId()).append(",\n");
            out.append("        \"timestampMs\": ").append(values.timestampMs()).append(",\n");
            out.append("        \"logPosition\": ").append(values.logPosition()).append('\n');
            out.append("      }\n");
            out.append(i + 1 < fixtures.size() ? "    },\n" : "    }\n");
        }
        out.append("  ]\n");
        out.append("}\n");
        return out.toString();
    }

    private record CandidateTermValues(long candidateTermId, long timestampMs, long logPosition) {
    }

    private record Fixture(
        String name,
        String action,
        String description,
        CandidateTermValues values) {
    }
}
