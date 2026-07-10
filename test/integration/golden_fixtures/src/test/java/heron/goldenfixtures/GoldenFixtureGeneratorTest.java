package heron.goldenfixtures;

import io.aeron.cluster.RecordingLogFixtureGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GoldenFixtureGeneratorTest {
    private static final Path REPO_ROOT = Path.of("..", "..", "..");

    @Test
    void recordingLogFixturesMatchCommitted(@TempDir final Path regenerated) throws IOException {
        RecordingLogFixtureGenerator.main(new String[] {regenerated.toString()});
        assertMatchesCommitted(REPO_ROOT.resolve("cluster/recordinglog/testdata"), regenerated);
    }

    @Test
    void nodeStateFixturesMatchCommitted(@TempDir final Path regenerated) throws IOException {
        NodeStateFixtureGenerator.main(new String[] {regenerated.toString()});
        assertMatchesCommitted(REPO_ROOT.resolve("cluster/election/testdata/node-state"), regenerated);
    }

    private static void assertMatchesCommitted(
        final Path committed,
        final Path regenerated) throws IOException {
        final Set<String> freshNames = fileNames(regenerated);
        assertEquals(fileNames(committed), freshNames, "committed and generated file sets differ");

        for (final String file : freshNames) {
            assertArrayEquals(
                Files.readAllBytes(committed.resolve(file)),
                Files.readAllBytes(regenerated.resolve(file)),
                file);
        }
    }

    private static Set<String> fileNames(final Path dir) throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries
                .filter(Files::isRegularFile)
                .map(p -> p.getFileName().toString())
                .collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
        }
    }
}
