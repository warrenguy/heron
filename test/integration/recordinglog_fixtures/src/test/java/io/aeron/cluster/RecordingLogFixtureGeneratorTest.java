package io.aeron.cluster;

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

class RecordingLogFixtureGeneratorTest
{
    private static final Path COMMITTED =
        Path.of("..", "..", "..", "cluster", "recordinglog", "testdata");

    @Test
    void committedFixturesMatchGeneratorOutput(@TempDir final Path regenerated) throws IOException
    {
        RecordingLogFixtureGenerator.main(new String[] {regenerated.toString()});

        final Set<String> freshNames = fileNames(regenerated);
        assertEquals(fileNames(COMMITTED), freshNames, "committed and generated file sets differ");

        for (final String file : freshNames)
        {
            final byte[] committed = Files.readAllBytes(COMMITTED.resolve(file));
            final byte[] fresh = Files.readAllBytes(regenerated.resolve(file));
            assertArrayEquals(committed, fresh, file);
        }
    }

    private static Set<String> fileNames(final Path dir) throws IOException
    {
        try (Stream<Path> entries = Files.list(dir))
        {
            return entries
                .filter(Files::isRegularFile)
                .map(p -> p.getFileName().toString())
                .collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
        }
    }
}
