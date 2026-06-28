package io.aeron.cluster;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.agrona.IoUtil;

/**
 * Writes cluster/recordinglog golden fixtures via the Aeron RecordingLog write API.
 * Package io.aeron.cluster grants access to invalidateEntry and removeEntry.
 */
public final class RecordingLogFixtureGenerator
{
    private static final String MIXED = "mixed-entries.recordinglog";
    private static final String STANDBY_THEN_TERM = "standby-then-term.recordinglog";
    private static final String INVALID_SNAPSHOT = "invalid-snapshot.recordinglog";
    private static final String NULL_SLOT = "null-slot.recordinglog";
    private static final String TRUNCATED_TAIL = "truncated-tail.recordinglog";

    private static final String STANDBY_ENDPOINT = "aeron:udp?endpoint=standby.example.com:8020";

    private static final Field POSITION_FIELD = positionField();

    private RecordingLogFixtureGenerator()
    {
    }

    public static void main(final String[] args) throws IOException
    {
        if (args.length != 1)
        {
            throw new IllegalArgumentException("usage: RecordingLogFixtureGenerator <testdata-dir>");
        }

        final Path outputDir = Path.of(args[0]);
        Files.createDirectories(outputDir);

        final List<Fixture> fixtures = new ArrayList<>();
        fixtures.add(writeMixedEntries(outputDir));
        fixtures.add(writeStandbyThenTerm(outputDir));
        fixtures.add(writeInvalidSnapshot(outputDir));
        fixtures.add(writeNullSlot(outputDir));
        fixtures.add(writeTruncatedTail(outputDir));

        final String json = renderManifest(fixtures);
        Files.write(outputDir.resolve("fixtures.json"), json.getBytes(StandardCharsets.UTF_8));
    }

    @FunctionalInterface
    private interface RecordingLogPopulator
    {
        void populate(RecordingLog log);
    }

    private static Fixture writeMixedEntries(final Path outputDir) throws IOException
    {
        return writeFixture(outputDir, MIXED, "recordinglog-mixed", log ->
        {
            log.appendTerm(1, 7, 0, 1000);
            log.appendSnapshot(2, 7, 0, 1024, 2000, 0);
            log.appendStandbySnapshot(3, 7, 0, 2048, 3000, 0, STANDBY_ENDPOINT);
        });
    }

    private static Fixture writeStandbyThenTerm(final Path outputDir) throws IOException
    {
        return writeFixture(outputDir, STANDBY_THEN_TERM, "recordinglog-standby-term", log ->
        {
            log.appendStandbySnapshot(3, 1, 0, 100, 2000, 0, STANDBY_ENDPOINT);
            log.appendTerm(10, 2, 0, 3000);
        });
    }

    private static Fixture writeInvalidSnapshot(final Path outputDir) throws IOException
    {
        return writeFixture(outputDir, INVALID_SNAPSHOT, "recordinglog-invalid", log ->
        {
            log.appendSnapshot(5, 3, 0, 512, 4000, 0);
            log.invalidateEntry(0);
        });
    }

    private static Fixture writeNullSlot(final Path outputDir) throws IOException
    {
        return writeFixture(outputDir, NULL_SLOT, "recordinglog-null", log ->
        {
            log.appendTerm(9, 0, 0, 1000);
            log.appendSnapshot(10, 0, 0, 64, 2000, 0);
            log.appendSnapshot(11, 0, 0, 128, 3000, 1);
            log.removeEntry(0, 1);
        });
    }

    private static Fixture writeTruncatedTail(final Path outputDir) throws IOException
    {
        final Path work = Files.createTempDirectory("recordinglog-truncated");
        try
        {
            try (RecordingLog log = new RecordingLog(work.toFile(), true))
            {
                log.appendTerm(4, 2, 0, 5000);
                log.appendSnapshot(8, 2, 0, 256, 6000, 0);
            }

            final Path source = work.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
            try (RandomAccessFile raf = new RandomAccessFile(source.toFile(), "rw"))
            {
                raf.setLength(64 + 30);
            }

            return publishFixture(outputDir, TRUNCATED_TAIL, work);
        }
        finally
        {
            IoUtil.delete(work.toFile(), true);
        }
    }

    private static Fixture writeFixture(
        final Path outputDir,
        final String name,
        final String prefix,
        final RecordingLogPopulator populator) throws IOException
    {
        final Path work = Files.createTempDirectory(prefix);
        try
        {
            try (RecordingLog log = new RecordingLog(work.toFile(), true))
            {
                populator.populate(log);
            }
            return publishFixture(outputDir, name, work);
        }
        finally
        {
            IoUtil.delete(work.toFile(), true);
        }
    }

    private static Fixture publishFixture(final Path outputDir, final String name, final Path work)
        throws IOException
    {
        final Path source = work.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        Files.copy(source, outputDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
        return new Fixture(name, decodeFromWork(work));
    }

    private static List<Row> decodeFromWork(final Path work) throws IOException
    {
        try (RecordingLog log = new RecordingLog(work.toFile(), false))
        {
            final List<Row> rows = new ArrayList<>();
            for (final RecordingLog.Entry entry : log.entries())
            {
                rows.add(Row.of(entry));
            }
            return rows;
        }
    }

    private static String renderManifest(final List<Fixture> fixtures)
    {
        final StringBuilder out = new StringBuilder();
        out.append("{\n");
        out.append("  \"fixtures\": [\n");
        for (int f = 0; f < fixtures.size(); f++)
        {
            final Fixture fixture = fixtures.get(f);
            out.append("    {\n");
            out.append("      \"file\": ").append(quote(fixture.name())).append(",\n");
            out.append("      \"entries\": [");
            final List<Row> entries = fixture.entries();
            if (entries.isEmpty())
            {
                out.append("]\n");
            }
            else
            {
                out.append('\n');
                for (int e = 0; e < entries.size(); e++)
                {
                    out.append(renderEntry(entries.get(e)));
                    out.append(e + 1 < entries.size() ? ",\n" : "\n");
                }
                out.append("      ]\n");
            }
            out.append(f + 1 < fixtures.size() ? "    },\n" : "    }\n");
        }
        out.append("  ]\n");
        out.append("}\n");
        return out.toString();
    }

    private static String renderEntry(final Row row)
    {
        return "        {"
            + "\"recordingId\": " + row.recordingId() + ", "
            + "\"leadershipTermId\": " + row.leadershipTermId() + ", "
            + "\"termBaseLogPosition\": " + row.termBaseLogPosition() + ", "
            + "\"logPosition\": " + row.logPosition() + ", "
            + "\"timestamp\": " + row.timestamp() + ", "
            + "\"serviceId\": " + row.serviceId() + ", "
            + "\"type\": " + row.type() + ", "
            + "\"valid\": " + row.valid() + ", "
            + "\"archiveEndpoint\": " + quote(row.archiveEndpoint()) + ", "
            + "\"position\": " + row.position() + ", "
            + "\"index\": " + row.index()
            + "}";
    }

    private static String quote(final String value)
    {
        final StringBuilder out = new StringBuilder(value.length() + 2);
        out.append('"');
        for (int i = 0; i < value.length(); i++)
        {
            final char c = value.charAt(i);
            switch (c)
            {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> out.append(c);
            }
        }
        out.append('"');
        return out.toString();
    }

    private static Field positionField()
    {
        try
        {
            final Field field = RecordingLog.Entry.class.getDeclaredField("position");
            field.setAccessible(true);
            return field;
        }
        catch (final NoSuchFieldException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private static long positionOf(final RecordingLog.Entry entry)
    {
        try
        {
            return POSITION_FIELD.getLong(entry);
        }
        catch (final IllegalAccessException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    private record Fixture(String name, List<Row> entries)
    {
    }

    private record Row(
        long recordingId,
        long leadershipTermId,
        long termBaseLogPosition,
        long logPosition,
        long timestamp,
        int serviceId,
        int type,
        boolean valid,
        String archiveEndpoint,
        long position,
        int index)
    {
        static Row of(final RecordingLog.Entry entry)
        {
            return new Row(
                entry.recordingId,
                entry.leadershipTermId,
                entry.termBaseLogPosition,
                entry.logPosition,
                entry.timestamp,
                entry.serviceId,
                entry.type,
                entry.isValid,
                entry.archiveEndpoint == null ? "" : entry.archiveEndpoint,
                positionOf(entry),
                entry.entryIndex);
        }
    }
}
