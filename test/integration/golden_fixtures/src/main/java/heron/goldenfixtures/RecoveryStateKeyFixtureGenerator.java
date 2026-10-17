package heron.goldenfixtures;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.RecoveryState;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.CountersReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes the recovery state key golden fixtures. Each row allocates a counter
 * through RecoveryState.allocate and copies its whole key region. Manifest
 * values come from the RecoveryState getters, or from the row where no getter
 * exists.
 */
public final class RecoveryStateKeyFixtureGenerator {
    private RecoveryStateKeyFixtureGenerator() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: RecoveryStateKeyFixtureGenerator <testdata-dir>");
        }

        final Path outputDir = Path.of(args[0]);
        Files.createDirectories(outputDir);

        final List<Row> rows = List.of(
            new Row(
                "snapshot-two-services",
                "snapshot taken with two services",
                7L, 65536L, 1700000000123L, 1,
                new long[] {10L, 11L}),
            new Row(
                "term-only",
                "no snapshot: ConsensusModuleAgent allocates a term with zero position and timestamp",
                -1L, 0L, 0L, 2,
                new long[0]),
            new Row(
                "max-services",
                "ten services fill the key region",
                42L, 9223372036854775807L, -1L, -3,
                new long[] {100L, 101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L, -1L}));

        final Path work = Files.createTempDirectory("recovery-state-key-fixture-");
        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(work.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
        final List<Fixture> fixtures = new ArrayList<>();
        try (MediaDriver driver = MediaDriver.launch(driverContext);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()))) {
            final List<Counter> counters = new ArrayList<>();
            try {
                for (final Row row : rows) {
                    final Counter counter = RecoveryState.allocate(
                        aeron,
                        row.leadershipTermId(),
                        row.logPosition(),
                        row.timestamp(),
                        row.clusterId(),
                        row.snapshotRecordingIds());
                    counters.add(counter);
                    fixtures.add(writeFixture(outputDir, aeron.countersReader(), counter.id(), row));
                }
            } finally {
                counters.forEach(Counter::close);
            }
        } finally {
            IoUtil.delete(work.toFile(), true);
        }

        final String json = renderManifest(fixtures);
        Files.write(outputDir.resolve("fixtures.json"), json.getBytes(StandardCharsets.UTF_8));
    }

    private static Fixture writeFixture(
        final Path outputDir,
        final CountersReader countersReader,
        final int counterId,
        final Row row) throws IOException {
        final int foundCounterId = RecoveryState.findCounterId(countersReader, row.clusterId());
        if (foundCounterId != counterId) {
            throw new IllegalStateException(
                row.name() + ": findCounterId returned " + foundCounterId + ", want " + counterId);
        }

        final int serviceCount = row.snapshotRecordingIds().length;
        final long[] snapshotRecordingIds = new long[serviceCount];
        for (int serviceId = 0; serviceId < serviceCount; serviceId++) {
            snapshotRecordingIds[serviceId] =
                RecoveryState.getSnapshotRecordingId(countersReader, counterId, serviceId);
        }
        assertServiceCountBound(countersReader, counterId, serviceCount, row.name());

        final KeyValues values = new KeyValues(
            RecoveryState.getLeadershipTermId(countersReader, counterId),
            RecoveryState.getLogPosition(countersReader, counterId),
            RecoveryState.getTimestamp(countersReader, counterId),
            row.clusterId(),
            serviceCount,
            snapshotRecordingIds);

        final DirectBuffer metaData = countersReader.metaDataBuffer();
        final byte[] key = new byte[CountersReader.MAX_KEY_LENGTH];
        metaData.getBytes(CountersReader.metaDataOffset(counterId) + CountersReader.KEY_OFFSET, key);
        final String file = row.name() + ".key";
        Files.write(outputDir.resolve(file), key);

        return new Fixture(file, row.description(), values);
    }

    private static void assertServiceCountBound(
        final CountersReader countersReader,
        final int counterId,
        final int serviceCount,
        final String name) {
        try {
            RecoveryState.getSnapshotRecordingId(countersReader, counterId, serviceCount);
        } catch (final ClusterException expected) {
            return;
        }
        throw new IllegalStateException(
            name + ": getSnapshotRecordingId accepted service id " + serviceCount);
    }

    private static String renderManifest(final List<Fixture> fixtures) {
        final StringBuilder out = new StringBuilder();
        out.append("{\n");
        out.append("  \"keyLength\": ").append(CountersReader.MAX_KEY_LENGTH).append(",\n");
        out.append("  \"fixtures\": [\n");
        for (int i = 0; i < fixtures.size(); i++) {
            final Fixture fixture = fixtures.get(i);
            final KeyValues values = fixture.values();
            out.append("    {\n");
            out.append("      \"file\": ").append(Json.quote(fixture.file())).append(",\n");
            out.append("      \"description\": ").append(Json.quote(fixture.description())).append(",\n");
            out.append("      \"leadershipTermId\": ").append(values.leadershipTermId()).append(",\n");
            out.append("      \"logPosition\": ").append(values.logPosition()).append(",\n");
            out.append("      \"timestamp\": ").append(values.timestamp()).append(",\n");
            out.append("      \"clusterId\": ").append(values.clusterId()).append(",\n");
            out.append("      \"serviceCount\": ").append(values.serviceCount()).append(",\n");
            out.append("      \"snapshotRecordingIds\": [");
            final long[] ids = values.snapshotRecordingIds();
            for (int j = 0; j < ids.length; j++) {
                out.append(j > 0 ? ", " : "").append(ids[j]);
            }
            out.append("]\n");
            out.append(i + 1 < fixtures.size() ? "    },\n" : "    }\n");
        }
        out.append("  ]\n");
        out.append("}\n");
        return out.toString();
    }

    private record Row(
        String name,
        String description,
        long leadershipTermId,
        long logPosition,
        long timestamp,
        int clusterId,
        long[] snapshotRecordingIds) {
    }

    private record KeyValues(
        long leadershipTermId,
        long logPosition,
        long timestamp,
        int clusterId,
        int serviceCount,
        long[] snapshotRecordingIds) {
    }

    private record Fixture(String file, String description, KeyValues values) {
    }
}
