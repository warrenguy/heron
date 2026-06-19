package heron.clustersbe;

import java.util.Map;

/** One catalog fixture row. */
public record FixtureEntry(
    String name,
    String template,
    int actingVersion,
    Integer blockLength,
    String hex,
    Map<String, Object> fields
) {
}
