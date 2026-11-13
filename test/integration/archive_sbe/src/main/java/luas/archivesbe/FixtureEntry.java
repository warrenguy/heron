package luas.archivesbe;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** One catalog fixture row. assertConsumed rejects a field that the encoder did not read. */
public final class FixtureEntry {
    private final String name;
    private final String template;
    private final int actingVersion;
    private final String hex;
    private final Map<String, Object> fields;
    private final Set<String> readKeys = new HashSet<>();

    public FixtureEntry(
        final String name,
        final String template,
        final int actingVersion,
        final String hex,
        final Map<String, Object> fields
    ) {
        this.name = name;
        this.template = template;
        this.actingVersion = actingVersion;
        this.hex = hex;
        this.fields = fields;
    }

    public String name() {
        return name;
    }

    public String template() {
        return template;
    }

    public int actingVersion() {
        return actingVersion;
    }

    public String hex() {
        return hex;
    }

    public Map<String, Object> fields() {
        return fields;
    }

    boolean has(final String key) {
        return fields.containsKey(key);
    }

    Object read(final String key) {
        if (!fields.containsKey(key)) {
            throw new IllegalArgumentException("missing " + key);
        }
        readKeys.add(key);
        return fields.get(key);
    }

    long int64(final String key) {
        return JsonValues.int64(read(key), key);
    }

    int int32(final String key) {
        return JsonValues.int32(read(key), key);
    }

    String asciiOrEmpty(final String key) {
        if (!has(key)) {
            return "";
        }
        return JsonValues.ascii(read(key), key);
    }

    byte[] dataOrEmpty(final String key) {
        if (!has(key)) {
            return JsonValues.EMPTY;
        }
        return JsonValues.data(read(key), key);
    }

    <E extends Enum<E>> E enumValue(final Class<E> type, final String key) {
        return JsonValues.enumValue(type, read(key), key);
    }

    void assertConsumed() {
        for (final String key : fields.keySet()) {
            if (!readKeys.contains(key)) {
                throw new IllegalArgumentException("unread field " + key);
            }
        }
    }
}
