package heron.goldenfixtures;

/**
 * Minimal JSON string quoting for fixture manifests. Escapes only quote and
 * backslash: manifest values are ASCII constants with no control characters,
 * and committed bytes must stay stable.
 */
public final class Json {
    private Json() {
    }

    public static String quote(final String value) {
        final StringBuilder out = new StringBuilder(value.length() + 2);
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> out.append(c);
            }
        }
        out.append('"');
        return out.toString();
    }
}
