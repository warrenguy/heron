package luas.archivesbe;

import java.math.BigInteger;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;

/** Parses catalog row values from decoded JSON. */
final class JsonValues {
    static final byte[] EMPTY = new byte[0];
    private static final HexFormat HEX = HexFormat.of();

    private JsonValues() {
    }

    static long int64(final Object value, final String key) {
        try {
            return integral(value, key).longValueExact();
        } catch (final ArithmeticException ex) {
            throw new IllegalArgumentException(key + ": out of int64 range", ex);
        }
    }

    static int int32(final Object value, final String key) {
        final long wide = int64(value, key);
        if (wide != (int) wide) {
            throw new IllegalArgumentException(key + ": out of int32 range");
        }
        return (int) wide;
    }

    static String ascii(final Object value, final String key) {
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException(key + ": expected string for ascii field");
        }
        if (text.isEmpty()) {
            throw new IllegalArgumentException(key + ": empty var value");
        }
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) > 0x7f) {
                throw new IllegalArgumentException(key + ": non-ASCII character");
            }
        }
        return text;
    }

    static byte[] data(final Object value, final String key) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException(key + ": expected hex object for data field");
        }
        if (map.size() != 1 || !(map.get("hex") instanceof String hex)) {
            throw new IllegalArgumentException(key + ": expected {\"hex\": string} object");
        }
        if (hex.isEmpty()) {
            throw new IllegalArgumentException(key + ": empty var value");
        }
        checkHexDigits(hex, key);
        return HEX.parseHex(hex);
    }

    static <E extends Enum<E>> E enumValue(final Class<E> type, final Object value, final String key) {
        if (!(value instanceof String name)) {
            throw new IllegalArgumentException(key + ": expected enum name");
        }
        if (name.equals("NULL_VAL")) {
            throw new IllegalArgumentException(key + ": NULL_VAL is not a catalog value");
        }
        try {
            return Enum.valueOf(type, name);
        } catch (final IllegalArgumentException ex) {
            throw new IllegalArgumentException(key + ": unknown value " + name, ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static void checkEnum(final Class<?> type, final Object value, final String key) {
        enumValue((Class) type, value, key);
    }

    private static BigInteger integral(final Object value, final String key) {
        if (value instanceof BigInteger big) {
            return big;
        }
        throw new IllegalArgumentException(key + ": expected integral number");
    }

    static void checkHexDigits(final String hex, final String key) {
        if (!hex.equals(hex.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException(key + ": uppercase hex");
        }
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException(key + ": odd hex length");
        }
        for (int i = 0; i < hex.length(); i++) {
            if (!HexFormat.isHexDigit(hex.charAt(i))) {
                throw new IllegalArgumentException(key + ": invalid hex");
            }
        }
    }
}
