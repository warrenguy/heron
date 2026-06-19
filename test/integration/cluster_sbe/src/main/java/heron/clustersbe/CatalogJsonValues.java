package heron.clustersbe;

import java.math.BigInteger;

public final class CatalogJsonValues {
    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    public static long int64FromJsonValue(final Object value) {
        if (!(value instanceof BigInteger big)) {
            throw new IllegalArgumentException("expected number");
        }
        if (big.compareTo(INT64_MIN) < 0 || big.compareTo(INT64_MAX) > 0) {
            throw new IllegalArgumentException("out of int64 range");
        }
        return big.longValue();
    }

    private CatalogJsonValues() {
    }
}
