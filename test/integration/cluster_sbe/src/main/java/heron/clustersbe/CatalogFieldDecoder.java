package heron.clustersbe;

import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.SessionConnectRequestEncoder;
import io.aeron.cluster.codecs.SessionEventEncoder;

import java.util.HexFormat;
import java.util.Map;

final class CatalogFieldDecoder {
    private static final HexFormat HEX = HexFormat.of();

    private CatalogFieldDecoder() {
    }

    static byte[] encode(final FixtureEntry entry, final Map<String, byte[]> blobs) {
        return switch (entry.template()) {
            case "sessionMessageHeader" -> encodeSessionMessageHeaderFields(entry.fields());
            case "sessionEvent" -> encodeSessionEventFields(entry);
            case "sessionConnectRequest" -> encodeSessionConnectRequestFields(entry, blobs);
            case "sessionCloseRequest" -> encodeSessionCloseRequestFields(entry.fields());
            case "sessionKeepAlive" -> encodeSessionKeepAliveFields(entry.fields());
            case "newLeaderEvent" -> encodeNewLeaderEventFields(entry.fields());
            case "challenge" -> encodeChallengeFields(entry.fields(), blobs);
            case "challengeResponse" -> encodeChallengeResponseFields(entry.fields(), blobs);
            case "adminRequest" -> encodeAdminRequestFields(entry.fields(), blobs);
            case "adminResponse" -> encodeAdminResponseFields(entry.fields(), blobs);
            default -> throw new IllegalArgumentException("unknown template: " + entry.template());
        };
    }

    private static byte[] encodeSessionMessageHeaderFields(final Map<String, Object> fields) {
        return ClusterSbeFixtures.encodeSessionMessageHeader(
            requireInt64(fields, "leadershipTermId"),
            requireInt64(fields, "clusterSessionId"),
            requireInt64(fields, "timestamp")
        );
    }

    private static byte[] encodeSessionEventFields(final FixtureEntry entry) {
        final Map<String, Object> fields = entry.fields();
        final int actingVersion = entry.actingVersion();
        final int blockLength = sessionEventBlockLength(actingVersion, entry.blockLength());
        return ClusterSbeFixtures.encodeSessionEvent(
            actingVersion,
            blockLength,
            requireInt64(fields, "clusterSessionId"),
            requireInt64(fields, "correlationId"),
            requireInt64(fields, "leadershipTermId"),
            requireInt32(fields, "leaderMemberId"),
            EventCode.valueOf(stringField(fields, "code")),
            optionalInt32(fields, "version"),
            requireHeartbeat(fields),
            optionalString(fields, "detail")
        );
    }

    private static byte[] encodeSessionConnectRequestFields(
        final FixtureEntry entry,
        final Map<String, byte[]> blobs
    ) {
        final Map<String, Object> fields = entry.fields();
        final int actingVersion = entry.actingVersion();
        return ClusterSbeFixtures.encodeSessionConnectRequest(
            actingVersion,
            sessionConnectRequestBlockLength(actingVersion),
            requireInt64(fields, "correlationId"),
            requireInt32(fields, "responseStreamId"),
            optionalInt32(fields, "version"),
            optionalString(fields, "responseChannel"),
            decodeBytesField(fields.get("encodedCredentials"), blobs),
            optionalString(fields, "clientInfo")
        );
    }

    private static byte[] encodeSessionCloseRequestFields(final Map<String, Object> fields) {
        return ClusterSbeFixtures.encodeSessionCloseRequest(
            requireInt64(fields, "leadershipTermId"),
            requireInt64(fields, "clusterSessionId")
        );
    }

    private static byte[] encodeSessionKeepAliveFields(final Map<String, Object> fields) {
        return ClusterSbeFixtures.encodeSessionKeepAlive(
            requireInt64(fields, "leadershipTermId"),
            requireInt64(fields, "clusterSessionId")
        );
    }

    private static byte[] encodeNewLeaderEventFields(final Map<String, Object> fields) {
        return ClusterSbeFixtures.encodeNewLeaderEvent(
            requireInt64(fields, "leadershipTermId"),
            requireInt64(fields, "clusterSessionId"),
            requireInt32(fields, "leaderMemberId"),
            optionalString(fields, "ingressEndpoints")
        );
    }

    private static byte[] encodeChallengeFields(
        final Map<String, Object> fields,
        final Map<String, byte[]> blobs
    ) {
        return ClusterSbeFixtures.encodeChallenge(
            requireInt64(fields, "correlationId"),
            requireInt64(fields, "clusterSessionId"),
            decodeBytesField(fields.get("encodedChallenge"), blobs)
        );
    }

    private static byte[] encodeChallengeResponseFields(
        final Map<String, Object> fields,
        final Map<String, byte[]> blobs
    ) {
        return ClusterSbeFixtures.encodeChallengeResponse(
            requireInt64(fields, "correlationId"),
            requireInt64(fields, "clusterSessionId"),
            decodeBytesField(fields.get("encodedCredentials"), blobs)
        );
    }

    private static byte[] encodeAdminRequestFields(
        final Map<String, Object> fields,
        final Map<String, byte[]> blobs
    ) {
        return ClusterSbeFixtures.encodeAdminRequest(
            requireInt64(fields, "leadershipTermId"),
            requireInt64(fields, "clusterSessionId"),
            requireInt64(fields, "correlationId"),
            AdminRequestType.valueOf(stringField(fields, "requestType")),
            decodeBytesField(fields.get("payload"), blobs)
        );
    }

    private static byte[] encodeAdminResponseFields(
        final Map<String, Object> fields,
        final Map<String, byte[]> blobs
    ) {
        return ClusterSbeFixtures.encodeAdminResponse(
            requireInt64(fields, "clusterSessionId"),
            requireInt64(fields, "correlationId"),
            AdminRequestType.valueOf(stringField(fields, "requestType")),
            AdminResponseCode.valueOf(stringField(fields, "responseCode")),
            optionalString(fields, "message"),
            decodeBytesField(fields.get("payload"), blobs)
        );
    }

    private static int sessionEventBlockLength(final int actingVersion, final Integer override) {
        if (override != null) {
            return override;
        }
        if (actingVersion < 6) {
            return 32;
        }
        if (actingVersion < 13) {
            return 36;
        }
        return SessionEventEncoder.BLOCK_LENGTH;
    }

    private static int sessionConnectRequestBlockLength(final int actingVersion) {
        return actingVersion < 2 ? 12 : SessionConnectRequestEncoder.BLOCK_LENGTH;
    }

    private static long requireInt64(final Map<String, Object> fields, final String key) {
        final Object value = fields.get(key);
        if (value == null) {
            throw new IllegalArgumentException("missing " + key);
        }
        return CatalogJsonValues.int64FromJsonValue(value);
    }

    private static int requireInt32(final Map<String, Object> fields, final String key) {
        final long value = requireInt64(fields, key);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(key + ": out of int32 range");
        }
        return (int) value;
    }

    private static int optionalInt32(final Map<String, Object> fields, final String key) {
        if (!fields.containsKey(key)) {
            return 0;
        }
        return requireInt32(fields, key);
    }

    private static String optionalString(final Map<String, Object> fields, final String key) {
        if (!fields.containsKey(key)) {
            return null;
        }
        return stringField(fields, key);
    }

    private static String stringField(final Map<String, Object> fields, final String key) {
        final Object value = fields.get(key);
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException(key + ": expected string");
        }
        return text;
    }

    private static long requireHeartbeat(final Map<String, Object> fields) {
        if (!fields.containsKey("leaderHeartbeatTimeoutNs")) {
            throw new IllegalArgumentException("missing leaderHeartbeatTimeoutNs");
        }
        final Object value = fields.get("leaderHeartbeatTimeoutNs");
        if (value instanceof String sentinel) {
            if (CatalogConstants.HEARTBEAT_ABSENT_SENTINEL.equals(sentinel)) {
                return Long.MIN_VALUE;
            }
            throw new IllegalArgumentException("unknown heartbeat sentinel: " + sentinel);
        }
        return CatalogJsonValues.int64FromJsonValue(value);
    }

    private static byte[] decodeBytesField(final Object value, final Map<String, byte[]> blobs) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("expected hex or ref object");
        }
        final Object hexVal = map.get("hex");
        final Object refVal = map.get("ref");
        if (refVal != null) {
            if (hexVal != null) {
                throw new IllegalArgumentException("expected exactly one of hex or ref");
            }
            if (!(refVal instanceof String ref) || ref.isEmpty()) {
                throw new IllegalArgumentException("ref must be non-empty string");
            }
            final byte[] data = blobs.get(ref);
            if (data == null) {
                throw new IllegalArgumentException("unknown blob ref: " + ref);
            }
            return data;
        }
        if (hexVal == null) {
            throw new IllegalArgumentException("expected exactly one of hex or ref");
        }
        if (!(hexVal instanceof String hex)) {
            throw new IllegalArgumentException("hex must be string");
        }
        return HEX.parseHex(hex);
    }
}
