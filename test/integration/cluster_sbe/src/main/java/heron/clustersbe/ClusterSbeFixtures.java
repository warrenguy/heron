package heron.clustersbe;

import io.aeron.cluster.codecs.AdminRequestEncoder;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.AdminResponseEncoder;
import io.aeron.cluster.codecs.ChallengeEncoder;
import io.aeron.cluster.codecs.ChallengeResponseEncoder;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeaderEventEncoder;
import io.aeron.cluster.codecs.SessionCloseRequestEncoder;
import io.aeron.cluster.codecs.SessionConnectRequestEncoder;
import io.aeron.cluster.codecs.SessionEventEncoder;
import io.aeron.cluster.codecs.SessionKeepAliveEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Map;
import java.util.function.Consumer;

public final class ClusterSbeFixtures {
    private static final int SCHEMA_VERSION_16 = SessionMessageHeaderEncoder.SCHEMA_VERSION;
    private static final byte[] EMPTY_BYTES = new byte[0];

    private ClusterSbeFixtures() {
    }

    /**
     * Encodes one catalog fixture row to wire bytes. {@code blobs} must be the map from
     * resolveBlobs on the loaded catalog file so {@code ref} fields resolve to materialized
     * byte patterns.
     */
    public static byte[] encode(final FixtureEntry entry, final Map<String, byte[]> blobs) {
        return CatalogFieldDecoder.encode(entry, blobs);
    }

    static byte[] encodeSessionMessageHeader(
        final long leadershipTermId,
        final long clusterSessionId,
        final long timestamp
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SessionMessageHeaderEncoder encoder = new SessionMessageHeaderEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .timestamp(timestamp);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            SessionMessageHeaderEncoder.TEMPLATE_ID,
            SessionMessageHeaderEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeSessionCloseRequest(
        final long leadershipTermId,
        final long clusterSessionId
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SessionCloseRequestEncoder encoder = new SessionCloseRequestEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            SessionCloseRequestEncoder.TEMPLATE_ID,
            SessionCloseRequestEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeSessionKeepAlive(
        final long leadershipTermId,
        final long clusterSessionId
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SessionKeepAliveEncoder encoder = new SessionKeepAliveEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            SessionKeepAliveEncoder.TEMPLATE_ID,
            SessionKeepAliveEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeSessionEvent(
        final int actingVersion,
        final int blockLength,
        final long clusterSessionId,
        final long correlationId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final int version,
        final long leaderHeartbeatTimeoutNs,
        final String detail
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final boolean currentSchema =
            actingVersion == SCHEMA_VERSION_16 && blockLength == SessionEventEncoder.BLOCK_LENGTH;
        final SessionEventEncoder encoder = openSessionEventEncoder(
            buffer,
            actingVersion,
            blockLength,
            currentSchema
        );
        populateSessionEvent(
            encoder,
            actingVersion,
            clusterSessionId,
            correlationId,
            leadershipTermId,
            leaderMemberId,
            code,
            version,
            leaderHeartbeatTimeoutNs,
            detail
        );
        return currentSchema
            ? finishWire(
                buffer,
                encoder.limit(),
                SessionEventEncoder.TEMPLATE_ID,
                blockLength,
                actingVersion,
                SessionMessageHeaderEncoder.SCHEMA_ID
            )
            : copy(buffer, encoder.limit());
    }

    private static void populateSessionEvent(
        final SessionEventEncoder encoder,
        final int actingVersion,
        final long clusterSessionId,
        final long correlationId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final int version,
        final long leaderHeartbeatTimeoutNs,
        final String detail
    ) {
        encoder.clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .code(code);
        if (actingVersion >= 6 && version != 0) {
            encoder.version(version);
        }
        if (actingVersion >= 13 && leaderHeartbeatTimeoutNs != Long.MIN_VALUE) {
            encoder.leaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs);
        }
        putVarAscii(encoder::detail, detail);
    }

    static byte[] encodeSessionConnectRequest(
        final int actingVersion,
        final int blockLength,
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials,
        final String clientInfo
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        applyHeader(
            buffer,
            0,
            blockLength,
            SessionConnectRequestEncoder.TEMPLATE_ID,
            SessionConnectRequestEncoder.SCHEMA_ID,
            actingVersion
        );
        final SessionConnectRequestEncoder encoder = new SessionConnectRequestEncoder();
        encoder.wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
        encoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + blockLength);
        encoder.correlationId(correlationId).responseStreamId(responseStreamId);
        if (actingVersion >= 2 && version != 0) {
            encoder.version(version);
        }
        putVarAscii(encoder::responseChannel, responseChannel);
        putVarData(encoder::putEncodedCredentials, encodedCredentials);
        if (actingVersion >= 14) {
            putVarAscii(encoder::clientInfo, clientInfo);
        }
        return finishWire(
            buffer,
            encoder.limit(),
            SessionConnectRequestEncoder.TEMPLATE_ID,
            blockLength,
            actingVersion,
            SessionConnectRequestEncoder.SCHEMA_ID
        );
    }

    static byte[] encodeNewLeaderEvent(
        final long leadershipTermId,
        final long clusterSessionId,
        final int leaderMemberId,
        final String ingressEndpoints
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final NewLeaderEventEncoder encoder = new NewLeaderEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .leaderMemberId(leaderMemberId);
        putVarAscii(encoder::ingressEndpoints, ingressEndpoints);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            NewLeaderEventEncoder.TEMPLATE_ID,
            NewLeaderEventEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeChallenge(
        final long correlationId,
        final long clusterSessionId,
        final byte[] encodedChallenge
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ChallengeEncoder encoder = new ChallengeEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .correlationId(correlationId)
            .clusterSessionId(clusterSessionId);
        putVarData(encoder::putEncodedChallenge, encodedChallenge);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            ChallengeEncoder.TEMPLATE_ID,
            ChallengeEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeChallengeResponse(
        final long correlationId,
        final long clusterSessionId,
        final byte[] encodedCredentials
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ChallengeResponseEncoder encoder = new ChallengeResponseEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .correlationId(correlationId)
            .clusterSessionId(clusterSessionId);
        putVarData(encoder::putEncodedCredentials, encodedCredentials);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            ChallengeResponseEncoder.TEMPLATE_ID,
            ChallengeResponseEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeAdminRequest(
        final long leadershipTermId,
        final long clusterSessionId,
        final long correlationId,
        final AdminRequestType requestType,
        final byte[] payload
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final AdminRequestEncoder encoder = new AdminRequestEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .requestType(requestType);
        putVarData(encoder::putPayload, payload);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            AdminRequestEncoder.TEMPLATE_ID,
            AdminRequestEncoder.BLOCK_LENGTH
        );
    }

    static byte[] encodeAdminResponse(
        final long clusterSessionId,
        final long correlationId,
        final AdminRequestType requestType,
        final AdminResponseCode responseCode,
        final String message,
        final byte[] payload
    ) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final AdminResponseEncoder encoder = new AdminResponseEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .requestType(requestType)
            .responseCode(responseCode);
        putVarAscii(encoder::message, message);
        putVarData(encoder::putPayload, payload);
        return finishCurrentSchema(
            buffer,
            encoder.limit(),
            AdminResponseEncoder.TEMPLATE_ID,
            AdminResponseEncoder.BLOCK_LENGTH
        );
    }

    private static SessionEventEncoder openSessionEventEncoder(
        final ExpandableArrayBuffer buffer,
        final int actingVersion,
        final int blockLength,
        final boolean currentSchema
    ) {
        final SessionEventEncoder encoder = new SessionEventEncoder();
        if (currentSchema) {
            encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
            return encoder;
        }
        applyHeader(
            buffer,
            0,
            blockLength,
            SessionEventEncoder.TEMPLATE_ID,
            SessionEventEncoder.SCHEMA_ID,
            actingVersion
        );
        encoder.wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
        encoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + blockLength);
        return encoder;
    }

    private static void applyHeader(
        final MutableDirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int templateId,
        final int schemaId,
        final int actingVersion
    ) {
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        header.wrap(buffer, offset)
            .blockLength(blockLength)
            .templateId(templateId)
            .schemaId(schemaId)
            .version(actingVersion);
    }

    private static byte[] finishCurrentSchema(
        final MutableDirectBuffer buffer,
        final int length,
        final int templateId,
        final int blockLength
    ) {
        return finishWire(
            buffer,
            length,
            templateId,
            blockLength,
            SCHEMA_VERSION_16,
            SessionMessageHeaderEncoder.SCHEMA_ID
        );
    }

    private static byte[] finishWire(
        final MutableDirectBuffer buffer,
        final int length,
        final int templateId,
        final int blockLength,
        final int actingVersion,
        final int schemaId
    ) {
        applyHeader(buffer, 0, blockLength, templateId, schemaId, actingVersion);
        return copy(buffer, length);
    }

    private static byte[] copy(final MutableDirectBuffer buffer, final int length) {
        final byte[] out = new byte[length];
        buffer.getBytes(0, out, 0, length);
        return out;
    }

    private static void putVarAscii(final Consumer<String> setter, final String value) {
        setter.accept(value == null ? "" : value);
    }

    @FunctionalInterface
    private interface VarDataPutter {
        void put(byte[] data, int offset, int length);
    }

    private static void putVarData(final VarDataPutter setter, final byte[] value) {
        if (value == null || value.length == 0) {
            setter.put(EMPTY_BYTES, 0, 0);
            return;
        }
        setter.put(value, 0, value.length);
    }
}
