package luas.archivesbe;

import io.aeron.archive.codecs.ArchiveIdRequestDecoder;
import io.aeron.archive.codecs.ArchiveIdRequestEncoder;
import io.aeron.archive.codecs.AuthConnectRequestDecoder;
import io.aeron.archive.codecs.AuthConnectRequestEncoder;
import io.aeron.archive.codecs.BooleanType;
import io.aeron.archive.codecs.ChallengeDecoder;
import io.aeron.archive.codecs.ChallengeEncoder;
import io.aeron.archive.codecs.ChallengeResponseDecoder;
import io.aeron.archive.codecs.ChallengeResponseEncoder;
import io.aeron.archive.codecs.CloseSessionRequestDecoder;
import io.aeron.archive.codecs.CloseSessionRequestEncoder;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseDecoder;
import io.aeron.archive.codecs.ControlResponseEncoder;
import io.aeron.archive.codecs.ExtendRecordingRequest2Decoder;
import io.aeron.archive.codecs.ExtendRecordingRequest2Encoder;
import io.aeron.archive.codecs.KeepAliveRequestDecoder;
import io.aeron.archive.codecs.KeepAliveRequestEncoder;
import io.aeron.archive.codecs.ListRecordingRequestDecoder;
import io.aeron.archive.codecs.ListRecordingRequestEncoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.RecordingSignalEventDecoder;
import io.aeron.archive.codecs.RecordingSignalEventEncoder;
import io.aeron.archive.codecs.ReplayRequestDecoder;
import io.aeron.archive.codecs.ReplayRequestEncoder;
import io.aeron.archive.codecs.ReplicateRequest2Decoder;
import io.aeron.archive.codecs.ReplicateRequest2Encoder;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.codecs.StartRecordingRequest2Decoder;
import io.aeron.archive.codecs.StartRecordingRequest2Encoder;
import io.aeron.archive.codecs.StartRecordingRequestDecoder;
import io.aeron.archive.codecs.StartRecordingRequestEncoder;
import io.aeron.archive.codecs.StopRecordingByIdentityRequestDecoder;
import io.aeron.archive.codecs.StopRecordingByIdentityRequestEncoder;
import io.aeron.archive.codecs.StopRecordingRequestDecoder;
import io.aeron.archive.codecs.StopRecordingRequestEncoder;
import io.aeron.archive.codecs.StopReplayRequestDecoder;
import io.aeron.archive.codecs.StopReplayRequestEncoder;
import io.aeron.archive.codecs.StopReplicationRequestDecoder;
import io.aeron.archive.codecs.StopReplicationRequestEncoder;
import io.aeron.archive.codecs.TruncateRecordingRequestDecoder;
import io.aeron.archive.codecs.TruncateRecordingRequestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/** Encodes catalog rows with the upstream archive control codecs. */
public final class ArchiveSbeFixtures {
    private ArchiveSbeFixtures() {
    }

    public static byte[] encode(final FixtureEntry entry) {
        TemplateIndex.check(entry);
        final byte[] wire = switch (entry.template()) {
            case "controlResponse" -> encodeControlResponse(entry);
            case "closeSessionRequest" -> encodeCloseSession(entry);
            case "startRecordingRequest" -> encodeStartRecording(entry);
            case "stopRecordingRequest" -> encodeStopRecording(entry);
            case "replayRequest" -> encodeReplay(entry);
            case "stopReplayRequest" -> encodeStopReplay(entry);
            case "listRecordingRequest" -> encodeListRecording(entry);
            case "truncateRecordingRequest" -> encodeTruncate(entry);
            case "recordingDescriptor" -> encodeRecordingDescriptor(entry);
            case "recordingSignalEvent" -> encodeRecordingSignal(entry);
            case "stopReplicationRequest" -> encodeStopReplication(entry);
            case "authConnectRequest" -> encodeAuthConnect(entry);
            case "challenge" -> encodeChallenge(entry);
            case "challengeResponse" -> encodeChallengeResponse(entry);
            case "keepAliveRequest" -> encodeKeepAlive(entry);
            case "startRecordingRequest2" -> encodeStartRecording2(entry);
            case "extendRecordingRequest2" -> encodeExtendRecording2(entry);
            case "stopRecordingByIdentityRequest" -> encodeStopByIdentity(entry);
            case "replicateRequest2" -> encodeReplicate2(entry);
            case "archiveIdRequest" -> encodeArchiveId(entry);
            default -> throw new IllegalArgumentException("unknown template " + entry.template());
        };
        entry.assertConsumed();
        return wire;
    }

    static List<String> templateNames() {
        return TemplateIndex.names();
    }

    static int schemaVersion(final String template) {
        return TemplateIndex.spec(template).schemaVersion;
    }

    static List<FieldSpec> fields(final String template) {
        return TemplateIndex.spec(template).fields;
    }

    private static byte[] encodeControlResponse(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long relevantId = entry.int64("relevantId");
        final ControlResponseCode code = entry.enumValue(ControlResponseCode.class, "code");
        final boolean versionOnWire = admits(entry, "version");
        final int version = versionOnWire ? entry.int32("version") : 0;
        final String errorMessage = entry.asciiOrEmpty("errorMessage");
        final ControlResponseEncoder encoder = new ControlResponseEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .relevantId(relevantId)
                .code(code);
            if (versionOnWire) {
                encoder.version(version);
            }
            encoder.errorMessage(errorMessage);
            return encoder.limit();
        });
    }

    private static byte[] encodeCloseSession(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final CloseSessionRequestEncoder encoder = new CloseSessionRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId);
            return encoder.limit();
        });
    }

    private static byte[] encodeStartRecording(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final int streamId = entry.int32("streamId");
        final SourceLocation sourceLocation = entry.enumValue(SourceLocation.class, "sourceLocation");
        final String channel = entry.asciiOrEmpty("channel");
        final StartRecordingRequestEncoder encoder = new StartRecordingRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .streamId(streamId)
                .sourceLocation(sourceLocation);
            encoder.channel(channel);
            return encoder.limit();
        });
    }

    private static byte[] encodeStopRecording(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final int streamId = entry.int32("streamId");
        final String channel = entry.asciiOrEmpty("channel");
        final StopRecordingRequestEncoder encoder = new StopRecordingRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId).streamId(streamId);
            encoder.channel(channel);
            return encoder.limit();
        });
    }

    private static byte[] encodeReplay(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final long position = entry.int64("position");
        final long length = entry.int64("length");
        final int replayStreamId = entry.int32("replayStreamId");
        final boolean fileIo = admits(entry, "fileIoMaxLength");
        final int fileIoMaxLength = fileIo ? entry.int32("fileIoMaxLength") : 0;
        final boolean token = admits(entry, "replayToken");
        final long replayToken = token ? entry.int64("replayToken") : 0L;
        final String replayChannel = entry.asciiOrEmpty("replayChannel");
        final ReplayRequestEncoder encoder = new ReplayRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .recordingId(recordingId)
                .position(position)
                .length(length)
                .replayStreamId(replayStreamId);
            if (fileIo) {
                encoder.fileIoMaxLength(fileIoMaxLength);
            }
            if (token) {
                encoder.replayToken(replayToken);
            }
            encoder.replayChannel(replayChannel);
            return encoder.limit();
        });
    }

    private static byte[] encodeStopReplay(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long replaySessionId = entry.int64("replaySessionId");
        final StopReplayRequestEncoder encoder = new StopReplayRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .replaySessionId(replaySessionId);
            return encoder.limit();
        });
    }

    private static byte[] encodeListRecording(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final ListRecordingRequestEncoder encoder = new ListRecordingRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId).recordingId(recordingId);
            return encoder.limit();
        });
    }

    private static byte[] encodeTruncate(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final long position = entry.int64("position");
        final TruncateRecordingRequestEncoder encoder = new TruncateRecordingRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .recordingId(recordingId)
                .position(position);
            return encoder.limit();
        });
    }

    private static byte[] encodeRecordingDescriptor(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final long startTimestamp = entry.int64("startTimestamp");
        final long stopTimestamp = entry.int64("stopTimestamp");
        final long startPosition = entry.int64("startPosition");
        final long stopPosition = entry.int64("stopPosition");
        final int initialTermId = entry.int32("initialTermId");
        final int segmentFileLength = entry.int32("segmentFileLength");
        final int termBufferLength = entry.int32("termBufferLength");
        final int mtuLength = entry.int32("mtuLength");
        final int sessionId = entry.int32("sessionId");
        final int streamId = entry.int32("streamId");
        final String strippedChannel = entry.asciiOrEmpty("strippedChannel");
        final String originalChannel = entry.asciiOrEmpty("originalChannel");
        final String sourceIdentity = entry.asciiOrEmpty("sourceIdentity");
        final RecordingDescriptorEncoder encoder = new RecordingDescriptorEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .recordingId(recordingId)
                .startTimestamp(startTimestamp)
                .stopTimestamp(stopTimestamp)
                .startPosition(startPosition)
                .stopPosition(stopPosition)
                .initialTermId(initialTermId)
                .segmentFileLength(segmentFileLength)
                .termBufferLength(termBufferLength)
                .mtuLength(mtuLength)
                .sessionId(sessionId)
                .streamId(streamId);
            encoder.strippedChannel(strippedChannel);
            encoder.originalChannel(originalChannel);
            encoder.sourceIdentity(sourceIdentity);
            return encoder.limit();
        });
    }

    private static byte[] encodeRecordingSignal(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final long subscriptionId = entry.int64("subscriptionId");
        final long position = entry.int64("position");
        final RecordingSignal signal = entry.enumValue(RecordingSignal.class, "signal");
        final RecordingSignalEventEncoder encoder = new RecordingSignalEventEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .recordingId(recordingId)
                .subscriptionId(subscriptionId)
                .position(position)
                .signal(signal);
            return encoder.limit();
        });
    }

    private static byte[] encodeStopReplication(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long replicationId = entry.int64("replicationId");
        final StopReplicationRequestEncoder encoder = new StopReplicationRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .replicationId(replicationId);
            return encoder.limit();
        });
    }

    private static byte[] encodeAuthConnect(final FixtureEntry entry) {
        final long correlationId = entry.int64("correlationId");
        final int responseStreamId = entry.int32("responseStreamId");
        final int version = entry.int32("version");
        final String responseChannel = entry.asciiOrEmpty("responseChannel");
        final byte[] encodedCredentials = entry.dataOrEmpty("encodedCredentials");
        final boolean clientInfoOnWire = admits(entry, "clientInfo");
        final String clientInfo = entry.asciiOrEmpty("clientInfo");
        final AuthConnectRequestEncoder encoder = new AuthConnectRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.correlationId(correlationId).responseStreamId(responseStreamId).version(version);
            encoder.responseChannel(responseChannel);
            encoder.putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);
            if (clientInfoOnWire) {
                encoder.clientInfo(clientInfo);
            }
            return encoder.limit();
        });
    }

    private static byte[] encodeChallenge(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final int version = entry.int32("version");
        final byte[] encodedChallenge = entry.dataOrEmpty("encodedChallenge");
        final ChallengeEncoder encoder = new ChallengeEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId).version(version);
            encoder.putEncodedChallenge(encodedChallenge, 0, encodedChallenge.length);
            return encoder.limit();
        });
    }

    private static byte[] encodeChallengeResponse(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final byte[] encodedCredentials = entry.dataOrEmpty("encodedCredentials");
        final ChallengeResponseEncoder encoder = new ChallengeResponseEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId);
            encoder.putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);
            return encoder.limit();
        });
    }

    private static byte[] encodeKeepAlive(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final KeepAliveRequestEncoder encoder = new KeepAliveRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId);
            return encoder.limit();
        });
    }

    private static byte[] encodeStartRecording2(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final int streamId = entry.int32("streamId");
        final SourceLocation sourceLocation = entry.enumValue(SourceLocation.class, "sourceLocation");
        final BooleanType autoStop = entry.enumValue(BooleanType.class, "autoStop");
        final String channel = entry.asciiOrEmpty("channel");
        final StartRecordingRequest2Encoder encoder = new StartRecordingRequest2Encoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .streamId(streamId)
                .sourceLocation(sourceLocation)
                .autoStop(autoStop);
            encoder.channel(channel);
            return encoder.limit();
        });
    }

    private static byte[] encodeExtendRecording2(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final int streamId = entry.int32("streamId");
        final SourceLocation sourceLocation = entry.enumValue(SourceLocation.class, "sourceLocation");
        final BooleanType autoStop = entry.enumValue(BooleanType.class, "autoStop");
        final String channel = entry.asciiOrEmpty("channel");
        final ExtendRecordingRequest2Encoder encoder = new ExtendRecordingRequest2Encoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .recordingId(recordingId)
                .streamId(streamId)
                .sourceLocation(sourceLocation)
                .autoStop(autoStop);
            encoder.channel(channel);
            return encoder.limit();
        });
    }

    private static byte[] encodeStopByIdentity(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long recordingId = entry.int64("recordingId");
        final StopRecordingByIdentityRequestEncoder encoder = new StopRecordingByIdentityRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId).recordingId(recordingId);
            return encoder.limit();
        });
    }

    private static byte[] encodeReplicate2(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final long srcRecordingId = entry.int64("srcRecordingId");
        final long dstRecordingId = entry.int64("dstRecordingId");
        final long stopPosition = entry.int64("stopPosition");
        final long channelTagId = entry.int64("channelTagId");
        final long subscriptionTagId = entry.int64("subscriptionTagId");
        final int srcControlStreamId = entry.int32("srcControlStreamId");
        final boolean fileIo = admits(entry, "fileIoMaxLength");
        final int fileIoMaxLength = fileIo ? entry.int32("fileIoMaxLength") : 0;
        final boolean session = admits(entry, "replicationSessionId");
        final int replicationSessionId = session ? entry.int32("replicationSessionId") : 0;
        final String srcControlChannel = entry.asciiOrEmpty("srcControlChannel");
        final String liveDestination = entry.asciiOrEmpty("liveDestination");
        final String replicationChannel = entry.asciiOrEmpty("replicationChannel");
        final boolean credentials = admits(entry, "encodedCredentials");
        final byte[] encodedCredentials = entry.dataOrEmpty("encodedCredentials");
        final boolean srcResponse = admits(entry, "srcResponseChannel");
        final String srcResponseChannel = entry.asciiOrEmpty("srcResponseChannel");
        final ReplicateRequest2Encoder encoder = new ReplicateRequest2Encoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId)
                .correlationId(correlationId)
                .srcRecordingId(srcRecordingId)
                .dstRecordingId(dstRecordingId)
                .stopPosition(stopPosition)
                .channelTagId(channelTagId)
                .subscriptionTagId(subscriptionTagId)
                .srcControlStreamId(srcControlStreamId);
            if (fileIo) {
                encoder.fileIoMaxLength(fileIoMaxLength);
            }
            if (session) {
                encoder.replicationSessionId(replicationSessionId);
            }
            encoder.srcControlChannel(srcControlChannel);
            encoder.liveDestination(liveDestination);
            encoder.replicationChannel(replicationChannel);
            if (credentials) {
                encoder.putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);
            }
            if (srcResponse) {
                encoder.srcResponseChannel(srcResponseChannel);
            }
            return encoder.limit();
        });
    }

    private static byte[] encodeArchiveId(final FixtureEntry entry) {
        final long controlSessionId = entry.int64("controlSessionId");
        final long correlationId = entry.int64("correlationId");
        final ArchiveIdRequestEncoder encoder = new ArchiveIdRequestEncoder();
        return finish(entry, encoder::wrap, encoder::limit, () -> {
            encoder.controlSessionId(controlSessionId).correlationId(correlationId);
            return encoder.limit();
        });
    }

    private static boolean admits(final FixtureEntry entry, final String field) {
        final FieldSpec found = TemplateIndex.spec(entry.template()).byName.get(field);
        if (found == null) {
            throw new IllegalStateException("unknown field " + field);
        }
        return entry.actingVersion() >= found.sinceVersion;
    }

    @FunctionalInterface
    private interface WrapOp {
        void wrap(MutableDirectBuffer buffer, int offset);
    }

    @FunctionalInterface
    private interface LimitOp {
        void limit(int limit);
    }

    private static byte[] finish(
        final FixtureEntry entry,
        final WrapOp wrap,
        final LimitOp limit,
        final IntSupplier writeBody
    ) {
        final TemplateSpec spec = TemplateIndex.spec(entry.template());
        final int actingVersion = entry.actingVersion();
        final int blockLength = TemplateIndex.actingBlockLength(spec, actingVersion);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        wrap.wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
        limit.limit(MessageHeaderEncoder.ENCODED_LENGTH + blockLength);
        final int wireLength = writeBody.getAsInt();
        applyHeader(buffer, blockLength, spec.templateId, actingVersion);
        final byte[] out = new byte[wireLength];
        buffer.getBytes(0, out);
        return out;
    }

    private static void applyHeader(
        final MutableDirectBuffer buffer,
        final int blockLength,
        final int templateId,
        final int actingVersion
    ) {
        new MessageHeaderEncoder().wrap(buffer, 0)
            .blockLength(blockLength)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(actingVersion);
    }

    private static final class TemplateIndex {
        private static final Map<String, TemplateSpec> BY_NAME = new LinkedHashMap<>();

        static {
            register("controlResponse", ControlResponseDecoder.class, ControlResponseEncoder.class);
            register("closeSessionRequest", CloseSessionRequestDecoder.class, CloseSessionRequestEncoder.class);
            register("startRecordingRequest", StartRecordingRequestDecoder.class, StartRecordingRequestEncoder.class);
            register("stopRecordingRequest", StopRecordingRequestDecoder.class, StopRecordingRequestEncoder.class);
            register("replayRequest", ReplayRequestDecoder.class, ReplayRequestEncoder.class);
            register("stopReplayRequest", StopReplayRequestDecoder.class, StopReplayRequestEncoder.class);
            register("listRecordingRequest", ListRecordingRequestDecoder.class, ListRecordingRequestEncoder.class);
            register(
                "truncateRecordingRequest",
                TruncateRecordingRequestDecoder.class,
                TruncateRecordingRequestEncoder.class
            );
            register("recordingDescriptor", RecordingDescriptorDecoder.class, RecordingDescriptorEncoder.class);
            register("recordingSignalEvent", RecordingSignalEventDecoder.class, RecordingSignalEventEncoder.class);
            register(
                "stopReplicationRequest",
                StopReplicationRequestDecoder.class,
                StopReplicationRequestEncoder.class
            );
            register("authConnectRequest", AuthConnectRequestDecoder.class, AuthConnectRequestEncoder.class);
            register("challenge", ChallengeDecoder.class, ChallengeEncoder.class);
            register("challengeResponse", ChallengeResponseDecoder.class, ChallengeResponseEncoder.class);
            register("keepAliveRequest", KeepAliveRequestDecoder.class, KeepAliveRequestEncoder.class);
            register(
                "startRecordingRequest2",
                StartRecordingRequest2Decoder.class,
                StartRecordingRequest2Encoder.class
            );
            register(
                "extendRecordingRequest2",
                ExtendRecordingRequest2Decoder.class,
                ExtendRecordingRequest2Encoder.class
            );
            register(
                "stopRecordingByIdentityRequest",
                StopRecordingByIdentityRequestDecoder.class,
                StopRecordingByIdentityRequestEncoder.class
            );
            register("replicateRequest2", ReplicateRequest2Decoder.class, ReplicateRequest2Encoder.class);
            register("archiveIdRequest", ArchiveIdRequestDecoder.class, ArchiveIdRequestEncoder.class);
        }

        private TemplateIndex() {
        }

        static List<String> names() {
            return List.copyOf(BY_NAME.keySet());
        }

        static TemplateSpec spec(final String template) {
            final TemplateSpec found = BY_NAME.get(template);
            if (found == null) {
                throw new IllegalArgumentException("unknown template " + template);
            }
            return found;
        }

        static int actingBlockLength(final TemplateSpec spec, final int actingVersion) {
            if (actingVersion >= spec.schemaVersion) {
                return spec.blockLength;
            }
            int minOffset = Integer.MAX_VALUE;
            for (final FieldSpec field : spec.fields) {
                if (field.fixed && field.sinceVersion > actingVersion && field.encodingOffset < minOffset) {
                    minOffset = field.encodingOffset;
                }
            }
            if (minOffset == Integer.MAX_VALUE) {
                return spec.blockLength;
            }
            return minOffset;
        }

        static void check(final FixtureEntry entry) {
            final TemplateSpec spec = spec(entry.template());
            for (final FieldSpec field : spec.fields) {
                if (field.fixed && field.sinceVersion <= entry.actingVersion() && !entry.has(field.name)) {
                    throw new IllegalArgumentException("missing " + field.name);
                }
            }
            for (final Map.Entry<String, Object> fieldValue : entry.fields().entrySet()) {
                final FieldSpec field = spec.byName.get(fieldValue.getKey());
                if (field == null) {
                    throw new IllegalArgumentException("unknown key " + fieldValue.getKey());
                }
                if (field.sinceVersion > entry.actingVersion()) {
                    throw new IllegalArgumentException("gated field " + field.name);
                }
                if (!field.fixed) {
                    if (field.ascii) {
                        JsonValues.ascii(fieldValue.getValue(), field.name);
                    } else {
                        JsonValues.data(fieldValue.getValue(), field.name);
                    }
                    continue;
                }
                if (field.enumType != null) {
                    JsonValues.checkEnum(field.enumType, fieldValue.getValue(), field.name);
                    continue;
                }
                if (field.int64) {
                    JsonValues.int64(fieldValue.getValue(), field.name);
                    continue;
                }
                JsonValues.int32(fieldValue.getValue(), field.name);
            }
        }

        private static void register(final String name, final Class<?> decoder, final Class<?> encoder) {
            BY_NAME.put(name, TemplateSpec.discover(decoder, encoder));
        }
    }

    record FieldSpec(
        String name,
        int sinceVersion,
        int encodingOffset,
        boolean fixed,
        boolean ascii,
        boolean int64,
        Class<?> enumType
    ) {
    }

    private record TemplateSpec(
        int templateId,
        int schemaVersion,
        int blockLength,
        List<FieldSpec> fields,
        Map<String, FieldSpec> byName
    ) {
        private static TemplateSpec discover(final Class<?> decoder, final Class<?> encoder) {
            final List<FieldSpec> fields = discoverFields(decoder);
            final Map<String, FieldSpec> byName = new LinkedHashMap<>();
            for (final FieldSpec field : fields) {
                byName.put(field.name, field);
            }
            return new TemplateSpec(
                staticInt(encoder, "TEMPLATE_ID"),
                staticInt(encoder, "SCHEMA_VERSION"),
                staticInt(decoder, "BLOCK_LENGTH"),
                List.copyOf(fields),
                Map.copyOf(byName)
            );
        }

        private static List<FieldSpec> discoverFields(final Class<?> decoder) {
            final Map<String, FieldBuilder> builders = new LinkedHashMap<>();
            for (final Method method : decoder.getDeclaredMethods()) {
                if (!Modifier.isStatic(method.getModifiers()) || method.getParameterCount() != 0) {
                    continue;
                }
                final String methodName = method.getName();
                if (methodName.endsWith("SinceVersion") && method.getReturnType() == int.class) {
                    builder(builders, strip(methodName, "SinceVersion")).sinceMethod = method;
                } else if (methodName.endsWith("EncodingOffset") && method.getReturnType() == int.class) {
                    builder(builders, strip(methodName, "EncodingOffset")).offsetMethod = method;
                } else if (methodName.endsWith("HeaderLength") && method.getReturnType() == int.class) {
                    builder(builders, strip(methodName, "HeaderLength")).headerMethod = method;
                } else if (methodName.endsWith("CharacterEncoding") && method.getReturnType() == String.class) {
                    builder(builders, strip(methodName, "CharacterEncoding")).ascii = true;
                } else if (methodName.endsWith("EncodingLength") && method.getReturnType() == int.class) {
                    builder(builders, strip(methodName, "EncodingLength")).lengthMethod = method;
                }
            }
            final Map<String, Class<?>> enumTypes = enumGetters(decoder);
            final List<FieldSpec> fields = new ArrayList<>();
            for (final FieldBuilder builder : builders.values()) {
                if (builder.sinceMethod == null) {
                    continue;
                }
                final boolean fixed = builder.offsetMethod != null;
                final boolean var = builder.headerMethod != null;
                if (fixed == var) {
                    continue;
                }
                final Class<?> enumType = enumTypes.get(builder.name);
                final boolean int64 = fixed && enumType == null && invoke(builder.lengthMethod) == 8;
                fields.add(new FieldSpec(
                    builder.name,
                    invoke(builder.sinceMethod),
                    fixed ? invoke(builder.offsetMethod) : -1,
                    fixed,
                    builder.ascii,
                    int64,
                    enumType
                ));
            }
            return fields;
        }

        private static Map<String, Class<?>> enumGetters(final Class<?> decoder) {
            final Map<String, Class<?>> types = new LinkedHashMap<>();
            for (final Method method : decoder.getDeclaredMethods()) {
                if (Modifier.isStatic(method.getModifiers()) || method.getParameterCount() != 0) {
                    continue;
                }
                if (method.getReturnType().isEnum()) {
                    types.put(method.getName(), method.getReturnType());
                }
            }
            return types;
        }

        private static FieldBuilder builder(final Map<String, FieldBuilder> builders, final String name) {
            return builders.computeIfAbsent(name, FieldBuilder::new);
        }

        private static String strip(final String methodName, final String suffix) {
            return methodName.substring(0, methodName.length() - suffix.length());
        }

        private static int staticInt(final Class<?> type, final String field) {
            try {
                return type.getField(field).getInt(null);
            } catch (final ReflectiveOperationException ex) {
                throw new IllegalStateException(type.getSimpleName() + " " + field, ex);
            }
        }

        private static int invoke(final Method method) {
            try {
                return (int) method.invoke(null);
            } catch (final IllegalAccessException | InvocationTargetException ex) {
                throw new IllegalStateException(method.getName(), ex);
            }
        }
    }

    private static final class FieldBuilder {
        private final String name;
        private Method sinceMethod;
        private Method offsetMethod;
        private Method headerMethod;
        private Method lengthMethod;
        private boolean ascii;

        private FieldBuilder(final String name) {
            this.name = name;
        }
    }
}
