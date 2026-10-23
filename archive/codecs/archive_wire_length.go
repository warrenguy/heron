package codecs

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	// archiveWireMaxRootBlockLength is the largest archive root block used in the
	// per-field var-length DoS budget (RecordingDescriptor @ current schema version).
	archiveWireMaxRootBlockLength = archivewire.RecordingDescriptorSbeBlockLength

	// archiveWireMaxVarFieldCount is the maximum var fields on any archive wire template
	// (ReplicateRequest2 with SrcResponseChannel).
	archiveWireMaxVarFieldCount = 5

	// authConnectRequestMaxVarFieldCount is AuthConnectRequest's own var-field
	// cardinality at acting versions that admit ClientInfo; do not alias to
	// archiveWireMaxVarFieldCount when another template may raise the global max.
	authConnectRequestMaxVarFieldCount = 3

	// replicateRequest2MaxVarFieldCount is the ReplicateRequest2 var-field
	// count at acting versions that admit SrcResponseChannel.
	replicateRequest2MaxVarFieldCount = 5
)

var (
	archiveControlResponseVersionMinVersion = uint16((&archivewire.ControlResponse{}).
						VersionSinceVersion())
	authConnectRequestClientInfoMinVersion = uint16((&archivewire.AuthConnectRequest{}).
						ClientInfoSinceVersion())
	replayRequestFileIoMaxLengthMinVersion = uint16((&archivewire.ReplayRequest{}).
						FileIoMaxLengthSinceVersion())
	replayRequestReplayTokenMinVersion = uint16((&archivewire.ReplayRequest{}).
						ReplayTokenSinceVersion())
	replicateRequest2FileIoMaxLengthMinVersion = uint16((&archivewire.ReplicateRequest2{}).
							FileIoMaxLengthSinceVersion())
	replicateRequest2ReplicationSessionIDMinVersion = uint16((&archivewire.ReplicateRequest2{}).
							ReplicationSessionIdSinceVersion())
	replicateRequest2EncodedCredentialsMinVersion = uint16((&archivewire.ReplicateRequest2{}).
							EncodedCredentialsSinceVersion())
	replicateRequest2SrcResponseChannelMinVersion = uint16((&archivewire.ReplicateRequest2{}).
							SrcResponseChannelSinceVersion())
	replayRequestFileIoMaxLengthOffset = uint16((&archivewire.ReplayRequest{}).
						FileIoMaxLengthEncodingOffset())
	replayRequestReplayTokenOffset = uint16((&archivewire.ReplayRequest{}).
					ReplayTokenEncodingOffset())
	replicateRequest2FileIoMaxLengthOffset = uint16((&archivewire.ReplicateRequest2{}).
						FileIoMaxLengthEncodingOffset())
	replicateRequest2ReplicationSessionIDOffset = uint16((&archivewire.ReplicateRequest2{}).
							ReplicationSessionIdEncodingOffset())
)

var (
	archiveVarLenFixedOverhead = messageHeaderEncodedLength +
		int(archiveWireMaxRootBlockLength) +
		archiveWireMaxVarFieldCount*VarLengthPrefixSize

	// ArchiveDecodeMaxVarLen bounds each varAscii/varData field during
	// TryDecodeByTemplate. Per-field cap is logbuffer.MaxMessageLength minus
	// archiveVarLenFixedOverhead.
	ArchiveDecodeMaxVarLen = logbuffer.MaxMessageLength - archiveVarLenFixedOverhead

	// ArchiveEncodeMaxVarLen bounds each var field during Encode* preflight
	// (archiveWireBodyLen). Same per-field cap; total wire is also capped at
	// logbuffer.MaxMessageLength.
	ArchiveEncodeMaxVarLen = ArchiveDecodeMaxVarLen
)

func archiveWireBodyLen(blockLength uint16, varLens ...int) (bodyLen, messageWireLen int, ok bool) {
	bodyLen = int(blockLength)
	for _, n := range varLens {
		if n < 0 || n > ArchiveEncodeMaxVarLen {
			return 0, 0, false
		}
		bodyLen += VarLengthPrefixSize + n
	}
	messageWireLen = messageHeaderEncodedLength + bodyLen
	if messageWireLen > logbuffer.MaxMessageLength {
		return 0, 0, false
	}
	return bodyLen, messageWireLen, true
}

// ArchiveWireMessageLen returns MessageHeaderEncodedLen() + bodyLen for an archive wire
// message with the given root block length and variable field payload sizes.
func ArchiveWireMessageLen(blockLength uint16, varLens ...int) (int, bool) {
	_, messageWireLen, ok := archiveWireBodyLen(blockLength, varLens...)
	if !ok {
		return 0, false
	}
	return messageWireLen, true
}

// AuthConnectRequestVarLens returns ordered var-field payload lengths for an
// AuthConnectRequest at actingVersion. ClientInfo is omitted below
// authConnectRequestClientInfoMinVersion, matching EncodeAuthConnectRequest.
func AuthConnectRequestVarLens(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) (varLens []int, ok bool) {
	var scratch [authConnectRequestMaxVarFieldCount]int
	count, ok := authConnectRequestVarLensInto(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
		scratch[:],
	)
	if !ok {
		return nil, false
	}
	return append([]int(nil), scratch[:count]...), true
}

func authConnectRequestVarLensInto(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
	dst []int,
) (count int, ok bool) {
	if responseChannelLen < 0 || encodedCredentialsLen < 0 || clientInfoLen < 0 {
		return 0, false
	}
	need := 2
	if actingVersion >= authConnectRequestClientInfoMinVersion {
		need = authConnectRequestMaxVarFieldCount
	}
	if len(dst) < need {
		return 0, false
	}
	dst[0] = responseChannelLen
	dst[1] = encodedCredentialsLen
	if need == authConnectRequestMaxVarFieldCount {
		dst[2] = clientInfoLen
	}
	return need, true
}

// authConnectRequestBlockLength returns the AuthConnectRequest root block length
// for actingVersion. The version_t field sits in the base block at all versions.
func authConnectRequestBlockLength(actingVersion uint16) uint16 {
	return ArchiveMinRootBlockLength(
		archivewire.AuthConnectRequestSbeTemplateID,
		actingVersion,
		archivewire.AuthConnectRequestSbeBlockLength,
	)
}

// AuthConnectRequestWireLen returns the encoded wire length of an AuthConnectRequest
// at actingVersion with the given variable-field byte lengths. The ClientInfo field is
// included only when actingVersion admits it, matching EncodeAuthConnectRequest.
func AuthConnectRequestWireLen(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) (int, bool) {
	var scratch [authConnectRequestMaxVarFieldCount]int
	count, ok := authConnectRequestVarLensInto(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
		scratch[:],
	)
	if !ok {
		return 0, false
	}
	return ArchiveWireMessageLen(
		authConnectRequestBlockLength(actingVersion),
		scratch[:count]...,
	)
}

// replayRequestBlockLength returns the ReplayRequest root block length for
// actingVersion.
func replayRequestBlockLength(actingVersion uint16) uint16 {
	return ArchiveMinRootBlockLength(
		archivewire.ReplayRequestSbeTemplateID,
		actingVersion,
		archivewire.ReplayRequestSbeBlockLength,
	)
}

// ReplayRequestWireLen returns the encoded wire length of a ReplayRequest at
// actingVersion with the given variable-field byte length.
func ReplayRequestWireLen(
	actingVersion uint16,
	replayChannelLen int,
) (int, bool) {
	if replayChannelLen < 0 {
		return 0, false
	}
	return ArchiveWireMessageLen(
		replayRequestBlockLength(actingVersion),
		replayChannelLen,
	)
}

// replicateRequest2VarLens returns the ReplicateRequest2 var-field lengths in
// wire order. count is the number of fields actingVersion admits.
func replicateRequest2VarLens(
	actingVersion uint16,
	fields ReplicateRequest2Fields,
) (lens [replicateRequest2MaxVarFieldCount]int, count int, ok bool) {
	lens = [replicateRequest2MaxVarFieldCount]int{
		len(fields.SrcControlChannel),
		len(fields.LiveDestination),
		len(fields.ReplicationChannel),
		len(fields.EncodedCredentials),
		len(fields.SrcResponseChannel),
	}
	count, ok = ArchiveVarFieldCount(archivewire.ReplicateRequest2SbeTemplateID, actingVersion)
	return lens, count, ok
}

// replicateRequest2BlockLength returns the ReplicateRequest2 root block length
// for actingVersion.
func replicateRequest2BlockLength(actingVersion uint16) uint16 {
	return ArchiveMinRootBlockLength(
		archivewire.ReplicateRequest2SbeTemplateID,
		actingVersion,
		archivewire.ReplicateRequest2SbeBlockLength,
	)
}

// ReplicateRequest2WireLen returns the encoded wire length of fields at
// actingVersion. EncodedCredentials and SrcResponseChannel count only when
// actingVersion admits them.
func ReplicateRequest2WireLen(
	actingVersion uint16,
	fields ReplicateRequest2Fields,
) (int, bool) {
	lens, count, ok := replicateRequest2VarLens(actingVersion, fields)
	if !ok {
		return 0, false
	}
	return ArchiveWireMessageLen(
		replicateRequest2BlockLength(actingVersion),
		lens[:count]...,
	)
}
