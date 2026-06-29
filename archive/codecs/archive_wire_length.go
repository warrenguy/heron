package codecs

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const (
	// archiveWireMaxRootBlockLength is the largest archive root block used in the
	// per-field var-length DoS budget (RecordingDescriptor @ current schema version).
	archiveWireMaxRootBlockLength = gensbe.RecordingDescriptorSbeBlockLength
	// archiveWireMaxVarFieldCount is the maximum var fields on any archive wire template
	// (RecordingDescriptor with three varAscii, AuthConnectRequest with ClientInfo).
	archiveWireMaxVarFieldCount = 3
	// authConnectRequestMaxVarFieldCount is AuthConnectRequest's own var-field
	// cardinality at acting versions that admit ClientInfo; do not alias to
	// archiveWireMaxVarFieldCount when another template may raise the global max.
	authConnectRequestMaxVarFieldCount = 3
)

var (
	archiveControlResponseVersionMinVersion = uint16(
		(&gensbe.ControlResponse{}).VersionSinceVersion(),
	)
	archiveConnectRequestVersionMinVersion = uint16(
		(&gensbe.ConnectRequest{}).VersionSinceVersion(),
	)
	authConnectRequestClientInfoMinVersion = uint16(
		(&gensbe.AuthConnectRequest{}).ClientInfoSinceVersion(),
	)
	replayRequestFileIoMaxLengthMinVersion = uint16(
		(&gensbe.ReplayRequest{}).FileIoMaxLengthSinceVersion(),
	)
	replayRequestReplayTokenMinVersion = uint16(
		(&gensbe.ReplayRequest{}).ReplayTokenSinceVersion(),
	)
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

// connectRequestBlockLength returns the ConnectRequest root block length for
// actingVersion.
func connectRequestBlockLength(actingVersion uint16) uint16 {
	return ArchiveMinRootBlockLength(
		gensbe.ConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.ConnectRequestSbeBlockLength,
	)
}

// ConnectRequestWireLen returns the encoded wire length of a ConnectRequest at
// actingVersion with the given variable-field byte length.
func ConnectRequestWireLen(
	actingVersion uint16,
	responseChannelLen int,
) (int, bool) {
	if responseChannelLen < 0 {
		return 0, false
	}
	return ArchiveWireMessageLen(
		connectRequestBlockLength(actingVersion),
		responseChannelLen,
	)
}

// authConnectRequestBlockLength returns the AuthConnectRequest root block length
// for actingVersion. The version_t field sits in the base block at all versions.
func authConnectRequestBlockLength(actingVersion uint16) uint16 {
	return ArchiveMinRootBlockLength(
		gensbe.AuthConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.AuthConnectRequestSbeBlockLength,
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
		gensbe.ReplayRequestSbeTemplateID,
		actingVersion,
		gensbe.ReplayRequestSbeBlockLength,
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
