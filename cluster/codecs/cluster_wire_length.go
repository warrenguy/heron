package codecs

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

const (
	// clusterWireMaxRootBlockLength is the largest cluster root block used in the
	// per-field var-length DoS budget (SessionEvent @ current schema version).
	clusterWireMaxRootBlockLength = gensbe.SessionEventSbeBlockLength
	// clusterWireMaxVarFieldCount is the maximum var fields on any cluster wire template
	// (SessionConnectRequest with ClientInfo @ actingVersion >= 14).
	clusterWireMaxVarFieldCount = 3
)

var (
	clusterVarLenFixedOverhead = messageHeaderEncodedLength +
		int(clusterWireMaxRootBlockLength) +
		clusterWireMaxVarFieldCount*VarLengthPrefixSize

	// ClusterDecodeMaxVarLen bounds each varAscii/varData field during egress
	// TryDecodeByTemplate. Per-field cap is logbuffer.MaxMessageLength minus
	// clusterVarLenFixedOverhead (~16 MiB - 64).
	ClusterDecodeMaxVarLen = logbuffer.MaxMessageLength - clusterVarLenFixedOverhead
	// ClusterEncodeMaxVarLen bounds each var field during Encode* preflight
	// (clusterWireBodyLen). Same per-field cap; total wire is also capped at
	// logbuffer.MaxMessageLength.
	ClusterEncodeMaxVarLen = ClusterDecodeMaxVarLen
)

func clusterWireBodyLen(blockLength uint16, varLens ...int) (bodyLen int, ok bool) {
	bodyLen = int(blockLength)
	for _, n := range varLens {
		if n < 0 || n > ClusterEncodeMaxVarLen {
			return 0, false
		}
		bodyLen += VarLengthPrefixSize + n
	}
	wireLen := messageHeaderEncodedLength + bodyLen
	if wireLen > logbuffer.MaxMessageLength {
		return 0, false
	}
	return bodyLen, true
}

// ClusterWireMessageLen returns MessageHeaderEncodedLen() + bodyLen for a cluster wire
// message with the given root block length and variable field payload sizes.
func ClusterWireMessageLen(blockLength uint16, varLens ...int) (int, bool) {
	bodyLen, ok := clusterWireBodyLen(blockLength, varLens...)
	if !ok {
		return 0, false
	}
	return messageHeaderEncodedLength + bodyLen, true
}

// SessionConnectRequestVarLens returns ordered var-field payload lengths for a
// SessionConnectRequest at actingVersion. ClientInfo is omitted below
// SessionConnectClientInfoMinVersion, matching EncodeSessionConnectRequest.
func SessionConnectRequestVarLens(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) (varLens []int, ok bool) {
	if responseChannelLen < 0 || encodedCredentialsLen < 0 || clientInfoLen < 0 {
		return nil, false
	}
	varLens = []int{responseChannelLen, encodedCredentialsLen, clientInfoLen}
	if actingVersion < SessionConnectClientInfoMinVersion {
		varLens = varLens[:2]
	}
	return varLens, true
}

// sessionConnectRequestVarLens returns the root block length and ordered var-field
// lengths for a SessionConnectRequest at actingVersion.
func sessionConnectRequestVarLens(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) (blockLength uint16, varLens []int) {
	blockLength = ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	)
	varLens, _ = SessionConnectRequestVarLens(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	return blockLength, varLens
}

// SessionConnectRequestWireLen returns the encoded wire length of a SessionConnectRequest
// at actingVersion with the given variable-field byte lengths. The ClientInfo field is
// included only when actingVersion admits it, matching EncodeSessionConnectRequest.
func SessionConnectRequestWireLen(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) (int, bool) {
	blockLength, varLens := sessionConnectRequestVarLens(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	return ClusterWireMessageLen(blockLength, varLens...)
}
