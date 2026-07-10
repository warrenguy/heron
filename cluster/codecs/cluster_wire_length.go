package codecs

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

const (
	// clusterWireMaxRootBlockLength is the largest cluster root block used in the
	// per-field var-length DoS budget (NewLeadershipTerm @ current schema version).
	clusterWireMaxRootBlockLength = clusterwire.NewLeadershipTermSbeBlockLength
	// clusterWireMaxVarFieldCount is the maximum var fields on any cluster wire template
	// (SessionConnectRequest with ClientInfo @ actingVersion >= 14).
	clusterWireMaxVarFieldCount = 3
	// sessionConnectRequestMaxVarFieldCount is SessionConnectRequest's own var-field
	// cardinality at acting versions that admit ClientInfo; do not alias to
	// clusterWireMaxVarFieldCount when another template may raise the global max.
	sessionConnectRequestMaxVarFieldCount = 3
)

var (
	clusterVarLenFixedOverhead = messageHeaderEncodedLength +
		int(clusterWireMaxRootBlockLength) +
		clusterWireMaxVarFieldCount*VarLengthPrefixSize

	// ClusterDecodeMaxVarLen bounds each varAscii/varData field during egress
	// TryDecodeByTemplate. Per-field cap is logbuffer.MaxMessageLength minus
	// clusterVarLenFixedOverhead (~16 MiB - 116).
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
	var scratch [sessionConnectRequestMaxVarFieldCount]int
	count, ok := sessionConnectRequestVarLensInto(
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

// sessionConnectRequestVarLensInto writes ordered var-field payload lengths into dst
// without heap allocation. Callers pass a stack [sessionConnectRequestMaxVarFieldCount]int
// scratch buffer for steady-state zero-alloc connect encode.
func sessionConnectRequestVarLensInto(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
	dst []int,
) (count int, ok bool) {
	if responseChannelLen < 0 || encodedCredentialsLen < 0 || clientInfoLen < 0 {
		return 0, false
	}
	need := 2
	if actingVersion >= SessionConnectClientInfoMinVersion {
		need = sessionConnectRequestMaxVarFieldCount
	}
	if len(dst) < need {
		return 0, false
	}
	dst[0] = responseChannelLen
	dst[1] = encodedCredentialsLen
	if need == sessionConnectRequestMaxVarFieldCount {
		dst[2] = clientInfoLen
	}
	return need, true
}

// sessionConnectRequestBlockLength returns the SessionConnectRequest root block
// length for actingVersion.
func sessionConnectRequestBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.SessionConnectRequestSbeTemplateID,
		actingVersion,
		clusterwire.SessionConnectRequestSbeBlockLength,
	)
}

// SessionConnectRequestWireLen returns the encoded wire length of a SessionConnectRequest
// at actingVersion with the given variable-field byte lengths. The ClientInfo field is
// included only when actingVersion admits it, matching EncodeSessionConnectRequest.
func SessionConnectRequestWireLen(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) (int, bool) {
	var varLensScratch [sessionConnectRequestMaxVarFieldCount]int
	count, ok := sessionConnectRequestVarLensInto(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
		varLensScratch[:],
	)
	if !ok {
		return 0, false
	}
	return ClusterWireMessageLen(
		sessionConnectRequestBlockLength(actingVersion),
		varLensScratch[:count]...,
	)
}
