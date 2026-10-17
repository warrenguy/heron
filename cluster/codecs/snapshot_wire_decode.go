package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

func decodeSnapshotMarkerFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SnapshotMarkerFields {
	var msg clusterwire.SnapshotMarker
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SnapshotMarkerFields{
		TypeID:           msg.TypeId(),
		LogPosition:      msg.LogPosition(),
		LeadershipTermID: msg.LeadershipTermId(),
		Index:            msg.Index(),
		Mark:             msg.Mark(),
		TimeUnit:         msg.TimeUnit(),
		AppVersion:       msg.AppVersion(),
	}
}

// SnapshotMarkerFieldsFromLayout decodes SnapshotMarker from a parsed layout.
// ok is false on a template mismatch or when the fixed block does not fit buf.
func SnapshotMarkerFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SnapshotMarkerFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.SnapshotMarkerSbeTemplateID,
		decodeSnapshotMarkerFields,
	)
}

// TryDecodeSnapshotMarkerAt decodes SnapshotMarker (template 100) without
// interface boxing.
func TryDecodeSnapshotMarkerAt(buf []byte, offset int) (SnapshotMarkerFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.SnapshotMarkerSbeTemplateID, 0, decodeSnapshotMarkerFields,
	)
}

// clientSessionFields builds fixed fields with WrapForDecode and attaches
// caller-supplied var slots. Fast and boxed paths share this helper.
func clientSessionFields(
	buf []byte,
	body, block, version, bufLen uint64,
	vars templateVarFields,
) ClientSessionFields {
	var msg clusterwire.ClientSession
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ClientSessionFields{
		ClusterSessionID: msg.ClusterSessionId(),
		ResponseStreamID: msg.ResponseStreamId(),
		ResponseChannel:  vars.responseChannel,
		EncodedPrincipal: vars.encodedPrincipal,
	}
}

func decodeClientSessionFields(
	buf []byte,
	body, block, version, bufLen uint64,
) ClientSessionFields {
	var vars templateVarFields
	var next int
	vars.responseChannel, next = varFieldBytes(buf, bufLen, varFieldOffset(body, block))
	vars.encodedPrincipal, _ = varFieldBytes(buf, bufLen, next)
	return clientSessionFields(buf, body, block, version, bufLen, vars)
}

// TryDecodeClientSessionAt decodes ClientSession (template 102) without
// interface boxing. Passes ClusterDecodeMaxVarLen so var field lengths
// validate before decode. Var fields are copies. Callers may retain them.
func TryDecodeClientSessionAt(buf []byte, offset int) (ClientSessionFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.ClientSessionSbeTemplateID,
		ClusterDecodeMaxVarLen,
		decodeClientSessionFields,
	)
}

func decodeTimerFields(
	buf []byte,
	body, block, version, bufLen uint64,
) TimerFields {
	var msg clusterwire.Timer
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return TimerFields{
		CorrelationID: msg.CorrelationId(),
		Deadline:      msg.Deadline(),
	}
}

// TimerFieldsFromLayout decodes Timer from a parsed layout. ok is false on a
// template mismatch or when the fixed block does not fit buf.
func TimerFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (TimerFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.TimerSbeTemplateID,
		decodeTimerFields,
	)
}

// TryDecodeTimerAt decodes Timer (template 104) without interface boxing.
func TryDecodeTimerAt(buf []byte, offset int) (TimerFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.TimerSbeTemplateID, 0, decodeTimerFields,
	)
}

func decodeConsensusModuleFields(
	buf []byte,
	body, block, version, bufLen uint64,
) ConsensusModuleFields {
	var msg clusterwire.ConsensusModule
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ConsensusModuleFields{
		NextSessionID:          msg.NextSessionId(),
		NextServiceSessionID:   msg.NextServiceSessionId(),
		LogServiceSessionID:    msg.LogServiceSessionId(),
		PendingMessageCapacity: msg.PendingMessageCapacity(),
	}
}

// ConsensusModuleFieldsFromLayout decodes ConsensusModule from a parsed
// layout. ok is false on a template mismatch or when the fixed block does not
// fit buf.
func ConsensusModuleFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (ConsensusModuleFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.ConsensusModuleSbeTemplateID,
		decodeConsensusModuleFields,
	)
}

// TryDecodeConsensusModuleAt decodes ConsensusModule (template 105) without
// interface boxing.
func TryDecodeConsensusModuleAt(buf []byte, offset int) (ConsensusModuleFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.ConsensusModuleSbeTemplateID, 0, decodeConsensusModuleFields,
	)
}
