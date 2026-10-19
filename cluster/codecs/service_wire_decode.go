package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

func decodeTimerEventFields(
	buf []byte,
	body, block, version, bufLen uint64,
) TimerEventFields {
	var msg clusterwire.TimerEvent
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return TimerEventFields{
		LeadershipTermID: msg.LeadershipTermId(),
		CorrelationID:    msg.CorrelationId(),
		Timestamp:        msg.Timestamp(),
	}
}

func TimerEventFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (TimerEventFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.TimerEventSbeTemplateID,
		decodeTimerEventFields,
	)
}

// TryDecodeTimerEventAt decodes TimerEvent without interface boxing.
func TryDecodeTimerEventAt(buf []byte, offset int) (TimerEventFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.TimerEventSbeTemplateID, 0, decodeTimerEventFields,
	)
}

func decodeSessionOpenEventFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SessionOpenEventFields {
	var msg clusterwire.SessionOpenEvent
	msg.WrapForDecode(buf, body, block, version, bufLen)
	responseChannel, next := varFieldBytes(buf, bufLen, varFieldOffset(body, block))
	encodedPrincipal, _ := varFieldBytes(buf, bufLen, next)
	return SessionOpenEventFields{
		LeadershipTermID: msg.LeadershipTermId(),
		CorrelationID:    msg.CorrelationId(),
		ClusterSessionID: msg.ClusterSessionId(),
		Timestamp:        msg.Timestamp(),
		ResponseStreamID: msg.ResponseStreamId(),
		ResponseChannel:  responseChannel,
		EncodedPrincipal: encodedPrincipal,
	}
}

// SessionOpenEventFieldsFromLayout decodes SessionOpenEvent from a parsed
// layout. ok is template-ID match only. A corrupt var length decodes as nil.
// Prefer TryDecodeSessionOpenEventAt when var lengths must validate first.
func SessionOpenEventFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SessionOpenEventFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.SessionOpenEventSbeTemplateID,
		decodeSessionOpenEventFields,
	)
}

// TryDecodeSessionOpenEventAt decodes SessionOpenEvent without interface boxing.
// Passes ClusterDecodeMaxVarLen so var field lengths validate before decode.
func TryDecodeSessionOpenEventAt(buf []byte, offset int) (SessionOpenEventFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.SessionOpenEventSbeTemplateID,
		ClusterDecodeMaxVarLen,
		decodeSessionOpenEventFields,
	)
}

func decodeSessionCloseEventFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SessionCloseEventFields {
	var msg clusterwire.SessionCloseEvent
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SessionCloseEventFields{
		LeadershipTermID: msg.LeadershipTermId(),
		ClusterSessionID: msg.ClusterSessionId(),
		Timestamp:        msg.Timestamp(),
		CloseReason:      msg.CloseReason(),
	}
}

func SessionCloseEventFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SessionCloseEventFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.SessionCloseEventSbeTemplateID,
		decodeSessionCloseEventFields,
	)
}

// TryDecodeSessionCloseEventAt decodes SessionCloseEvent without interface boxing.
func TryDecodeSessionCloseEventAt(buf []byte, offset int) (SessionCloseEventFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.SessionCloseEventSbeTemplateID, 0, decodeSessionCloseEventFields,
	)
}

func decodeClusterActionRequestFields(
	buf []byte,
	body, block, version, bufLen uint64,
) ClusterActionRequestFields {
	var msg clusterwire.ClusterActionRequest
	msg.WrapForDecode(buf, body, block, version, bufLen)
	flags := msg.Flags()
	if flags == msg.FlagsNullValue() {
		flags = 0
	}
	return ClusterActionRequestFields{
		LeadershipTermID: msg.LeadershipTermId(),
		LogPosition:      msg.LogPosition(),
		Timestamp:        msg.Timestamp(),
		Action:           msg.Action(),
		Flags:            flags,
	}
}

// ClusterActionRequestFieldsFromLayout decodes ClusterActionRequest from a
// parsed layout. ok is template-ID match only.
func ClusterActionRequestFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (ClusterActionRequestFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.ClusterActionRequestSbeTemplateID,
		decodeClusterActionRequestFields,
	)
}

// TryDecodeClusterActionRequestAt decodes ClusterActionRequest without interface boxing.
func TryDecodeClusterActionRequestAt(buf []byte, offset int) (ClusterActionRequestFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.ClusterActionRequestSbeTemplateID,
		0,
		decodeClusterActionRequestFields,
	)
}

func decodeScheduleTimerFields(
	buf []byte,
	body, block, version, bufLen uint64,
) ScheduleTimerFields {
	var msg clusterwire.ScheduleTimer
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ScheduleTimerFields{
		CorrelationID: msg.CorrelationId(),
		Deadline:      msg.Deadline(),
	}
}

func ScheduleTimerFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (ScheduleTimerFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.ScheduleTimerSbeTemplateID,
		decodeScheduleTimerFields,
	)
}

// TryDecodeScheduleTimerAt decodes ScheduleTimer without interface boxing.
func TryDecodeScheduleTimerAt(buf []byte, offset int) (ScheduleTimerFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.ScheduleTimerSbeTemplateID, 0, decodeScheduleTimerFields,
	)
}

func decodeCancelTimerFields(
	buf []byte,
	body, block, version, bufLen uint64,
) CancelTimerFields {
	var msg clusterwire.CancelTimer
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return CancelTimerFields{
		CorrelationID: msg.CorrelationId(),
	}
}

func CancelTimerFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (CancelTimerFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.CancelTimerSbeTemplateID,
		decodeCancelTimerFields,
	)
}

// TryDecodeCancelTimerAt decodes CancelTimer without interface boxing.
func TryDecodeCancelTimerAt(buf []byte, offset int) (CancelTimerFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.CancelTimerSbeTemplateID, 0, decodeCancelTimerFields,
	)
}

func decodeCloseSessionFields(
	buf []byte,
	body, block, version, bufLen uint64,
) CloseSessionFields {
	var msg clusterwire.CloseSession
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return CloseSessionFields{
		ClusterSessionID: msg.ClusterSessionId(),
	}
}

// TryDecodeCloseSessionAt decodes CloseSession without interface boxing.
func TryDecodeCloseSessionAt(buf []byte, offset int) (CloseSessionFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.CloseSessionSbeTemplateID, 0, decodeCloseSessionFields,
	)
}

func decodeServiceAckFields(
	buf []byte,
	body, block, version, bufLen uint64,
) ServiceAckFields {
	var msg clusterwire.ServiceAck
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ServiceAckFields{
		LogPosition: msg.LogPosition(),
		Timestamp:   msg.Timestamp(),
		AckID:       msg.AckId(),
		RelevantID:  msg.RelevantId(),
		ServiceID:   msg.ServiceId(),
	}
}

// TryDecodeServiceAckAt decodes ServiceAck without interface boxing.
func TryDecodeServiceAckAt(buf []byte, offset int) (ServiceAckFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.ServiceAckSbeTemplateID, 0, decodeServiceAckFields,
	)
}

func decodeJoinLogFields(
	buf []byte,
	body, block, version, bufLen uint64,
) JoinLogFields {
	var msg clusterwire.JoinLog
	msg.WrapForDecode(buf, body, block, version, bufLen)
	logChannel, _ := varFieldBytes(buf, bufLen, varFieldOffset(body, block))
	return JoinLogFields{
		LogPosition:    msg.LogPosition(),
		MaxLogPosition: msg.MaxLogPosition(),
		MemberID:       msg.MemberId(),
		LogSessionID:   msg.LogSessionId(),
		LogStreamID:    msg.LogStreamId(),
		IsStartup:      msg.IsStartup(),
		Role:           msg.Role(),
		IsStandby:      msg.IsStandby(),
		LogChannel:     logChannel,
	}
}

// TryDecodeJoinLogAt decodes JoinLog without interface boxing. Passes
// ClusterDecodeMaxVarLen so the log channel length validates before decode.
func TryDecodeJoinLogAt(buf []byte, offset int) (JoinLogFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.JoinLogSbeTemplateID,
		ClusterDecodeMaxVarLen,
		decodeJoinLogFields,
	)
}

func decodeRequestServiceAckFields(
	buf []byte,
	body, block, version, bufLen uint64,
) RequestServiceAckFields {
	var msg clusterwire.RequestServiceAck
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return RequestServiceAckFields{
		LogPosition: msg.LogPosition(),
	}
}

// TryDecodeRequestServiceAckAt decodes RequestServiceAck without interface boxing.
func TryDecodeRequestServiceAckAt(buf []byte, offset int) (RequestServiceAckFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.RequestServiceAckSbeTemplateID,
		0,
		decodeRequestServiceAckFields,
	)
}
