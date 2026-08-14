package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

// JoinLogBlockLength returns the JoinLog root block length for actingVersion.
//
//   - Below JoinLogIsStandbyMinVersion: min root 36 (ids 1-7).
//   - At and above that version: full schema block 40.
func JoinLogBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.JoinLogSbeTemplateID,
		actingVersion,
		clusterwire.JoinLogSbeBlockLength,
	)
}

var (
	timerEventEncodedLen = int(
		(&clusterwire.TimerEvent{}).SbeBlockAndHeaderLength(),
	)
	sessionCloseEventEncodedLen = int(
		(&clusterwire.SessionCloseEvent{}).SbeBlockAndHeaderLength(),
	)
	scheduleTimerEncodedLen = int(
		(&clusterwire.ScheduleTimer{}).SbeBlockAndHeaderLength(),
	)
	cancelTimerEncodedLen = int(
		(&clusterwire.CancelTimer{}).SbeBlockAndHeaderLength(),
	)
	serviceAckEncodedLen = int(
		(&clusterwire.ServiceAck{}).SbeBlockAndHeaderLength(),
	)
	requestServiceAckEncodedLen = int(
		(&clusterwire.RequestServiceAck{}).SbeBlockAndHeaderLength(),
	)
)

// TimerEventEncodedLen is MessageHeader plus full schema block (8+24=32).
func TimerEventEncodedLen() int {
	return timerEventEncodedLen
}

// SessionCloseEventEncodedLen is MessageHeader plus full schema block (8+28=36).
func SessionCloseEventEncodedLen() int {
	return sessionCloseEventEncodedLen
}

// ScheduleTimerEncodedLen is MessageHeader plus full schema block (8+16=24).
func ScheduleTimerEncodedLen() int {
	return scheduleTimerEncodedLen
}

// CancelTimerEncodedLen is MessageHeader plus full schema block (8+8=16).
func CancelTimerEncodedLen() int {
	return cancelTimerEncodedLen
}

// ServiceAckEncodedLen is MessageHeader plus full schema block (8+36=44).
func ServiceAckEncodedLen() int {
	return serviceAckEncodedLen
}

// RequestServiceAckEncodedLen is MessageHeader plus full schema block (8+8=16).
func RequestServiceAckEncodedLen() int {
	return requestServiceAckEncodedLen
}

// EncodeTimerEvent writes a cluster TimerEvent (SBE template id 20) at offset.
// Short or mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeTimerEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields TimerEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.TimerEventSbeBlockLength,
		clusterwire.TimerEventSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.TimerEvent
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetCorrelationId(fields.CorrelationID).
				SetTimestamp(fields.Timestamp)
		},
	)
}

// EncodeSessionOpenEvent writes a cluster SessionOpenEvent (SBE template id 21)
// at offset. Short or mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeSessionOpenEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SessionOpenEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.SessionOpenEventSbeBlockLength,
		clusterwire.SessionOpenEventSbeTemplateID,
		actingVersion,
		[]int{len(fields.ResponseChannel), len(fields.EncodedPrincipal)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionOpenEvent
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetTimestamp(fields.Timestamp).
				SetResponseStreamId(fields.ResponseStreamID)
			msg.PutResponseChannel(clusterVarString(fields.ResponseChannel))
			msg.PutEncodedPrincipal(clusterVarString(fields.EncodedPrincipal))
		},
	)
}

// EncodeSessionCloseEvent writes a cluster SessionCloseEvent (SBE template id
// 22) at offset. Short or mis-offset buffers return wireEnd=0, ok=false without
// panic.
func EncodeSessionCloseEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SessionCloseEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.SessionCloseEventSbeBlockLength,
		clusterwire.SessionCloseEventSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionCloseEvent
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetTimestamp(fields.Timestamp).
				SetCloseReason(fields.CloseReason)
		},
	)
}

// EncodeScheduleTimer writes a cluster ScheduleTimer (SBE template id 31) at
// offset. Short or mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeScheduleTimer(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ScheduleTimerFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.ScheduleTimerSbeBlockLength,
		clusterwire.ScheduleTimerSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.ScheduleTimer
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetDeadline(fields.Deadline)
		},
	)
}

// EncodeCancelTimer writes a cluster CancelTimer (SBE template id 32) at
// offset. Short or mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeCancelTimer(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields CancelTimerFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.CancelTimerSbeBlockLength,
		clusterwire.CancelTimerSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.CancelTimer
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID)
		},
	)
}

// EncodeServiceAck writes a cluster ServiceAck (SBE template id 33) at offset.
// Short or mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeServiceAck(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ServiceAckFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.ServiceAckSbeBlockLength,
		clusterwire.ServiceAckSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.ServiceAck
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLogPosition(fields.LogPosition).
				SetTimestamp(fields.Timestamp).
				SetAckId(fields.AckID).
				SetRelevantId(fields.RelevantID).
				SetServiceId(fields.ServiceID)
		},
	)
}

// EncodeJoinLog writes a cluster JoinLog (SBE template id 40) at offset. When
// actingVersion >= JoinLogIsStandbyMinVersion, encode always writes IsStandby,
// including FALSE. Exclusive TryClaim buffers are not zeroed. Short or
// mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeJoinLog(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields JoinLogFields,
) (wireEnd int, ok bool) {
	blockLength := JoinLogBlockLength(actingVersion)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.JoinLogSbeTemplateID,
		actingVersion,
		[]int{len(fields.LogChannel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.JoinLog
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLogPosition(fields.LogPosition).
				SetMaxLogPosition(fields.MaxLogPosition).
				SetMemberId(fields.MemberID).
				SetLogSessionId(fields.LogSessionID).
				SetLogStreamId(fields.LogStreamID).
				SetIsStartup(fields.IsStartup).
				SetRole(fields.Role)
			if actingVersion >= JoinLogIsStandbyMinVersion {
				msg.SetIsStandby(fields.IsStandby)
			}
			msg.PutLogChannel(clusterVarString(fields.LogChannel))
		},
	)
}

// EncodeRequestServiceAck writes a cluster RequestServiceAck (SBE template id
// 108) at offset. Template id 108 is not a stream id. The message travels on
// the service control stream (cluster.DefaultServiceStreamID). Short or
// mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeRequestServiceAck(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields RequestServiceAckFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.RequestServiceAckSbeBlockLength,
		clusterwire.RequestServiceAckSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.RequestServiceAck
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLogPosition(fields.LogPosition)
		},
	)
}
