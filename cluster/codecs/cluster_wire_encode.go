package codecs

import (
	"math"
	"unsafe"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// ClusterMinRootBlockLength returns the minimum root block length for templateID at
// actingVersion, or fallback when bounds are unavailable.
func ClusterMinRootBlockLength(templateID, actingVersion, fallback uint16) uint16 {
	minBlock, _, ok := ClusterRootBlockLengthBounds(templateID, actingVersion)
	if !ok {
		return fallback
	}
	return minBlock
}

// SessionEventBlockLength returns the SessionEvent root block length for actingVersion.
func SessionEventBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.SessionEventSbeTemplateID,
		actingVersion,
		clusterwire.SessionEventSbeBlockLength,
	)
}

// NewLeadershipTermEventBlockLength returns the NewLeadershipTermEvent root block
// length for actingVersion.
//
// Below NewLeadershipTermEventTimeUnitMinVersion the min root is 40 (ids 1-6).
// At and above that version the full schema block is 48.
func NewLeadershipTermEventBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.NewLeadershipTermEventSbeTemplateID,
		actingVersion,
		clusterwire.NewLeadershipTermEventSbeBlockLength,
	)
}

// newLeadershipTermEventEncodedLen caches MessageHeader + full schema block
// (8+48=56) for the exclusive tryClaim hot path.
var newLeadershipTermEventEncodedLen = int(
	(&clusterwire.NewLeadershipTermEvent{}).SbeBlockAndHeaderLength(),
)

// NewLeadershipTermEventEncodedLen is MessageHeader plus full schema block
// (8+48=56).
//
// Production tryClaim always uses schema version 16 full block, matching Java:
// MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH.
// Encoding below NewLeadershipTermEventTimeUnitMinVersion writes a shorter wire
// (8+40=48), so a claim sized by this length must encode at or above that
// version or the trailing 8 claimed bytes stay unwritten.
func NewLeadershipTermEventEncodedLen() int {
	return newLeadershipTermEventEncodedLen
}

// EncodeNewLeadershipTermEvent writes a cluster NewLeadershipTermEvent (SBE
// template id 24) at offset.
//
// MessageHeader carries versioned root blockLength, template 24, schema 111,
// and actingVersion. Required body fields (ids 1-6) always write. When
// actingVersion >= NewLeadershipTermEventTimeUnitMinVersion, TimeUnit and
// AppVersion always write, including AppVersion 0: exclusive tryClaim term
// buffers are not zeroed, so omit would leave residual bytes. Matches Java
// LogPublisher chaining timeUnit and appVersion on every append.
//
// Pure: no shared mutable state.
//
// Caller owns buf for the write and for lifetime of returned wire bytes.
//
// Short or mis-offset buffers return wireEnd=0, ok=false without panic.
func EncodeNewLeadershipTermEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields NewLeadershipTermEventFields,
) (wireEnd int, ok bool) {
	blockLength := NewLeadershipTermEventBlockLength(actingVersion)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.NewLeadershipTermEventSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.NewLeadershipTermEvent
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLeadershipTermId(fields.LeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetTimestamp(fields.Timestamp).
				SetTermBaseLogPosition(fields.TermBaseLogPosition).
				SetLeaderMemberId(fields.LeaderMemberID).
				SetLogSessionId(fields.LogSessionID)
			if actingVersion >= NewLeadershipTermEventTimeUnitMinVersion {
				msg.SetTimeUnit(fields.TimeUnit)
				msg.SetAppVersion(fields.AppVersion)
			}
		},
	)
}

func encodeWithClusterWireHeader(
	buf []byte,
	offset int,
	blockLength, templateID, actingVersion uint16,
	varLens []int,
	encodeBody func(dst []byte, bodyOffset int, blockLength, actingVersion uint16),
) (wireEnd int, ok bool) {
	if offset < 0 || offset > len(buf)-messageHeaderEncodedLength {
		return 0, false
	}
	bodyLen, ok := clusterWireBodyLen(blockLength, varLens...)
	if !ok {
		return 0, false
	}
	bodyOffset := offset + messageHeaderEncodedLength
	wireEnd = bodyOffset + bodyLen
	if wireEnd > len(buf) {
		return 0, false
	}
	var hdr clusterwire.MessageHeader
	hdr.Wrap(buf, uint64(offset), uint64(actingVersion), uint64(len(buf)))
	hdr.SetBlockLength(blockLength).
		SetTemplateId(templateID).
		SetSchemaId(clusterSchemaID).
		SetVersion(actingVersion)
	encodeBody(buf, bodyOffset, blockLength, actingVersion)
	return wireEnd, true
}

func clusterVarString(data []byte) string {
	//nolint:gosec // G103: converts data to a string without copying; the SBE setter copies it
	return unsafe.String(unsafe.SliceData(data), len(data))
}

// EncodeSessionMessageHeader writes a cluster SessionMessageHeader at offset.
func EncodeSessionMessageHeader(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SessionMessageHeaderFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.SessionMessageHeaderSbeBlockLength,
		clusterwire.SessionMessageHeaderSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionMessageHeader
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetTimestamp(fields.Timestamp)
		},
	)
}

// EncodeSessionEvent writes a cluster SessionEvent at offset.
func EncodeSessionEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	blockLength uint16,
	fields SessionEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.SessionEventSbeTemplateID,
		actingVersion,
		[]int{len(fields.Detail)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionEvent
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetLeaderMemberId(fields.LeaderMemberID).
				SetCode(fields.Code)
			if actingVersion >= SessionEventVersionMinVersion && fields.Version != 0 {
				msg.SetVersion(fields.Version)
			}
			if actingVersion >= SessionEventLeaderHeartbeatMinVersion &&
				fields.LeaderHeartbeatTimeoutNs != math.MinInt64 {
				msg.SetLeaderHeartbeatTimeoutNs(fields.LeaderHeartbeatTimeoutNs)
			}
			msg.PutDetail(clusterVarString(fields.Detail))
		},
	)
}

// EncodeSessionConnectRequest writes a cluster SessionConnectRequest at offset.
func EncodeSessionConnectRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SessionConnectRequestFields,
) (wireEnd int, ok bool) {
	var scratch [sessionConnectRequestMaxVarFieldCount]int
	count, ok := sessionConnectRequestVarLensInto(
		actingVersion,
		len(fields.ResponseChannel),
		len(fields.EncodedCredentials),
		len(fields.ClientInfo),
		scratch[:],
	)
	if !ok {
		return 0, false
	}
	return encodeWithClusterWireHeader(
		buf,
		offset,
		sessionConnectRequestBlockLength(actingVersion),
		clusterwire.SessionConnectRequestSbeTemplateID,
		actingVersion,
		scratch[:count],
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionConnectRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetResponseStreamId(fields.ResponseStreamID)
			if actingVersion >= SessionConnectVersionMinVersion && fields.Version != 0 {
				msg.SetVersion(fields.Version)
			}
			msg.PutResponseChannel(clusterVarString(fields.ResponseChannel))
			msg.PutEncodedCredentials(clusterVarString(fields.EncodedCredentials))
			if actingVersion >= SessionConnectClientInfoMinVersion {
				msg.PutClientInfo(clusterVarString(fields.ClientInfo))
			}
		},
	)
}

// EncodeSessionCloseRequest writes a cluster SessionCloseRequest at offset.
func EncodeSessionCloseRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SessionCloseRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.SessionCloseRequestSbeBlockLength,
		clusterwire.SessionCloseRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionCloseRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID)
		},
	)
}

// EncodeSessionKeepAlive writes a cluster SessionKeepAlive at offset.
func EncodeSessionKeepAlive(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SessionKeepAliveFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.SessionKeepAliveSbeBlockLength,
		clusterwire.SessionKeepAliveSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.SessionKeepAlive
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID)
		},
	)
}

// EncodeNewLeaderEvent writes a cluster NewLeaderEvent at offset.
func EncodeNewLeaderEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields NewLeaderEventFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.NewLeaderEventSbeBlockLength,
		clusterwire.NewLeaderEventSbeTemplateID,
		actingVersion,
		[]int{len(fields.IngressEndpoints)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.NewLeaderEvent
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetLeaderMemberId(fields.LeaderMemberID)
			msg.PutIngressEndpoints(clusterVarString(fields.IngressEndpoints))
		},
	)
}

// EncodeChallenge writes a cluster Challenge at offset.
func EncodeChallenge(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ChallengeFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.ChallengeSbeBlockLength,
		clusterwire.ChallengeSbeTemplateID,
		actingVersion,
		[]int{len(fields.EncodedChallenge)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.Challenge
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID)
			msg.PutEncodedChallenge(clusterVarString(fields.EncodedChallenge))
		},
	)
}

// EncodeChallengeResponse writes a cluster ChallengeResponse at offset.
func EncodeChallengeResponse(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ChallengeResponseFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.ChallengeResponseSbeBlockLength,
		clusterwire.ChallengeResponseSbeTemplateID,
		actingVersion,
		[]int{len(fields.EncodedCredentials)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.ChallengeResponse
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID)
			msg.PutEncodedCredentials(clusterVarString(fields.EncodedCredentials))
		},
	)
}

// EncodeAdminRequest writes a cluster AdminRequest at offset.
func EncodeAdminRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields AdminRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.AdminRequestSbeBlockLength,
		clusterwire.AdminRequestSbeTemplateID,
		actingVersion,
		[]int{len(fields.Payload)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.AdminRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRequestType(fields.RequestType)
			msg.PutPayload(clusterVarString(fields.Payload))
		},
	)
}

// EncodeAdminResponse writes a cluster AdminResponse at offset.
func EncodeAdminResponse(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields AdminResponseFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.AdminResponseSbeBlockLength,
		clusterwire.AdminResponseSbeTemplateID,
		actingVersion,
		[]int{len(fields.Message), len(fields.Payload)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.AdminResponse
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRequestType(fields.RequestType).
				SetResponseCode(fields.ResponseCode)
			msg.PutMessage(clusterVarString(fields.Message))
			msg.PutPayload(clusterVarString(fields.Payload))
		},
	)
}
