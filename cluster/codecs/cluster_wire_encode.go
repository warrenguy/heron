package codecs

import (
	"math"
	"unsafe"

	"github.com/warrenguy/heron/cluster/codecs/gensbe"
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
		gensbe.SessionEventSbeTemplateID,
		actingVersion,
		gensbe.SessionEventSbeBlockLength,
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
	var hdr gensbe.MessageHeader
	hdr.Wrap(buf, uint64(offset), uint64(actingVersion), uint64(len(buf)))
	hdr.SetBlockLength(blockLength).
		SetTemplateId(templateID).
		SetSchemaId(clusterSchemaID).
		SetVersion(actingVersion)
	encodeBody(buf, bodyOffset, blockLength, actingVersion)
	return wireEnd, true
}

func clusterVarString(data []byte) string {
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
		gensbe.SessionMessageHeaderSbeBlockLength,
		gensbe.SessionMessageHeaderSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.SessionMessageHeader
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
		gensbe.SessionEventSbeTemplateID,
		actingVersion,
		[]int{len(fields.Detail)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.SessionEvent
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
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		scratch[:count],
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.SessionConnectRequest
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
		gensbe.SessionCloseRequestSbeBlockLength,
		gensbe.SessionCloseRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.SessionCloseRequest
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
		gensbe.SessionKeepAliveSbeBlockLength,
		gensbe.SessionKeepAliveSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.SessionKeepAlive
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
		gensbe.NewLeaderEventSbeBlockLength,
		gensbe.NewLeaderEventSbeTemplateID,
		actingVersion,
		[]int{len(fields.IngressEndpoints)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.NewLeaderEvent
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
		gensbe.ChallengeSbeBlockLength,
		gensbe.ChallengeSbeTemplateID,
		actingVersion,
		[]int{len(fields.EncodedChallenge)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.Challenge
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
		gensbe.ChallengeResponseSbeBlockLength,
		gensbe.ChallengeResponseSbeTemplateID,
		actingVersion,
		[]int{len(fields.EncodedCredentials)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.ChallengeResponse
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
		gensbe.AdminRequestSbeBlockLength,
		gensbe.AdminRequestSbeTemplateID,
		actingVersion,
		[]int{len(fields.Payload)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.AdminRequest
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
		gensbe.AdminResponseSbeBlockLength,
		gensbe.AdminResponseSbeTemplateID,
		actingVersion,
		[]int{len(fields.Message), len(fields.Payload)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg gensbe.AdminResponse
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
