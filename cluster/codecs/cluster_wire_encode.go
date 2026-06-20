package codecs

import (
	"math"
	"unsafe"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
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

// encodeWithClusterWireHeader writes cluster wire header then encodeBody. ok only means
// header fits at offset; full message may still panic if buf too small. Size for header
// + root block + var fields. Ingress framing: EncodeIngressHeader + ErrIngressHeaderBufferTooSmall.
func encodeWithClusterWireHeader(
	buf []byte,
	offset int,
	blockLength, templateID, actingVersion uint16,
	encodeBody func(bodyOffset int, blockLength, actingVersion uint16) int,
) (wireEnd int, ok bool) {
	if offset < 0 || offset > len(buf)-messageHeaderEncodedLength {
		return 0, false
	}
	var hdr gensbe.MessageHeader
	hdr.Wrap(buf, uint64(offset), uint64(actingVersion), uint64(len(buf)))
	hdr.SetBlockLength(blockLength).
		SetTemplateId(templateID).
		SetSchemaId(clusterSchemaID).
		SetVersion(actingVersion)
	bodyOffset := offset + messageHeaderEncodedLength
	return bodyOffset + encodeBody(bodyOffset, blockLength, actingVersion), true
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionMessageHeader
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetTimestamp(fields.Timestamp)
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionEvent
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
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
			return int(msg.EncodedLength())
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
	blockLength := ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionConnectRequest
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
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
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionCloseRequest
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID)
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.SessionKeepAlive
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID)
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.NewLeaderEvent
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetLeaderMemberId(fields.LeaderMemberID)
			msg.PutIngressEndpoints(clusterVarString(fields.IngressEndpoints))
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.Challenge
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID)
			msg.PutEncodedChallenge(clusterVarString(fields.EncodedChallenge))
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.ChallengeResponse
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetClusterSessionId(fields.ClusterSessionID)
			msg.PutEncodedCredentials(clusterVarString(fields.EncodedCredentials))
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.AdminRequest
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRequestType(fields.RequestType)
			msg.PutPayload(clusterVarString(fields.Payload))
			return int(msg.EncodedLength())
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
		func(bodyOffset int, blockLength, actingVersion uint16) int {
			var msg gensbe.AdminResponse
			msg.Wrap(buf, uint64(bodyOffset), uint64(len(buf)), uint64(blockLength), uint64(actingVersion)).
				SetClusterSessionId(fields.ClusterSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRequestType(fields.RequestType).
				SetResponseCode(fields.ResponseCode)
			msg.PutMessage(clusterVarString(fields.Message))
			msg.PutPayload(clusterVarString(fields.Payload))
			return int(msg.EncodedLength())
		},
	)
}
