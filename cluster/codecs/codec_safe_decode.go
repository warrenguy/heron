package codecs

import (
	"bytes"
	"encoding/binary"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

var (
	messageHeaderLayout = &gensbe.MessageHeader{}

	messageHeaderEncodedLength       = int(messageHeaderLayout.EncodedLength())
	clusterSchemaID                  = messageHeaderLayout.SbeSchemaId()
	messageHeaderBlockLengthOffset   = int(messageHeaderLayout.BlockLengthEncodingOffset())
	messageHeaderTemplateIDOffset    = int(messageHeaderLayout.TemplateIdEncodingOffset())
	messageHeaderSchemaIDOffset      = int(messageHeaderLayout.SchemaIdEncodingOffset())
	messageHeaderActingVersionOffset = int(messageHeaderLayout.VersionEncodingOffset())
)

// MessageHeaderEncodedLen is the wire size of the SBE message header.
func MessageHeaderEncodedLen() int {
	return messageHeaderEncodedLength
}

// DecodedMessage is a template ID plus plain field struct decoded from wire bytes.
// Fields holds a concrete type from codec_fields.go (for example SessionEventFields).
// Var payloads in Fields are copied out of buf; callers may retain them after buf is reused.
type DecodedMessage struct {
	TemplateID uint16
	Fields     any // concrete type per template; see codec_fields.go
}

// DecodeMessageHeader reads the SBE message header at offset.
func DecodeMessageHeader(
	buf []byte,
	offset int,
) (blockLength, templateID, schemaID, version uint16, ok bool) {
	if offset < 0 || offset > len(buf)-messageHeaderEncodedLength {
		return 0, 0, 0, 0, false
	}
	blockLength = binary.LittleEndian.Uint16(buf[offset+messageHeaderBlockLengthOffset:])
	templateID = binary.LittleEndian.Uint16(buf[offset+messageHeaderTemplateIDOffset:])
	schemaID = binary.LittleEndian.Uint16(buf[offset+messageHeaderSchemaIDOffset:])
	version = binary.LittleEndian.Uint16(buf[offset+messageHeaderActingVersionOffset:])
	return blockLength, templateID, schemaID, version, true
}

// TryDecodeByTemplate decodes a cluster SBE message into plain field structs.
// maxVarLen bounds each varAscii/varData field; use ClusterDecodeMaxVarLen for egress.
// Rejects maxVarLen < 0. Trailing bytes after the last var field are ignored so
// Aeron fragment buffers decode without requiring exact buffer length.
func TryDecodeByTemplate(buf []byte, offset, maxVarLen int) (decoded DecodedMessage, ok bool) {
	layout, ok := clusterMessageAt(buf, offset, maxVarLen)
	if !ok {
		return decoded, ok
	}

	vars, _, ok := scanTemplateVarFields(
		buf,
		layout.varOffset,
		layout.templateID,
		layout.actingVersion,
		maxVarLen,
		true,
	)
	if !ok {
		return decoded, ok
	}

	fields, ok := decodeTemplateFields(
		buf,
		layout.bodyOffset,
		layout.blockLength,
		layout.templateID,
		layout.actingVersion,
		vars,
	)
	if !ok {
		return decoded, ok
	}
	return DecodedMessage{TemplateID: layout.templateID, Fields: fields}, true
}

type templateVarFields struct {
	detail             []byte
	responseChannel    []byte
	encodedCredentials []byte
	clientInfo         []byte
	ingressEndpoints   []byte
	encodedChallenge   []byte
	message            []byte
	payload            []byte
}

func clusterFixedBlockLength(blockLength uint16) (minBlock, maxBlock uint16, ok bool) {
	return blockLength, blockLength, true
}

// ClusterRootBlockLengthBounds returns min and max blockLength accepted for templateID
// at actingVersion.
func ClusterRootBlockLengthBounds(
	templateID, actingVersion uint16,
) (minBlock, maxBlock uint16, ok bool) {
	switch templateID {
	case gensbe.SessionMessageHeaderSbeTemplateID:
		return clusterFixedBlockLength(gensbe.SessionMessageHeaderSbeBlockLength)
	case gensbe.SessionEventSbeTemplateID:
		maxBlock = gensbe.SessionEventSbeBlockLength
		switch {
		case actingVersion < SessionEventVersionMinVersion:
			return 32, maxBlock, true
		case actingVersion < SessionEventLeaderHeartbeatMinVersion:
			return 36, maxBlock, true
		default:
			return clusterFixedBlockLength(maxBlock)
		}
	case gensbe.SessionConnectRequestSbeTemplateID:
		maxBlock = gensbe.SessionConnectRequestSbeBlockLength
		if actingVersion < SessionConnectVersionMinVersion {
			return 12, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)
	case gensbe.SessionCloseRequestSbeTemplateID:
		return clusterFixedBlockLength(gensbe.SessionCloseRequestSbeBlockLength)
	case gensbe.SessionKeepAliveSbeTemplateID:
		return clusterFixedBlockLength(gensbe.SessionKeepAliveSbeBlockLength)
	case gensbe.NewLeaderEventSbeTemplateID:
		return clusterFixedBlockLength(gensbe.NewLeaderEventSbeBlockLength)
	case gensbe.ChallengeSbeTemplateID:
		return clusterFixedBlockLength(gensbe.ChallengeSbeBlockLength)
	case gensbe.ChallengeResponseSbeTemplateID:
		return clusterFixedBlockLength(gensbe.ChallengeResponseSbeBlockLength)
	case gensbe.AdminRequestSbeTemplateID:
		return clusterFixedBlockLength(gensbe.AdminRequestSbeBlockLength)
	case gensbe.AdminResponseSbeTemplateID:
		return clusterFixedBlockLength(gensbe.AdminResponseSbeBlockLength)
	default:
		return 0, 0, false
	}
}

// ClusterVarFieldCount returns the number of variable-length fields for templateID
// at actingVersion.
func ClusterVarFieldCount(templateID, actingVersion uint16) (count int, ok bool) {
	switch templateID {
	case gensbe.SessionMessageHeaderSbeTemplateID,
		gensbe.SessionCloseRequestSbeTemplateID,
		gensbe.SessionKeepAliveSbeTemplateID:
		return 0, true
	case gensbe.SessionEventSbeTemplateID,
		gensbe.NewLeaderEventSbeTemplateID,
		gensbe.ChallengeSbeTemplateID,
		gensbe.ChallengeResponseSbeTemplateID,
		gensbe.AdminRequestSbeTemplateID:
		return 1, true
	case gensbe.SessionConnectRequestSbeTemplateID:
		if actingVersion >= SessionConnectClientInfoMinVersion {
			return 3, true
		}
		return 2, true
	case gensbe.AdminResponseSbeTemplateID:
		return 2, true
	default:
		return 0, false
	}
}

type clusterMessageLayout struct {
	bodyOffset, varOffset                  int
	blockLength, templateID, actingVersion uint16
}

func clusterMessageAt(buf []byte, offset, maxVarLen int) (clusterMessageLayout, bool) {
	if maxVarLen < 0 {
		return clusterMessageLayout{}, false
	}

	blockLength, templateID, schemaID, actingVersion, ok := DecodeMessageHeader(buf, offset)
	if !ok || schemaID != clusterSchemaID {
		return clusterMessageLayout{}, false
	}

	minBlock, maxBlock, known := ClusterRootBlockLengthBounds(templateID, actingVersion)
	if !known || blockLength < minBlock || blockLength > maxBlock {
		return clusterMessageLayout{}, false
	}

	bodyOffset := offset + messageHeaderEncodedLength
	if len(buf)-bodyOffset < int(blockLength) {
		return clusterMessageLayout{}, false
	}

	return clusterMessageLayout{
		bodyOffset:    bodyOffset,
		varOffset:     bodyOffset + int(blockLength),
		blockLength:   blockLength,
		templateID:    templateID,
		actingVersion: actingVersion,
	}, true
}

// ClusterMessageWireEnd returns the offset after the last var field for a cluster message.
// Rejects maxVarLen < 0 (consistent with TryDecodeByTemplate).
func ClusterMessageWireEnd(buf []byte, offset, maxVarLen int) (wireEnd int, ok bool) {
	layout, ok := clusterMessageAt(buf, offset, maxVarLen)
	if !ok {
		return wireEnd, ok
	}
	_, wireEnd, ok = scanTemplateVarFields(
		buf,
		layout.varOffset,
		layout.templateID,
		layout.actingVersion,
		maxVarLen,
		false,
	)
	return wireEnd, ok
}

func scanTemplateVarFields(
	buf []byte,
	offset int,
	templateID, actingVersion uint16,
	maxVarLen int,
	clone bool,
) (vars templateVarFields, next int, ok bool) {
	count, ok := ClusterVarFieldCount(templateID, actingVersion)
	if !ok {
		return templateVarFields{}, 0, false
	}
	if count == 0 {
		return templateVarFields{}, offset, true
	}

	var raw [][]byte
	if clone {
		raw = make([][]byte, count)
	}
	pos := offset
	for i := range count {
		slice, nextPos, err := ReadVarSlice(buf, pos, maxVarLen)
		if err != nil {
			return templateVarFields{}, 0, false
		}
		if clone {
			raw[i] = cloneVarBytes(slice.Data)
		}
		pos = nextPos
	}
	if !clone {
		return templateVarFields{}, pos, true
	}
	vars, ok = assignTemplateVarFields(templateID, raw)
	return vars, pos, ok
}

func cloneVarBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}

func assignTemplateVarFields(templateID uint16, raw [][]byte) (templateVarFields, bool) {
	switch templateID {
	case gensbe.SessionEventSbeTemplateID:
		return templateVarFields{detail: raw[0]}, true
	case gensbe.SessionConnectRequestSbeTemplateID:
		vars := templateVarFields{
			responseChannel:    raw[0],
			encodedCredentials: raw[1],
		}
		if len(raw) > 2 {
			vars.clientInfo = raw[2]
		}
		return vars, true
	case gensbe.NewLeaderEventSbeTemplateID:
		return templateVarFields{ingressEndpoints: raw[0]}, true
	case gensbe.ChallengeSbeTemplateID:
		return templateVarFields{encodedChallenge: raw[0]}, true
	case gensbe.ChallengeResponseSbeTemplateID:
		return templateVarFields{encodedCredentials: raw[0]}, true
	case gensbe.AdminRequestSbeTemplateID:
		return templateVarFields{payload: raw[0]}, true
	case gensbe.AdminResponseSbeTemplateID:
		return templateVarFields{message: raw[0], payload: raw[1]}, true
	default:
		return templateVarFields{}, false
	}
}

func decodeTemplateFields(
	buf []byte,
	bodyOffset int,
	blockLength, templateID, actingVersion uint16,
	vars templateVarFields,
) (fields any, ok bool) {
	defer func() {
		if recover() != nil {
			fields = nil
			ok = false
		}
	}()

	bufLen := uint64(len(buf))
	body := uint64(bodyOffset)
	block := uint64(blockLength)
	version := uint64(actingVersion)

	switch templateID {
	case gensbe.SessionMessageHeaderSbeTemplateID:
		var msg gensbe.SessionMessageHeader
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionMessageHeaderFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
			Timestamp:        msg.Timestamp(),
		}, true
	case gensbe.SessionEventSbeTemplateID:
		var msg gensbe.SessionEvent
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionEventFields{
			ClusterSessionID:         msg.ClusterSessionId(),
			CorrelationID:            msg.CorrelationId(),
			LeadershipTermID:         msg.LeadershipTermId(),
			LeaderMemberID:           msg.LeaderMemberId(),
			Code:                     msg.Code(),
			Version:                  msg.Version(),
			LeaderHeartbeatTimeoutNs: msg.LeaderHeartbeatTimeoutNs(),
			Detail:                   vars.detail,
		}, true
	case gensbe.SessionConnectRequestSbeTemplateID:
		var msg gensbe.SessionConnectRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionConnectRequestFields{
			CorrelationID:      msg.CorrelationId(),
			ResponseStreamID:   msg.ResponseStreamId(),
			Version:            msg.Version(),
			ResponseChannel:    vars.responseChannel,
			EncodedCredentials: vars.encodedCredentials,
			ClientInfo:         vars.clientInfo,
		}, true
	case gensbe.SessionCloseRequestSbeTemplateID:
		var msg gensbe.SessionCloseRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionCloseRequestFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
		}, true
	case gensbe.SessionKeepAliveSbeTemplateID:
		var msg gensbe.SessionKeepAlive
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionKeepAliveFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
		}, true
	case gensbe.NewLeaderEventSbeTemplateID:
		var msg gensbe.NewLeaderEvent
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return NewLeaderEventFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
			LeaderMemberID:   msg.LeaderMemberId(),
			IngressEndpoints: vars.ingressEndpoints,
		}, true
	case gensbe.ChallengeSbeTemplateID:
		var msg gensbe.Challenge
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ChallengeFields{
			CorrelationID:    msg.CorrelationId(),
			ClusterSessionID: msg.ClusterSessionId(),
			EncodedChallenge: vars.encodedChallenge,
		}, true
	case gensbe.ChallengeResponseSbeTemplateID:
		var msg gensbe.ChallengeResponse
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ChallengeResponseFields{
			CorrelationID:      msg.CorrelationId(),
			ClusterSessionID:   msg.ClusterSessionId(),
			EncodedCredentials: vars.encodedCredentials,
		}, true
	case gensbe.AdminRequestSbeTemplateID:
		var msg gensbe.AdminRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return AdminRequestFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
			CorrelationID:    msg.CorrelationId(),
			RequestType:      msg.RequestType(),
			Payload:          vars.payload,
		}, true
	case gensbe.AdminResponseSbeTemplateID:
		var msg gensbe.AdminResponse
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return AdminResponseFields{
			ClusterSessionID: msg.ClusterSessionId(),
			CorrelationID:    msg.CorrelationId(),
			RequestType:      msg.RequestType(),
			ResponseCode:     msg.ResponseCode(),
			Message:          vars.message,
			Payload:          vars.payload,
		}, true
	default:
		return nil, false
	}
}
