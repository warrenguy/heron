package codecs

import (
	"bytes"
	"encoding/binary"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

var (
	messageHeaderLayout = &clusterwire.MessageHeader{}

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

// ClusterMessageLayout is the parsed SBE header and fixed block for a cluster message.
type ClusterMessageLayout struct {
	TemplateID    uint16
	BodyOffset    int
	BlockLength   uint16
	ActingVersion uint16
}

// ClusterMessageLayoutAt parses the cluster message header and fixed block at offset.
func ClusterMessageLayoutAt(buf []byte, offset, maxVarLen int) (ClusterMessageLayout, bool) {
	return clusterMessageAt(buf, offset, maxVarLen)
}

// SessionMessageHeaderFieldsFromLayout decodes SessionMessageHeader from a parsed layout.
func SessionMessageHeaderFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SessionMessageHeaderFields, bool) {
	if layout.TemplateID != clusterwire.SessionMessageHeaderSbeTemplateID {
		return SessionMessageHeaderFields{}, false
	}
	return decodeSessionMessageHeaderFields(
		buf,
		uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
	), true
}

// TryDecodeSessionMessageHeaderAt decodes SessionMessageHeader without interface boxing.
func TryDecodeSessionMessageHeaderAt(buf []byte, offset int) (SessionMessageHeaderFields, bool) {
	layout, ok := ClusterMessageLayoutAt(buf, offset, 0)
	if !ok || layout.TemplateID != clusterwire.SessionMessageHeaderSbeTemplateID {
		return SessionMessageHeaderFields{}, false
	}
	return SessionMessageHeaderFieldsFromLayout(buf, layout)
}

func decodeSessionMessageHeaderFields(
	buf []byte,
	body uint64,
	block uint64,
	version uint64,
	bufLen uint64,
) SessionMessageHeaderFields {
	var msg clusterwire.SessionMessageHeader
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SessionMessageHeaderFields{
		LeadershipTermID: msg.LeadershipTermId(),
		ClusterSessionID: msg.ClusterSessionId(),
		Timestamp:        msg.Timestamp(),
	}
}

// TryDecodeByTemplate decodes a cluster SBE message into plain field structs.
// maxVarLen bounds each varAscii/varData field; use ClusterDecodeMaxVarLen for egress.
// Rejects maxVarLen < 0. Trailing bytes after the last var field are ignored so
// Aeron fragment buffers decode without requiring exact buffer length.
func TryDecodeByTemplate(buf []byte, offset, maxVarLen int) (decoded DecodedMessage, ok bool) {
	layout, ok := ClusterMessageLayoutAt(buf, offset, maxVarLen)
	if !ok {
		return decoded, ok
	}
	return TryDecodeFromClusterLayout(buf, layout, maxVarLen)
}

// TryDecodeFromClusterLayout decodes cluster message fields from a parsed layout.
func TryDecodeFromClusterLayout(
	buf []byte,
	layout ClusterMessageLayout,
	maxVarLen int,
) (decoded DecodedMessage, ok bool) {
	if maxVarLen < 0 {
		return decoded, false
	}

	varOffset := layout.BodyOffset + int(layout.BlockLength)
	vars, _, ok := scanTemplateVarFields(
		buf,
		varOffset,
		layout.TemplateID,
		layout.ActingVersion,
		maxVarLen,
		true,
	)
	if !ok {
		return decoded, ok
	}

	fields, ok := decodeTemplateFields(
		buf,
		layout.BodyOffset,
		layout.BlockLength,
		layout.TemplateID,
		layout.ActingVersion,
		vars,
	)
	if !ok {
		return decoded, ok
	}
	return DecodedMessage{TemplateID: layout.TemplateID, Fields: fields}, true
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
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionMessageHeaderSbeBlockLength)
	case clusterwire.SessionEventSbeTemplateID:
		maxBlock = clusterwire.SessionEventSbeBlockLength
		switch {
		case actingVersion < SessionEventVersionMinVersion:
			return 32, maxBlock, true
		case actingVersion < SessionEventLeaderHeartbeatMinVersion:
			return 36, maxBlock, true
		default:
			return clusterFixedBlockLength(maxBlock)
		}
	case clusterwire.SessionConnectRequestSbeTemplateID:
		maxBlock = clusterwire.SessionConnectRequestSbeBlockLength
		if actingVersion < SessionConnectVersionMinVersion {
			return 12, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)
	case clusterwire.SessionCloseRequestSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionCloseRequestSbeBlockLength)
	case clusterwire.SessionKeepAliveSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionKeepAliveSbeBlockLength)
	case clusterwire.NewLeaderEventSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.NewLeaderEventSbeBlockLength)
	case clusterwire.ChallengeSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.ChallengeSbeBlockLength)
	case clusterwire.ChallengeResponseSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.ChallengeResponseSbeBlockLength)
	case clusterwire.AdminRequestSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.AdminRequestSbeBlockLength)
	case clusterwire.AdminResponseSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.AdminResponseSbeBlockLength)
	default:
		return 0, 0, false
	}
}

// ClusterVarFieldCount returns the number of variable-length fields for templateID
// at actingVersion.
func ClusterVarFieldCount(templateID, actingVersion uint16) (count int, ok bool) {
	switch templateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID,
		clusterwire.SessionCloseRequestSbeTemplateID,
		clusterwire.SessionKeepAliveSbeTemplateID:
		return 0, true
	case clusterwire.SessionEventSbeTemplateID,
		clusterwire.NewLeaderEventSbeTemplateID,
		clusterwire.ChallengeSbeTemplateID,
		clusterwire.ChallengeResponseSbeTemplateID,
		clusterwire.AdminRequestSbeTemplateID:
		return 1, true
	case clusterwire.SessionConnectRequestSbeTemplateID:
		if actingVersion >= SessionConnectClientInfoMinVersion {
			return 3, true
		}
		return 2, true
	case clusterwire.AdminResponseSbeTemplateID:
		return 2, true
	default:
		return 0, false
	}
}

func clusterMessageAt(buf []byte, offset, maxVarLen int) (ClusterMessageLayout, bool) {
	if maxVarLen < 0 {
		return ClusterMessageLayout{}, false
	}

	blockLength, templateID, schemaID, actingVersion, ok := DecodeMessageHeader(buf, offset)
	if !ok || schemaID != clusterSchemaID {
		return ClusterMessageLayout{}, false
	}

	minBlock, maxBlock, known := ClusterRootBlockLengthBounds(templateID, actingVersion)
	if !known || blockLength < minBlock || blockLength > maxBlock {
		return ClusterMessageLayout{}, false
	}

	bodyOffset := offset + messageHeaderEncodedLength
	if len(buf)-bodyOffset < int(blockLength) {
		return ClusterMessageLayout{}, false
	}

	return ClusterMessageLayout{
		BodyOffset:    bodyOffset,
		BlockLength:   blockLength,
		TemplateID:    templateID,
		ActingVersion: actingVersion,
	}, true
}

// ClusterMessageWireEnd returns the offset after the last var field for a cluster message.
// Rejects maxVarLen < 0 (consistent with TryDecodeByTemplate).
func ClusterMessageWireEnd(buf []byte, offset, maxVarLen int) (wireEnd int, ok bool) {
	layout, ok := clusterMessageAt(buf, offset, maxVarLen)
	if !ok {
		return wireEnd, ok
	}
	varOffset := layout.BodyOffset + int(layout.BlockLength)
	_, wireEnd, ok = scanTemplateVarFields(
		buf,
		varOffset,
		layout.TemplateID,
		layout.ActingVersion,
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
	case clusterwire.SessionEventSbeTemplateID:
		return templateVarFields{detail: raw[0]}, true
	case clusterwire.SessionConnectRequestSbeTemplateID:
		vars := templateVarFields{
			responseChannel:    raw[0],
			encodedCredentials: raw[1],
		}
		if len(raw) > 2 {
			vars.clientInfo = raw[2]
		}
		return vars, true
	case clusterwire.NewLeaderEventSbeTemplateID:
		return templateVarFields{ingressEndpoints: raw[0]}, true
	case clusterwire.ChallengeSbeTemplateID:
		return templateVarFields{encodedChallenge: raw[0]}, true
	case clusterwire.ChallengeResponseSbeTemplateID:
		return templateVarFields{encodedCredentials: raw[0]}, true
	case clusterwire.AdminRequestSbeTemplateID:
		return templateVarFields{payload: raw[0]}, true
	case clusterwire.AdminResponseSbeTemplateID:
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
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		return decodeSessionMessageHeaderFields(buf, body, block, version, bufLen), true
	case clusterwire.SessionEventSbeTemplateID:
		var msg clusterwire.SessionEvent
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
	case clusterwire.SessionConnectRequestSbeTemplateID:
		var msg clusterwire.SessionConnectRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionConnectRequestFields{
			CorrelationID:      msg.CorrelationId(),
			ResponseStreamID:   msg.ResponseStreamId(),
			Version:            msg.Version(),
			ResponseChannel:    vars.responseChannel,
			EncodedCredentials: vars.encodedCredentials,
			ClientInfo:         vars.clientInfo,
		}, true
	case clusterwire.SessionCloseRequestSbeTemplateID:
		var msg clusterwire.SessionCloseRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionCloseRequestFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
		}, true
	case clusterwire.SessionKeepAliveSbeTemplateID:
		var msg clusterwire.SessionKeepAlive
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionKeepAliveFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
		}, true
	case clusterwire.NewLeaderEventSbeTemplateID:
		var msg clusterwire.NewLeaderEvent
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return NewLeaderEventFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
			LeaderMemberID:   msg.LeaderMemberId(),
			IngressEndpoints: vars.ingressEndpoints,
		}, true
	case clusterwire.ChallengeSbeTemplateID:
		var msg clusterwire.Challenge
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ChallengeFields{
			CorrelationID:    msg.CorrelationId(),
			ClusterSessionID: msg.ClusterSessionId(),
			EncodedChallenge: vars.encodedChallenge,
		}, true
	case clusterwire.ChallengeResponseSbeTemplateID:
		var msg clusterwire.ChallengeResponse
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ChallengeResponseFields{
			CorrelationID:      msg.CorrelationId(),
			ClusterSessionID:   msg.ClusterSessionId(),
			EncodedCredentials: vars.encodedCredentials,
		}, true
	case clusterwire.AdminRequestSbeTemplateID:
		var msg clusterwire.AdminRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return AdminRequestFields{
			LeadershipTermID: msg.LeadershipTermId(),
			ClusterSessionID: msg.ClusterSessionId(),
			CorrelationID:    msg.CorrelationId(),
			RequestType:      msg.RequestType(),
			Payload:          vars.payload,
		}, true
	case clusterwire.AdminResponseSbeTemplateID:
		var msg clusterwire.AdminResponse
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
