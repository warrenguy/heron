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

// ClusterMessageLayoutAt parses the cluster message header and fixed block at
// offset. Does not validate var fields.
func ClusterMessageLayoutAt(buf []byte, offset, maxVarLen int) (ClusterMessageLayout, bool) {
	return clusterMessageAt(buf, offset, maxVarLen)
}

// wrapArgs returns the WrapForDecode arguments for the fixed block. ok is
// false when the block does not fit in buf.
func (layout ClusterMessageLayout) wrapArgs(
	buf []byte,
) (body, block, version, bufLen uint64, ok bool) {
	if layout.BodyOffset < 0 || layout.BodyOffset > len(buf)-int(layout.BlockLength) {
		return 0, 0, 0, 0, false
	}
	return uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
		true
}

// clusterFieldsFromLayout decodes fixed-block fields from an already parsed
// layout. Returns false on template mismatch. Does not rescan var fields; use
// tryDecodeClusterAt when var bounds still need checking.
func clusterFieldsFromLayout[T any](
	buf []byte,
	layout ClusterMessageLayout,
	templateID uint16,
	decode func(buf []byte, body, block, version, bufLen uint64) T,
) (fields T, ok bool) {
	if layout.TemplateID != templateID {
		return fields, false
	}
	body, block, version, bufLen, ok := layout.wrapArgs(buf)
	if !ok {
		return fields, false
	}
	return decode(buf, body, block, version, bufLen), true
}

// SessionMessageHeaderFieldsFromLayout decodes SessionMessageHeader from a parsed layout.
func SessionMessageHeaderFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SessionMessageHeaderFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.SessionMessageHeaderSbeTemplateID,
		decodeSessionMessageHeaderFields,
	)
}

// SessionMessagePayloadFromLayout returns the app payload span past the
// SessionMessageHeader fixed block.
//
// The payload is the remainder of buf after the fixed block. The span is not an
// SBE var field with a length prefix. Callers must pass a message-exact buf so
// len(buf) is the frame end. A larger buf includes trailing bytes as payload.
//
// Bounds-checked. Zero-length payloads are allowed and return an empty non-nil
// span. Returns nil on template mismatch or when the block end is past buf.
//
// The returned slice aliases buf. Valid only while buf remains live.
func SessionMessagePayloadFromLayout(buf []byte, layout ClusterMessageLayout) []byte {
	if layout.TemplateID != clusterwire.SessionMessageHeaderSbeTemplateID {
		return nil
	}
	start := layout.BodyOffset + int(layout.BlockLength)
	if start < 0 || start > len(buf) {
		return nil
	}
	return buf[start:]
}

// SessionKeepAliveFieldsFromLayout decodes SessionKeepAlive from a parsed layout.
func SessionKeepAliveFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SessionKeepAliveFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.SessionKeepAliveSbeTemplateID,
		decodeSessionKeepAliveFields,
	)
}

// SessionCloseRequestFieldsFromLayout decodes SessionCloseRequest from a parsed layout.
func SessionCloseRequestFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (SessionCloseRequestFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.SessionCloseRequestSbeTemplateID,
		decodeSessionCloseRequestFields,
	)
}

// TryDecodeSessionMessageHeaderAt decodes SessionMessageHeader without interface boxing.
func TryDecodeSessionMessageHeaderAt(buf []byte, offset int) (SessionMessageHeaderFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.SessionMessageHeaderSbeTemplateID,
		0,
		decodeSessionMessageHeaderFields,
	)
}

func decodeSessionMessageHeaderFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SessionMessageHeaderFields {
	var msg clusterwire.SessionMessageHeader
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SessionMessageHeaderFields{
		LeadershipTermID: msg.LeadershipTermId(),
		ClusterSessionID: msg.ClusterSessionId(),
		Timestamp:        msg.Timestamp(),
	}
}

func decodeNewLeadershipTermEventFields(
	buf []byte,
	body, block, version, bufLen uint64,
) NewLeadershipTermEventFields {
	var msg clusterwire.NewLeadershipTermEvent
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return NewLeadershipTermEventFields{
		LeadershipTermID:    msg.LeadershipTermId(),
		LogPosition:         msg.LogPosition(),
		Timestamp:           msg.Timestamp(),
		TermBaseLogPosition: msg.TermBaseLogPosition(),
		LeaderMemberID:      msg.LeaderMemberId(),
		LogSessionID:        msg.LogSessionId(),
		TimeUnit:            msg.TimeUnit(),
		AppVersion:          msg.AppVersion(),
	}
}

// NewLeadershipTermEventFieldsFromLayout decodes NewLeadershipTermEvent from a
// parsed layout.
func NewLeadershipTermEventFieldsFromLayout(
	buf []byte,
	layout ClusterMessageLayout,
) (NewLeadershipTermEventFields, bool) {
	return clusterFieldsFromLayout(
		buf,
		layout,
		clusterwire.NewLeadershipTermEventSbeTemplateID,
		decodeNewLeadershipTermEventFields,
	)
}

// TryDecodeNewLeadershipTermEventAt decodes NewLeadershipTermEvent (template 24)
// without interface boxing.
func TryDecodeNewLeadershipTermEventAt(
	buf []byte,
	offset int,
) (NewLeadershipTermEventFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.NewLeadershipTermEventSbeTemplateID,
		0,
		decodeNewLeadershipTermEventFields,
	)
}

// sessionConnectRequestFields builds fixed fields with WrapForDecode and
// attaches caller-supplied var slots. Fast and boxed paths share this helper
// so both report the same field mapping.
func sessionConnectRequestFields(
	buf []byte,
	body, block, version, bufLen uint64,
	vars templateVarFields,
) SessionConnectRequestFields {
	var msg clusterwire.SessionConnectRequest
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SessionConnectRequestFields{
		CorrelationID:      msg.CorrelationId(),
		ResponseStreamID:   msg.ResponseStreamId(),
		Version:            msg.Version(),
		ResponseChannel:    vars.responseChannel,
		EncodedCredentials: vars.encodedCredentials,
		ClientInfo:         vars.clientInfo,
	}
}

func decodeSessionConnectRequestFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SessionConnectRequestFields {
	var vars templateVarFields
	next := varFieldOffset(body, block)
	vars.responseChannel, next = varFieldBytes(buf, bufLen, next)
	vars.encodedCredentials, next = varFieldBytes(buf, bufLen, next)
	if version >= uint64(SessionConnectClientInfoMinVersion) {
		vars.clientInfo, _ = varFieldBytes(buf, bufLen, next)
	}
	return sessionConnectRequestFields(buf, body, block, version, bufLen, vars)
}

// TryDecodeSessionConnectRequestAt decodes SessionConnectRequest (template 3)
// without interface boxing.
//
// Var fields are copied out of buf. Callers may retain them after buf is reused.
func TryDecodeSessionConnectRequestAt(
	buf []byte,
	offset int,
) (SessionConnectRequestFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.SessionConnectRequestSbeTemplateID,
		ClusterDecodeMaxVarLen,
		decodeSessionConnectRequestFields,
	)
}

// challengeResponseFields builds fixed fields with WrapForDecode and
// attaches the caller-supplied var slot. Fast and boxed paths share this
// helper so both report the same field mapping.
func challengeResponseFields(
	buf []byte,
	body, block, version, bufLen uint64,
	vars templateVarFields,
) ChallengeResponseFields {
	var msg clusterwire.ChallengeResponse
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ChallengeResponseFields{
		CorrelationID:      msg.CorrelationId(),
		ClusterSessionID:   msg.ClusterSessionId(),
		EncodedCredentials: vars.encodedCredentials,
	}
}

func decodeChallengeResponseFields(
	buf []byte,
	body, block, version, bufLen uint64,
) ChallengeResponseFields {
	var vars templateVarFields
	vars.encodedCredentials, _ = varFieldBytes(buf, bufLen, varFieldOffset(body, block))
	return challengeResponseFields(buf, body, block, version, bufLen, vars)
}

// TryDecodeChallengeResponseAt decodes ChallengeResponse (template 8)
// without interface boxing.
//
// Var fields are copied out of buf. Callers may retain them after buf is
// reused.
func TryDecodeChallengeResponseAt(
	buf []byte,
	offset int,
) (ChallengeResponseFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.ChallengeResponseSbeTemplateID,
		ClusterDecodeMaxVarLen,
		decodeChallengeResponseFields,
	)
}

func decodeSessionCloseRequestFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SessionCloseRequestFields {
	var msg clusterwire.SessionCloseRequest
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SessionCloseRequestFields{
		LeadershipTermID: msg.LeadershipTermId(),
		ClusterSessionID: msg.ClusterSessionId(),
	}
}

// TryDecodeSessionCloseRequestAt decodes SessionCloseRequest (template 4)
// without interface boxing.
func TryDecodeSessionCloseRequestAt(
	buf []byte,
	offset int,
) (SessionCloseRequestFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.SessionCloseRequestSbeTemplateID,
		0,
		decodeSessionCloseRequestFields,
	)
}

func decodeSessionKeepAliveFields(
	buf []byte,
	body, block, version, bufLen uint64,
) SessionKeepAliveFields {
	var msg clusterwire.SessionKeepAlive
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return SessionKeepAliveFields{
		LeadershipTermID: msg.LeadershipTermId(),
		ClusterSessionID: msg.ClusterSessionId(),
	}
}

// TryDecodeSessionKeepAliveAt decodes SessionKeepAlive (template 5) without
// interface boxing.
func TryDecodeSessionKeepAliveAt(
	buf []byte,
	offset int,
) (SessionKeepAliveFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.SessionKeepAliveSbeTemplateID,
		0,
		decodeSessionKeepAliveFields,
	)
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

	fields, ok := decodeTemplateFields(buf, layout, vars)
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
	catchupEndpoint    []byte
	encodedPrincipal   []byte
	logChannel         []byte
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

// varFieldBytes clones the var field at offset. Returns the clone and the
// offset of the next var field. The clone never aliases buf.
//
// Behaviour: fail-closed. ReadVarSlice bounds-checks the length prefix and the
// payload. A corrupt length returns nil, not a panic. ClusterMessageLayoutAt
// validates the header and fixed block only. A layout passed to a
// *FieldsFromLayout decoder may therefore carry unvalidated var fields. The
// generated GetXAsString getters index the buffer unchecked and panic on that
// input.
//
// Prefer varFieldBytes over GetXAsString for []byte fields. GetXAsString
// allocates a string that a second copy then discards.
//
// A bufLen below len(buf) bounds the readable prefix. A bufLen above len(buf)
// has no effect.
//
// Returns:
//   - empty field: nil, offset after the field.
//   - rejected field: nil, offset unchanged.
//
// nil does not distinguish an empty field from a rejected field. A caller that
// needs the distinction must validate var lengths first. tryDecodeClusterAt
// does so with a positive maxVarLen.
func varFieldBytes(buf []byte, bufLen uint64, offset int) (field []byte, next int) {
	if bufLen < uint64(len(buf)) {
		buf = buf[:bufLen]
	}
	slice, next, err := ReadVarSlice(buf, offset, -1)
	if err != nil {
		return nil, offset
	}
	return cloneVarBytes(slice.Data), next
}

// varFieldOffset is the offset of the first var field after the fixed block.
func varFieldOffset(body, block uint64) int {
	//nolint:gosec // G115: wrapArgs keeps body+block within len(buf)
	return int(body + block)
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
	case clusterwire.CatchupPositionSbeTemplateID:
		return templateVarFields{catchupEndpoint: raw[0]}, true
	case clusterwire.SessionOpenEventSbeTemplateID:
		return templateVarFields{
			responseChannel:  raw[0],
			encodedPrincipal: raw[1],
		}, true
	case clusterwire.JoinLogSbeTemplateID:
		return templateVarFields{logChannel: raw[0]}, true
	case clusterwire.ClientSessionSbeTemplateID:
		return templateVarFields{
			responseChannel:  raw[0],
			encodedPrincipal: raw[1],
		}, true
	default:
		return templateVarFields{}, false
	}
}

func decodeTemplateFields(
	buf []byte,
	layout ClusterMessageLayout,
	vars templateVarFields,
) (fields any, ok bool) {
	defer func() {
		if recover() != nil {
			fields = nil
			ok = false
		}
	}()

	body, block, version, bufLen, ok := layout.wrapArgs(buf)
	if !ok {
		return nil, false
	}

	switch layout.TemplateID {
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
		return sessionConnectRequestFields(buf, body, block, version, bufLen, vars), true
	case clusterwire.SessionCloseRequestSbeTemplateID:
		return decodeSessionCloseRequestFields(buf, body, block, version, bufLen), true
	case clusterwire.SessionKeepAliveSbeTemplateID:
		return decodeSessionKeepAliveFields(buf, body, block, version, bufLen), true
	case clusterwire.NewLeadershipTermEventSbeTemplateID:
		return decodeNewLeadershipTermEventFields(buf, body, block, version, bufLen), true
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
		return challengeResponseFields(buf, body, block, version, bufLen, vars), true
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
	case clusterwire.CanvassPositionSbeTemplateID:
		return decodeCanvassPositionFields(buf, body, block, version, bufLen), true
	case clusterwire.RequestVoteSbeTemplateID:
		return decodeRequestVoteFields(buf, body, block, version, bufLen), true
	case clusterwire.VoteSbeTemplateID:
		return decodeVoteFields(buf, body, block, version, bufLen), true
	case clusterwire.NewLeadershipTermSbeTemplateID:
		return decodeNewLeadershipTermFields(buf, body, block, version, bufLen), true
	case clusterwire.AppendPositionSbeTemplateID:
		return decodeAppendPositionFields(buf, body, block, version, bufLen), true
	case clusterwire.CommitPositionSbeTemplateID:
		return decodeCommitPositionFields(buf, body, block, version, bufLen), true
	case clusterwire.CatchupPositionSbeTemplateID:
		var msg clusterwire.CatchupPosition
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return CatchupPositionFields{
			LeadershipTermID: msg.LeadershipTermId(),
			LogPosition:      msg.LogPosition(),
			FollowerMemberID: msg.FollowerMemberId(),
			CatchupEndpoint:  string(vars.catchupEndpoint),
		}, true
	case clusterwire.StopCatchupSbeTemplateID:
		return decodeStopCatchupFields(buf, body, block, version, bufLen), true
	case clusterwire.TimerEventSbeTemplateID:
		return decodeTimerEventFields(buf, body, block, version, bufLen), true
	case clusterwire.SessionOpenEventSbeTemplateID:
		var msg clusterwire.SessionOpenEvent
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return SessionOpenEventFields{
			LeadershipTermID: msg.LeadershipTermId(),
			CorrelationID:    msg.CorrelationId(),
			ClusterSessionID: msg.ClusterSessionId(),
			Timestamp:        msg.Timestamp(),
			ResponseStreamID: msg.ResponseStreamId(),
			ResponseChannel:  vars.responseChannel,
			EncodedPrincipal: vars.encodedPrincipal,
		}, true
	case clusterwire.SessionCloseEventSbeTemplateID:
		return decodeSessionCloseEventFields(buf, body, block, version, bufLen), true
	case clusterwire.ClusterActionRequestSbeTemplateID:
		return decodeClusterActionRequestFields(buf, body, block, version, bufLen), true
	case clusterwire.ScheduleTimerSbeTemplateID:
		return decodeScheduleTimerFields(buf, body, block, version, bufLen), true
	case clusterwire.CancelTimerSbeTemplateID:
		return decodeCancelTimerFields(buf, body, block, version, bufLen), true
	case clusterwire.ServiceAckSbeTemplateID:
		return decodeServiceAckFields(buf, body, block, version, bufLen), true
	case clusterwire.JoinLogSbeTemplateID:
		var msg clusterwire.JoinLog
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return JoinLogFields{
			LogPosition:    msg.LogPosition(),
			MaxLogPosition: msg.MaxLogPosition(),
			MemberID:       msg.MemberId(),
			LogSessionID:   msg.LogSessionId(),
			LogStreamID:    msg.LogStreamId(),
			IsStartup:      msg.IsStartup(),
			Role:           msg.Role(),
			IsStandby:      msg.IsStandby(),
			LogChannel:     vars.logChannel,
		}, true
	case clusterwire.RequestServiceAckSbeTemplateID:
		return decodeRequestServiceAckFields(buf, body, block, version, bufLen), true
	case clusterwire.SnapshotMarkerSbeTemplateID:
		return decodeSnapshotMarkerFields(buf, body, block, version, bufLen), true
	case clusterwire.ClientSessionSbeTemplateID:
		return clientSessionFields(buf, body, block, version, bufLen, vars), true
	case clusterwire.TimerSbeTemplateID:
		return decodeTimerFields(buf, body, block, version, bufLen), true
	case clusterwire.ConsensusModuleSbeTemplateID:
		return decodeConsensusModuleFields(buf, body, block, version, bufLen), true
	default:
		return nil, false
	}
}
