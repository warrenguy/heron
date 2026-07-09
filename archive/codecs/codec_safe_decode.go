package codecs

import (
	"bytes"
	"encoding/binary"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

var (
	messageHeaderLayout = &archivewire.MessageHeader{}

	messageHeaderEncodedLength       = int(messageHeaderLayout.EncodedLength())
	archiveSchemaID                  = messageHeaderLayout.SbeSchemaId()
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
// Fields holds a concrete type from codec_fields.go (for example ControlResponseFields).
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

// ArchiveMessageLayout is the parsed SBE header and fixed block for an archive message.
type ArchiveMessageLayout struct {
	TemplateID    uint16
	BodyOffset    int
	BlockLength   uint16
	ActingVersion uint16
}

// ArchiveMessageLayoutAt parses the archive message header and fixed block at offset.
func ArchiveMessageLayoutAt(buf []byte, offset, maxVarLen int) (ArchiveMessageLayout, bool) {
	return archiveMessageAt(buf, offset, maxVarLen)
}

// TryDecodeControlResponseAt decodes ControlResponse without interface boxing. The
// returned ErrorMessage is a sub-slice view into buf; consume before buf is reused.
func TryDecodeControlResponseAt(buf []byte, offset, maxVarLen int) (ControlResponseFields, bool) {
	layout, ok := archiveMessageAt(buf, offset, maxVarLen)
	if !ok || layout.TemplateID != archivewire.ControlResponseSbeTemplateID {
		return ControlResponseFields{}, false
	}
	var views [archiveWireMaxVarFieldCount]VarSlice
	if !tryDecodeVarViewsAt(buf, layout, maxVarLen, views[:]) {
		return ControlResponseFields{}, false
	}
	return decodeControlResponseFields(
		buf,
		uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
		views[0].Data,
	), true
}

// TryDecodeRecordingSignalEventAt decodes RecordingSignalEvent without interface boxing.
func TryDecodeRecordingSignalEventAt(
	buf []byte,
	offset, maxVarLen int,
) (RecordingSignalEventFields, bool) {
	layout, ok := archiveMessageAt(buf, offset, maxVarLen)
	if !ok || layout.TemplateID != archivewire.RecordingSignalEventSbeTemplateID {
		return RecordingSignalEventFields{}, false
	}
	return decodeRecordingSignalEventFields(
		buf,
		uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
	), true
}

// TryDecodeRecordingDescriptorAt decodes RecordingDescriptor without interface boxing.
// The returned channel and identity slices are sub-slice views into buf; consume before
// buf is reused.
func TryDecodeRecordingDescriptorAt(
	buf []byte,
	offset, maxVarLen int,
) (RecordingDescriptorFields, bool) {
	layout, ok := archiveMessageAt(buf, offset, maxVarLen)
	if !ok || layout.TemplateID != archivewire.RecordingDescriptorSbeTemplateID {
		return RecordingDescriptorFields{}, false
	}
	var views [archiveWireMaxVarFieldCount]VarSlice
	if !tryDecodeVarViewsAt(buf, layout, maxVarLen, views[:]) {
		return RecordingDescriptorFields{}, false
	}
	return decodeRecordingDescriptorFields(
		buf,
		uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
		views[0].Data,
		views[1].Data,
		views[2].Data,
	), true
}

// TryDecodeChallengeAt decodes Challenge without interface boxing. The returned
// EncodedChallenge is a sub-slice view into buf; consume before buf is reused.
func TryDecodeChallengeAt(buf []byte, offset, maxVarLen int) (ChallengeFields, bool) {
	layout, ok := archiveMessageAt(buf, offset, maxVarLen)
	if !ok || layout.TemplateID != archivewire.ChallengeSbeTemplateID {
		return ChallengeFields{}, false
	}
	var views [archiveWireMaxVarFieldCount]VarSlice
	if !tryDecodeVarViewsAt(buf, layout, maxVarLen, views[:]) {
		return ChallengeFields{}, false
	}
	return decodeChallengeFields(
		buf,
		uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
		views[0].Data,
	), true
}

func archiveVarFieldOffset(layout ArchiveMessageLayout) int {
	return layout.BodyOffset + int(layout.BlockLength)
}

func tryDecodeVarViewsAt(
	buf []byte,
	layout ArchiveMessageLayout,
	maxVarLen int,
	viewDst []VarSlice,
) bool {
	count, ok := ArchiveVarFieldCount(layout.TemplateID, layout.ActingVersion)
	if !ok {
		return false
	}
	if count == 0 {
		return true
	}
	if len(viewDst) < count {
		return false
	}
	_, _, ok = scanTemplateVarFields(
		buf,
		archiveVarFieldOffset(layout),
		layout.TemplateID,
		layout.ActingVersion,
		maxVarLen,
		false,
		viewDst[:count],
	)
	return ok
}

func decodeControlResponseFields(
	buf []byte,
	body uint64,
	block uint64,
	version uint64,
	bufLen uint64,
	errorMessage []byte,
) ControlResponseFields {
	var msg archivewire.ControlResponse
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ControlResponseFields{
		ControlSessionID: msg.ControlSessionId(),
		CorrelationID:    msg.CorrelationId(),
		RelevantID:       msg.RelevantId(),
		Code:             msg.Code(),
		Version:          msg.Version(),
		ErrorMessage:     errorMessage,
	}
}

func decodeRecordingSignalEventFields(
	buf []byte,
	body uint64,
	block uint64,
	version uint64,
	bufLen uint64,
) RecordingSignalEventFields {
	var msg archivewire.RecordingSignalEvent
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return RecordingSignalEventFields{
		ControlSessionID: msg.ControlSessionId(),
		CorrelationID:    msg.CorrelationId(),
		RecordingID:      msg.RecordingId(),
		SubscriptionID:   msg.SubscriptionId(),
		Position:         msg.Position(),
		Signal:           msg.Signal(),
	}
}

func decodeRecordingDescriptorFields(
	buf []byte,
	body uint64,
	block uint64,
	version uint64,
	bufLen uint64,
	strippedChannel []byte,
	originalChannel []byte,
	sourceIdentity []byte,
) RecordingDescriptorFields {
	var msg archivewire.RecordingDescriptor
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return RecordingDescriptorFields{
		ControlSessionID:  msg.ControlSessionId(),
		CorrelationID:     msg.CorrelationId(),
		RecordingID:       msg.RecordingId(),
		StartTimestamp:    msg.StartTimestamp(),
		StopTimestamp:     msg.StopTimestamp(),
		StartPosition:     msg.StartPosition(),
		StopPosition:      msg.StopPosition(),
		InitialTermID:     msg.InitialTermId(),
		SegmentFileLength: msg.SegmentFileLength(),
		TermBufferLength:  msg.TermBufferLength(),
		MtuLength:         msg.MtuLength(),
		SessionID:         msg.SessionId(),
		StreamID:          msg.StreamId(),
		StrippedChannel:   strippedChannel,
		OriginalChannel:   originalChannel,
		SourceIdentity:    sourceIdentity,
	}
}

func decodeChallengeFields(
	buf []byte,
	body uint64,
	block uint64,
	version uint64,
	bufLen uint64,
	encodedChallenge []byte,
) ChallengeFields {
	var msg archivewire.Challenge
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return ChallengeFields{
		ControlSessionID: msg.ControlSessionId(),
		CorrelationID:    msg.CorrelationId(),
		Version:          msg.Version(),
		EncodedChallenge: encodedChallenge,
	}
}

// TryDecodeByTemplate decodes an archive SBE message into plain field structs.
// maxVarLen bounds each varAscii/varData field; use ArchiveDecodeMaxVarLen for control.
// Rejects maxVarLen < 0. Trailing bytes after the last var field are ignored so
// Aeron fragment buffers decode without requiring exact buffer length.
func TryDecodeByTemplate(buf []byte, offset, maxVarLen int) (decoded DecodedMessage, ok bool) {
	layout, ok := archiveMessageAt(buf, offset, maxVarLen)
	if !ok {
		return decoded, ok
	}
	return TryDecodeFromArchiveLayout(buf, layout, maxVarLen)
}

// TryDecodeFromArchiveLayout decodes archive message fields from a parsed layout.
func TryDecodeFromArchiveLayout(
	buf []byte,
	layout ArchiveMessageLayout,
	maxVarLen int,
) (decoded DecodedMessage, ok bool) {
	if maxVarLen < 0 {
		return decoded, false
	}

	vars, _, ok := scanTemplateVarFields(
		buf,
		archiveVarFieldOffset(layout),
		layout.TemplateID,
		layout.ActingVersion,
		maxVarLen,
		true,
		nil,
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
	errorMessage       []byte
	responseChannel    []byte
	channel            []byte
	replayChannel      []byte
	strippedChannel    []byte
	originalChannel    []byte
	sourceIdentity     []byte
	encodedCredentials []byte
	clientInfo         []byte
	encodedChallenge   []byte
}

// ArchiveRootBlockLengthBounds returns min and max blockLength accepted for templateID
// at actingVersion.
func ArchiveRootBlockLengthBounds(
	templateID, actingVersion uint16,
) (minBlock, maxBlock uint16, ok bool) {
	switch templateID {
	case archivewire.ControlResponseSbeTemplateID:
		maxBlock = archivewire.ControlResponseSbeBlockLength
		if actingVersion < archiveControlResponseVersionMinVersion {
			return 28, maxBlock, true
		}
		return maxBlock, maxBlock, true
	case archivewire.ConnectRequestSbeTemplateID:
		maxBlock = archivewire.ConnectRequestSbeBlockLength
		if actingVersion < archiveConnectRequestVersionMinVersion {
			return 12, maxBlock, true
		}
		return maxBlock, maxBlock, true
	case archivewire.CloseSessionRequestSbeTemplateID:
		bl := archivewire.CloseSessionRequestSbeBlockLength
		return bl, bl, true
	case archivewire.StartRecordingRequestSbeTemplateID:
		bl := archivewire.StartRecordingRequestSbeBlockLength
		return bl, bl, true
	case archivewire.StopRecordingRequestSbeTemplateID:
		bl := archivewire.StopRecordingRequestSbeBlockLength
		return bl, bl, true
	case archivewire.ReplayRequestSbeTemplateID:
		maxBlock = archivewire.ReplayRequestSbeBlockLength
		switch {
		case actingVersion < replayRequestFileIoMaxLengthMinVersion:
			return 44, maxBlock, true
		case actingVersion < replayRequestReplayTokenMinVersion:
			return 48, maxBlock, true
		default:
			return maxBlock, maxBlock, true
		}
	case archivewire.StopReplayRequestSbeTemplateID:
		bl := archivewire.StopReplayRequestSbeBlockLength
		return bl, bl, true
	case archivewire.ListRecordingRequestSbeTemplateID:
		bl := archivewire.ListRecordingRequestSbeBlockLength
		return bl, bl, true
	case archivewire.RecordingDescriptorSbeTemplateID:
		bl := archivewire.RecordingDescriptorSbeBlockLength
		return bl, bl, true
	case archivewire.RecordingSignalEventSbeTemplateID:
		bl := archivewire.RecordingSignalEventSbeBlockLength
		return bl, bl, true
	case archivewire.AuthConnectRequestSbeTemplateID:
		bl := archivewire.AuthConnectRequestSbeBlockLength
		return bl, bl, true
	case archivewire.ChallengeSbeTemplateID:
		bl := archivewire.ChallengeSbeBlockLength
		return bl, bl, true
	case archivewire.ChallengeResponseSbeTemplateID:
		bl := archivewire.ChallengeResponseSbeBlockLength
		return bl, bl, true
	case archivewire.KeepAliveRequestSbeTemplateID:
		bl := archivewire.KeepAliveRequestSbeBlockLength
		return bl, bl, true
	case archivewire.StartRecordingRequest2SbeTemplateID:
		bl := archivewire.StartRecordingRequest2SbeBlockLength
		return bl, bl, true
	default:
		return 0, 0, false
	}
}

// ArchiveVarFieldCount returns the number of variable-length fields for templateID
// at actingVersion.
func ArchiveVarFieldCount(templateID, actingVersion uint16) (count int, ok bool) {
	switch templateID {
	case archivewire.CloseSessionRequestSbeTemplateID,
		archivewire.StopReplayRequestSbeTemplateID,
		archivewire.ListRecordingRequestSbeTemplateID,
		archivewire.RecordingSignalEventSbeTemplateID,
		archivewire.KeepAliveRequestSbeTemplateID:
		return 0, true
	case archivewire.ControlResponseSbeTemplateID,
		archivewire.ConnectRequestSbeTemplateID,
		archivewire.StartRecordingRequestSbeTemplateID,
		archivewire.StopRecordingRequestSbeTemplateID,
		archivewire.ReplayRequestSbeTemplateID,
		archivewire.ChallengeSbeTemplateID,
		archivewire.ChallengeResponseSbeTemplateID,
		archivewire.StartRecordingRequest2SbeTemplateID:
		return 1, true
	case archivewire.RecordingDescriptorSbeTemplateID:
		return 3, true
	case archivewire.AuthConnectRequestSbeTemplateID:
		if actingVersion >= authConnectRequestClientInfoMinVersion {
			return authConnectRequestMaxVarFieldCount, true
		}
		return 2, true
	default:
		return 0, false
	}
}

func archiveMessageAt(buf []byte, offset, maxVarLen int) (ArchiveMessageLayout, bool) {
	if maxVarLen < 0 {
		return ArchiveMessageLayout{}, false
	}

	blockLength, templateID, schemaID, actingVersion, ok := DecodeMessageHeader(buf, offset)
	if !ok || schemaID != archiveSchemaID {
		return ArchiveMessageLayout{}, false
	}

	minBlock, maxBlock, known := ArchiveRootBlockLengthBounds(templateID, actingVersion)
	if !known || blockLength < minBlock || blockLength > maxBlock {
		return ArchiveMessageLayout{}, false
	}

	bodyOffset := offset + messageHeaderEncodedLength
	if len(buf)-bodyOffset < int(blockLength) {
		return ArchiveMessageLayout{}, false
	}

	return ArchiveMessageLayout{
		BodyOffset:    bodyOffset,
		BlockLength:   blockLength,
		TemplateID:    templateID,
		ActingVersion: actingVersion,
	}, true
}

// ArchiveMessageWireEnd returns the offset after the last var field for an archive message.
// Rejects maxVarLen < 0 (consistent with TryDecodeByTemplate).
func ArchiveMessageWireEnd(buf []byte, offset, maxVarLen int) (wireEnd int, ok bool) {
	layout, ok := archiveMessageAt(buf, offset, maxVarLen)
	if !ok {
		return wireEnd, ok
	}
	_, wireEnd, ok = scanTemplateVarFields(
		buf,
		archiveVarFieldOffset(layout),
		layout.TemplateID,
		layout.ActingVersion,
		maxVarLen,
		false,
		nil,
	)
	return wireEnd, ok
}

func scanTemplateVarFields(
	buf []byte,
	offset int,
	templateID, actingVersion uint16,
	maxVarLen int,
	clone bool,
	viewDst []VarSlice,
) (vars templateVarFields, next int, ok bool) {
	count, ok := ArchiveVarFieldCount(templateID, actingVersion)
	if !ok {
		return templateVarFields{}, 0, false
	}
	if count == 0 {
		return templateVarFields{}, offset, true
	}
	if viewDst != nil && len(viewDst) < count {
		return templateVarFields{}, 0, false
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
		if viewDst != nil {
			viewDst[i] = slice
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
	case archivewire.ControlResponseSbeTemplateID:
		return templateVarFields{errorMessage: raw[0]}, true
	case archivewire.ConnectRequestSbeTemplateID:
		return templateVarFields{responseChannel: raw[0]}, true
	case archivewire.StartRecordingRequestSbeTemplateID,
		archivewire.StopRecordingRequestSbeTemplateID,
		archivewire.StartRecordingRequest2SbeTemplateID:
		return templateVarFields{channel: raw[0]}, true
	case archivewire.ReplayRequestSbeTemplateID:
		return templateVarFields{replayChannel: raw[0]}, true
	case archivewire.RecordingDescriptorSbeTemplateID:
		return templateVarFields{
			strippedChannel: raw[0],
			originalChannel: raw[1],
			sourceIdentity:  raw[2],
		}, true
	case archivewire.AuthConnectRequestSbeTemplateID:
		vars := templateVarFields{
			responseChannel:    raw[0],
			encodedCredentials: raw[1],
		}
		if len(raw) > 2 {
			vars.clientInfo = raw[2]
		}
		return vars, true
	case archivewire.ChallengeSbeTemplateID:
		return templateVarFields{encodedChallenge: raw[0]}, true
	case archivewire.ChallengeResponseSbeTemplateID:
		return templateVarFields{encodedCredentials: raw[0]}, true
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
	case archivewire.ControlResponseSbeTemplateID:
		return decodeControlResponseFields(buf, body, block, version, bufLen, vars.errorMessage), true
	case archivewire.ConnectRequestSbeTemplateID:
		var msg archivewire.ConnectRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ConnectRequestFields{
			CorrelationID:    msg.CorrelationId(),
			ResponseStreamID: msg.ResponseStreamId(),
			Version:          msg.Version(),
			ResponseChannel:  vars.responseChannel,
		}, true
	case archivewire.CloseSessionRequestSbeTemplateID:
		var msg archivewire.CloseSessionRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return CloseSessionRequestFields{
			ControlSessionID: msg.ControlSessionId(),
		}, true
	case archivewire.StartRecordingRequestSbeTemplateID:
		var msg archivewire.StartRecordingRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return StartRecordingRequestFields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
			StreamID:         msg.StreamId(),
			SourceLocation:   msg.SourceLocation(),
			Channel:          vars.channel,
		}, true
	case archivewire.StopRecordingRequestSbeTemplateID:
		var msg archivewire.StopRecordingRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return StopRecordingRequestFields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
			StreamID:         msg.StreamId(),
			Channel:          vars.channel,
		}, true
	case archivewire.ReplayRequestSbeTemplateID:
		var msg archivewire.ReplayRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ReplayRequestFields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
			RecordingID:      msg.RecordingId(),
			Position:         msg.Position(),
			Length:           msg.Length(),
			ReplayStreamID:   msg.ReplayStreamId(),
			FileIoMaxLength:  msg.FileIoMaxLength(),
			ReplayToken:      msg.ReplayToken(),
			ReplayChannel:    vars.replayChannel,
		}, true
	case archivewire.StopReplayRequestSbeTemplateID:
		var msg archivewire.StopReplayRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return StopReplayRequestFields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
			ReplaySessionID:  msg.ReplaySessionId(),
		}, true
	case archivewire.ListRecordingRequestSbeTemplateID:
		var msg archivewire.ListRecordingRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ListRecordingRequestFields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
			RecordingID:      msg.RecordingId(),
		}, true
	case archivewire.RecordingDescriptorSbeTemplateID:
		return decodeRecordingDescriptorFields(
			buf,
			body,
			block,
			version,
			bufLen,
			vars.strippedChannel,
			vars.originalChannel,
			vars.sourceIdentity,
		), true
	case archivewire.RecordingSignalEventSbeTemplateID:
		return decodeRecordingSignalEventFields(buf, body, block, version, bufLen), true
	case archivewire.AuthConnectRequestSbeTemplateID:
		var msg archivewire.AuthConnectRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return AuthConnectRequestFields{
			CorrelationID:      msg.CorrelationId(),
			ResponseStreamID:   msg.ResponseStreamId(),
			Version:            msg.Version(),
			ResponseChannel:    vars.responseChannel,
			EncodedCredentials: vars.encodedCredentials,
			ClientInfo:         vars.clientInfo,
		}, true
	case archivewire.ChallengeSbeTemplateID:
		return decodeChallengeFields(buf, body, block, version, bufLen, vars.encodedChallenge), true
	case archivewire.ChallengeResponseSbeTemplateID:
		var msg archivewire.ChallengeResponse
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return ChallengeResponseFields{
			ControlSessionID:   msg.ControlSessionId(),
			CorrelationID:      msg.CorrelationId(),
			EncodedCredentials: vars.encodedCredentials,
		}, true
	case archivewire.KeepAliveRequestSbeTemplateID:
		var msg archivewire.KeepAliveRequest
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return KeepAliveRequestFields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
		}, true
	case archivewire.StartRecordingRequest2SbeTemplateID:
		var msg archivewire.StartRecordingRequest2
		msg.WrapForDecode(buf, body, block, version, bufLen)
		return StartRecordingRequest2Fields{
			ControlSessionID: msg.ControlSessionId(),
			CorrelationID:    msg.CorrelationId(),
			StreamID:         msg.StreamId(),
			SourceLocation:   msg.SourceLocation(),
			AutoStop:         msg.AutoStop(),
			Channel:          vars.channel,
		}, true
	default:
		return nil, false
	}
}
