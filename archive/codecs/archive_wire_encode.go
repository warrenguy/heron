package codecs

import (
	"unsafe"

	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// ArchiveMinRootBlockLength returns the minimum root block length for templateID at
// actingVersion, or fallback when bounds are unavailable.
func ArchiveMinRootBlockLength(templateID, actingVersion, fallback uint16) uint16 {
	minBlock, _, ok := ArchiveRootBlockLengthBounds(templateID, actingVersion)
	if !ok {
		return fallback
	}
	return minBlock
}

func encodeWithArchiveWireHeader(
	buf []byte,
	offset int,
	blockLength, templateID, actingVersion uint16,
	varLens []int,
	encodeBody func(dst []byte, bodyOffset int, blockLength, actingVersion uint16),
) (wireEnd int, ok bool) {
	if offset < 0 || offset > len(buf)-messageHeaderEncodedLength {
		return 0, false
	}
	_, messageWireLen, ok := archiveWireBodyLen(blockLength, varLens...)
	if !ok {
		return 0, false
	}
	bodyOffset := offset + messageHeaderEncodedLength
	wireEnd = offset + messageWireLen
	if wireEnd > len(buf) {
		return 0, false
	}
	var hdr archivewire.MessageHeader
	hdr.Wrap(buf, uint64(offset), uint64(actingVersion), uint64(len(buf)))
	hdr.SetBlockLength(blockLength).
		SetTemplateId(templateID).
		SetSchemaId(archiveSchemaID).
		SetVersion(actingVersion)
	encodeBody(buf, bodyOffset, blockLength, actingVersion)
	return wireEnd, true
}

func archiveVarString(data []byte) string {
	//nolint:gosec // G103: converts data to a string without copying; the SBE setter copies it
	return unsafe.String(unsafe.SliceData(data), len(data))
}

// EncodeConnectRequest writes an archive ConnectRequest at offset.
func EncodeConnectRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ConnectRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		connectRequestBlockLength(actingVersion),
		archivewire.ConnectRequestSbeTemplateID,
		actingVersion,
		[]int{len(fields.ResponseChannel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.ConnectRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetResponseStreamId(fields.ResponseStreamID)
			if actingVersion >= archiveConnectRequestVersionMinVersion {
				msg.SetVersion(fields.Version)
			}
			msg.PutResponseChannel(archiveVarString(fields.ResponseChannel))
		},
	)
}

// EncodeAuthConnectRequest writes an archive AuthConnectRequest at offset.
func EncodeAuthConnectRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields AuthConnectRequestFields,
) (wireEnd int, ok bool) {
	var scratch [authConnectRequestMaxVarFieldCount]int
	count, ok := authConnectRequestVarLensInto(
		actingVersion,
		len(fields.ResponseChannel),
		len(fields.EncodedCredentials),
		len(fields.ClientInfo),
		scratch[:],
	)
	if !ok {
		return 0, false
	}
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		authConnectRequestBlockLength(actingVersion),
		archivewire.AuthConnectRequestSbeTemplateID,
		actingVersion,
		scratch[:count],
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.AuthConnectRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetResponseStreamId(fields.ResponseStreamID).
				SetVersion(fields.Version)
			msg.PutResponseChannel(archiveVarString(fields.ResponseChannel))
			msg.PutEncodedCredentials(archiveVarString(fields.EncodedCredentials))
			if actingVersion >= authConnectRequestClientInfoMinVersion {
				msg.PutClientInfo(archiveVarString(fields.ClientInfo))
			}
		},
	)
}

// EncodeCloseSessionRequest writes an archive CloseSessionRequest at offset.
func EncodeCloseSessionRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields CloseSessionRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.CloseSessionRequestSbeBlockLength,
		archivewire.CloseSessionRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.CloseSessionRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID)
		},
	)
}

// EncodeStartRecordingRequest writes an archive StartRecordingRequest at offset.
func EncodeStartRecordingRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields StartRecordingRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.StartRecordingRequestSbeBlockLength,
		archivewire.StartRecordingRequestSbeTemplateID,
		actingVersion,
		[]int{len(fields.Channel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.StartRecordingRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetStreamId(fields.StreamID).
				SetSourceLocation(fields.SourceLocation)
			msg.PutChannel(archiveVarString(fields.Channel))
		},
	)
}

// EncodeStartRecordingRequest2 writes an archive StartRecordingRequest2 at offset.
func EncodeStartRecordingRequest2(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields StartRecordingRequest2Fields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.StartRecordingRequest2SbeBlockLength,
		archivewire.StartRecordingRequest2SbeTemplateID,
		actingVersion,
		[]int{len(fields.Channel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.StartRecordingRequest2
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetStreamId(fields.StreamID).
				SetSourceLocation(fields.SourceLocation).
				SetAutoStop(fields.AutoStop)
			msg.PutChannel(archiveVarString(fields.Channel))
		},
	)
}

// EncodeExtendRecordingRequest2 writes an archive ExtendRecordingRequest2 at offset.
func EncodeExtendRecordingRequest2(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ExtendRecordingRequest2Fields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.ExtendRecordingRequest2SbeBlockLength,
		archivewire.ExtendRecordingRequest2SbeTemplateID,
		actingVersion,
		[]int{len(fields.Channel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.ExtendRecordingRequest2
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRecordingId(fields.RecordingID).
				SetStreamId(fields.StreamID).
				SetSourceLocation(fields.SourceLocation).
				SetAutoStop(fields.AutoStop)
			msg.PutChannel(archiveVarString(fields.Channel))
		},
	)
}

// EncodeStopRecordingRequest writes an archive StopRecordingRequest at offset.
func EncodeStopRecordingRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields StopRecordingRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.StopRecordingRequestSbeBlockLength,
		archivewire.StopRecordingRequestSbeTemplateID,
		actingVersion,
		[]int{len(fields.Channel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.StopRecordingRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetStreamId(fields.StreamID)
			msg.PutChannel(archiveVarString(fields.Channel))
		},
	)
}

// EncodeListRecordingRequest writes an archive ListRecordingRequest at offset.
func EncodeListRecordingRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ListRecordingRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.ListRecordingRequestSbeBlockLength,
		archivewire.ListRecordingRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.ListRecordingRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRecordingId(fields.RecordingID)
		},
	)
}

// EncodeReplayRequest writes an archive ReplayRequest at offset.
func EncodeReplayRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ReplayRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		replayRequestBlockLength(actingVersion),
		archivewire.ReplayRequestSbeTemplateID,
		actingVersion,
		[]int{len(fields.ReplayChannel)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.ReplayRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRecordingId(fields.RecordingID).
				SetPosition(fields.Position).
				SetLength(fields.Length).
				SetReplayStreamId(fields.ReplayStreamID)
			if actingVersion >= replayRequestFileIoMaxLengthMinVersion {
				msg.SetFileIoMaxLength(fields.FileIoMaxLength)
			}
			if actingVersion >= replayRequestReplayTokenMinVersion {
				msg.SetReplayToken(fields.ReplayToken)
			}
			msg.PutReplayChannel(archiveVarString(fields.ReplayChannel))
		},
	)
}

// EncodeStopReplayRequest writes an archive StopReplayRequest at offset.
func EncodeStopReplayRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields StopReplayRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.StopReplayRequestSbeBlockLength,
		archivewire.StopReplayRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.StopReplayRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetReplaySessionId(fields.ReplaySessionID)
		},
	)
}

// EncodeTruncateRecordingRequest writes an archive TruncateRecordingRequest at
// offset.
func EncodeTruncateRecordingRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields TruncateRecordingRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.TruncateRecordingRequestSbeBlockLength,
		archivewire.TruncateRecordingRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.TruncateRecordingRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID).
				SetRecordingId(fields.RecordingID).
				SetPosition(fields.Position)
		},
	)
}

// EncodeChallengeResponse writes an archive ChallengeResponse at offset.
func EncodeChallengeResponse(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ChallengeResponseFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.ChallengeResponseSbeBlockLength,
		archivewire.ChallengeResponseSbeTemplateID,
		actingVersion,
		[]int{len(fields.EncodedCredentials)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.ChallengeResponse
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID)
			msg.PutEncodedCredentials(archiveVarString(fields.EncodedCredentials))
		},
	)
}

// EncodeKeepAliveRequest writes an archive KeepAliveRequest at offset.
func EncodeKeepAliveRequest(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields KeepAliveRequestFields,
) (wireEnd int, ok bool) {
	return encodeWithArchiveWireHeader(
		buf,
		offset,
		archivewire.KeepAliveRequestSbeBlockLength,
		archivewire.KeepAliveRequestSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg archivewire.KeepAliveRequest
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetControlSessionId(fields.ControlSessionID).
				SetCorrelationId(fields.CorrelationID)
		},
	)
}
