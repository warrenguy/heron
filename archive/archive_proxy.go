package archive

import (
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// Proxy encodes archive control requests. Proxy offers each request on the
// control publication. Each Try method offers once. Proxy is not safe for
// concurrent use.
//
// Returns:
//   - BackPressured, AdminAction: retry after DoWork
//   - Closed, NotConnected, MaxPositionExceeded, UnsupportedMessageLength:
//     terminal (negativeCode, false)
//   - wire-length preflight or encode failure: (0, false)
type Proxy struct {
	publication   ControlPublication
	actingVersion uint16
	credentials   CredentialsSupplier
	clientInfo    []byte
	wireBuffer    []byte
}

// NewProxy constructs a post-connect control request encoder.
func NewProxy(pub ControlPublication, cfg Config) *Proxy {
	cfg = cfg.Normalize()
	return &Proxy{
		publication:   pub,
		actingVersion: archivewire.MessageHeaderSbeSchemaVersion,
		credentials:   cfg.CredentialsSupplier,
		clientInfo:    archiveConnectClientInfo(cfg.ClientName),
	}
}

// Close releases the control publication. Try methods offer on a closed publication
// after Close and fail.
func (p *Proxy) Close() error {
	if p.publication == nil {
		return nil
	}
	return p.publication.Close()
}

// TryConnect encodes AuthConnectRequest (template 58) and offers it once.
func (p *Proxy) TryConnect(
	responseChannel []byte,
	responseStreamID int32,
	correlationID int64,
) (int64, bool) {
	credentials := p.credentials.EncodedCredentials()
	wireLen, ok := authConnectRequestWireLen(
		len(responseChannel),
		len(credentials),
		len(p.clientInfo),
	)
	return p.tryOffer(wireLen, ok, func(dst []byte) (int, bool) {
		return encodeAuthConnectRequest(
			dst,
			correlationID,
			responseStreamID,
			responseChannel,
			credentials,
			p.clientInfo,
		)
	})
}

// TryChallengeResponse encodes ChallengeResponse (template 60) and offers it once.
func (p *Proxy) TryChallengeResponse(
	controlSessionID int64,
	correlationID int64,
	encodedCredentials []byte,
) (int64, bool) {
	wireLen, ok := challengeResponseWireLen(len(encodedCredentials))
	return p.tryOffer(wireLen, ok, func(dst []byte) (int, bool) {
		return encodeChallengeResponseRequest(
			dst,
			controlSessionID,
			correlationID,
			encodedCredentials,
		)
	})
}

// TryKeepAlive encodes KeepAliveRequest (template 61) and offers it once.
func (p *Proxy) TryKeepAlive(
	controlSessionID int64,
	correlationID int64,
) (int64, bool) {
	fields := codecs.KeepAliveRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
	}
	return p.tryArchiveWireOffer(
		archivewire.KeepAliveRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeKeepAliveRequest(dst, 0, actingVersion, fields)
		},
	)
}

// TryCloseSession encodes CloseSessionRequest (template 3) and offers it once.
// The wire message carries controlSessionId only; there is no correlation id field.
func (p *Proxy) TryCloseSession(controlSessionID int64) (int64, bool) {
	fields := codecs.CloseSessionRequestFields{
		ControlSessionID: controlSessionID,
	}
	return p.tryArchiveWireOffer(
		archivewire.CloseSessionRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeCloseSessionRequest(dst, 0, actingVersion, fields)
		},
	)
}

// TryStartRecording encodes StartRecordingRequest2 (template 63) and offers it once.
func (p *Proxy) TryStartRecording(
	controlSessionID int64,
	correlationID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop archivewire.BooleanType,
	channel []byte,
) (int64, bool) {
	fields := codecs.StartRecordingRequest2Fields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		StreamID:         streamID,
		SourceLocation:   sourceLocation,
		AutoStop:         autoStop,
		Channel:          channel,
	}
	return p.tryArchiveWireOffer(
		archivewire.StartRecordingRequest2SbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeStartRecordingRequest2(dst, 0, actingVersion, fields)
		},
		len(channel),
	)
}

// TryExtendRecording encodes ExtendRecordingRequest2 (template 64) and offers it once.
func (p *Proxy) TryExtendRecording(
	controlSessionID int64,
	correlationID int64,
	recordingID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop archivewire.BooleanType,
	channel []byte,
) (int64, bool) {
	fields := codecs.ExtendRecordingRequest2Fields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      recordingID,
		StreamID:         streamID,
		SourceLocation:   sourceLocation,
		AutoStop:         autoStop,
		Channel:          channel,
	}
	return p.tryArchiveWireOffer(
		archivewire.ExtendRecordingRequest2SbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeExtendRecordingRequest2(dst, 0, actingVersion, fields)
		},
		len(channel),
	)
}

// TryStopRecording encodes StopRecordingRequest (template 5) and offers it once.
func (p *Proxy) TryStopRecording(
	controlSessionID int64,
	correlationID int64,
	streamID int32,
	channel []byte,
) (int64, bool) {
	fields := codecs.StopRecordingRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		StreamID:         streamID,
		Channel:          channel,
	}
	return p.tryArchiveWireOffer(
		archivewire.StopRecordingRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeStopRecordingRequest(dst, 0, actingVersion, fields)
		},
		len(channel),
	)
}

// TryReplay encodes ReplayRequest (template 6) and offers it once.
func (p *Proxy) TryReplay(
	controlSessionID int64,
	correlationID int64,
	recordingID int64,
	position int64,
	length int64,
	replayStreamID int32,
	replayChannel []byte,
) (int64, bool) {
	fields := codecs.ReplayRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      recordingID,
		Position:         position,
		Length:           length,
		ReplayStreamID:   replayStreamID,
		ReplayChannel:    replayChannel,
	}
	wireLen, ok := codecs.ReplayRequestWireLen(p.actingVersion, len(replayChannel))
	return p.tryOffer(wireLen, ok, func(dst []byte) (int, bool) {
		return codecs.EncodeReplayRequest(dst, 0, p.actingVersion, fields)
	})
}

// TryStopReplay encodes StopReplayRequest (template 7) and offers it once.
func (p *Proxy) TryStopReplay(
	controlSessionID int64,
	correlationID int64,
	replaySessionID int64,
) (int64, bool) {
	fields := codecs.StopReplayRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		ReplaySessionID:  replaySessionID,
	}
	return p.tryArchiveWireOffer(
		archivewire.StopReplayRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeStopReplayRequest(dst, 0, actingVersion, fields)
		},
	)
}

// TryListRecording encodes ListRecordingRequest (template 10) and offers it once.
func (p *Proxy) TryListRecording(
	controlSessionID int64,
	correlationID int64,
	recordingID int64,
) (int64, bool) {
	fields := codecs.ListRecordingRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      recordingID,
	}
	return p.tryArchiveWireOffer(
		archivewire.ListRecordingRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeListRecordingRequest(dst, 0, actingVersion, fields)
		},
	)
}

// TryStopRecordingByIdentity encodes StopRecordingByIdentityRequest (template
// 65) and offers it once.
func (p *Proxy) TryStopRecordingByIdentity(
	controlSessionID int64,
	correlationID int64,
	recordingID int64,
) (int64, bool) {
	fields := codecs.StopRecordingByIdentityRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      recordingID,
	}
	return p.tryArchiveWireOffer(
		archivewire.StopRecordingByIdentityRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeStopRecordingByIdentityRequest(dst, 0, actingVersion, fields)
		},
	)
}

// TryTruncateRecording encodes TruncateRecordingRequest (template 13) and offers
// it once.
func (p *Proxy) TryTruncateRecording(
	controlSessionID int64,
	correlationID int64,
	recordingID int64,
	position int64,
) (int64, bool) {
	fields := codecs.TruncateRecordingRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      recordingID,
		Position:         position,
	}
	return p.tryArchiveWireOffer(
		archivewire.TruncateRecordingRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeTruncateRecordingRequest(dst, 0, actingVersion, fields)
		},
	)
}

func (p *Proxy) tryArchiveWireOffer(
	blockLength uint16,
	encode func([]byte, uint16) (int, bool),
	varLens ...int,
) (int64, bool) {
	wireLen, ok := codecs.ArchiveWireMessageLen(blockLength, varLens...)
	return p.tryOffer(wireLen, ok, func(dst []byte) (int, bool) {
		return encode(dst, p.actingVersion)
	})
}

func (p *Proxy) tryOffer(
	wireLen int,
	ok bool,
	encode func([]byte) (int, bool),
) (int64, bool) {
	if !ok {
		return 0, false
	}
	p.wireBuffer = resizeSensitiveWireBuffer(p.wireBuffer, wireLen)
	end, ok := encode(p.wireBuffer)
	if !ok {
		return 0, false
	}
	result := p.publication.Offer(p.wireBuffer[:end])
	return result, result > 0
}
