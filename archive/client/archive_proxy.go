package client

import (
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

// ArchiveProxy encodes archive control requests and offers them on the control
// publication. Each Try method performs a single Offer; callers retry after DoWork
// when the returned code is BackPressured or AdminAction. Closed, NotConnected, and
// MaxPositionExceeded are terminal (negativeCode, false). Wire-length preflight and
// encode failures return (0, false). ArchiveProxy is not safe for concurrent use.
type ArchiveProxy struct {
	publication   ControlPublication
	actingVersion uint16
	credentials   CredentialsSupplier
	clientInfo    []byte
	wireBuffer    []byte
}

// NewArchiveProxy constructs a post-connect control request encoder.
func NewArchiveProxy(pub ControlPublication, ctx ArchiveContext) *ArchiveProxy {
	ctx = ctx.Normalize()
	return &ArchiveProxy{
		publication:   pub,
		actingVersion: gensbe.MessageHeaderSbeSchemaVersion,
		credentials:   ctx.CredentialsSupplier,
		clientInfo:    archiveConnectClientInfo(ctx.ClientName),
	}
}

// TryConnect encodes AuthConnectRequest (template 58) and offers it once.
func (p *ArchiveProxy) TryConnect(
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
func (p *ArchiveProxy) TryChallengeResponse(
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
func (p *ArchiveProxy) TryKeepAlive(
	controlSessionID int64,
	correlationID int64,
) (int64, bool) {
	fields := codecs.KeepAliveRequestFields{
		ControlSessionID: controlSessionID,
		CorrelationID:    correlationID,
	}
	return p.tryArchiveWireOffer(
		gensbe.KeepAliveRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeKeepAliveRequest(dst, 0, actingVersion, fields)
		},
	)
}

// TryCloseSession encodes CloseSessionRequest (template 3) and offers it once.
// The wire message carries controlSessionId only; there is no correlation id field.
func (p *ArchiveProxy) TryCloseSession(controlSessionID int64) (int64, bool) {
	fields := codecs.CloseSessionRequestFields{
		ControlSessionID: controlSessionID,
	}
	return p.tryArchiveWireOffer(
		gensbe.CloseSessionRequestSbeBlockLength,
		func(dst []byte, actingVersion uint16) (int, bool) {
			return codecs.EncodeCloseSessionRequest(dst, 0, actingVersion, fields)
		},
	)
}

func (p *ArchiveProxy) tryArchiveWireOffer(
	blockLength uint16,
	encode func([]byte, uint16) (int, bool),
) (int64, bool) {
	wireLen, ok := codecs.ArchiveWireMessageLen(blockLength)
	return p.tryOffer(wireLen, ok, func(dst []byte) (int, bool) {
		return encode(dst, p.actingVersion)
	})
}

func (p *ArchiveProxy) tryOffer(
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
