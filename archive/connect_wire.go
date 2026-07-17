package archive

import (
	"errors"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const archiveConnectClientLabel = "heron"

func archiveConnectClientInfo(clientName string) []byte {
	if clientName == "" {
		clientName = defaultClientName
	}
	return []byte("name=" + clientName + " " + archiveConnectClientLabel)
}

func authConnectRequestWireLen(
	responseChannelLen, credentialsLen, clientInfoLen int,
) (int, bool) {
	return codecs.AuthConnectRequestWireLen(
		archivewire.MessageHeaderSbeSchemaVersion,
		responseChannelLen,
		credentialsLen,
		clientInfoLen,
	)
}

func encodeAuthConnectRequest(
	dst []byte,
	correlationID int64,
	responseStreamID int32,
	responseChannel []byte,
	credentials []byte,
	clientInfo []byte,
) (int, bool) {
	fields := codecs.AuthConnectRequestFields{
		CorrelationID:      correlationID,
		ResponseStreamID:   responseStreamID,
		Version:            ProtocolSemanticVersion(),
		ResponseChannel:    responseChannel,
		EncodedCredentials: credentials,
		ClientInfo:         clientInfo,
	}
	return codecs.EncodeAuthConnectRequest(
		dst,
		0,
		archivewire.MessageHeaderSbeSchemaVersion,
		fields,
	)
}

func challengeResponseWireLen(encodedCredentialsLen int) (int, bool) {
	return codecs.ArchiveWireMessageLen(
		archivewire.ChallengeResponseSbeBlockLength,
		encodedCredentialsLen,
	)
}

func encodeChallengeResponseRequest(
	dst []byte,
	controlSessionID int64,
	correlationID int64,
	encodedCredentials []byte,
) (int, bool) {
	fields := codecs.ChallengeResponseFields{
		ControlSessionID:   controlSessionID,
		CorrelationID:      correlationID,
		EncodedCredentials: encodedCredentials,
	}
	return codecs.EncodeChallengeResponse(
		dst,
		0,
		archivewire.ChallengeResponseSbeSchemaVersion,
		fields,
	)
}

// resizeSensitiveWireBuffer sizes buf to wireLen. Reused backing stores are fully
// cleared before re-slice so credential bytes from a prior encode cannot linger in
// [wireLen:cap] between connect and challenge handshake steps.
func resizeSensitiveWireBuffer(buf []byte, wireLen int) []byte {
	if cap(buf) < wireLen {
		if cap(buf) > 0 {
			clear(buf[:cap(buf)])
		}
		return make([]byte, wireLen)
	}
	clear(buf[:cap(buf)])
	return buf[:wireLen]
}

func (ac *AsyncConnect) resizeAndEncodeConnectWire(
	wireLen int,
	encode func([]byte) (int, bool),
) (int, bool) {
	ac.wire.buffer = resizeSensitiveWireBuffer(ac.wire.buffer, wireLen)
	return encode(ac.wire.buffer)
}

func (ac *AsyncConnect) encodeConnectWire(
	wireLen int,
	ok bool,
	invalidLenMsg, encodeFailMsg string,
	encode func([]byte) (int, bool),
) error {
	if !ok {
		return errors.New(invalidLenMsg)
	}
	end, ok := ac.resizeAndEncodeConnectWire(wireLen, encode)
	if !ok {
		return errors.New(encodeFailMsg)
	}
	ac.wire.length = end
	return nil
}

// prepareConnectRequest sizes and encodes the archive connect wire message.
// Wires larger than the publication maxMessageLength are not rejected here; Java
// defers that ceiling to Publication.offer; see pollSendMessage.
func (ac *AsyncConnect) prepareConnectRequest() error {
	responseChannel, ok := ac.resolvedResponseChannel()
	if !ok {
		return errors.New("archive: control response subscription cannot resolve response channel")
	}
	ac.ensureCorrelationID()
	responseChannelBytes := []byte(responseChannel)
	credentials := ac.archiveCtx.CredentialsSupplier.EncodedCredentials()
	clientInfo := archiveConnectClientInfo(ac.archiveCtx.ClientName)
	wireLen, ok := authConnectRequestWireLen(
		len(responseChannelBytes),
		len(credentials),
		len(clientInfo),
	)
	return ac.encodeConnectWire(
		wireLen,
		ok,
		"archive: auth connect request wire length invalid",
		"archive: failed to encode auth connect request",
		func(dst []byte) (int, bool) {
			return encodeAuthConnectRequest(
				dst,
				ac.session.correlationID,
				ac.archiveCtx.EffectiveControlResponseStreamID(),
				responseChannelBytes,
				credentials,
				clientInfo,
			)
		},
	)
}

// prepareChallengeResponse sizes and encodes the challenge response wire message.
// Publication maxMessageLength overflow is deferred to Offer; see pollSendMessage.
func (ac *AsyncConnect) prepareChallengeResponse() error {
	ac.ensureCorrelationID()
	challenge := ac.wire.encodedChallenge
	if challenge == nil {
		challenge = []byte{}
	}
	credentials := ac.archiveCtx.CredentialsSupplier.RespondToChallenge(challenge)
	wireLen, ok := challengeResponseWireLen(len(credentials))
	return ac.encodeConnectWire(
		wireLen,
		ok,
		"archive: challenge response wire length invalid",
		"archive: failed to encode challenge response",
		func(dst []byte) (int, bool) {
			return encodeChallengeResponseRequest(
				dst,
				ac.session.controlSessionID,
				ac.session.correlationID,
				credentials,
			)
		},
	)
}
