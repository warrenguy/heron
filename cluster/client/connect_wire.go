package client

import (
	"errors"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"

	"github.com/warrenguy/heron/cluster/codecs"
)

const (
	defaultClusterClientName  = "cluster-client"
	clusterConnectClientLabel = "heron"
)

func sessionConnectClientInfo(clientName string) []byte {
	if clientName == "" {
		clientName = defaultClusterClientName
	}
	return []byte("name=" + clientName + " " + clusterConnectClientLabel)
}

// resizeConnectWireBuffer sizes ac.connectWireBuffer to wireLen. Reused backing stores
// are fully cleared before re-slice so credential bytes from a prior prepare cannot
// linger in [wireLen:cap] between connect and challenge handshake steps.
func (ac *AsyncConnect) resizeConnectWireBuffer(wireLen int) {
	if cap(ac.connectWireBuffer) < wireLen {
		if cap(ac.connectWireBuffer) > 0 {
			clear(ac.connectWireBuffer[:cap(ac.connectWireBuffer)])
		}
		ac.connectWireBuffer = make([]byte, wireLen)
		return
	}
	clear(ac.connectWireBuffer[:cap(ac.connectWireBuffer)])
	ac.connectWireBuffer = ac.connectWireBuffer[:wireLen]
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
	ac.resizeConnectWireBuffer(wireLen)
	end, ok := encode(ac.connectWireBuffer)
	if !ok {
		return errors.New(encodeFailMsg)
	}
	ac.connectWireLength = end
	return nil
}

// prepareConnectRequest sizes and encodes the session connect wire message.
// Wires larger than the publication maxMessageLength are not rejected here; Java
// defers that ceiling to Publication.offer; see pollSendMessage.
func (ac *AsyncConnect) prepareConnectRequest() error {
	responseChannel, ok := ac.resolvedResponseChannel()
	if !ok {
		return errors.New("cluster client: egress subscription cannot resolve response channel")
	}
	ac.ensureCorrelationID()
	credentials := ac.clusterCtx.CredentialsSupplier.EncodedCredentials()
	clientInfo := sessionConnectClientInfo(ac.clusterCtx.ClientName)
	actingVersion := clusterwire.SessionConnectRequestSbeSchemaVersion
	wireLen, ok := codecs.SessionConnectRequestWireLen(
		actingVersion,
		len(responseChannel),
		len(credentials),
		len(clientInfo),
	)
	fields := codecs.SessionConnectRequestFields{
		CorrelationID:      ac.correlationID,
		ResponseStreamID:   ac.clusterCtx.ResponseStreamID,
		Version:            ProtocolSemanticVersion(),
		ResponseChannel:    []byte(responseChannel),
		EncodedCredentials: credentials,
		ClientInfo:         clientInfo,
	}
	return ac.encodeConnectWire(
		wireLen,
		ok,
		"cluster client: session connect request wire length invalid",
		"cluster client: failed to encode session connect request",
		func(dst []byte) (int, bool) {
			return codecs.EncodeSessionConnectRequest(dst, 0, actingVersion, fields)
		},
	)
}

// prepareChallengeResponse sizes and encodes the challenge response wire message.
// Publication maxMessageLength overflow is deferred to Offer; see pollSendMessage.
func (ac *AsyncConnect) prepareChallengeResponse() error {
	ac.ensureCorrelationID()
	challenge := ac.encodedChallenge
	if challenge == nil {
		challenge = []byte{}
	}
	credentials := ac.clusterCtx.CredentialsSupplier.RespondToChallenge(challenge)
	actingVersion := clusterwire.ChallengeResponseSbeSchemaVersion
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.ChallengeResponseSbeBlockLength,
		len(credentials),
	)
	fields := codecs.ChallengeResponseFields{
		CorrelationID:      ac.correlationID,
		ClusterSessionID:   ac.clusterSessionID,
		EncodedCredentials: credentials,
	}
	return ac.encodeConnectWire(
		wireLen,
		ok,
		"cluster client: challenge response wire length invalid",
		"cluster client: failed to encode challenge response",
		func(dst []byte) (int, bool) {
			return codecs.EncodeChallengeResponse(dst, 0, actingVersion, fields)
		},
	)
}
