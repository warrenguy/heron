package client

import (
	"errors"
	"fmt"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

	"github.com/warrenguy/heron/cluster/codecs"
)

const (
	connectWireBufferSize = codecs.ClusterMaxVarLen

	// sessionConnectResponseChannelMaxLen is the responseChannel byte budget used
	// when deriving maxEncodedCredentialsLen. Matches aeron:ipc returned by IPC egress
	// subscriptions during async connect; longer subscription channels are rejected by
	// the total wire-length guard in prepareConnectRequest.
	sessionConnectResponseChannelMaxLen = len("aeron:ipc")

	sessionConnectClientInfoPrefixLen = len("name=")
	sessionConnectClientInfoSuffixLen = len(" " + clusterConnectClientLabel)

	defaultClusterClientName  = "cluster-client"
	clusterConnectClientLabel = "heron"
)

const sessionConnectClientInfoMaxLen = sessionConnectClientInfoPrefixLen +
	MaxClientNameLength +
	sessionConnectClientInfoSuffixLen

var (
	maxEncodedCredentialsLen = connectWireBufferSize - sessionConnectRequestWireLen(
		gensbe.SessionConnectRequestSbeSchemaVersion,
		sessionConnectResponseChannelMaxLen,
		0,
		sessionConnectClientInfoMaxLen,
	)
	maxChallengeResponseLen = connectWireBufferSize - challengeResponseWireLen(0)
)

func clusterVarDataWireLen(dataLen int) int {
	return codecs.VarLengthPrefixSize + dataLen
}

func sessionConnectRequestWireLen(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) int {
	blockLength := int(codecs.ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	))
	wireLen := codecs.MessageHeaderEncodedLen() + blockLength
	wireLen += clusterVarDataWireLen(responseChannelLen)
	wireLen += clusterVarDataWireLen(encodedCredentialsLen)
	if actingVersion >= codecs.SessionConnectClientInfoMinVersion {
		wireLen += clusterVarDataWireLen(clientInfoLen)
	}
	return wireLen
}

func challengeResponseWireLen(encodedCredentialsLen int) int {
	return codecs.MessageHeaderEncodedLen() +
		int(gensbe.ChallengeResponseSbeBlockLength) +
		clusterVarDataWireLen(encodedCredentialsLen)
}

func (ac *AsyncConnect) prepareConnectRequest() error {
	responseChannel, ok := resolveResponseChannel(ac.egressSubscription)
	if !ok {
		return errors.New("cluster client: egress subscription not ready for response channel")
	}
	ac.ensureCorrelationID()
	credentials := ac.ctx.CredentialsSupplier.EncodedCredentials()
	if len(credentials) > maxEncodedCredentialsLen {
		return fmt.Errorf(
			"cluster client: encoded credentials exceed max length %d",
			maxEncodedCredentialsLen,
		)
	}
	clientName := ac.ctx.ClientName
	if clientName == "" {
		clientName = defaultClusterClientName
	}
	clientInfo := []byte("name=" + clientName + " " + clusterConnectClientLabel)
	actingVersion := gensbe.SessionConnectRequestSbeSchemaVersion
	wireLen := sessionConnectRequestWireLen(
		actingVersion,
		len(responseChannel),
		len(credentials),
		len(clientInfo),
	)
	if wireLen > connectWireBufferSize {
		return fmt.Errorf(
			"cluster client: session connect request wire length %d exceeds max %d",
			wireLen,
			connectWireBufferSize,
		)
	}
	fields := codecs.SessionConnectRequestFields{
		CorrelationID:      ac.correlationID,
		ResponseStreamID:   ac.ctx.ResponseStreamID,
		Version:            ProtocolSemanticVersion(),
		ResponseChannel:    []byte(responseChannel),
		EncodedCredentials: credentials,
		ClientInfo:         clientInfo,
	}
	end, ok := codecs.EncodeSessionConnectRequest(
		ac.connectWireBuffer[:],
		0,
		actingVersion,
		fields,
	)
	if !ok {
		return errors.New("cluster client: failed to encode session connect request")
	}
	ac.connectWireLength = end
	return nil
}

func (ac *AsyncConnect) prepareChallengeResponse() error {
	ac.ensureCorrelationID()
	challenge := ac.encodedChallenge
	if challenge == nil {
		challenge = []byte{}
	}
	credentials := ac.ctx.CredentialsSupplier.RespondToChallenge(challenge)
	if len(credentials) > maxChallengeResponseLen {
		return fmt.Errorf(
			"cluster client: challenge response credentials exceed max length %d",
			maxChallengeResponseLen,
		)
	}
	actingVersion := gensbe.ChallengeResponseSbeSchemaVersion
	wireLen := challengeResponseWireLen(len(credentials))
	if wireLen > connectWireBufferSize {
		return fmt.Errorf(
			"cluster client: challenge response wire length %d exceeds max %d",
			wireLen,
			connectWireBufferSize,
		)
	}
	fields := codecs.ChallengeResponseFields{
		CorrelationID:      ac.correlationID,
		ClusterSessionID:   ac.clusterSessionID,
		EncodedCredentials: credentials,
	}
	end, ok := codecs.EncodeChallengeResponse(
		ac.connectWireBuffer[:],
		0,
		actingVersion,
		fields,
	)
	if !ok {
		return errors.New("cluster client: failed to encode challenge response")
	}
	ac.connectWireLength = end
	return nil
}
