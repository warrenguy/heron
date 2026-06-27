package client

import (
	"github.com/warrenguy/heron/cluster/codecs/gensbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/codecs"
)

const challengeSbeTemplateID = int32(gensbe.ChallengeSbeTemplateID)

// handleConnectEgressFragment decodes cluster connect egress SBE and captures
// terminal templates on poller. Unlike handleEgressFragment there is no
// clusterSessionID filter and Challenge is captured.
func handleConnectEgressFragment(
	poller *ConnectEgressPoller,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	if poller.pollComplete {
		return
	}

	fragment, ok := egressFragmentPayload(buffer, offset, length)
	if !ok {
		return
	}
	_, templateID, _, _, headerOK := codecs.DecodeMessageHeader(fragment, 0)
	if !headerOK {
		return
	}
	switch templateID {
	case gensbe.SessionEventSbeTemplateID,
		gensbe.NewLeaderEventSbeTemplateID,
		gensbe.ChallengeSbeTemplateID,
		gensbe.SessionMessageHeaderSbeTemplateID:
	default:
		return
	}

	decoded, ok := decodeEgressFragment(fragment)
	if !ok {
		return
	}

	switch decoded.TemplateID {
	case gensbe.SessionEventSbeTemplateID:
		ev, ok := decoded.Fields.(codecs.SessionEventFields)
		if !ok {
			return
		}
		poller.clusterSessionID = ev.ClusterSessionID
		poller.correlationID = ev.CorrelationID
		poller.leadershipTermID = ev.LeadershipTermID
		poller.leaderMemberID = ev.LeaderMemberID
		poller.leaderHeartbeatTimeoutNs = ev.LeaderHeartbeatTimeoutNs
		poller.eventCode = EventCode(ev.Code)
		poller.detail = ev.Detail
		captureConnectEgressTerminal(poller, decoded.TemplateID, header)

	case gensbe.NewLeaderEventSbeTemplateID:
		leader, ok := decoded.Fields.(codecs.NewLeaderEventFields)
		if !ok {
			return
		}
		poller.clusterSessionID = leader.ClusterSessionID
		poller.leadershipTermID = leader.LeadershipTermID
		poller.leaderMemberID = leader.LeaderMemberID
		poller.detail = leader.IngressEndpoints
		captureConnectEgressTerminal(poller, decoded.TemplateID, header)

	case gensbe.ChallengeSbeTemplateID:
		challenge, ok := decoded.Fields.(codecs.ChallengeFields)
		if !ok {
			return
		}
		poller.correlationID = challenge.CorrelationID
		poller.clusterSessionID = challenge.ClusterSessionID
		poller.encodedChallenge = challenge.EncodedChallenge
		captureConnectEgressTerminal(poller, decoded.TemplateID, header)

	case gensbe.SessionMessageHeaderSbeTemplateID:
		captureConnectEgressTerminal(poller, decoded.TemplateID, header)
	}
}

func captureConnectEgressTerminal(
	poller *ConnectEgressPoller,
	templateID uint16,
	header *aeron.Header,
) {
	poller.templateID = int32(templateID)
	switch templateID {
	case gensbe.SessionEventSbeTemplateID, gensbe.NewLeaderEventSbeTemplateID:
		poller.egressImage = imageFromHeader(header)
	}
	poller.pollComplete = true
}
