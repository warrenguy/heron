package client

import (
	"errors"
	"fmt"
	"strings"
)

const rejoinParam = "rejoin="

// applyUDPRejoinFalse returns egressChannel with rejoin=false on UDP media.
//
// Not wired into NewAsyncConnect while validateClusterEgressChannel rejects UDP
// egress. Reserved for future UDP response-channel support (Java parity). Idempotent
// when rejoin=false is already set. IPC, ipcx, and other valid aeron: media pass
// through unchanged. Malformed aeron: URIs return cluster client: invalid egress
// channel.
func applyUDPRejoinFalse(egressChannel string) (string, error) {
	media, ok := ingressChannelMedia(egressChannel)
	if !ok {
		return "", fmt.Errorf("cluster client: invalid egress channel: %s", egressChannel)
	}
	if media != "udp" {
		return egressChannel, nil
	}
	if idx := channelParamIndex(egressChannel, rejoinParam, true); idx >= 0 {
		return replaceChannelParamValue(egressChannel, idx, rejoinParam, "false"), nil
	}
	return appendChannelParam(egressChannel, rejoinParam, "false"), nil
}

// validateClusterEgressChannel rejects egress media that cannot supply a wire
// response channel (ipc and ipcx only).
func validateClusterEgressChannel(egressChannel string) error {
	if ingressChannelPassthrough(egressChannel) {
		return nil
	}
	media, ok := ingressChannelMedia(egressChannel)
	if !ok {
		return fmt.Errorf("cluster client: invalid egress channel: %s", egressChannel)
	}
	if media == "udp" {
		return errors.New(
			"cluster client: UDP egress response channel not supported; use ipc or ipcx",
		)
	}
	return fmt.Errorf(
		"cluster client: %s egress response channel not supported; use ipc or ipcx",
		media,
	)
}

type responseChannelSubscription interface {
	Channel() string
}

// resolveResponseChannel returns the wire response channel URI for SessionConnectRequest.
//
// Returns the normalized aeron:ipc/ipcx URI (spy prefix stripped) when sub is non-nil and
// the channel is passthrough; false when sub is nil or the channel is not passthrough
// (e.g. UDP). Egress images are not required because the cluster creates the response
// publication after connect (Java tryResolveChannelEndpointPort parity). Ingress
// publication connectivity is gated separately in pollAwaitPublicationConnected.
func resolveResponseChannel(sub responseChannelSubscription) (string, bool) {
	if sub == nil {
		return "", false
	}
	channel := sub.Channel()
	if !ingressChannelPassthrough(channel) {
		return "", false
	}
	return strings.TrimPrefix(channel, aeronSpyPrefix), true
}
