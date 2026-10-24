package client

import (
	"fmt"
	"strings"

	"github.com/warrenguy/heron/aeron"
)

// applyUDPRejoinFalse returns egressChannel with rejoin=false on UDP media.
//
// Called from concludeConfig after ValidateForConnect. Idempotent when
// rejoin=false is already set. IPC, ipcx, and other valid aeron: media pass
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
	return aeron.SetChannelParam(egressChannel, aeron.ChannelRejoinParam, "false"), nil
}

// validateClusterEgressChannel rejects egress media that cannot supply a wire
// response channel. Supported: ipc, ipcx, and udp.
func validateClusterEgressChannel(egressChannel string) error {
	media, ok := ingressChannelMedia(egressChannel)
	if !ok {
		return fmt.Errorf("cluster client: invalid egress channel: %s", egressChannel)
	}
	if ingressPassthroughMedia(media) || media == "udp" {
		return nil
	}
	return fmt.Errorf(
		"cluster client: %s egress response channel not supported; use ipc, ipcx, or udp",
		media,
	)
}

type responseChannelResolver interface {
	Channel() string
	TryResolveChannelEndpointPort() (string, bool)
}

// resolveResponseChannel returns the wire response channel URI for SessionConnectRequest.
//
// Returns the normalized aeron:ipc/ipcx URI (spy prefix stripped) or a resolved UDP
// endpoint when sub is non-nil. False when sub is nil or UDP wildcard endpoint=:0 is
// not yet active (Java tryResolveChannelEndpointPort parity). Egress images are not
// required because the cluster creates the response publication after connect. Ingress
// publication connectivity is gated separately in pollAwaitPublicationConnected.
func resolveResponseChannel(sub responseChannelResolver) (string, bool) {
	if sub == nil {
		return "", false
	}
	channel := sub.Channel()
	media, ok := ingressChannelMedia(channel)
	if !ok {
		return "", false
	}
	if ingressPassthroughMedia(media) {
		return strings.TrimPrefix(channel, aeronSpyPrefix), true
	}
	if media != "udp" {
		return "", false
	}
	resolved, ok := sub.TryResolveChannelEndpointPort()
	if !ok {
		return "", false
	}
	return strings.TrimPrefix(resolved, aeronSpyPrefix), true
}
