package client

import (
	"errors"
	"fmt"
	"strings"

	"github.com/warrenguy/heron/aeron"
)

const (
	aeronSchemePrefix = "aeron:"
	aeronSpyPrefix    = "aeron-spy:"
)

// ingressChannelMedia strips aeron-spy: and returns the media token (e.g. "ipc",
// "ipcx", "udp").
func ingressChannelMedia(channel string) (media string, ok bool) {
	channel = strings.TrimPrefix(channel, aeronSpyPrefix)
	if !strings.HasPrefix(channel, aeronSchemePrefix) {
		return "", false
	}
	media, _, _ = strings.Cut(channel[len(aeronSchemePrefix):], "?")
	if media == "" {
		return "", false
	}
	return media, true
}

func ingressChannelMediaIs(channel, want string) bool {
	media, ok := ingressChannelMedia(channel)
	return ok && media == want
}

func ingressPassthroughMedia(media string) bool {
	return media == "ipc" || media == "ipcx"
}

// ingressChannelPassthrough reports media that do not receive endpoint substitution.
//
// ipc and ipcx are passthrough media; Java MemberIngress only mutates UDP channels.
func ingressChannelPassthrough(channel string) bool {
	media, ok := ingressChannelMedia(channel)
	return ok && ingressPassthroughMedia(media)
}

// ingressChannelIsUDP reports UDP ingress (including aeron-spy:aeron:udp?...).
func ingressChannelIsUDP(channel string) bool {
	return ingressChannelMediaIs(channel, "udp")
}

// ingressChannelQueryEndpointParamIndex returns the index of endpoint= immediately
// after ?, or -1 when absent. Piped |endpoint= is not matched.
func ingressChannelQueryEndpointParamIndex(channel string) int {
	return aeron.ChannelParamIndex(channel, aeron.ChannelEndpointParam, false)
}

// ingressChannelEndpointParamIndex returns the index of endpoint= at a query param
// boundary (immediately after ? or |), or -1 when absent.
func ingressChannelEndpointParamIndex(channel string) int {
	return aeron.ChannelParamIndex(channel, aeron.ChannelEndpointParam, true)
}

func validateIngressMemberEndpoint(memberEndpoint string) error {
	if memberEndpoint == "" {
		return errors.New("cluster client: ingress member endpoint is empty")
	}
	if strings.ContainsAny(memberEndpoint, "|?=") {
		return fmt.Errorf(
			"cluster client: invalid ingress member endpoint: %s",
			memberEndpoint,
		)
	}
	return nil
}

func validateIngressUDPChannel(ingressChannel string) error {
	if !ingressChannelIsUDP(ingressChannel) {
		return fmt.Errorf("cluster client: invalid ingress channel: %s", ingressChannel)
	}
	return nil
}

func validateIngressChannelForMemberEndpoints(ingressChannel string) error {
	if ingressChannelPassthrough(ingressChannel) {
		return nil
	}
	return validateIngressUDPChannel(ingressChannel)
}

// BuildIngressChannel returns the Aeron channel URI for offering to memberEndpoint.
//
// Passthrough media (ipc, ipcx) return unchanged. UDP channels get endpoint=
// substituted or appended. Unknown or malformed aeron: URIs return cluster client:
// invalid ingress channel.
func BuildIngressChannel(ingressChannel, memberEndpoint string) (string, error) {
	if ingressChannelPassthrough(ingressChannel) {
		return ingressChannel, nil
	}
	if err := validateIngressUDPChannel(ingressChannel); err != nil {
		return "", err
	}
	if err := validateIngressMemberEndpoint(memberEndpoint); err != nil {
		return "", err
	}
	idx := ingressChannelEndpointParamIndex(ingressChannel)
	if idx >= 0 {
		return aeron.ReplaceChannelParamValue(
			ingressChannel, idx, aeron.ChannelEndpointParam, memberEndpoint,
		), nil
	}
	return aeron.AppendChannelParam(ingressChannel, aeron.ChannelEndpointParam, memberEndpoint), nil
}
