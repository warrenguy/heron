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

// IngressChannelPassthrough reports media that do not receive endpoint substitution.
//
// ipc and ipcx are passthrough media; Java MemberIngress only mutates UDP channels.
func IngressChannelPassthrough(channel string) bool {
	media, ok := ingressChannelMedia(channel)
	return ok && ingressPassthroughMedia(media)
}

// IngressChannelIsUDP reports UDP ingress (including aeron-spy:aeron:udp?...).
func IngressChannelIsUDP(channel string) bool {
	return ingressChannelMediaIs(channel, "udp")
}

// IngressChannelHasEndpointParam reports whether the ingress channel URI
// contains an endpoint= parameter at a query boundary (? or |).
//
// Matches BuildIngressChannel substitution detection. Distinct from the
// narrower query-only check in validateClusterIngressEndpoints when
// IngressEndpoints is configured (that rule rejects only ?endpoint=, not
// piped |endpoint=). Use this helper for CLI validation and documentation;
// do not assume it mirrors Validate() behavior for map-based ingress.
func IngressChannelHasEndpointParam(channel string) bool {
	return aeron.ChannelEndpointParamIndex(channel) >= 0
}

// ingressChannelQueryHasEndpointParam reports whether the ingress channel URI
// contains endpoint= immediately after ?. Piped |endpoint= is not matched.
//
// Used by validateClusterIngressEndpoints when IngressEndpoints is configured.
// Distinct from aeron.ChannelEndpointParamIndex, which also matches piped
// |endpoint= for BuildIngressChannel substitution.
func ingressChannelQueryHasEndpointParam(channel string) bool {
	return aeron.ChannelParamIndex(channel, aeron.ChannelEndpointParam, false) >= 0
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
	if !IngressChannelIsUDP(ingressChannel) {
		return fmt.Errorf("cluster client: invalid ingress channel: %s", ingressChannel)
	}
	return nil
}

func validateIngressChannelForMemberEndpoints(ingressChannel string) error {
	if IngressChannelPassthrough(ingressChannel) {
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
	if IngressChannelPassthrough(ingressChannel) {
		return ingressChannel, nil
	}
	if err := validateIngressUDPChannel(ingressChannel); err != nil {
		return "", err
	}
	if err := validateIngressMemberEndpoint(memberEndpoint); err != nil {
		return "", err
	}
	idx := aeron.ChannelEndpointParamIndex(ingressChannel)
	if idx >= 0 {
		return aeron.ReplaceChannelParamValue(
			ingressChannel, idx, aeron.ChannelEndpointParam, memberEndpoint,
		), nil
	}
	return aeron.AppendChannelParam(ingressChannel, aeron.ChannelEndpointParam, memberEndpoint), nil
}
