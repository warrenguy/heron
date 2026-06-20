package client

import (
	"fmt"
	"strings"
)

const (
	ingressEndpointParam = "endpoint="
	aeronSchemePrefix    = "aeron:"
	aeronSpyPrefix       = "aeron-spy:"
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

func ingressEndpointParamIndex(channel string, acceptPipeBoundary bool) int {
	searchStart := 0
	for searchStart < len(channel) {
		rel := strings.Index(channel[searchStart:], ingressEndpointParam)
		if rel < 0 {
			return -1
		}
		idx := searchStart + rel
		if idx > 0 {
			prev := channel[idx-1]
			if prev == '?' || (acceptPipeBoundary && prev == '|') {
				return idx
			}
		}
		searchStart = idx + 1
	}
	return -1
}

// ingressChannelQueryEndpointParamIndex returns the index of endpoint= immediately
// after ?, or -1 when absent. Piped |endpoint= is not matched.
func ingressChannelQueryEndpointParamIndex(channel string) int {
	return ingressEndpointParamIndex(channel, false)
}

// ingressChannelEndpointParamIndex returns the index of endpoint= at a query param
// boundary (immediately after ? or |), or -1 when absent.
func ingressChannelEndpointParamIndex(channel string) int {
	return ingressEndpointParamIndex(channel, true)
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
	media, ok := ingressChannelMedia(ingressChannel)
	if !ok || media != "udp" {
		return "", fmt.Errorf("cluster client: invalid ingress channel: %s", ingressChannel)
	}
	idx := ingressChannelEndpointParamIndex(ingressChannel)
	if idx >= 0 {
		start := idx + len(ingressEndpointParam)
		rest := ingressChannel[start:]
		if pipe := strings.IndexByte(rest, '|'); pipe >= 0 {
			return ingressChannel[:start] + memberEndpoint + rest[pipe:], nil
		}
		return ingressChannel[:start] + memberEndpoint, nil
	}
	if strings.HasSuffix(ingressChannel, "?") {
		return ingressChannel + ingressEndpointParam + memberEndpoint, nil
	}
	if strings.Contains(ingressChannel, "?") {
		return ingressChannel + "|" + ingressEndpointParam + memberEndpoint, nil
	}
	return ingressChannel + "?" + ingressEndpointParam + memberEndpoint, nil
}
