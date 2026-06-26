// Channel URI param helpers for endpoint resolution and substitution.
//
// Shared by subscription wildcard resolution and cluster/client ingress/egress
// channel builders.
package aeron

import "strings"

const ChannelEndpointParam = "endpoint="

func ChannelParamIndex(channel, param string, acceptPipeBoundary bool) int {
	searchStart := 0
	for searchStart < len(channel) {
		rel := strings.Index(channel[searchStart:], param)
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

func ChannelEndpointParamIndex(channel string) int {
	return ChannelParamIndex(channel, ChannelEndpointParam, true)
}

func ChannelParamValue(channel string, paramIdx int, param string) string {
	start := paramIdx + len(param)
	rest := channel[start:]
	if before, _, ok := strings.Cut(rest, "|"); ok {
		return before
	}
	return rest
}

func ReplaceChannelParamValue(channel string, paramIdx int, param, value string) string {
	start := paramIdx + len(param)
	rest := channel[start:]
	if pipe := strings.IndexByte(rest, '|'); pipe >= 0 {
		return channel[:start] + value + rest[pipe:]
	}
	return channel[:start] + value
}

func AppendChannelParam(channel, param, value string) string {
	if strings.HasSuffix(channel, "?") {
		return channel + param + value
	}
	if strings.Contains(channel, "?") {
		return channel + "|" + param + value
	}
	return channel + "?" + param + value
}

func ChannelEndpointNeedsWildcardPortResolution(channel string) bool {
	paramIdx := ChannelEndpointParamIndex(channel)
	if paramIdx < 0 {
		return false
	}
	return strings.HasSuffix(ChannelParamValue(channel, paramIdx, ChannelEndpointParam), ":0")
}

func ReplaceChannelEndpoint(channel, endpoint string) string {
	paramIdx := ChannelEndpointParamIndex(channel)
	if paramIdx < 0 {
		return channel
	}
	return ReplaceChannelParamValue(channel, paramIdx, ChannelEndpointParam, endpoint)
}
