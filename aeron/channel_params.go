package aeron

import "strings"

// ChannelEndpointParam is the endpoint= URI param prefix.
const ChannelEndpointParam = "endpoint="

// ChannelSessionIDParam is the session-id= URI param prefix.
const ChannelSessionIDParam = "session-id="

// ChannelRejoinParam is the rejoin= URI param prefix.
const ChannelRejoinParam = "rejoin="

// ChannelControlModeParam is the control-mode= URI param prefix.
const ChannelControlModeParam = "control-mode="

// ChannelControlModeResponse is the control-mode value for response channels.
const ChannelControlModeResponse = "response"

// ChannelResponseCorrelationIDParam is the response-correlation-id= URI param
// prefix.
const ChannelResponseCorrelationIDParam = "response-correlation-id="

// ChannelParamIndex returns the index of param in channel, or -1 when absent.
// A match counts only after '?'. With acceptPipeBoundary, a match after '|'
// also counts.
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

// ChannelEndpointParamIndex returns the index of the endpoint= param in
// channel, or -1 when absent. A match after '?' or '|' counts.
func ChannelEndpointParamIndex(channel string) int {
	return ChannelParamIndex(channel, ChannelEndpointParam, true)
}

// ChannelParamValue returns the value of param at paramIdx in channel. The
// value ends at the next '|' or at the end of channel. paramIdx must come from
// ChannelParamIndex for the same param.
func ChannelParamValue(channel string, paramIdx int, param string) string {
	start := paramIdx + len(param)
	rest := channel[start:]
	if before, _, ok := strings.Cut(rest, "|"); ok {
		return before
	}
	return rest
}

// ReplaceChannelParamValue returns channel with the value of param at paramIdx
// set to value. paramIdx must come from ChannelParamIndex for the same param.
func ReplaceChannelParamValue(channel string, paramIdx int, param, value string) string {
	start := paramIdx + len(param)
	rest := channel[start:]
	if pipe := strings.IndexByte(rest, '|'); pipe >= 0 {
		return channel[:start] + value + rest[pipe:]
	}
	return channel[:start] + value
}

// AppendChannelParam returns channel with param and value appended. It adds
// '?' when channel has no query, and '|' when the query already holds a param.
func AppendChannelParam(channel, param, value string) string {
	if strings.HasSuffix(channel, "?") {
		return channel + param + value
	}
	if strings.Contains(channel, "?") {
		return channel + "|" + param + value
	}
	return channel + "?" + param + value
}

// ChannelEndpointNeedsWildcardPortResolution reports whether the endpoint=
// value in channel ends in ":0". The media driver picks the port for such an
// endpoint.
func ChannelEndpointNeedsWildcardPortResolution(channel string) bool {
	paramIdx := ChannelEndpointParamIndex(channel)
	if paramIdx < 0 {
		return false
	}
	return strings.HasSuffix(ChannelParamValue(channel, paramIdx, ChannelEndpointParam), ":0")
}

// IsControlModeResponse reports whether channel sets control-mode=response.
func IsControlModeResponse(channel string) bool {
	paramIdx := ChannelParamIndex(channel, ChannelControlModeParam, true)
	if paramIdx < 0 {
		return false
	}
	return ChannelParamValue(channel, paramIdx, ChannelControlModeParam) ==
		ChannelControlModeResponse
}

// ReplaceChannelEndpoint returns channel with the endpoint= value set to
// endpoint. It returns channel unchanged when channel has no endpoint= param.
func ReplaceChannelEndpoint(channel, endpoint string) string {
	paramIdx := ChannelEndpointParamIndex(channel)
	if paramIdx < 0 {
		return channel
	}
	return ReplaceChannelParamValue(channel, paramIdx, ChannelEndpointParam, endpoint)
}

// SetChannelParam returns channel with param set to value. It appends the
// param when absent and replaces the value when present.
func SetChannelParam(channel, param, value string) string {
	paramIdx := ChannelParamIndex(channel, param, true)
	if paramIdx < 0 {
		return AppendChannelParam(channel, param, value)
	}
	return ReplaceChannelParamValue(channel, paramIdx, param, value)
}
