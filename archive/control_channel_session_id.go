package archive

import (
	"strconv"

	"github.com/warrenguy/heron/aeron"
)

// usesControlChannelSessionID reports whether connect puts session-id on the
// control channels. A response channel with control-mode=response opts out
// (Java parity).
func usesControlChannelSessionID(cfg Config) bool {
	return !aeron.IsControlModeResponse(cfg.effectiveControlResponseChannel)
}

// applyControlChannelSessionID puts session-id=sessionID on both effective
// control channels. It replaces a session-id the caller set.
func applyControlChannelSessionID(cfg Config, sessionID int32) Config {
	value := strconv.FormatInt(int64(sessionID), 10)
	cfg.effectiveControlRequestChannel = aeron.SetChannelParam(
		cfg.effectiveControlRequestChannel,
		aeron.ChannelSessionIDParam,
		value,
	)
	cfg.effectiveControlResponseChannel = aeron.SetChannelParam(
		cfg.effectiveControlResponseChannel,
		aeron.ChannelSessionIDParam,
		value,
	)
	return cfg
}
