package client

import "github.com/warrenguy/heron/idlestrategy"

// NewDefaultIdleStrategy returns the shared archive/cluster default backoff idle strategy.
func NewDefaultIdleStrategy() idlestrategy.IdleStrategy {
	return idlestrategy.NewDefaultBackoff()
}
