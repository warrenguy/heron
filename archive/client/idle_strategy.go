package client

import (
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

// NewDefaultIdleStrategy returns BackoffIdleStrategy(1, 10, 1000ns, 1000ns) matching
// Java Aeron Archive.Context defaults.
func NewDefaultIdleStrategy() idlestrategy.IdleStrategy {
	park := idlestrategy.DefaultParkPeriod
	return idlestrategy.NewBackoff(1, 10, park, park)
}
