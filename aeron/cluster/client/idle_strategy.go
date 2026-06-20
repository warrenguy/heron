package client

import (
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

// NewDefaultIdleStrategy returns BackoffIdleStrategy(1, 10, 1000ns, 1000ns) matching
// Java AeronCluster.Context.conclude() defaults. With those defaults, the 13th
// consecutive Idle(0) enters PARKING, but the first time.Sleep happens on the
// 14th call; entering PARKING only transitions state, matching Agrona.
func NewDefaultIdleStrategy() idlestrategy.IdleStrategy {
	park := idlestrategy.DefaultParkPeriod
	return idlestrategy.NewBackoff(1, 10, park, park)
}
