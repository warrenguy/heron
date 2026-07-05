package client

import (
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

// DefaultControlOfferTimeout bounds control offer retry loops when
// ControlOfferConfig.Timeout is zero or negative.
const DefaultControlOfferTimeout = 10 * time.Second

// ControlRetriableCode is an alias for aeron.IsIngressRetriable.
func ControlRetriableCode(position int64) bool {
	return aeron.IsIngressRetriable(position)
}

// ControlOfferConfig holds retry pacing for ControlOfferUntilPositive.
type ControlOfferConfig struct {
	// Timeout bounds the retry loop; zero or negative uses DefaultControlOfferTimeout.
	Timeout time.Duration

	// IdleStrategy paces BackPressured retries. Nil uses NewDefaultIdleStrategy().
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses time.Now().UnixNano().
	NanoClock func() int64

	// PollFn optionally polls the control response subscription during retriable
	// offer failure (for example poller.Poll). Nil skips response polling.
	PollFn func() int
}

// ControlOfferUntilPositive retries offerFn on ControlRetriableCode until success,
// timeout, or a terminal publication code.
//
// Contract:
//
//   - Requires non-nil aeronClient and offerFn.
//
//   - Calls aeronClient.DoWork() before each offerFn invocation.
//
//   - On retriable failure, calls cfg.PollFn when set. On BackPressured, calls
//     aeronClient.DoWork() again, then paces via cfg.IdleStrategy.Idle(0).
//     AdminAction retries immediately on the next iteration (no pacing).
//
//   - Calls idle.Reset() on all exit paths (success, terminal, timeout).
//
//   - Returns stream position (>0) on success. Returns terminal codes immediately.
//     On timeout after retriable codes, returns the last retriable status.
//
// Invoker-only: this helper drives DoWork on behalf of the caller for the duration
// of the retry loop only. Archive control request and response share one driver
// client; polling the response subscription during offer retry unblocks publication
// backpressure when the archive replies on the response stream.
func ControlOfferUntilPositive(
	aeronClient *aeron.Client,
	cfg ControlOfferConfig,
	offerFn func() int64,
) int64 {
	if aeronClient == nil || offerFn == nil {
		return aeron.NotConnected
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultControlOfferTimeout
	}
	idle := cfg.IdleStrategy
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	clock := cfg.NanoClock
	if clock == nil {
		clock = func() int64 { return time.Now().UnixNano() }
	}
	deadlineNs := clock() + timeout.Nanoseconds()
	defer idle.Reset()
	doTransportWork := func() {
		aeronClient.DoWork()
		recordControlOfferRetryDoWork()
	}
	var pos int64
	for {
		doTransportWork()
		pos = offerFn()
		if pos > 0 || !ControlRetriableCode(pos) {
			return pos
		}
		if cfg.PollFn != nil {
			cfg.PollFn()
		}
		if pos == aeron.BackPressured {
			doTransportWork()
			idle.Idle(0)
		}
		if clock() >= deadlineNs {
			return pos
		}
	}
}
