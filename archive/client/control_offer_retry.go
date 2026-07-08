package client

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

var (
	errNilControlAeronClient = errors.New("archive client: nil aeron client")
	errNilControlOfferFn     = errors.New("archive client: nil offer function")
)

// ControlOfferConfig holds retry pacing for ControlOfferUntilPositive.
type ControlOfferConfig struct {
	// IdleStrategy paces BackPressured retries. Nil uses NewDefaultIdleStrategy().
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses time.Now().UnixNano(). The clock must advance for
	// ControlOfferRetryTimeout to bound retries; a frozen clock causes unbounded
	// retry. The head ctx.Err() check bounds frozen-clock spins only for
	// cancelable contexts.
	NanoClock func() int64

	// PollFn optionally polls the control response subscription during retriable
	// offer failure (for example poller.Poll). Nil skips response polling.
	PollFn func() int
}

// ControlOfferUntilPositive retries offerFn on aeron.IsIngressRetriable until success,
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
//   - Returns stream position (>0) and nil error on success. On failure returns 0 and a
//     non-nil error: ControlTerminalError, ControlOfferTimeoutError, ctx.Err(), or a
//     nil-argument error.
//
// Uses ArchiveContext.ControlOfferRetryTimeout. When ctx carries a deadline, the
// effective bound is the earlier of ControlOfferRetryTimeout and ctx.Deadline().
//
// Invoker-only: this helper drives DoWork on behalf of the caller for the duration
// of the retry loop only. Archive control request and response share one driver
// client; polling the response subscription during offer retry unblocks publication
// backpressure when the archive replies on the response stream.
func ControlOfferUntilPositive(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	cfg ControlOfferConfig,
	offerFn func() int64,
) (int64, error) {
	switch {
	case aeronClient == nil:
		return 0, errNilControlAeronClient
	case offerFn == nil:
		return 0, errNilControlOfferFn
	}
	timeout := archiveCtx.ControlOfferRetryTimeout
	if timeout == 0 {
		timeout = DefaultControlOfferRetryTimeout
	}
	idle := cfg.IdleStrategy
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	clock := resolveNanoClock(cfg.NanoClock)
	startNs := clock()
	deadlineNs, bound := aeron.RetryDeadlineNs(ctx, startNs, timeout)
	defer idle.Reset()
	doTransportWork := func() {
		aeronClient.DoWork()
		recordControlOfferRetryDoWork()
	}
	var pos int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		doTransportWork()
		pos = offerFn()
		if pos > 0 {
			return pos, nil
		}
		if !aeron.IsIngressRetriable(pos) {
			return 0, aeron.PreferContextError(ctx, &ControlTerminalError{Status: pos})
		}
		if cfg.PollFn != nil {
			cfg.PollFn()
		}
		if pos == aeron.BackPressured {
			doTransportWork()
			idle.Idle(0)
		}
		if clock() >= deadlineNs {
			return 0, aeron.PreferContextError(ctx, &ControlOfferTimeoutError{Status: pos, Timeout: bound})
		}
	}
}
