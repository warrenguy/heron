package archive

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/idlestrategy"
)

var (
	errNilSession            = errors.New("archive: nil session")
	errNilControlAeronClient = errors.New("archive: nil aeron client")
	errNilControlOfferFn     = errors.New("archive: nil offer function")
)

// ControlOfferConfig holds retry pacing for archive control offer retries.
type ControlOfferConfig struct {
	// IdleStrategy paces BackPressured retries. Nil uses Config.IdleStrategy
	// when set; if that is also nil, NewDefaultIdleStrategy().
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses Config.AeronConfig.NanoClock when set, else wall-clock
	// time.Now().UnixNano(). The clock must advance for ControlOfferRetryTimeout
	// to bound retries; a frozen clock causes unbounded retry. The head ctx.Err()
	// check bounds frozen-clock spins only for cancelable contexts.
	NanoClock func() int64

	// PollFn optionally polls the control response subscription during retriable
	// offer failure (for example poller.Poll). Nil skips response polling.
	PollFn func() int
}

// controlOfferUntilPositive retries offerFn on aeron.IsIngressRetriable until success,
// timeout, or a terminal publication code. It serves in-package callers without a
// session wrapper; post-connect callers use [Client.ControlOfferUntilPositive].
func controlOfferUntilPositive(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	offerCfg ControlOfferConfig,
	offerFn func() int64,
) (int64, error) {
	switch {
	case aeronClient == nil:
		return 0, errNilControlAeronClient
	case offerFn == nil:
		return 0, errNilControlOfferFn
	}
	timeout := cfg.ControlOfferRetryTimeout
	if timeout == 0 {
		timeout = DefaultControlOfferRetryTimeout
	}
	idle := offerCfg.IdleStrategy
	if idle == nil {
		idle = cfg.IdleStrategy
	}
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	clock := offerCfg.NanoClock
	if clock == nil {
		clock = cfg.AeronConfig.NanoClock
	}
	clock = resolveNanoClock(clock)
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
		if offerCfg.PollFn != nil {
			offerCfg.PollFn()
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

// ControlOfferUntilPositive retries offerFn on aeron.IsIngressRetriable until success,
// timeout, or a terminal publication code.
//
// Contract:
//
//   - Calls the session's Aeron client DoWork() before each offerFn invocation.
//
//   - On retriable failure, calls cfg.PollFn when set. On BackPressured, calls
//     DoWork() again, then paces via the idle strategy's Idle(0). AdminAction retries
//     immediately on the next iteration (no pacing).
//
//   - Calls idle.Reset() on all exit paths (success, terminal, timeout). Nil
//     cfg.IdleStrategy uses session Config.IdleStrategy when set; otherwise default.
//     Nil cfg.NanoClock uses session AeronConfig.NanoClock when set, else wall clock.
//
//   - Returns stream position (>0) and nil error on success. On failure returns 0 and a
//     non-nil error: ControlTerminalError, ControlOfferTimeoutError, ctx.Err(), or a
//     nil-argument error (nil session, nil offerFn, or nil client, including after
//     Close on an owned client).
//
// Uses [Config.ControlOfferRetryTimeout] from the connected session. When ctx
// carries a deadline, the effective bound is the earlier of ControlOfferRetryTimeout and
// ctx.Deadline().
//
// Invoker-only: this method drives DoWork on behalf of the caller for the duration
// of the retry loop only. Archive control request and response share one driver
// client; polling the response subscription during offer retry unblocks publication
// backpressure when the archive replies on the response stream.
//
// Example:
//
//	pos, err := archive.ControlOfferUntilPositive(ctx, ControlOfferConfig{
//	    PollFn: archive.Poll, // operation-specific
//	}, offerFn)
func (client *Client) ControlOfferUntilPositive(
	ctx context.Context,
	cfg ControlOfferConfig,
	offerFn func() int64,
) (int64, error) {
	if client == nil {
		return 0, errNilSession
	}
	return controlOfferUntilPositive(
		ctx,
		client.aeronClient,
		client.cfg,
		cfg,
		offerFn,
	)
}
