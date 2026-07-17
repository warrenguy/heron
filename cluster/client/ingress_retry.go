package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/idlestrategy"
)

var (
	errNilIngressAeronClient = errors.New("cluster client: nil aeron client")
	errNilIngressSession     = errors.New("cluster client: nil session")
	errNilIngressOfferFn     = errors.New("cluster client: nil offer function")
)

// IngressTerminalError reports a non-retriable ingress offer result from a retry loop.
type IngressTerminalError struct {
	Status int64
}

func (err *IngressTerminalError) Error() string {
	return fmt.Sprintf(
		"cluster client: ingress offer failed: status=%s",
		aeron.PositionCodeName(err.Status),
	)
}

// IngressTimeoutError reports ingress retry deadline expiry after retriable codes.
type IngressTimeoutError struct {
	Status  int64
	Timeout time.Duration
}

func (err *IngressTimeoutError) Error() string {
	return fmt.Sprintf(
		"cluster client: ingress offer timed out within %s: status=%s",
		err.Timeout,
		aeron.PositionCodeName(err.Status),
	)
}

// IngressOfferConfig holds retry pacing for IngressOfferUntilPositive.
type IngressOfferConfig struct {
	// IdleStrategy paces BackPressured retries. Nil uses session.Context().IdleStrategy
	// when set; if that is also nil, NewDefaultIdleStrategy(). Requires a non-nil
	// session. Use one instance across connect, SpinOnce, and ingress retry.
	// idle.Reset() runs on all exit paths so a shared strategy is not left in PARKING.
	// For fixed-sleep pacing, pass idlestrategy.NewSleeping(d).
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses session.nanoClock(), which defaults to wall-clock
	// time.Now().UnixNano(). The clock must advance for IngressRetryTimeout to
	// bound retries; a frozen clock causes unbounded retry.
	NanoClock func() int64
}

func ingressOfferIdleStrategy(
	cfg IngressOfferConfig,
	session *AeronCluster,
) idlestrategy.IdleStrategy {
	if cfg.IdleStrategy != nil {
		return cfg.IdleStrategy
	}
	if idle := session.clusterCtx.IdleStrategy; idle != nil {
		return idle
	}
	return NewDefaultIdleStrategy()
}

func ingressOfferUntilPositive(
	ctx context.Context,
	session *AeronCluster,
	cfg IngressOfferConfig,
	offerFn func() int64,
) (int64, error) {
	switch {
	case session == nil:
		return 0, errNilIngressSession
	case offerFn == nil:
		return 0, errNilIngressOfferFn
	}
	aeronClient := session.Aeron()
	if aeronClient == nil {
		return 0, errNilIngressAeronClient
	}
	idle := ingressOfferIdleStrategy(cfg, session)
	defer idle.Reset()
	clock := cfg.NanoClock
	if clock == nil {
		clock = session.nanoClock
	}
	startNs := clock()
	deadlineNs, bound := aeron.RetryDeadlineNs(ctx, startNs, session.clusterCtx.IngressRetryTimeout)
	doTransportWork := func() {
		aeronClient.DoWork()
		recordIngressRetryDoWork()
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
			return 0, aeron.PreferContextError(ctx, &IngressTerminalError{Status: pos})
		}
		session.PollEgress()
		if pos == aeron.BackPressured {
			doTransportWork()
			idle.Idle(0)
		}
		if clock() >= deadlineNs {
			return 0, aeron.PreferContextError(ctx, &IngressTimeoutError{Status: pos, Timeout: bound})
		}
	}
}

// IngressOfferUntilPositive retries offerFn on aeron.IsIngressRetriable until success,
// timeout, or a terminal publication code.
//
// Contract:
//
//   - Calls session.Aeron().DoWork() before each offerFn invocation.
//
//   - On retriable failure, calls PollEgress(). On BackPressured, calls DoWork()
//     again, then paces via cfg.IdleStrategy.Idle(0). AdminAction retries
//     immediately on the next iteration (no pacing), matching Publication offer
//     semantics.
//
//   - Calls idle.Reset() on all exit paths (success, terminal, timeout). Nil
//     cfg.IdleStrategy uses session context idle when set; otherwise default.
//
//   - Retry aborts on any non-retriable code, including session-guard NotConnected
//     returned after PollEgress advances recovery or egress teardown mutates state.
//
//   - Concurrent ingress may return AdminAction on fragmented Offer; a mid-fragment
//     AdminAction can commit an orphan BEGIN frame before padding and a successful
//     full retry. See github.com/warrenguy/heron/test/integration/harness.OfferWithRetry
//     godoc for the publication-level caveat.
//
//   - offerFn must use app-guarded APIs (Offer, SendKeepAlive, SendSnapshotRequest).
//     Do not wrap recovery-bypass paths such as offerSessionKeepAlive. Single-shot
//     methods are not modified.
//
// Returns stream position (>0) and nil error on success. On failure returns 0 and a
// non-nil error: IngressTerminalError, IngressTimeoutError, ctx.Err(), or a nil-argument
// error. Returns ctx.Err() when the context is done before success (context.Canceled or
// context.DeadlineExceeded). IngressTerminalError and IngressTimeoutError apply only
// while the context remains active. Use single-shot Offer or SendKeepAlive when the
// Aeron status code itself is the contract.
//
// Uses ClusterContext.IngressRetryTimeout. When ctx carries a deadline, the effective
// bound is the earlier of IngressRetryTimeout and ctx.Deadline().
//
// Invoker-only: this helper drives DoWork on behalf of the caller for the duration
// of the retry loop only. The application still owns the outer duty cycle
// (PollEgress between business logic, idle keep-alive timer, etc.).
//
// Not zero-alloc gated: intended for idle keep-alive and integration-style sends,
// not per-message hot paths. Single-shot Offer/SendKeepAlive remain zero-alloc clean.
func (ac *AeronCluster) IngressOfferUntilPositive(
	ctx context.Context,
	cfg IngressOfferConfig,
	offerFn func() int64,
) (int64, error) {
	if ac == nil {
		return 0, errNilIngressSession
	}
	return ingressOfferUntilPositive(ctx, ac, cfg, offerFn)
}

// SendKeepAliveUntilPositive retries SendKeepAlive via IngressOfferUntilPositive.
// Inherits that method's contract. Intended for idle keep-alive duty cycles, not
// per-message hot paths.
func (ac *AeronCluster) SendKeepAliveUntilPositive(
	ctx context.Context,
	cfg IngressOfferConfig,
) (int64, error) {
	return ac.IngressOfferUntilPositive(ctx, cfg, ac.SendKeepAlive)
}
