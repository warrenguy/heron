package client

import (
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

// DefaultIngressOfferTimeout bounds ingress retry loops when IngressOfferConfig.Timeout
// is zero or negative. 10s covers leader election plus ingress repoint during failover.
const DefaultIngressOfferTimeout = 10 * time.Second

// IngressRetriableCode is an alias for aeron.IsIngressRetriable.
func IngressRetriableCode(position int64) bool {
	return aeron.IsIngressRetriable(position)
}

// IngressOfferConfig holds retry pacing for IngressOfferUntilPositive.
type IngressOfferConfig struct {
	// Timeout bounds the retry loop; zero or negative uses DefaultIngressOfferTimeout.
	Timeout time.Duration

	// IdleStrategy paces BackPressured retries. Nil uses NewDefaultIdleStrategy().
	// Prefer a dedicated instance per retry loop; idle.Reset() runs on all exit
	// paths so a shared strategy is not left in PARKING. For fixed-sleep pacing,
	// pass idlestrategy.NewSleeping(d).
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses session.nanoClock(), which defaults to wall-clock
	// time.Now().UnixNano(). The clock must advance for Timeout to bound
	// retries; a frozen clock causes unbounded retry.
	NanoClock func() int64
}

// IngressOfferUntilPositive retries offerFn on IngressRetriableCode until success,
// timeout, or a terminal publication code.
//
// Contract:
//
//   - Requires non-nil aeronClient and session.
//
//   - Calls aeronClient.DoWork() before each offerFn invocation.
//
//   - On retriable failure, calls session.PollEgress(). On BackPressured, calls
//     aeronClient.DoWork() again, then paces via cfg.IdleStrategy.Idle(0).
//     AdminAction retries immediately on the next iteration (no pacing),
//     matching Publication offer semantics.
//
//   - Calls idle.Reset() on all exit paths (success, terminal, timeout). When
//     cfg.IdleStrategy is nil, the default idle strategy is still reset on exit
//     so a shared strategy is not left in PARKING.
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
//   - Returns stream position (>0) on success. Returns terminal codes immediately.
//     On timeout after retriable codes, returns the last retriable status.
//
// Invoker-only: this helper drives DoWork on behalf of the caller for the duration
// of the retry loop only. The application still owns the outer duty cycle
// (PollEgress between business logic, idle keep-alive timer, etc.).
//
// Not zero-alloc gated: intended for idle keep-alive and integration-style sends,
// not per-message hot paths. Single-shot Offer/SendKeepAlive remain zero-alloc clean.
func IngressOfferUntilPositive(
	aeronClient *aeron.Client,
	session *AeronCluster,
	cfg IngressOfferConfig,
	offerFn func() int64,
) int64 {
	if aeronClient == nil || session == nil || offerFn == nil {
		return aeron.NotConnected
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultIngressOfferTimeout
	}
	idle := cfg.IdleStrategy
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	clock := cfg.NanoClock
	if clock == nil {
		clock = session.nanoClock
	}
	deadlineNs := clock() + timeout.Nanoseconds()
	defer idle.Reset()
	doTransportWork := func() {
		aeronClient.DoWork()
		recordIngressRetryDoWork()
	}
	var pos int64
	for {
		doTransportWork()
		pos = offerFn()
		if pos > 0 || !IngressRetriableCode(pos) {
			return pos
		}
		session.PollEgress()
		if pos == aeron.BackPressured {
			doTransportWork()
			idle.Idle(0)
		}
		if clock() >= deadlineNs {
			return pos
		}
	}
}

// SendKeepAliveUntilPositive retries SendKeepAlive via IngressOfferUntilPositive.
//
// Inherits the full IngressOfferUntilPositive contract: requires non-nil
// aeronClient and session, drives DoWork and PollEgress during retry, paces
// BackPressured via IdleStrategy, and returns immediately on terminal codes
// (Closed, NotConnected, MaxPositionExceeded, UnsupportedMessageLength).
//
// Intended for idle keep-alive duty cycles, not per-message hot paths.
func SendKeepAliveUntilPositive(
	aeronClient *aeron.Client,
	session *AeronCluster,
	cfg IngressOfferConfig,
) int64 {
	return IngressOfferUntilPositive(aeronClient, session, cfg, session.SendKeepAlive)
}
