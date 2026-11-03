package idlestrategy

import (
	"testing"
	"time"
)

const (
	BackoffNotIdle  = int(backoffStateNotIdle)
	BackoffSpinning = int(backoffStateSpinning)
	BackoffYielding = int(backoffStateYielding)
	BackoffParking  = int(backoffStateParking)
)

func BackoffState(tb testing.TB, strategy IdleStrategy) int {
	tb.Helper()
	return int(backoffOf(tb, strategy).state)
}

func BackoffParkPeriod(tb testing.TB, strategy IdleStrategy) time.Duration {
	tb.Helper()
	return backoffOf(tb, strategy).parkPeriod
}

func backoffOf(tb testing.TB, strategy IdleStrategy) *backoffIdleStrategy {
	tb.Helper()
	backoff, ok := strategy.(*backoffIdleStrategy)
	if !ok {
		tb.Fatalf("strategy is %T, want *backoffIdleStrategy", strategy)
	}
	return backoff
}
