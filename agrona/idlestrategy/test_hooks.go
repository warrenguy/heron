//go:build testhooks

package idlestrategy

import (
	"testing"
	"time"
)

// Cross-package test hooks. Values mirror export_test.go but live here because
// export_test.go helpers are not visible to external test packages.
const (
	BackoffNotIdle  = int(backoffStateNotIdle)
	BackoffSpinning = int(backoffStateSpinning)
	BackoffYielding = int(backoffStateYielding)
	BackoffParking  = int(backoffStateParking)
)

func BackoffState(tb testing.TB, strategy IdleStrategy) int {
	tb.Helper()
	backoff, ok := strategy.(*backoffIdleStrategy)
	if !ok {
		tb.Fatalf("BackoffState: strategy is %T, want *backoffIdleStrategy", strategy)
	}
	return int(backoff.state)
}

func BackoffParkPeriod(tb testing.TB, strategy IdleStrategy) time.Duration {
	tb.Helper()
	backoff, ok := strategy.(*backoffIdleStrategy)
	if !ok {
		tb.Fatalf("BackoffParkPeriod: strategy is %T, want *backoffIdleStrategy", strategy)
	}
	return backoff.parkPeriod
}
