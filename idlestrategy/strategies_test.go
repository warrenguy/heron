package idlestrategy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/idlestrategy"
)

func TestSleeping(t *testing.T) {
	// time.Sleep waits at least its argument, so a lower bound cannot fail on a
	// slow machine. No upper bound: that limit is schedule-dependent.
	//
	// The hour-long cases must not sleep. A missed skip hits the test timeout.
	tests := []struct {
		name        string
		strategy    idlestrategy.IdleStrategy
		workCount   int
		minDuration time.Duration
	}{
		{
			name:        "positive_period",
			strategy:    idlestrategy.NewSleeping(5 * time.Millisecond),
			minDuration: 5 * time.Millisecond,
		},
		{
			name:        "default_millis",
			strategy:    idlestrategy.DefaultSleepingMillis(),
			minDuration: time.Millisecond,
		},
		{
			name:     "negative_period_skips_sleep",
			strategy: idlestrategy.NewSleeping(-time.Hour),
		},
		{
			name:      "work_skips_sleep",
			strategy:  idlestrategy.NewSleeping(time.Hour),
			workCount: 1,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			start := time.Now()
			testCase.strategy.Idle(testCase.workCount)
			require.GreaterOrEqual(t, time.Since(start), testCase.minDuration)
		})
	}
}
