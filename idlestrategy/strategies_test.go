package idlestrategy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/idlestrategy"
)

func TestBusySpin_idleDoesNotPanic(t *testing.T) {
	require.NotPanics(t, func() {
		for range 8 {
			idlestrategy.BusySpin.Idle(0)
		}
	})
}

func TestYielding_idleDoesNotPanic(t *testing.T) {
	require.NotPanics(t, func() {
		idlestrategy.Yielding.Idle(0)
	})
}

func TestSleeping(t *testing.T) {
	// time.Sleep waits at least its argument, so a lower bound cannot fail on a
	// slow machine. No upper bound: that limit is schedule-dependent.
	tests := []struct {
		name        string
		strategy    idlestrategy.IdleStrategy
		minDuration time.Duration
	}{
		{
			name:     "zero_period",
			strategy: idlestrategy.NewSleeping(0),
		},
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
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			start := time.Now()
			testCase.strategy.Idle(0)
			require.GreaterOrEqual(t, time.Since(start), testCase.minDuration)
		})
	}
}

func TestStatelessStrategies_resetNoOp(t *testing.T) {
	strategies := []idlestrategy.IdleStrategy{
		idlestrategy.NoOp,
		idlestrategy.BusySpin,
		idlestrategy.Yielding,
		idlestrategy.DefaultSleeping(),
		idlestrategy.DefaultSleepingMillis(),
	}

	for _, strategy := range strategies {
		require.NotPanics(t, func() {
			strategy.Reset()
			strategy.Idle(0)
		})
	}
}
