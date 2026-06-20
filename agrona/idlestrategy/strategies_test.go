package idlestrategy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/agrona/idlestrategy"
)

func TestNoOp_idleDoesNotBlock(t *testing.T) {
	start := time.Now()
	for range 1000 {
		idlestrategy.NoOp.Idle(0)
	}
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

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
	tests := []struct {
		name        string
		strategy    idlestrategy.IdleStrategy
		minDuration time.Duration
		maxDuration time.Duration
	}{
		{
			name:        "zero_period",
			strategy:    idlestrategy.NewSleeping(0),
			minDuration: 0,
			maxDuration: 10 * time.Millisecond,
		},
		{
			name:        "positive_period",
			strategy:    idlestrategy.NewSleeping(5 * time.Millisecond),
			minDuration: 4 * time.Millisecond,
			maxDuration: time.Second,
		},
		{
			name:        "default_millis",
			strategy:    idlestrategy.DefaultSleepingMillis(),
			minDuration: time.Millisecond,
			maxDuration: time.Second,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			start := time.Now()
			testCase.strategy.Idle(0)
			elapsed := time.Since(start)
			require.GreaterOrEqual(t, elapsed, testCase.minDuration)
			require.Less(t, elapsed, testCase.maxDuration)
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
