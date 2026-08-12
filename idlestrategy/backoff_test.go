package idlestrategy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/idlestrategy"
)

func callIdleZero(strategy idlestrategy.IdleStrategy, count int) {
	for range count {
		strategy.Idle(0)
	}
}

func TestNewBackoff_stateTransitions(t *testing.T) {
	park := idlestrategy.DefaultParkPeriod

	tests := []struct {
		name      string
		idleCalls int
		wantState int
	}{
		{name: "initial_not_idle", idleCalls: 0, wantState: idlestrategy.BackoffNotIdle},
		{name: "first_idle_enters_spinning", idleCalls: 1, wantState: idlestrategy.BackoffSpinning},
		{name: "second_idle_enters_yielding", idleCalls: 2, wantState: idlestrategy.BackoffYielding},
		{name: "third_idle_stays_yielding", idleCalls: 3, wantState: idlestrategy.BackoffYielding},
		{
			name:      "boundary_twelfth_idle_stays_yielding",
			idleCalls: 12,
			wantState: idlestrategy.BackoffYielding,
		},
		{name: "thirteenth_idle_enters_parking", idleCalls: 13, wantState: idlestrategy.BackoffParking},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			strategy := idlestrategy.NewBackoff(1, 10, park, park)
			callIdleZero(strategy, testCase.idleCalls)
			require.Equal(t, testCase.wantState, idlestrategy.BackoffState(t, strategy))
		})
	}
}

func TestBackoff_resetOnWork(t *testing.T) {
	park := idlestrategy.DefaultParkPeriod
	strategy := idlestrategy.NewBackoff(1, 10, park, park)

	callIdleZero(strategy, 20)
	require.NotEqual(t, idlestrategy.BackoffNotIdle, idlestrategy.BackoffState(t, strategy))

	strategy.Idle(1)
	require.Equal(t, idlestrategy.BackoffNotIdle, idlestrategy.BackoffState(t, strategy))
}

// TestBackoff_parkingIdleSleeps asserts the parking branch waits instead of
// spinning. No other test in the package fails when the park is dropped.
//
// time.Sleep waits at least its argument, so a lower bound cannot fail on a
// slow machine. No upper bound: that limit is schedule-dependent.
func TestBackoff_parkingIdleSleeps(t *testing.T) {
	park := time.Millisecond
	strategy := idlestrategy.NewBackoff(1, 10, park, park)

	// The thirteenth Idle enters parking without sleeping. Only the next Idle
	// reaches the park.
	callIdleZero(strategy, 13)
	require.Equal(t, idlestrategy.BackoffParking, idlestrategy.BackoffState(t, strategy))

	start := time.Now()
	strategy.Idle(0)
	require.GreaterOrEqual(t, time.Since(start), park)
}

func TestNewBackoff_parkPeriodDoublesUntilCap(t *testing.T) {
	minPark := 10 * time.Nanosecond
	maxPark := 40 * time.Nanosecond
	strategy := idlestrategy.NewBackoff(0, 0, minPark, maxPark)

	callIdleZero(strategy, 3)
	require.Equal(t, idlestrategy.BackoffParking, idlestrategy.BackoffState(t, strategy))
	require.Equal(t, minPark, idlestrategy.BackoffParkPeriod(t, strategy))

	strategy.Idle(0)
	require.Equal(t, 2*minPark, idlestrategy.BackoffParkPeriod(t, strategy))

	strategy.Idle(0)
	require.Equal(t, maxPark, idlestrategy.BackoffParkPeriod(t, strategy))

	strategy.Idle(0)
	require.Equal(t, maxPark, idlestrategy.BackoffParkPeriod(t, strategy))
}

func TestNewBackoff_customParameters(t *testing.T) {
	strategy := idlestrategy.NewBackoff(2, 1, 0, 0)
	state := func() int { return idlestrategy.BackoffState(t, strategy) }

	callIdleZero(strategy, 2)
	require.Equal(t, idlestrategy.BackoffSpinning, state())

	callIdleZero(strategy, 1)
	require.Equal(t, idlestrategy.BackoffYielding, state())

	callIdleZero(strategy, 1)
	require.Equal(t, idlestrategy.BackoffYielding, state())

	callIdleZero(strategy, 1)
	require.Equal(t, idlestrategy.BackoffParking, state())
}
