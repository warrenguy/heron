//go:build rapid

package idlestrategy_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/internal/testutil"
)

type backoffParams struct {
	maxSpins      int
	maxYields     int
	minParkPeriod time.Duration
	maxParkPeriod time.Duration
}

// want returns the state and park period after idleStreak idle calls since
// the last work.
func (p backoffParams) want(idleStreak int) (int, time.Duration) {
	// The first idle call spins even when maxSpins is 0.
	spinEnd := max(p.maxSpins, 1)
	yieldEnd := spinEnd + p.maxYields + 1
	switch {
	case idleStreak == 0:
		return idlestrategy.BackoffNotIdle, p.minParkPeriod
	case idleStreak <= spinEnd:
		return idlestrategy.BackoffSpinning, p.minParkPeriod
	case idleStreak <= yieldEnd:
		return idlestrategy.BackoffYielding, p.minParkPeriod
	}
	// The first park uses minParkPeriod. Each later park doubles it up to
	// maxParkPeriod.
	doublings := idleStreak - yieldEnd - 1
	if doublings == 0 {
		return idlestrategy.BackoffParking, p.minParkPeriod
	}
	// The draw keeps minParkPeriod under 2^7 ns, so a shift of maxDoublings
	// cannot overflow and already exceeds any drawn maxParkPeriod.
	const maxDoublings = 32
	doublings = min(doublings, maxDoublings)
	return idlestrategy.BackoffParking, min(p.minParkPeriod<<doublings, p.maxParkPeriod)
}

func TestBackoff_idleSequence_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		// Nanosecond park periods keep each parking sleep short.
		params := backoffParams{
			maxSpins:      rapid.IntRange(0, 32).Draw(rt, "maxSpins"),
			maxYields:     rapid.IntRange(0, 32).Draw(rt, "maxYields"),
			minParkPeriod: time.Duration(rapid.IntRange(0, 100).Draw(rt, "minParkPeriod")),
			maxParkPeriod: time.Duration(rapid.IntRange(0, 1000).Draw(rt, "maxParkPeriod")),
		}
		strategy := idlestrategy.NewBackoff(
			params.maxSpins,
			params.maxYields,
			params.minParkPeriod,
			params.maxParkPeriod,
		)

		requireState := func(idleStreak int, msgAndArgs ...any) {
			wantState, wantParkPeriod := params.want(idleStreak)
			require.Equal(rt, wantState, idlestrategy.BackoffState(t, strategy), msgAndArgs...)
			require.Equal(
				rt,
				wantParkPeriod,
				idlestrategy.BackoffParkPeriod(t, strategy),
				msgAndArgs...,
			)
		}

		// Each run is a streak of idle calls, then one call with work. A run of
		// up to 100 idle calls reaches parking for every drawn spin and yield
		// limit.
		runs := rapid.IntRange(1, 8).Draw(rt, "runs")
		for run := range runs {
			idleCalls := rapid.IntRange(0, 100).Draw(rt, fmt.Sprintf("idleCalls%d", run))
			for call := 1; call <= idleCalls; call++ {
				strategy.Idle(0)
				requireState(call, "run %d idle call %d", run, call)
			}

			workCount := rapid.IntRange(1, 128).Draw(rt, fmt.Sprintf("workCount%d", run))
			strategy.Idle(workCount)
			requireState(0, "run %d work", run)
		}
	})
}
