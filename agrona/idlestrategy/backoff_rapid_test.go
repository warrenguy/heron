//go:build rapid

package idlestrategy_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/agrona/idlestrategy"
)

type backoffRapidModel struct {
	maxSpins      int
	maxYields     int
	minParkPeriod time.Duration
	maxParkPeriod time.Duration

	state      int
	spins      int
	yields     int
	parkPeriod time.Duration
}

func newBackoffRapidModel(maxSpins, maxYields int) backoffRapidModel {
	return backoffRapidModel{
		maxSpins:      maxSpins,
		maxYields:     maxYields,
		minParkPeriod: 0,
		maxParkPeriod: 0,
		parkPeriod:    0,
		state:         idlestrategy.BackoffNotIdle,
	}
}

func (m *backoffRapidModel) reset() {
	m.state = idlestrategy.BackoffNotIdle
	m.spins = 0
	m.yields = 0
	m.parkPeriod = m.minParkPeriod
}

func (m *backoffRapidModel) idle(workCount int) {
	if workCount > 0 {
		m.reset()
		return
	}
	m.idleStep()
}

func (m *backoffRapidModel) idleStep() {
	switch m.state {
	case idlestrategy.BackoffNotIdle:
		m.state = idlestrategy.BackoffSpinning
		m.spins++
	case idlestrategy.BackoffSpinning:
		m.spins++
		if m.spins > m.maxSpins {
			m.state = idlestrategy.BackoffYielding
			m.yields = 0
		}
	case idlestrategy.BackoffYielding:
		m.yields++
		if m.yields > m.maxYields {
			m.state = idlestrategy.BackoffParking
			m.parkPeriod = m.minParkPeriod
		}
	case idlestrategy.BackoffParking:
		if m.minParkPeriod < m.maxParkPeriod {
			next := m.parkPeriod * 2
			if next > m.maxParkPeriod {
				next = m.maxParkPeriod
			}
			m.parkPeriod = next
		}
	}
}

func TestBackoff_idleSequence_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		maxSpins := rapid.IntRange(0, 32).Draw(rt, "maxSpins")
		maxYields := rapid.IntRange(0, 32).Draw(rt, "maxYields")

		strategy := idlestrategy.NewBackoff(maxSpins, maxYields, 0, 0)
		model := newBackoffRapidModel(maxSpins, maxYields)

		steps := rapid.IntRange(1, 256).Draw(rt, "steps")
		for i := range steps {
			workCount := 0
			if rapid.Bool().Draw(rt, fmt.Sprintf("hasWork%d", i)) {
				workCount = rapid.IntRange(1, 128).Draw(rt, fmt.Sprintf("workCount%d", i))
			}

			model.idle(workCount)
			strategy.Idle(workCount)

			require.Equal(rt, model.state, idlestrategy.BackoffState(t, strategy), "step %d", i)
			require.Equal(
				rt,
				model.parkPeriod,
				idlestrategy.BackoffParkPeriod(t, strategy),
				"step %d",
				i,
			)
		}
	})
}
