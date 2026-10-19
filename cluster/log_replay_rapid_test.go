//go:build rapid

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// progressStep is one observe input drawn for the progression property.
type progressStep struct {
	adapterPosition int64
	closedShort     bool
	held            bool
}

// replayProgressModel is the pure prefix fold of the observe update rule.
// expect recomputes position as the running max of adapter positions,
// clamped to the bound, and freezes at the first step that ends the replay:
// the bound while not held, or closedShort below the bound. The fold matches
// [replayProgress.observe]. Invariant checks pair with the model so a shared
// formula bug does not hide under model agreement alone.
type replayProgressModel struct {
	startPosition int64
	stopPosition  int64
}

// expect returns the position and done flag after the drawn prefix
// steps[:n+1]. When no step ends the replay, done is false.
func (m *replayProgressModel) expect(steps []progressStep, n int) (int64, bool) {
	position := m.startPosition
	for i, step := range steps {
		if i > n {
			break
		}
		position = min(max(position, step.adapterPosition), m.stopPosition)
		if position >= m.stopPosition {
			if !step.held {
				return position, true
			}
			continue
		}
		if step.closedShort {
			return position, true
		}
	}
	return position, false
}

// TestReplayProgress_progression_rapid asserts position monotonicity, bound
// clamp, stickiness, the snapshot hold at the bound, and model agreement for
// replayProgress.observe.
func TestReplayProgress_progression_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		startPosition := rapid.Int64Range(0, 1<<20).Draw(rt, "startPosition")

		// Span is at least 1 so the bound stays above the start. Matches the
		// constructor's refusal of a non-positive gap.
		span := rapid.Int64Range(1, 1<<20).Draw(rt, "span")
		stopPosition := startPosition + span

		// Draw deltas, not absolute positions. A monotonic absolute draw shrinks
		// into cases production cannot produce (same reason as
		// agent_rapid_test commit-position draws).
		nSteps := rapid.IntRange(1, 32).Draw(rt, "nSteps")
		steps := make([]progressStep, nSteps)
		cursor := startPosition
		for i := range steps {
			delta := rapid.Int64Range(0, 1<<16).Draw(rt, "delta")
			overshoot := rapid.Bool().Draw(rt, "overshoot")
			closedShort := rapid.Bool().Draw(rt, "closedShort")
			held := rapid.Bool().Draw(rt, "held")
			adapterPosition := cursor + delta
			if overshoot {
				adapterPosition = stopPosition + delta
			}
			steps[i] = progressStep{
				adapterPosition: adapterPosition,
				closedShort:     closedShort,
				held:            held,
			}
			if adapterPosition > cursor && adapterPosition <= stopPosition {
				cursor = adapterPosition
			}
		}

		model := &replayProgressModel{
			startPosition: startPosition,
			stopPosition:  stopPosition,
		}
		p := &replayProgress{
			startPosition: startPosition,
			stopPosition:  stopPosition,
			position:      startPosition,
		}

		previous := startPosition
		var atDone int64
		seenDone := false
		for i, step := range steps {
			wasDone := p.done
			p.observe(step.adapterPosition, step.closedShort, step.held)

			// 1. Position never goes backwards.
			require.GreaterOrEqual(rt, p.position, previous)

			// 2. Position never exceeds the bound.
			require.LessOrEqual(rt, p.position, stopPosition)

			// 3. Position never falls below the start.
			require.GreaterOrEqual(rt, p.position, startPosition)

			// 4-5. done is sticky; position freezes after done.
			if seenDone {
				require.True(rt, p.done)
				require.Equal(rt, atDone, p.position)
			}
			if p.done && !seenDone {
				seenDone = true
				atDone = p.position
			}

			// 6. Done never sets at the bound while held.
			if !wasDone && step.held && p.position >= stopPosition {
				require.False(rt, p.done)
			}

			// 7. Model and code agree at every step.
			wantPos, wantDone := model.expect(steps, i)
			require.Equal(rt, wantPos, p.position)
			require.Equal(rt, wantDone, p.done)

			// 8-9. Empty reason at the bound. Short end sets the short reason.
			if p.position >= stopPosition {
				require.Empty(rt, p.reason)
			}
			if p.done && p.position < stopPosition {
				require.Equal(rt, replayEndedShortReason, p.reason)
			}

			previous = p.position
		}
	})
}
