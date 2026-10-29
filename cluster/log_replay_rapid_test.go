//go:build rapid

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// progressStep is one observe input drawn for the progression property.
type progressStep struct {
	nowNs           int64
	adapterPosition int64
	image           replayImageState
	held            bool
}

// replayProgressModel is the pure prefix fold of the observe update rule.
// The stall deadline folds the same way as [replayProgress.observe].
// Invariant checks pair with the model. A shared formula bug fails an
// invariant check. Model agreement alone lets a shared formula bug pass.
type replayProgressModel struct {
	startPosition  int64
	stopPosition   int64
	stallTimeoutNs int64
}

// expect returns the position and the done flag after steps[:n+1].
func (m *replayProgressModel) expect(steps []progressStep, n int) (int64, bool) {
	position := m.startPosition
	var stallDeadlineNs int64
	for _, step := range steps[:n+1] {
		previous := position
		position = min(max(position, step.adapterPosition), m.stopPosition)
		if position >= m.stopPosition {
			if !step.held {
				return position, true
			}
			continue
		}
		if step.held {
			stallDeadlineNs = step.nowNs + m.stallTimeoutNs
			continue
		}
		switch {
		case step.image != replayImageOpen:
			return position, true
		case m.stallTimeoutNs == 0:
		case position > previous || stallDeadlineNs == 0:
			stallDeadlineNs = step.nowNs + m.stallTimeoutNs
		case step.nowNs >= stallDeadlineNs:
			return position, true
		}
	}
	return position, false
}

// TestReplayProgress_progression_rapid asserts replayProgress.observe.
// Position stays at or above the start and at or below the stop. done
// marks once. A hold blocks an end below the stop and at the stop.
// Position and done match the model.
func TestReplayProgress_progression_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		startPosition := rapid.Int64Range(0, 1<<20).Draw(rt, "startPosition")

		// Span is at least 1 so the bound stays above the start. Matches the
		// constructor's refusal of a non-positive gap.
		span := rapid.Int64Range(1, 1<<20).Draw(rt, "span")
		stopPosition := startPosition + span

		// Zero is archive mode. A positive draw is bound-image mode.
		stallTimeoutNs := int64(0)
		if rapid.Bool().Draw(rt, "stallEnabled") {
			stallTimeoutNs = rapid.Int64Range(1, 1<<20).Draw(rt, "stallTimeoutNs")
		}

		// Draw deltas, not absolute positions. A monotonic absolute draw shrinks
		// into cases production cannot produce (same reason as
		// agent_rapid_test commit-position draws). The clock is the running
		// sum of nowDelta. The clock does not go backwards.
		nSteps := rapid.IntRange(1, 32).Draw(rt, "nSteps")
		steps := make([]progressStep, nSteps)
		cursor := startPosition
		var nowNs int64
		imageState := rapid.SampledFrom([]replayImageState{
			replayImageOpen,
			replayImageEndOfStream,
			replayImageClosed,
		})
		for i := range steps {
			delta := rapid.Int64Range(0, 1<<16).Draw(rt, "delta")
			overshoot := rapid.Bool().Draw(rt, "overshoot")
			nowDelta := rapid.Int64Range(0, 1<<21).Draw(rt, "nowDelta")
			image := imageState.Draw(rt, "image")
			held := rapid.Bool().Draw(rt, "held")
			nowNs += nowDelta
			adapterPosition := cursor + delta
			if overshoot {
				adapterPosition = stopPosition + delta
			}
			steps[i] = progressStep{
				nowNs:           nowNs,
				adapterPosition: adapterPosition,
				image:           image,
				held:            held,
			}
			if adapterPosition > cursor && adapterPosition <= stopPosition {
				cursor = adapterPosition
			}
		}

		model := &replayProgressModel{
			startPosition:  startPosition,
			stopPosition:   stopPosition,
			stallTimeoutNs: stallTimeoutNs,
		}
		p := &replayProgress{
			startPosition:  startPosition,
			stopPosition:   stopPosition,
			position:       startPosition,
			stallTimeoutNs: stallTimeoutNs,
		}

		previous := startPosition
		var atDone int64
		var atReason string
		seenDone := false
		// progressNow is the latest time the deadline restarts: the first
		// step, a held step below the stop, or a position rise below the stop.
		var progressNow int64
		for i, step := range steps {
			wasDone := p.done
			p.observe(step.nowNs, step.adapterPosition, step.image, step.held)

			if !wasDone {
				rose := p.position > previous
				belowStop := p.position < stopPosition
				if i == 0 || (belowStop && (step.held || rose)) {
					progressNow = step.nowNs
				}
			}

			// 1. Position never goes backwards.
			require.GreaterOrEqual(rt, p.position, previous)

			// 2. Position never exceeds the bound.
			require.LessOrEqual(rt, p.position, stopPosition)

			// 3. Position never falls below the start.
			require.GreaterOrEqual(rt, p.position, startPosition)

			if seenDone {
				require.True(rt, p.done)
				require.Equal(rt, atDone, p.position)
				require.Equal(rt, atReason, p.reason)
			}
			if p.done && !seenDone {
				seenDone = true
				atDone = p.position
				atReason = p.reason
			}

			// 4. A hold does not set done.
			if !wasDone && step.held {
				require.False(rt, p.done)
			}

			// 5. Model and code agree at every step.
			wantPos, wantDone := model.expect(steps, i)
			require.Equal(rt, wantPos, p.position)
			require.Equal(rt, wantDone, p.done)

			if p.position >= stopPosition {
				require.Empty(rt, p.reason)
			}
			if !wasDone && p.reason == replayStalledReason {
				require.GreaterOrEqual(rt, step.nowNs-progressNow, stallTimeoutNs)
			}

			if !wasDone && p.done && p.position < stopPosition {
				switch step.image {
				case replayImageEndOfStream:
					require.Equal(rt, replayEndOfStreamReason, p.reason)
				case replayImageClosed:
					require.Equal(rt, replayEndedShortReason, p.reason)
				case replayImageOpen:
					require.Equal(rt, replayStalledReason, p.reason)
				}
			}

			if stallTimeoutNs == 0 {
				require.NotEqual(rt, replayStalledReason, p.reason)
			}

			previous = p.position
		}
	})
}
