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
	hold            replayHold
}

// replayProgressModel is the pure prefix fold of the [replayProgress.observe]
// update rule, both deadlines included. Invariant checks catch a formula bug
// shared with the model. Model agreement alone passes that bug.
type replayProgressModel struct {
	startPosition  int64
	stopPosition   int64
	stallTimeoutNs int64
	holdTimeoutNs  int64
}

// expect returns the position, the done flag, and the expired flag after
// steps[:n+1].
func (m *replayProgressModel) expect(steps []progressStep, n int) (int64, bool, bool) {
	position := m.startPosition
	var stallDeadlineNs int64
	var holdDeadlineNs int64

	for _, step := range steps[:n+1] {
		previous := position
		position = min(max(position, step.adapterPosition), m.stopPosition)

		if modelHoldDeadline(m.holdTimeoutNs, &holdDeadlineNs, step) {
			return position, false, true
		}

		held := step.hold != replayHoldNone

		if position >= m.stopPosition {
			if !held {
				return position, true, false
			}

			continue
		}

		if held {
			stallDeadlineNs = step.nowNs + m.stallTimeoutNs
			continue
		}

		switch {
		case step.image != replayImageOpen:
			return position, true, false
		case m.stallTimeoutNs == 0:
		case position > previous || stallDeadlineNs == 0:
			stallDeadlineNs = step.nowNs + m.stallTimeoutNs
		case step.nowNs >= stallDeadlineNs:
			return position, true, false
		}
	}

	return position, false, false
}

// modelHoldDeadline folds [replayProgress.stepHoldDeadline].
func modelHoldDeadline(holdTimeoutNs int64, holdDeadlineNs *int64, step progressStep) bool {
	closedAck := step.hold == replayHoldServiceAcks && step.image == replayImageClosed
	if !closedAck {
		*holdDeadlineNs = 0
		return false
	}

	if *holdDeadlineNs == 0 {
		*holdDeadlineNs = step.nowNs + holdTimeoutNs
		return false
	}

	return step.nowNs >= *holdDeadlineNs
}

// TestReplayProgress_progression_rapid asserts the observe invariants.
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

		holdTimeoutNs := rapid.Int64Range(1, 1<<20).Draw(rt, "holdTimeoutNs")

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
		holdState := rapid.SampledFrom([]replayHold{
			replayHoldNone,
			replayHoldServiceAcks,
			replayHoldSnapshotTake,
		})

		for i := range steps {
			delta := rapid.Int64Range(0, 1<<16).Draw(rt, "delta")
			overshoot := rapid.Bool().Draw(rt, "overshoot")
			nowDelta := rapid.Int64Range(0, 1<<21).Draw(rt, "nowDelta")
			image := imageState.Draw(rt, "image")
			hold := holdState.Draw(rt, "hold")
			nowNs += nowDelta
			adapterPosition := cursor + delta
			if overshoot {
				adapterPosition = stopPosition + delta
			}
			steps[i] = progressStep{
				nowNs:           nowNs,
				adapterPosition: adapterPosition,
				image:           image,
				hold:            hold,
			}
			if adapterPosition > cursor && adapterPosition <= stopPosition {
				cursor = adapterPosition
			}
		}

		model := &replayProgressModel{
			startPosition:  startPosition,
			stopPosition:   stopPosition,
			stallTimeoutNs: stallTimeoutNs,
			holdTimeoutNs:  holdTimeoutNs,
		}
		p := &replayProgress{
			startPosition:  startPosition,
			stopPosition:   stopPosition,
			position:       startPosition,
			stallTimeoutNs: stallTimeoutNs,
			holdTimeoutNs:  holdTimeoutNs,
		}

		previous := startPosition
		var atDone int64
		var atReason string
		seenDone := false
		var expiredPos int64
		var expiredReason string
		// progressNow is the time of the latest stall deadline restart.
		var progressNow int64
		// closedAckStartNs is the first step of the current closed-image ack
		// hold. Tracked apart from the model, so invariant 6 does not share
		// its deadline formula.
		var closedAckStartNs int64
		inClosedAck := false

		for i, step := range steps {
			wasDone := p.done
			wasExpired := p.holdExpired
			closedAck := step.hold == replayHoldServiceAcks && step.image == replayImageClosed
			if !closedAck {
				inClosedAck = false
			}

			if closedAck && !inClosedAck {
				inClosedAck = true
				closedAckStartNs = step.nowNs
			}

			p.observe(step.nowNs, step.adapterPosition, step.image, step.hold)

			if !wasDone {
				rose := p.position > previous
				belowStop := p.position < stopPosition
				if i == 0 || (belowStop && (step.hold != replayHoldNone || rose)) {
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
			if !wasDone && step.hold != replayHoldNone {
				require.False(rt, p.done)
			}

			// 5. Model and code agree at every step.
			wantPos, wantDone, wantExpired := model.expect(steps, i)
			require.Equal(rt, wantPos, p.position)
			require.Equal(rt, wantDone, p.done)
			require.Equal(rt, wantExpired, p.holdExpired)

			if p.position >= stopPosition && !p.holdExpired {
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

			if wasExpired {
				require.False(rt, p.done)
				require.Equal(rt, expiredPos, p.position)
				require.Equal(rt, expiredReason, p.reason)
			}

			// 6. Only a service-ack hold on an image closed short of end of
			// stream expires. Expiry waits holdTimeoutNs.
			if p.holdExpired && !wasExpired {
				expiredPos = p.position
				expiredReason = p.reason
				require.Equal(rt, replayHoldExpiredReason, p.reason)
				require.Equal(rt, replayHoldServiceAcks, step.hold)
				require.Equal(rt, replayImageClosed, step.image)
				require.GreaterOrEqual(rt, step.nowNs-closedAckStartNs, holdTimeoutNs)
			}

			previous = p.position
		}
	})
}
