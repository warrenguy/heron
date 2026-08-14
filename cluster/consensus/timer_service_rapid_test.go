//go:build rapid

package consensus

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/timerwheel"
)

type timerServiceOpKind int

const (
	timerOpSchedule timerServiceOpKind = iota
	timerOpCancel
	timerOpAdvance
	timerOpPoll
)

func TestTimerService_lifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		resShift := rapid.IntRange(0, 4).Draw(rt, "resShift")
		tickResolution := int64(1) << resShift
		spokeShift := rapid.IntRange(1, 3).Draw(rt, "spokeShift")
		ticksPerWheel := 1 << spokeShift
		allocShift := rapid.IntRange(0, 2).Draw(rt, "allocShift")
		initialAlloc := 1 << allocShift
		baseSlots := ticksPerWheel * initialAlloc
		maxMult := rapid.IntRange(0, 2).Draw(rt, "maxMult")
		maxSlots := baseSlots << maxMult

		svc := newTimerServiceForWheel(timerwheel.New(
			0,
			tickResolution,
			ticksPerWheel,
			initialAlloc,
			maxSlots,
		))
		model := make(map[int64]int64)
		now := int64(0)
		ids := []int64{-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, math.MaxInt64}

		assertCount := func() {
			rt.Helper()
			require.Equal(rt, int64(len(model)), svc.count())
			requireTimerMapsLen(rt, svc, len(model))
			for id, deadline := range model {
				timerID, ok := svc.timerIDByCorrelationID[id]
				require.True(rt, ok, "model id %d missing from maps", id)
				stored, ok := svc.wheel.Deadline(timerID)
				require.True(rt, ok, "model id %d timer %d missing from wheel", id, timerID)
				require.Equal(rt, deadline, stored, "model id %d stored deadline", id)
			}
		}

		pollModel := func(abort bool) int {
			rt.Helper()
			return svc.poll(now, func(correlationID, pollNow int64) bool {
				deadline, ok := model[correlationID]
				require.True(rt, ok, "expired id %d not scheduled", correlationID)
				require.Equal(rt, now, pollNow)
				require.LessOrEqual(rt, deadline, now)
				if abort {
					return false
				}
				delete(model, correlationID)
				return true
			})
		}

		assertQuiescence := func() {
			rt.Helper()
			// Two consecutive zeros: a catch-up that lands on the now wheel
			// tick returns 0 without scanning that spoke. The next poll scans
			// it. timerPollLimit also stops a single call early.
			idle := 0
			for range 1 << 16 {
				if pollModel(false) != 0 {
					idle = 0
					continue
				}
				idle++
				if idle >= 2 {
					break
				}
			}
			require.GreaterOrEqual(rt, idle, 2, "poll did not go idle")
			assertCount()
			for id, deadline := range model {
				require.Greater(rt, deadline, now,
					"id %d deadline %d still due at now %d",
					id, deadline, now)
			}
		}

		ops := []timerServiceOpKind{
			timerOpSchedule,
			timerOpSchedule,
			timerOpCancel,
			timerOpAdvance,
			timerOpPoll,
			timerOpPoll,
		}
		stepCount := rapid.IntRange(1, 64).Draw(rt, "stepCount")
		for range stepCount {
			switch rapid.SampledFrom(ops).Draw(rt, "op") {
			case timerOpSchedule:
				id := rapid.SampledFrom(ids).Draw(rt, "scheduleID")
				delta := rapid.Int64Range(-4*tickResolution, 16*tickResolution).
					Draw(rt, "deadlineDelta")
				deadline := now + delta
				if rapid.IntRange(0, 15).Draw(rt, "clampDeadline") == 0 {
					deadline = timerwheel.NullDeadline
				}
				ok := svc.schedule(id, deadline)
				if ok {
					if deadline == timerwheel.NullDeadline {
						deadline = timerwheel.NullDeadline - 1
					}
					model[id] = deadline
				} else {
					delete(model, id)
				}

			case timerOpCancel:
				id := rapid.SampledFrom(ids).Draw(rt, "cancelID")
				cancelled := svc.cancel(id)
				_, present := model[id]
				require.Equal(rt, present, cancelled)
				delete(model, id)

			case timerOpAdvance:
				delta := rapid.Int64Range(0, 8*tickResolution).Draw(rt, "advance")
				now += delta

			case timerOpPoll:
				abort := rapid.IntRange(0, 7).Draw(rt, "abort") == 0
				pollModel(abort)
			}

			assertCount()
		}

		assertQuiescence()
	})
}
