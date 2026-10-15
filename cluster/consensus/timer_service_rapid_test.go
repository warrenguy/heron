//go:build rapid

package consensus

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/timerwheel"
)

type timerServiceOpKind int

const (
	timerOpSchedule timerServiceOpKind = iota
	timerOpCancel
	timerOpAdvance
	timerOpPoll
	timerOpConsume
)

func drawTimerWheel(rt *rapid.T) (*timerwheel.Wheel, int64) {
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
	wheel := timerwheel.New(0, tickResolution, ticksPerWheel, initialAlloc, maxSlots)
	return wheel, tickResolution
}

func TestTimerService_lifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		wheel, tickResolution := drawTimerWheel(rt)
		svc := newTimerServiceForWheel(wheel)
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
			// Three consecutive zero returns drain due timers.
			//
			//   - First poll stops when CurrentTickTime is at now. That poll does not
			//     scan the now spoke.
			//   - When now is a wheel-tick boundary the next poll lands on the deadline
			//     spoke and does not scan that spoke.
			//   - The third poll scans the deadline spoke.
			//
			// timerPollLimit also stops a single call early.
			idle := 0
			for range 1 << 16 {
				if pollModel(false) != 0 {
					idle = 0
					continue
				}
				idle++
				if idle >= 3 {
					break
				}
			}
			require.GreaterOrEqual(rt, idle, 3, "poll did not go idle")
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

func TestTimerEventConsume_upstreamModel_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		wheel, tickResolution := drawTimerWheel(rt)
		a := newFollowerLogPollAgent(0, nil)
		a.timers = newTimerServiceForWheel(wheel)
		held := make(map[int64]int64)
		counts := make(map[int64]int64)
		now := int64(0)
		ids := []int64{-1, 0, 1, 2, 3, 4, math.MaxInt64}

		ops := []timerServiceOpKind{
			timerOpSchedule,
			timerOpSchedule,
			timerOpCancel,
			timerOpAdvance,
			timerOpPoll,
			timerOpConsume,
			timerOpConsume,
		}
		stepCount := rapid.IntRange(1, 64).Draw(rt, "stepCount")
		for range stepCount {
			switch rapid.SampledFrom(ops).Draw(rt, "op") {
			case timerOpSchedule:
				id := rapid.SampledFrom(ids).Draw(rt, "scheduleID")
				delta := rapid.Int64Range(-4*tickResolution, 16*tickResolution).
					Draw(rt, "deadlineDelta")
				deadline := now + delta
				scheduleTimer(a, id, deadline)
				switch {
				case counts[id] > 1:
					counts[id]--
				case counts[id] == 1:
					delete(counts, id)
				default:
					// The wheel may refuse growth. A refusal leaves no timer.
					if _, ok := a.timers.timerIDByCorrelationID[id]; ok {
						held[id] = deadline
					} else {
						require.ErrorIs(rt, a.TimerFailure(), errTimerCapacity)
						delete(held, id)
					}
				}

			case timerOpCancel:
				id := rapid.SampledFrom(ids).Draw(rt, "cancelID")
				a.handleCancelTimer(codecs.CancelTimerFields{CorrelationID: id})
				delete(held, id)

			case timerOpAdvance:
				now += rapid.Int64Range(0, 8*tickResolution).Draw(rt, "advance")

			case timerOpPoll:
				a.timers.poll(now, func(correlationID, pollNow int64) bool {
					deadline, ok := held[correlationID]
					require.True(rt, ok, "expired id %d not scheduled", correlationID)
					require.Equal(rt, now, pollNow)
					require.LessOrEqual(rt, deadline, now)
					delete(held, correlationID)
					return true
				})

			case timerOpConsume:
				id := rapid.SampledFrom(ids).Draw(rt, "consumeID")
				consumeTimerEventAboveCommit(a, id)
				if _, ok := held[id]; ok {
					delete(held, id)
				} else {
					counts[id]++
				}
			}

			requireAgentTimers(rt, a, held, counts)
		}
	})
}

// timerTraceOp is one leader-side step for a correlation id. A reschedule is
// a schedule the service sends from its TimerEvent callback.
type timerTraceOp byte

const (
	traceSchedule   timerTraceOp = 'S'
	traceCancel     timerTraceOp = 'C'
	traceEvent      timerTraceOp = 'F'
	traceReschedule timerTraceOp = 'R'
)

type timerTraceShape struct {
	ops     string
	inClass bool
}

// timerTraceShapes lists the histories the test draws from. A history is in
// class when no schedule is still waiting for its event while another
// schedule or cancel arrives.
var timerTraceShapes = []timerTraceShape{
	{ops: "SF", inClass: true},
	{ops: "SFR", inClass: true},
	{ops: "SC", inClass: true},
	{ops: "S", inClass: true},
	{ops: "SFC", inClass: true},
	{ops: "SS", inClass: true},
	{ops: "SCS", inClass: true},
	{ops: "SSF"},
	{ops: "SCF"},
	{ops: "SSFF"},
	{ops: "SCSF"},
}

// timerTraceRequest's cause is the log position of the frame that caused it.
type timerTraceRequest struct {
	correlationID int64
	cancel        bool
	deadline      int64
	cause         int64
}

type timerTraceEvent struct {
	correlationID int64
	position      int64
}

type followerTimerModel struct {
	schedules    int64
	events       int64
	cancelledEnd bool
	lastDeadline int64
}

func TestTimerEventConsume_followerMirror_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		idCount := rapid.IntRange(1, 12).Draw(rt, "idCount")
		shapes := make([]timerTraceShape, idCount)
		next := make([]int, idCount)
		active := make([]int, 0, idCount)
		for i := range shapes {
			shapes[i] = rapid.SampledFrom(timerTraceShapes).Draw(rt, "shape")
			active = append(active, i)
		}

		// Leader trace: interleave each id's ops.
		var (
			requests    []timerTraceRequest
			events      []timerTraceEvent
			logPosition int64
			deadline    int64
		)
		leaderLive := make(map[int64]int64)
		for len(active) > 0 {
			pick := rapid.IntRange(0, len(active)-1).Draw(rt, "pick")
			idx := active[pick]
			id := int64(idx)
			op := timerTraceOp(shapes[idx].ops[next[idx]])
			next[idx]++
			switch op {
			case traceSchedule:
				deadline++
				requests = append(requests, timerTraceRequest{
					correlationID: id,
					deadline:      deadline,
					cause:         logPosition,
				})
				leaderLive[id] = deadline
			case traceCancel:
				requests = append(requests, timerTraceRequest{
					correlationID: id,
					cancel:        true,
					cause:         logPosition,
				})
				delete(leaderLive, id)
			case traceEvent:
				logPosition += 64
				events = append(events, timerTraceEvent{correlationID: id, position: logPosition})
				delete(leaderLive, id)
				nextIsReschedule := next[idx] < len(shapes[idx].ops) &&
					timerTraceOp(shapes[idx].ops[next[idx]]) == traceReschedule
				if nextIsReschedule {
					next[idx]++
					deadline++
					requests = append(requests, timerTraceRequest{
						correlationID: id,
						deadline:      deadline,
						cause:         logPosition,
					})
					leaderLive[id] = deadline
				}
			}
			if next[idx] == len(shapes[idx].ops) {
				active = append(active[:pick], active[pick+1:]...)
			}
		}

		a := newFollowerLogPollAgent(0, nil)
		models := make(map[int64]*followerTimerModel)
		for i, shape := range shapes {
			if shape.inClass {
				models[int64(i)] = &followerTimerModel{}
			}
		}
		consumedPosition := int64(0)
		nextEvent, nextRequest := 0, 0

		requireStep := func() {
			rt.Helper()
			for i := range shapes {
				id := int64(i)
				_, held := a.timers.timerIDByCorrelationID[id]
				count := a.expiredTimerCountByCorrelationID[id]
				require.False(rt, held && count > 0, "id %d held and counted", id)
				model, ok := models[id]
				if !ok {
					continue
				}
				wantHeld := model.schedules > model.events && !model.cancelledEnd
				require.Equal(rt, wantHeld, held, "id %d held", id)
				if wantHeld {
					require.Equal(rt, model.lastDeadline, scheduledDeadline(rt, a.timers, id),
						"id %d deadline", id)
				}
				wantCount := max(model.events-model.schedules, 0)
				require.Equal(rt, wantCount, count, "id %d count", id)

				// Schedules still in flight: sent before the last TimerEvent
				// the follower saw, but not yet delivered.
				inFlight := int64(0)
				for _, request := range requests[nextRequest:] {
					if request.correlationID == id && !request.cancel &&
						request.cause < consumedPosition {
						inFlight++
					}
				}
				require.LessOrEqual(rt, count, inFlight, "id %d count bound", id)
			}
		}

		// Follower merge: TimerEvents in log order, requests in send order.
		// A request waits for every TimerEvent at or below its cause.
		for nextEvent < len(events) || nextRequest < len(requests) {
			requestReady := nextRequest < len(requests) &&
				(nextEvent == len(events) || events[nextEvent].position > requests[nextRequest].cause)
			eventReady := nextEvent < len(events)
			deliverEvent := eventReady &&
				(!requestReady || rapid.Bool().Draw(rt, "deliverEvent"))
			if deliverEvent {
				event := events[nextEvent]
				nextEvent++
				consumeTimerEventAboveCommit(a, event.correlationID)
				consumedPosition = event.position
				if model, ok := models[event.correlationID]; ok {
					model.events++
				}
			} else {
				request := requests[nextRequest]
				nextRequest++
				model := models[request.correlationID]
				if request.cancel {
					a.handleCancelTimer(codecs.CancelTimerFields{
						CorrelationID: request.correlationID,
					})
					if model != nil {
						model.cancelledEnd = true
					}
				} else {
					scheduleTimer(a, request.correlationID, request.deadline)
					if model != nil {
						model.schedules++
						model.cancelledEnd = false
						model.lastDeadline = request.deadline
					}
				}
			}
			requireStep()
		}

		// End: in-class ids match the leader.
		for i, shape := range shapes {
			id := int64(i)
			_, held := a.timers.timerIDByCorrelationID[id]
			_, counted := a.expiredTimerCountByCorrelationID[id]
			if !shape.inClass {
				require.False(rt, held && counted, "racy id %d", id)
				continue
			}
			require.False(rt, counted, "in-class id %d counted", id)
			liveDeadline, live := leaderLive[id]
			require.Equal(rt, live, held, "in-class id %d held", id)
			if live {
				require.Equal(rt, liveDeadline, scheduledDeadline(rt, a.timers, id),
					"in-class id %d deadline", id)
			}
		}
		require.NoError(rt, a.TimerFailure())
	})
}
